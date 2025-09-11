package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/sets"
)

type Coordinator struct {
	// task
	maps      map[string]TaskState        // MapFileState:  input-filename -> state
	reduces   map[string]TaskState        // ReduceThState: th -> state
	reducesTh map[string]sets.Set[string] // ReduceThFiles: th -> intermediate-filenames
	outputs   map[string]struct{}         // OutputFiles:   output-filename

	workers map[string]*worker // taskID -> worker

	phase TaskPhase // set-once, changed-once

	nReduce       int           // readOnly
	workerTimeout time.Duration // readOnly

	mu sync.RWMutex
}

type worker struct {
	taskID    string
	workerID  string
	assigned  time.Time
	started   *time.Time
	completed *time.Time
	taskType  TaskType
	input     string // Map:filename, Reduce:th
	output    []string
}

// RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *ReqTaskArg, reply *ReqTaskReply) error {
	// get possible task
	// c.mu.RLock()
	c.mu.Lock()         // lock-modify
	defer c.mu.Unlock() // lock-modify
	reply.TaskID = genTaskID()
	reply.TaskType = TaskNone
	reply.ReduceN = c.nReduce
	var input string
	switch c.phase {
	case PhaseMap:
		idleMapTask := getIdle(c.maps) // filename
		if len(idleMapTask) != 0 {
			reply.TaskType = TaskMap
			reply.RawFilename = idleMapTask
			input = idleMapTask
		} else {
			reply.TaskType = TaskWait
		}
	case PhaseReduce:
		idleReduceTask := getIdle(c.reduces) // th
		if len(idleReduceTask) != 0 {
			reply.TaskType = TaskReduce
			reply.ReduceTh = idleReduceTask
			reply.Interfilenames = c.reducesTh[idleReduceTask].UnsortedList()
			input = idleReduceTask
		}
	}
	// c.mu.RUnlock()

	// no task left
	if reply.TaskType == TaskNone || reply.TaskType == TaskWait {
		return nil
	}

	// update workers
	// c.mu.Lock()
	c.workers[reply.TaskID] = &worker{
		taskID:   reply.TaskID,
		workerID: args.WorkderID,
		assigned: time.Now(),
		taskType: reply.TaskType,
		input:    input,
	}
	// c.mu.Unlock()
	return nil
}

func (c *Coordinator) UpdateState(args *UpdateTaskArg, reply *UpdateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker, ok := c.workers[args.TaskID] // read from
	if !ok {
		return nil
	}

	switch args.State {
	case StateInProgress:
		worker.started = &args.Time
	case StateCompleted:
		worker.completed = &args.Time
	}

	switch worker.taskType {
	case TaskMap:
		c.maps[worker.input] = args.State
		for _, interfile := range args.Filename {
			th := parseInterfileTh(interfile)
			c.reduces[th] = StateIdle
			if _, ok := c.reducesTh[th]; !ok {
				c.reducesTh[th] = sets.New[string]()
			}
			c.reducesTh[th].Insert(interfile)
			worker.output = append(worker.output, interfile)
		}
	case TaskReduce:
		c.reduces[worker.input] = args.State
		for _, outfile := range args.Filename { // len must 1
			c.outputs[outfile] = struct{}{}
			worker.output = []string{outfile}
		}
	}

	c.workers[args.TaskID] = worker // write back

	c.updatePhase()
	return nil
}

func (c *Coordinator) updatePhase() {
	// update phase by maps
	switch c.phase {
	case PhaseDone:
		return
	case PhaseMap:
		mapsok := true
		for _, state := range c.maps {
			if state != StateCompleted {
				mapsok = false
				break
			}
		}
		if mapsok {
			c.phase = PhaseReduce
		}
	case PhaseReduce:
		reduceok := true
		for _, state := range c.reduces {
			if state != StateCompleted {
				reduceok = false
				break
			}
		}
		if reduceok {
			c.phase = PhaseDone
		}
	}
}

// func (c *Coordinator) SaveReduceFiles(args *UpdateTaskArg, reply *UpdateTaskReply) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	for _, interfile := range args.Filename {
// 		c.reduces[interfile] = TaskIdle
// 	}
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// for worker-early-exit and worker-crash
func (c *Coordinator) keepalive() {
	for {
		time.Sleep(time.Millisecond * 250)
		disconnects := make([]string, 0)
		curtime := time.Now()
		c.mu.Lock()
		for taskID, worker := range c.workers {
			if worker.started == nil ||
				curtime.Sub(*worker.started) > c.workerTimeout {
				if worker.taskType == TaskMap ||
					(worker.taskType == TaskReduce && worker.completed == nil) {
					disconnects = append(disconnects, taskID)
				}
			}
		}
		for _, taskID := range disconnects {
			worker := c.workers[taskID]
			switch worker.taskType {
			case TaskMap:
				c.maps[worker.input] = StateIdle
			case TaskReduce:
				c.reduces[worker.input] = StateIdle
			}
			delete(c.workers, taskID)
		}
		c.mu.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.phase == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	maptask := make(map[string]TaskState, len(files))
	for _, filename := range files {
		maptask[filename] = StateIdle
	}
	c := Coordinator{
		maps:          maptask,
		reduces:       make(map[string]TaskState, nReduce),
		reducesTh:     make(map[string]sets.Set[string], nReduce),
		outputs:       make(map[string]struct{}, nReduce),
		workers:       make(map[string]*worker, max(nReduce, len(files))),
		phase:         PhaseMap,
		nReduce:       nReduce,
		mu:            sync.RWMutex{},
		workerTimeout: time.Second * 10,
	}

	c.server()
	go c.keepalive()
	return &c
}

type ByTime []worker

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].assigned.Before(a[j].assigned) }

// inspect execution details of workers
func (c *Coordinator) LogWorker() {
	workers := make([]worker, 0)
	for _, worker := range c.workers {
		workers = append(workers, *worker)
	}
	sort.Sort(ByTime(workers))
	for _, worker := range workers {
		log.Printf("taskID: %s, dur: (%s~%s,%dms), workerID: %s, input: %s\n", worker.taskID, *worker.started, *worker.completed, worker.completed.Sub(*worker.started).Milliseconds(), worker.workerID, worker.input)
	}
}

func genTaskID() string {
	return uuid.NewString()
}

func getIdle(tasks map[string]TaskState) string {
	for filename, state := range tasks {
		if state == StateIdle {
			return filename
		}
	}
	return ""
}
