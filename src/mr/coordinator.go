package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Coordinator struct {
	// task
	MapFileState  *sync.Map // input-filename -> state
	ReduceThState *sync.Map // th -> state
	ReduceThFiles *sync.Map // th -> intermediate-filenames
	OutputFiles   *sync.Map // output-filenames

	mapQ    *taskQ // MapTaskQueue
	reduceQ *taskQ // ReduceTaskQueue

	workers *sync.Map // taskID -> *worker

	phase atomic.Value

	nReduce       int           // readOnly
	workerTimeout time.Duration // readOnly
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

const taskQRegularSize = 16

type taskQ struct {
	mu sync.Mutex
	tk []string
}

func newTaskQ(n ...int) *taskQ {
	return &taskQ{
		mu: sync.Mutex{},
		tk: make([]string, 0, func() int {
			if len(n) > 0 {
				return n[0]
			}
			return taskQRegularSize
		}()),
	}
}

func (q *taskQ) push(tk string) *taskQ {
	q.mu.Lock()
	defer q.mu.Unlock()
	en := true
	for _, tki := range q.tk {
		if tk == tki {
			en = false
			break
		}
	}
	if en {
		q.tk = append(q.tk, tk)
	}
	return q
}

func (q *taskQ) front() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tk) > 0 {
		return q.tk[0]
	}
	return ""
}

func (q *taskQ) pop() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tk) > 0 {
		tk := q.tk[0]
		q.tk = q.tk[1:]
		return tk
	}
	return ""
}

// RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *ReqTaskArg, reply *ReqTaskReply) error {
	reply.TaskID = genTaskID()
	reply.TaskType = TaskNone
	reply.ReduceN = c.nReduce
	var input string
	switch c.phase.Load() {
	case PhaseMap:
		idleMapTask := getIdleV2(c.mapQ) // filename
		if len(idleMapTask) != 0 {
			reply.TaskType = TaskMap
			reply.RawFilename = idleMapTask
			input = idleMapTask
			c.MapFileState.Store(input, StateReady)
		} else {
			reply.TaskType = TaskWait
		}
	case PhaseReduce:
		idleReduceTask := getIdleV2(c.reduceQ) // th
		if len(idleReduceTask) != 0 {
			reply.TaskType = TaskReduce
			reply.ReduceTh = idleReduceTask
			thFiles, _ := c.ReduceThFiles.Load(idleReduceTask)
			thFiles.(*sync.Map).Range(func(filename, _ any) bool {
				reply.Interfilenames = append(reply.Interfilenames, filename.(string))
				return true
			})
			input = idleReduceTask
			c.ReduceThState.Store(input, StateReady)
		}
	}

	// no task left
	if reply.TaskType == TaskNone || reply.TaskType == TaskWait {
		return nil
	}

	// update workers
	c.workers.Store(reply.TaskID, &worker{
		taskID:   reply.TaskID,
		workerID: args.WorkderID,
		assigned: time.Now(),
		taskType: reply.TaskType,
		input:    input,
	})
	return nil
}

func (c *Coordinator) UpdateState(args *UpdateTaskArg, reply *UpdateTaskReply) error {
	wrk, ok := c.workers.Load(args.TaskID) // read from
	if !ok {
		return nil
	}
	worker := wrk.(*worker)

	switch args.State {
	case StateInProgress:
		worker.started = &args.Time
	case StateCompleted:
		worker.completed = &args.Time
	}

	switch worker.taskType {
	case TaskMap:
		c.MapFileState.Store(worker.input, args.State)
		for _, interfile := range args.Filename {
			th := parseInterfileTh(interfile)
			c.ReduceThState.Store(th, StateIdle)
			files, _ := c.ReduceThFiles.LoadOrStore(th, &sync.Map{})
			files.(*sync.Map).Store(interfile, struct{}{})
			c.reduceQ.push(th)
			worker.output = append(worker.output, interfile)
		}
	case TaskReduce:
		c.ReduceThState.Store(worker.input, args.State)
		for _, outfile := range args.Filename { // len must 1
			c.OutputFiles.Store(outfile, struct{}{})
			worker.output = []string{outfile}
		}
	}

	c.workers.Store(args.TaskID, worker) // write back

	c.updatePhase()
	return nil
}

func (c *Coordinator) updatePhase() {
	// update phase by maps
	switch c.phase.Load() {
	case PhaseDone:
		return
	case PhaseMap:
		if allCompleted(c.MapFileState) {
			c.phase.Store(PhaseReduce)
		}
	case PhaseReduce:
		if allCompleted(c.ReduceThState) {
			c.phase.Store(PhaseDone)
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
		time.Sleep(time.Second * 1)
		disconnects := make([]string, 0)
		curtime := time.Now()
		c.workers.Range(func(taskID, wrk any) bool {
			worker := wrk.(*worker)
			if worker.started == nil ||
				curtime.Sub(*worker.started) > c.workerTimeout {
				if worker.taskType == TaskMap ||
					(worker.taskType == TaskReduce && worker.completed == nil) {
					disconnects = append(disconnects, taskID.(string))
				}
			}
			return true
		})
		for _, taskID := range disconnects {
			wrk, _ := c.workers.Load(taskID)
			worker := wrk.(*worker)
			switch worker.taskType {
			case TaskMap:
				c.MapFileState.Store(worker.input, StateIdle)
			case TaskReduce:
				c.ReduceThState.Store(worker.input, StateIdle)
			}
			c.workers.Delete(taskID)
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.phase.Load() == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapFileState:  &sync.Map{},
		ReduceThState: &sync.Map{},
		ReduceThFiles: &sync.Map{},
		OutputFiles:   &sync.Map{},
		mapQ:          newTaskQ(len(files)),
		reduceQ:       newTaskQ(nReduce),
		workers:       &sync.Map{},
		phase:         atomic.Value{},
		nReduce:       nReduce,
		workerTimeout: time.Second * 5,
	}
	for _, inputfile := range files {
		c.MapFileState.Store(inputfile, StateIdle)
		c.mapQ.push(inputfile)
	}
	c.phase.Store(PhaseMap)

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
	c.workers.Range(func(_, wrk any) bool {
		workers = append(workers, *wrk.(*worker))
		return true
	})
	sort.Sort(ByTime(workers))
	for _, worker := range workers {
		log.Printf("taskID: %s, dur: (%s~%s,%dms), workerID: %s, input: %s\n", worker.taskID, *worker.started, *worker.completed, worker.completed.Sub(*worker.started).Milliseconds(), worker.workerID, worker.input)
	}
}

func genTaskID() string {
	return uuid.NewString()
}

func getIdle(tasks *sync.Map) string {
	taskt := ""
	tasks.Range(func(t any, state any) bool {
		if state == StateIdle {
			taskt = t.(string)
			return false
		}
		return true
	})
	return taskt
}

func getIdleV2(tk *taskQ) string {
	return tk.pop()
}

func allCompleted(tasks *sync.Map) bool {
	ok := true
	tasks.Range(func(_, state any) bool {
		if state != StateCompleted {
			ok = false
			return false
		}
		return true
	})
	return ok
}
