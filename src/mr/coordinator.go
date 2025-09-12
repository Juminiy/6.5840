package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Coordinator struct {
	mapMu    sync.Mutex
	mapState map[int]TaskState // mapM -> TaskState
	mapFile  []string          // mapM -> mapFile, readOnly
	mapQ     *syncQ[int]

	reduceMu    sync.Mutex
	reduceState map[int]TaskState // reduceR -> TaskState
	reduceFiles map[int][]string  // reduceR -> ReduceFiles
	reduceQ     *syncQ[int]

	outputs []string

	phase atomic.Value

	workerMu      sync.Mutex
	workers       map[string]*worker // taskID -> worker
	workerTimeout time.Duration

	mapM    int // readOnly
	reduceR int // readOnly
}

type worker struct {
	taskID    string
	workerID  string
	requested time.Time
	assigned  time.Time
	started   *time.Time
	completed *time.Time
	taskType  TaskType
	taskSeq   int
	outputs   []string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *ReqTaskArg, reply *ReqTaskReply) error {
	reply.TaskID = uuid.NewString()
	reply.RespTime = time.Now()
	reply.TaskType = TypeNone
	reply.ReduceTotal = c.reduceR
	switch c.phase.Load() {
	case PhaseMap:
		mSeq, ok := c.mapQ.pop()
		if ok {
			reply.TaskType = TypeMap
			reply.TaskSeq = mSeq
			c.mapMu.Lock()
			c.mapState[mSeq] = StateEmit
			reply.Files = []string{c.mapFile[mSeq]}
			c.mapMu.Unlock()
		} else {
			reply.TaskType = TypeWait
		}

	case PhaseReduce:
		rSeq, ok := c.reduceQ.pop()
		if ok {
			reply.TaskType = TypeReduce
			reply.TaskSeq = rSeq
			c.reduceMu.Lock()
			c.reduceState[rSeq] = StateEmit
			reply.Files = c.reduceFiles[rSeq]
			c.reduceMu.Unlock()
		}
	}

	if reply.TaskType == TypeWait || reply.TaskType == TypeNone {
		return nil
	}

	c.workerMu.Lock()
	c.workers[reply.TaskID] = &worker{
		taskID:    reply.TaskID,
		workerID:  args.WorkerID,
		requested: args.ReqTime,
		assigned:  time.Now(),
		taskType:  reply.TaskType,
		taskSeq:   reply.TaskSeq,
	}
	c.workerMu.Unlock()
	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateTaskArg, reply *UpdateTaskReply) error {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()
	worker, ok := c.workers[args.TaskID]
	if !ok {
		return nil
	}

	c.mapMu.Lock()
	defer c.mapMu.Unlock()
	c.reduceMu.Lock()
	defer c.reduceMu.Unlock()

	switch worker.taskType {
	case TypeMap:
		c.mapState[worker.taskSeq] = args.TaskState
	case TypeReduce:
		c.reduceState[worker.taskSeq] = args.TaskState
	}

	switch args.TaskState {
	case StateInProgress:
		worker.started = &args.ReqTime
	case StateCompleted:
		worker.completed = &args.ReqTime
		worker.outputs = args.Files
		switch worker.taskType {
		case TypeMap:
			for _, interfile := range args.Files {
				seq := parseInterfileSeq(interfile)
				c.reduceState[seq] = StateIdle
				c.reduceFiles[seq] = append(c.reduceFiles[seq], interfile)
			}
		case TypeReduce:
			c.outputs = append(c.outputs, args.Files...)
		}
	}

	c.workers[args.TaskID] = worker

	c.updatePhase()
	return nil
}

func (c *Coordinator) updatePhase() {
	switch c.phase.Load() {
	case PhaseMap:
		if stateCompleted(c.mapState) && c.mapQ.empty() {
			c.phase.Store(PhaseReduce)
			for seq := range c.reduceState {
				c.reduceQ.push(seq)
			}
		}
	case PhaseReduce:
		if stateCompleted(c.reduceState) && c.reduceQ.empty() {
			c.phase.Store(PhaseDone)
		}
	}
}

func stateCompleted(states map[int]TaskState) bool {
	for _, state := range states {
		if state != StateCompleted {
			return false
		}
	}
	return true
}

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

// peek task state(ping worker)
func (c *Coordinator) keepalive() {
	for !c.Done() {
		curtime := time.Now()
		curphase := c.phase.Load()
		evicts := make([]string, 0)
		c.workerMu.Lock()
		for taskID, wrk := range c.workers {
			if wrk.started == nil || (curtime.Sub(*wrk.started) > c.workerTimeout) {
				if (curphase == PhaseMap && wrk.taskType == TypeMap) ||
					(curphase == PhaseReduce && wrk.taskType == TypeReduce && wrk.completed == nil) {
					evicts = append(evicts, taskID)
				}
			}
		}
		phaseMap, phaseReduce := false, false
		for _, taskID := range evicts {
			wrk := c.workers[taskID]
			switch wrk.taskType {
			case TypeMap:
				c.mapQ.push(wrk.taskSeq)
				phaseMap = true
			case TypeReduce:
				c.reduceQ.push(wrk.taskSeq)
				phaseReduce = true
			}
			delete(c.workers, taskID)
		}
		c.workerMu.Unlock()
		if phaseMap {
			c.phase.Store(PhaseMap)
		} else if phaseReduce {
			c.phase.Store(PhaseReduce)
		}
		time.Sleep(time.Second * 2)
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
	mapM := len(files)
	c := Coordinator{
		mapMu:    sync.Mutex{},
		mapState: make(map[int]TaskState, mapM),
		mapFile:  files,
		mapQ:     makeQ[int](mapM),

		reduceMu:    sync.Mutex{},
		reduceState: make(map[int]TaskState, nReduce),
		reduceFiles: make(map[int][]string, nReduce),
		reduceQ:     makeQ[int](nReduce),

		outputs: make([]string, 0, nReduce),
		phase:   atomic.Value{},

		workerMu:      sync.Mutex{},
		workers:       make(map[string]*worker, mapM+nReduce),
		workerTimeout: time.Second * 10,

		mapM:    mapM,
		reduceR: nReduce,
	}
	c.phase.Store(PhaseMap)
	for idx := range files {
		c.mapState[idx] = StateIdle
		c.mapQ.push(idx)
	}

	c.server()
	go c.keepalive()
	return &c
}
