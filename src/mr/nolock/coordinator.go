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
	ReduceThFiles *sync.Map // th -> intermediate-filenames
	OutputFiles   *sync.Map // output-filenames

	mapQ    *taskQ // MapTaskQueue
	reduceQ *taskQ // ReduceTaskQueue

	workers *sync.Map // taskID -> *worker

	phase atomic.Value

	mMap          int           // readOnly
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

type taskQ struct {
	mapSize int       // for Map
	th      *sync.Map // for Reduce
	mu      sync.Mutex
	tk      []string
	fi      *sync.Map
}

func newTaskQ(n int) *taskQ {
	return &taskQ{
		mapSize: n,
		th:      &sync.Map{},
		mu:      sync.Mutex{},
		tk:      make([]string, 0, n),
		fi:      &sync.Map{},
	}
}

func (q *taskQ) push(tk string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tk = append(q.tk, tk)
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
		log.Printf("currrent: %v, task pop: %s\n", q.tk, tk)
		q.tk = q.tk[1:]
		return tk
	}
	return ""
}

func (q *taskQ) empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tk) == 0
}

func (q *taskQ) list() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.tk
}

// RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *ReqTaskArg, reply *ReqTaskReply) error {
	reply.TaskID = genTaskID()
	reply.TaskType = TaskNone
	reply.ReduceN = c.nReduce
	var input string
	switch c.phase.Load() {
	case PhaseMap:
		idleMapTask := c.mapQ.pop() // filename
		if len(idleMapTask) != 0 {
			reply.TaskType = TaskMap
			reply.RawFilename = idleMapTask
			input = idleMapTask
		} else {
			reply.TaskType = TaskWait
		}
	case PhaseReduce:
		idleReduceTask := c.reduceQ.pop() // th
		if len(idleReduceTask) != 0 {
			reply.TaskType = TaskReduce
			reply.ReduceTh = idleReduceTask
			thFiles, _ := c.ReduceThFiles.Load(idleReduceTask)
			thFiles.(*sync.Map).Range(func(filename, _ any) bool {
				reply.Interfilenames = append(reply.Interfilenames, filename.(string))
				return true
			})
			log.Printf("reply reduce work: %v, size: %d\n", reply.Interfilenames, len(reply.Interfilenames))
			input = idleReduceTask
		}
	case PhaseDone:
		reply.TaskType = TaskNone
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
		for _, interfile := range args.Filename {
			th := parseInterfileTh(interfile)
			files, _ := c.ReduceThFiles.LoadOrStore(th, &sync.Map{})
			files.(*sync.Map).Store(interfile, struct{}{})
			c.reduceQ.th.Store(th, struct{}{})
			if mapSize(files.(*sync.Map)) == c.mMap {
				c.reduceQ.push(th)
			}
			worker.output = append(worker.output, interfile)
		}
	case TaskReduce:
		for _, outfile := range args.Filename { // len must 1
			c.OutputFiles.Store(outfile, struct{}{})
			worker.output = []string{outfile}
		}
	}

	c.workers.Store(args.TaskID, worker) // write back

	if args.State == StateCompleted {
		switch worker.taskType {
		case TaskMap:
			c.mapQ.fi.Store(worker.input, struct{}{})
		case TaskReduce:
			c.reduceQ.fi.Store(worker.input, struct{}{})
		}
	}

	c.updatePhase()
	return nil
}

func (c *Coordinator) updatePhase() {
	// update phase by maps
	switch c.phase.Load() {
	case PhaseDone:
		return
	case PhaseMap:
		if c.mapQ.mapSize == mapSize(c.mapQ.fi) && c.mapQ.empty() {
			c.phase.Store(PhaseReduce)
		}
	case PhaseReduce:
		if c.reduceQ.empty() && mapSize(c.reduceQ.fi) == mapSize(c.reduceQ.th) {
			c.phase.Store(PhaseDone)
		}
	}
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

// for worker-early-exit and worker-crash
func (c *Coordinator) keepalive() {
	for !c.Done() {
		c.LogWorker(true)
		disconnects := make([]string, 0)
		curtime := time.Now()
		c.workers.Range(func(taskID, wrk any) bool {
			worker := wrk.(*worker)
			if worker.started == nil ||
				curtime.Sub(*worker.started) > c.workerTimeout {
				if (worker.taskType == TaskMap && c.phase.Load() == PhaseMap) ||
					(worker.taskType == TaskReduce && worker.completed == nil && c.phase.Load() == PhaseReduce) {
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
				c.phase.Store(PhaseMap)
				c.mapQ.push(worker.input)
			case TaskReduce:
				c.phase.Store(PhaseReduce)
				c.reduceQ.push(worker.input)
			}
			log.Printf("%s %s timeout, curtime: %s, started: %s", worker.taskType.String(), worker.input, curtime, worker.started)
			c.workers.Delete(taskID)
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
	c := Coordinator{
		ReduceThFiles: &sync.Map{},
		OutputFiles:   &sync.Map{},
		mapQ:          newTaskQ(len(files)),
		reduceQ:       newTaskQ(nReduce),
		workers:       &sync.Map{},
		phase:         atomic.Value{},
		mMap:          len(files),
		nReduce:       nReduce,
		workerTimeout: time.Second * 10,
	}
	for _, inputfile := range files {
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
func (c *Coordinator) LogWorker(debug bool) {
	workers := make([]worker, 0)
	c.workers.Range(func(_, wrk any) bool {
		workers = append(workers, *wrk.(*worker))
		return true
	})
	sort.Sort(ByTime(workers))
	log.Println("----------------Turns----------------")
	log.Printf("map: %v, reduce: %v\n", c.mapQ.list(), c.reduceQ.list())
	for _, worker := range workers {
		if debug {
			log.Printf("taskID: %s, workerID: %s, input: %s\n", worker.taskID, worker.workerID, worker.input)
			continue
		}
		log.Printf("taskID: %s, dur: (%s~%s,%dms), workerID: %s, input: %s\n", worker.taskID, *worker.started, *worker.completed, worker.completed.Sub(*worker.started).Milliseconds(), worker.workerID, worker.input)
	}
}

func genTaskID() string {
	return uuid.NewString()
}

// func getIdleV2(tk *taskQ) string {
// 	return tk.pop()
// }

// func allCompletedV2(tk *taskQ) bool {
// 	return tk.empty() && countSz(tk.fi) == tk.normalSize
// }

func mapSize(m *sync.Map) int {
	cnt := 0
	m.Range(func(key, value any) bool {
		cnt++
		return true
	})
	return cnt
}

// func getIdle(tasks *sync.Map) string {
// 	taskt := ""
// 	tasks.Range(func(t any, state any) bool {
// 		if state == StateIdle {
// 			taskt = t.(string)
// 			return false
// 		}
// 		return true
// 	})
// 	return taskt
// }

// func allCompleted(tasks *sync.Map) bool {
// 	ok := true
// 	tasks.Range(func(_, state any) bool {
// 		if state != StateCompleted {
// 			ok = false
// 			return false
// 		}
// 		return true
// 	})
// 	return ok
// }
