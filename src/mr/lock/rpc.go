package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType int

const (
	TaskWait   TaskType = -1
	TaskNone   TaskType = 0
	TaskMap    TaskType = 1
	TaskReduce TaskType = 2
)

func (t TaskType) String() string {
	switch t {
	case TaskWait:
		return "wait task coming"
	case TaskMap:
		return "Map"
	case TaskReduce:
		return "Reduce"
	default:
		return "None"
	}
}

type TaskState int

const (
	StateIdle       TaskState = 0
	StateInProgress TaskState = 1
	StateCompleted  TaskState = 2
)

type TaskPhase int

const (
	PhaseMap    TaskPhase = 1
	PhaseReduce TaskPhase = 2
	PhaseDone   TaskPhase = 3
)

// Add your RPC definitions here.
type ReqTaskArg struct {
	WorkderID string
}
type ReqTaskReply struct {
	TaskID         string
	TaskType       TaskType
	RawFilename    string   // TaskMap filename
	ReduceTh       string   // TaskReduce th
	Interfilenames []string // TaskReduce filenames
	ReduceN        int
}

type UpdateTaskArg struct {
	TaskID   string
	State    TaskState
	Time     time.Time
	Filename []string // result-filename (inters,[...] OR output,[0])
}
type UpdateTaskReply struct {
}

// type KeyReduceArg struct {
// 	Key string
// }
// type KeyReduceReply struct {
// 	Th int
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
