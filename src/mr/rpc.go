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

type Phase int

const (
	PhaseMap    Phase = 1
	PhaseReduce Phase = 2
	PhaseDone   Phase = 3
)

type TaskType int

const (
	TypeNone   TaskType = 0
	TypeWait   TaskType = 1
	TypeMap    TaskType = 2
	TypeReduce TaskType = 3
)

type TaskState int

const (
	StateIdle       TaskState = 0
	StateEmit       TaskState = 1
	StateInProgress TaskState = 2
	StateCompleted  TaskState = 3
)

// Add your RPC definitions here.
type ReqTaskArg struct {
	WorkerID string
	ReqTime  time.Time
}
type ReqTaskReply struct {
	TaskID      string
	RespTime    time.Time
	TaskType    TaskType
	TaskSeq     int
	ReduceTotal int
	Files       []string // map-input(*.txt) OR reduce-inter-input(mr-X-Y)
}

type UpdateTaskArg struct {
	TaskID    string
	ReqTime   time.Time
	TaskState TaskState
	Files     []string // map-output(mr-X-Y) OR reduce-output(mr-out-X)
}
type UpdateTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
