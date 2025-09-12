package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := getWorkerID()
	logf, _ := os.OpenFile(workerID+".log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	log.SetOutput(logf)

	task := reqTask(workerID)
	for {
		logTask(workerID, task, false)
		switch task.TaskType {
		case TypeNone:
			time.Sleep(time.Second * 1)
			return
		case TypeWait:
			time.Sleep(time.Second * 1)
		default:
			workerDo(task, mapf, reducef)
			logTask(workerID, task, true)
			task = reqTask(workerID)
		}
		time.Sleep(time.Second * 1)
	}
}

func workerDo(
	task ReqTaskReply,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	updateTask(UpdateTaskArg{
		TaskID:    task.TaskID,
		ReqTime:   time.Now(),
		TaskState: StateInProgress,
	})

	outfiles := make([]string, 0)
	switch task.TaskType {
	case TypeMap:
		for _, inputfile := range task.Files {
			content, err := readFile(inputfile)
			if err != nil {
				log.Fatalf("[Worker] readfile: %s, error: %s", inputfile, err.Error())
			}
			kva := mapf(inputfile, string(content))
			// *.txt -> mr-X-Y
			inters := map[int][]KeyValue{}
			for _, kv := range kva {
				th := ihash(kv.Key) % task.ReduceTotal // ReduceTh(kv.Key, task.ReduceN)
				if _, ok := inters[th]; !ok {
					inters[th] = make([]KeyValue, 0)
				}
				inters[th] = append(inters[th], kv)
			}

			for th, kvs := range inters {
				interfilename := fmt.Sprintf("mr-%d-%d", task.TaskSeq, th)
				mapOutput(interfilename, kvs)
				outfiles = append(outfiles, interfilename)
			}
		}

	case TypeReduce:
		var kva []KeyValue
		// log.Printf("reduceSeq: %d, files: %v\n", task.TaskSeq, task.Files)
		for _, interfilename := range task.Files {
			fptr, err := os.Open(interfilename)
			if err != nil {
				log.Fatalf("[Worker] ReduceReadInterfile filename: %s, error: %s", interfilename, err.Error())
			}
			dec := json.NewDecoder(fptr)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					if err != io.EOF {
						log.Printf("[Worker] ReduceDecodeJSONStream filename: %s, error: %s\n", interfilename, err.Error())
					}
					break
				}
				kva = append(kva, kv)
			}
			fptr.Close()
		}

		// mr-X-Y -> mr-out-Y
		outfilename := fmt.Sprintf("mr-out-%d", task.TaskSeq)
		reduceOutput(kva, outfilename, reducef)
		outfiles = append(outfiles, outfilename)
	}

	updateTask(UpdateTaskArg{
		TaskID:    task.TaskID,
		ReqTime:   time.Now(),
		TaskState: StateCompleted,
		Files:     outfiles,
	})
}

func reqTask(workerID string) (reply ReqTaskReply) {
	call("Coordinator.GetTask", &ReqTaskArg{WorkerID: workerID, ReqTime: time.Now()}, &reply)
	return reply
}

func updateTask(arg UpdateTaskArg) {
	call("Coordinator.UpdateTask", &arg, &UpdateTaskReply{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func mapOutput(interfilename string, kvs []KeyValue) {
	tempfileFunc(interfilename, func(fptr *os.File) {
		enc := json.NewEncoder(fptr)
		for _, kv := range kvs {
			if err := enc.Encode(kv); err != nil {
				log.Fatalf("[Worker] EncodePairToInterFile filename: %s, error: %s", interfilename, err.Error())
			}
		}
	})
}

func reduceOutput(intermediate []KeyValue, oname string,
	reducef func(string, []string) string) {

	sort.Sort(ByKey(intermediate))

	tempfileFunc(oname, func(fptr *os.File) {
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(fptr, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
	})
}

func tempfileFunc(filename string, fn func(*os.File)) {
	if fileExists(filename) {
		log.Printf("rewrite: %s\n", filename)
		if err := os.Remove(filename); err != nil {
			log.Printf("[Worker] RemoveAlreadyExistsFile filename: %s, error: %s\n", filename, err.Error())
		}
	}

	fptr, err := os.CreateTemp("", filename)
	if err != nil {
		log.Fatalf("[Worker] OpenTempfile filename: %s, error: %s", filename, err.Error())
	}
	defer fptr.Close()
	fn(fptr)

	if err := os.Rename(fptr.Name(), filename); err != nil {
		log.Fatalf("[Worker] RenameTempfile filename: %s, error: %s", filename, err.Error())
	}
}

func readFile(filename string) ([]byte, error) {
	fptr, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fptr.Close()
	bs, err := io.ReadAll(fptr)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}

func getWorkerID() string {
	return fmt.Sprintf("mr-worker-%d", os.Getpid())
}

func parseInterfileSeq(interfile string) int {
	// mr-%d-%d
	parts := strings.Split(interfile, "-")
	seqStr := parts[len(parts)-1]
	seqInt, _ := strconv.ParseInt(seqStr, 10, 64)
	return int(seqInt)
}

func logTask(wid string, task ReqTaskReply, done bool) {
	detail := ""
	if task.TaskType == TypeMap {
		detail = fmt.Sprintf("mapSeq: %d", task.TaskSeq)
	} else if task.TaskType == TypeReduce {
		detail = fmt.Sprintf("reduceSeq: %d", task.TaskSeq)
	}
	if done {
		detail = fmt.Sprintf("%s, Done", detail)
	}
	log.Printf("Worker: %s, Type: %s, Detail: %s\n", wid, task.TaskType.String(), detail)
}
