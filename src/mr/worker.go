package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
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

func getUserID() string {
	return strconv.Itoa(os.Getuid() + rand.Intn(100))
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	workerID := getUserID() // unique and stable workerID
	workerLog, _ := os.OpenFile("worker-"+workerID+".log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	defer workerLog.Close()
	log.SetOutput(workerLog)
	task := ReqTask(workerID)
	for {
		logTask(workerID, task)
		switch task.TaskType {
		case TaskWait:
			time.Sleep(time.Second * 10)

		case TaskNone:
			time.Sleep(time.Second * 1)
			return

		default:
			workerRun(task, workerID, mapf, reducef)
			time.Sleep(time.Second * 1)
			task = ReqTask(workerID)
		}
	}
}

func logTask(wid string, task ReqTaskReply) {
	detail := ""
	if task.TaskType == TaskMap {
		detail = fmt.Sprintf("rawfile: %s", task.RawFilename)
	} else if task.TaskType == TaskReduce {
		detail = fmt.Sprintf("th: %s", task.ReduceTh)
	}
	log.Printf("Worker: %s, Type: %s, Detail: %s\n", wid, task.TaskType.String(), detail)
}

func workerRun(task ReqTaskReply,
	workerID string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	UpdateTask(UpdateTaskArg{
		TaskID: task.TaskID,
		State:  StateInProgress,
		Time:   time.Now(),
	})

	mapIntersOrReduceOutput := make([]string, 0)
	switch task.TaskType {
	case TaskMap:
		content, err := readFileContent(task.RawFilename)
		if err != nil {
			log.Fatalf("[Worker] readfile: %s, error: %s", task.RawFilename, err.Error())
		}
		kva := mapf(task.RawFilename, string(content))
		// *.txt -> mr-inters-{wid}-{th}
		inters := map[int][]KeyValue{}
		for _, kv := range kva {
			th := ihash(kv.Key) % task.ReduceN // ReduceTh(kv.Key, task.ReduceN)
			if _, ok := inters[th]; !ok {
				inters[th] = make([]KeyValue, 0)
			}
			inters[th] = append(inters[th], kv)
		}

		for th, kvs := range inters {
			interfilename := fmt.Sprintf("mr-inters-%s-%d", normalizedName(task.RawFilename), th)
			createInterfile(interfilename, kvs)
			mapIntersOrReduceOutput = append(mapIntersOrReduceOutput, interfilename)
		}

	case TaskReduce:
		var kva []KeyValue
		for _, interfilename := range task.Interfilenames {
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

		// mr-inters-*-{th} -> mr-out-{th}
		outfilename := "mr-out-" + task.ReduceTh
		reduceInterfile(kva, outfilename, reducef)
		mapIntersOrReduceOutput = append(mapIntersOrReduceOutput, outfilename)
	}

	UpdateTask(UpdateTaskArg{
		TaskID:   task.TaskID,
		State:    StateCompleted,
		Time:     time.Now(),
		Filename: mapIntersOrReduceOutput,
	})
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceInterfile(intermediate []KeyValue, oname string,
	reducef func(string, []string) string) {

	sort.Sort(ByKey(intermediate))

	tempfilef(oname, func(fptr *os.File) {
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

// RPC Call Wrapper
func ReqTask(workerID string) ReqTaskReply {
	task := ReqTaskReply{}
	call("Coordinator.GetTask", &ReqTaskArg{WorkderID: workerID}, &task)
	return task
}

func UpdateTask(arg UpdateTaskArg) {
	call("Coordinator.UpdateState", &arg, &UpdateTaskReply{})
}

// func SaveInters(files []string) {
// 	call("Coordinator.SaveReduceFiles", &UpdateTaskArg{Filename: files}, &UpdateTaskReply{})
// }

// func ReduceTh(key string, reduceN int) int {
// 	return ihash(key) % reduceN
// }

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

// file operation
func createInterfile(interfilename string, kvs []KeyValue) {
	tempfilef(interfilename, func(fptr *os.File) {
		enc := json.NewEncoder(fptr)
		for _, kv := range kvs {
			if err := enc.Encode(kv); err != nil {
				log.Fatalf("[Worker] EncodePairToInterFile filename: %s, error: %s", interfilename, err.Error())
			}
		}
	})
}

func readFileContent(filename string) ([]byte, error) {
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

func parseInterfileTh(interfilename string) string {
	fparts := strings.Split(interfilename, "-")
	return fparts[len(fparts)-1]
}

func normalizedName(filename string) string {
	sbuf := ""
	for _, ch := range filename {
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) {
			sbuf += string(ch)
		}
	}
	return sbuf
}

func tempfilef(filename string, fn func(*os.File)) {
	if fileExists(filename) {
		if err := os.Remove(filename); err != nil {
			log.Printf("[Worker] RemoveAlreadyExistsFile filename: %s, error: %s\n", filename, err.Error())
		}
	}

	fptr, err := os.CreateTemp("", filename) // os.Create(interfilename) // os.OpenFile(interfilename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("[Worker] OpenTempfile filename: %s, error: %s", filename, err.Error())
	}
	defer fptr.Close()
	fn(fptr)

	if err := os.Rename(fptr.Name(), filename); err != nil {
		log.Fatalf("[Worker] RenameTempfile filename: %s, error: %s", filename, err.Error())
	}
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

// func interfileMerge(interfilename string, kvs []KeyValue) {
// 	if fileExists(interfilename) {
// 		rawcontent, err := readFileContent(interfilename)
// 		if err != nil {
// 			log.Fatalf("[Worker] ReadIntermediateFile error: %s", err.Error())
// 		}
// 		raw := []KeyValue{}
// 		// bugs
// 		if len(rawcontent) > 0 {
// 			if err := json.Unmarshal([]byte(rawcontent), &raw); err != nil {
// 				log.Fatalf("[Worker] DecodeIntermediateJSON error: %s", err.Error())
// 			}
// 		}
// 		kvs = append(kvs, raw...)
// 	}
// 	newraw, err := json.Marshal(kvs)
// 	if err != nil {
// 		log.Fatalf("[Worker] EncodeIntermediateJSON error: %s", err.Error())
// 	}
// 	writeContent2File(interfilename, newraw)
// }

// func writeContent2File(filename string, content []byte) {
// 	fptr, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0664)
// 	if err != nil {
// 		log.Fatalf("[Worker] open file for create_append filename: %s, error: %s", filename, err.Error())
// 	}
// 	defer fptr.Close()
// 	fptr.Write(content)
// }
