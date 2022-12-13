package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

/*
- worker implementation should put the output of the X'th reduce task in the file mr-out-X.
- mr-out-X file should contain one line per Reduce function output, "%v %v" format, called with the key and value
- worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks
*/

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

func getTask() (TaskReply, bool) {
	args := TaskArgs{}
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply.file %v\n", reply.File)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply, ok
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// Get worker task
		task, ok := getTask()
		if !ok {
			fmt.Println("Failed to get task from coordinator")
			continue
		}

		switch task.Type {
		case MapTask:
		case ReduceTask:
		case WaitTask:
		default:

		}

	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	// send an RPC to the coordinator asking for a task.
	CallRequestTask(mapf, reducef)
}

// RPC call to request coordinator for task
func CallRequestTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := TaskArgs{}
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply.file %v\n", reply.File)
	} else {
		fmt.Printf("call failed!\n")
		return
	}

	// Read file
	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()

	// pass it to Map
	kva := mapf(reply.File, string(content))
	fmt.Println(kva)

	// accumulate the intermediate Map output.
	/*intermediate := []mr.KeyValue{}
	intermediate = append(intermediate, kva...)
	fmt.Println(intermediate)*/
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

	fmt.Println(err)
	return false
}
