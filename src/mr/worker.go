package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

/*
- worker implementation should put the output of the X'th reduce task in the file mr-out-X.
- mr-out-X file should contain one line per Reduce function output, "%v %v" format, called with the key and value
- worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks
*/

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	fmt.Println("Making a getTask call")
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply Type:%d ID:%d\n", reply.Type, reply.TaskID)
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
			log.Fatalln("Failed to connect and get task from coordinator")
			break
		}

		switch task.Type {
		case MapTask:
			files, err := CallMapTask(&task, mapf)
			if err != nil {
				log.Fatalln("Failed to run map")
				break
			}

			// Tell coordinator done map file and produced files
			task.Files = append(task.Files, files[1:]...)
			CallDone(&task)
		case ReduceTask:
			file, err := CallReduceTask(&task, reducef)
			if err != nil {
				log.Fatalln("Failed to run reduce")
				break
			}

			// Tell coordinator done reduce files and produced file
			task.Files = append(task.Files, file)
			CallDone(&task)
		case WaitTask:
			time.Sleep(2 * time.Second)
		default:
			log.Fatalln("Unknown task type received from coordinator")
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

// Process Map task
func CallMapTask(task *TaskReply, mapf func(string, string) []KeyValue) ([]string, error) {
	fileName := task.Files[0]

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//

	content, err := func(fileName string) ([]byte, error) {
		// Read file
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return nil, errors.New("Can't open task file")
		}
		defer file.Close()

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
			return nil, errors.New("cannot read task file")
		}

		return content, nil
	}(fileName)
	if err != nil {
		return nil, err
	}

	// pass it to Map
	intermediateKV := mapf(fileName, string(content))
	//fmt.Println(intermediateKV)

	// accumulate the intermediate Map output for n reducers
	bucketKV := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediateKV {
		bucket := ihash(kv.Key) % task.NReduce
		bucketKV[bucket] = append(bucketKV[bucket], kv)
	}

	// Write out map results to temporary files for reducer
	files := make([]string, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		oname := MapTmpDir + MapTmpFile + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		files[i] = oname
		err := func(oname string) error {
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			enc := json.NewEncoder(ofile)
			for _, kvs := range bucketKV[i] {
				err := enc.Encode(kvs)
				if err != nil {
					return errors.New("can't write encode to json to file")
				}
			}

			return nil
		}(oname)
		if err != nil {
			return files, err
		}
	}

	return files, nil
}

func sortIntermediateReduceFiles(files []string) ([]KeyValue, error) {
	var kva []KeyValue
	for _, filePath := range files {
		fmt.Printf("Attempting to open and decode %s\n", filePath)
		// Merge all key/value pairs in worker intermediate files from one mapper
		err := func(kva *[]KeyValue) error {
			// Open file
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot read %v", filePath)
				return errors.New("cannot read intermediate file")
			}
			defer file.Close()

			// decode json
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					//log.Fatalln("failed to decode json file")
					//return errors.New("cannot read intermediate file")
					break
				}
				*kva = append(*kva, kv)
			}

			return nil
		}(&kva)

		if err != nil {
			return nil, errors.New("cannot read intermediate file")
		}
	}

	// Order intermediate key/values pairs for reducer to work on a set
	sort.Sort(ByKey(kva))

	return kva, nil
}

// Process Reduce Task
func CallReduceTask(task *TaskReply, reducef func(string, []string) string) (string, error) {
	fmt.Printf("Reduce Files: %v\n", task.Files)
	fileNames := task.Files[1:]
	reduceFileNum := task.TaskID
	intermediates, err := sortIntermediateReduceFiles(fileNames)
	if err != nil {
		log.Fatalln("Failed to read and sort intermediate files")
		return "", err
	}

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir+"/"+MapTmpDir, MapTmpFile+"*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
		return "", err
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)
		// Write output of reducer key sum(values) to temp file
		fmt.Fprintf(tempFile, "%v %v\n", intermediates[i].Key, output)

		i = j
	}

	tempFile.Close()

	// TODO: Coordinator should rename file
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)

	return fn, nil
}

// RPC to pass task completion results to coordinator
func CallDone(task *TaskReply) {
	args := TaskArgs{}

	fmt.Println("Making CallDone call")
	ok := call("Coordinator.CallDone", task, &args)
	if !ok {
		fmt.Println("Unable to CallDone on coordinator")
	}
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
