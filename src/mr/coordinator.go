package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

/*
"main" routines for the coordinator and worker are in main/mrcoordinator.go and main/mrworker.go; don't change these files.
should put your implementation in mr/coordinator.go, mr/worker.go, and mr/rpc.go.

Coordinator:
- one coordinator process
- coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

Workers:
- workers will talk to the coordinator via RPC
- Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files
*/
type Coordinator struct {
	// Your definitions here.
	mapTasks    []string
	reduceTasks []string
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Respond to worker RPC with file name of an as-yet-unstarted map task
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	lastIdx := len(c.files) - 1

	if lastIdx < 0 {
		return errors.New("No more files for workers")
	}

	// Get file
	reply.File = c.files[lastIdx]

	// Remove last file
	c.files[lastIdx] = ""
	c.files = c.files[:lastIdx]

	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// returns true when the MapReduce job is completely finished
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// simple way to implement this is to use the return value from call()
	// if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, and so the worker can terminate too
	// Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
//	each mapper needs to create nReduce intermediate files for consumption by the reduce tasks.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// Track files as tasks for workers to complete
	c.mapTasks = files
	c.nReduce = nReduce
	/*for i, file := range files {
		fmt.Println(i, file)
	}*/

	c.server()
	return &c
}
