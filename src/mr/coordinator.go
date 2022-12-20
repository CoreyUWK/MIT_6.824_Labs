package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

/*
mapFiles [T1, T2, T3]

	w1 w2

- file name
- start time
- taskID

queue of tasks -> to process for map
- if worker dies, how know task not completed
*/
type TaskInfo struct {
	task      *TaskReply
	startTime time.Time
	timer     *time.Timer
	done      chan struct{}
}

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
	taskQueue    chan *TaskReply // Queue of map or reduce tasks
	tasksWaiting map[int]*TaskInfo
	phase        TaskType // track phase of Map-Reduce
	nReduce      int      // Store the number of reduce parameter from user
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
	select {
	case task := <-c.taskQueue: // Get map task from queue
		reply.Files = task.Files
		fmt.Printf("%s", task.Files[0])
		reply.NReduce = task.NReduce
		reply.TaskID = task.TaskID
		reply.Type = task.Type

		// Start task monitor
		go c.MonitorTask(task)
		fmt.Println("Started monitor")
	default:
		reply.Type = WaitTask
	}

	return nil
}

// Monitor an assigned task to worker
func (c *Coordinator) MonitorTask(task *TaskReply) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	fmt.Println("Montior Task")

	c.tasksWaiting[task.TaskID] = &TaskInfo{
		task:      task,
		startTime: time.Now(),
		timer:     timer,
		done:      make(chan struct{}),
	}

	select {
	case <-timer.C:
		// If timer triggers then worker took too long and need to reassign
		c.taskQueue <- task
	case <-c.tasksWaiting[task.TaskID].done:
	}

	// Clear task info
	c.tasksWaiting[task.TaskID] = nil
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

// Worker RPC calls when task done
func (c *Coordinator) CallDone(args *TaskReply, response *TaskArgs) error {
	fmt.Println("CallDone")
	// Stop done worker monitor
	c.tasksWaiting[args.TaskID].done <- struct{}{}

	// Check if finished all tasks
	/*switch c.phase {
	case MapTask:
		if len(c.taskQueue) == 0 && len(c.tasksWaiting) == 0 {
			c.phase = ReduceTask

		}
	case ReduceTask:
	}*/

	fmt.Println(args.TaskID, args.Type, args.Files)

	return nil
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
	// Your code here.
	c := Coordinator{
		taskQueue:    make(chan *TaskReply, len(files)),
		tasksWaiting: make(map[int]*TaskInfo),
		nReduce:      nReduce,
		phase:        MapTask,
	}

	// Starting in map
	for i, file := range files {
		c.taskQueue <- &TaskReply{
			Type:    MapTask,
			Files:   []string{file},
			NReduce: nReduce,
			TaskID:  i,
		}
		// fmt.Println(i, file)
	}

	c.server()
	return &c
}
