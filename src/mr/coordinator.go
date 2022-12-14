package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	taskQueue    chan *TaskReply // Queue of tasks
	tasksWaiting []*TaskInfo
	phase        TaskType // track phase of Map-Reduce

	mu              *sync.Mutex
	tempFiles       [][]string // To track reduce jobs per reducer
	mapJobsCount    int
	mapDoneCount    int
	nReduce         int // Store the number of reduce parameter from user
	reduceDoneCount int
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
	case task := <-c.taskQueue: // Get task from queue
		reply.Files = task.Files
		if DEBUG_MODE {
			fmt.Printf("Sending %d %d %v\n", task.Type, task.TaskID, task.Files)
		}
		reply.NReduce = task.NReduce
		reply.TaskID = task.TaskID
		reply.Type = task.Type

		// Place here to remove Race: says here but why if all threads access different array offset
		/*c.tasksWaiting[task.TaskID] = &TaskInfo{
			//task:      task,
			//startTime: time.Now(),
			//timer:     timer,
			done: make(chan struct{}),
		}*/

		// Start task monitor
		go c.MonitorTask(task)
		if DEBUG_MODE {
			fmt.Println("Started monitor")
		}
	default:
		reply.Type = WaitTask
	}

	return nil
}

// Monitor an assigned task to worker
func (c *Coordinator) MonitorTask(task *TaskReply) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	if DEBUG_MODE {
		fmt.Println("Montior Task")
	}

	select {
	case <-timer.C:
		// If timer triggers then worker took too long and need to reassign
		c.taskQueue <- task
		if DEBUG_MODE {
			fmt.Printf("Putting task back into queue: %d %d %v\n",
				task.TaskID, task.Type, task.Files)
		}
	case <-c.tasksWaiting[task.TaskID].done:
	}

	// Clear task info
	//c.tasksWaiting[task.TaskID] = nil
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
	if DEBUG_MODE {
		fmt.Printf("CallDone: %d %d %v\n", args.Type, args.TaskID, args.Files)
	}

	// Stop done worker monitor
	c.tasksWaiting[args.TaskID].done <- struct{}{}

	// Check if finished all tasks
	switch c.phase {
	case MapTask:
		// Track completed map tasks output files
		for _, filename := range args.Files[1:] {
			var mapTaskID, reduceTaskID int
			filepathFormat := /*MapTmpDir + "/" +*/ MapTmpFile + "%d-%d"
			fmt.Sscanf(filename, filepathFormat, &mapTaskID, &reduceTaskID)
			if DEBUG_MODE {
				fmt.Printf("Append Tmp: %s %s %d %d\n", filename, filepathFormat, mapTaskID, reduceTaskID)
			}
			c.mu.Lock()
			c.tempFiles[reduceTaskID] = append(c.tempFiles[reduceTaskID], filename)
			c.mu.Unlock()
		}

		c.mu.Lock()
		c.mapDoneCount++
		if c.mapDoneCount >= c.mapJobsCount && c.tempFiles != nil {
			if DEBUG_MODE {
				fmt.Printf("Is Done: %d %d %d\n", len(c.taskQueue), c.mapDoneCount, c.reduceDoneCount)
			}
			// Update to move to reduce phase
			c.phase = ReduceTask

			for i := 0; i < c.nReduce; i++ {
				if DEBUG_MODE {
					fmt.Printf("Putting reduce task: %d, %v\n", i, c.tempFiles[i])
				}
				c.taskQueue <- &TaskReply{
					Type:    ReduceTask,
					Files:   c.tempFiles[i],
					NReduce: 1,
					TaskID:  i,
				}
			}
			c.tempFiles = nil
		}
		c.mu.Unlock()

	case ReduceTask:
		c.mu.Lock()
		c.reduceDoneCount++
		c.mu.Unlock()
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	if DEBUG_MODE {
		fmt.Printf("Done: %d %d %d\n", len(c.taskQueue), c.mapDoneCount, c.reduceDoneCount)
	}
	if len(c.taskQueue) == 0 && c.tempFiles == nil &&
		c.mapDoneCount == c.mapJobsCount && c.reduceDoneCount == c.nReduce {
		ret = true
	}

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
		taskQueue:       make(chan *TaskReply, len(files)*nReduce),
		tasksWaiting:    make([]*TaskInfo, len(files)*nReduce),
		nReduce:         nReduce,
		phase:           MapTask,
		tempFiles:       make([][]string, nReduce),
		mu:              new(sync.Mutex),
		mapJobsCount:    len(files),
		mapDoneCount:    0,
		reduceDoneCount: 0,
	}

	for i := 0; i < len(files)*nReduce; i++ {
		// Place here to remove Race: says here but why if all threads access different array offset
		c.tasksWaiting[i] = &TaskInfo{
			//task:      task,
			//startTime: time.Now(),
			//timer:     timer,
			done: make(chan struct{}),
		}
	}

	// Starting in map
	for i, file := range files {
		if DEBUG_MODE {
			fmt.Printf("Putting Into Queue: map, %d, %d, %s\n",
				i, nReduce, file)
		}

		c.taskQueue <- &TaskReply{
			Type:    MapTask,
			Files:   []string{file},
			NReduce: nReduce,
			TaskID:  i,
		}

		// if DEBUG_MODE {
		// 	// fmt.Println(i, file)
		// }
	}

	c.server()
	return &c
}
