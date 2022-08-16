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

type TaskState int

const NotYet = 0
const Running = 1
const Done = 2

type Task struct {
	Map      bool   // map or reduce
	Filename string // map task's filename
	Tasknum  int    // task number
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    map[Task]TaskState // states of each map task
	reduceTasks map[Task]TaskState // state of each reduce task
	nMapTask    int                // total number of map tasks
	nReduceTask int                // total number of reduce tasks
	nextMap     int                // next number of map task
	nextReduce  int                // next number of reduce task
	lock        sync.Mutex         // mutex for shared data
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Task(args *AskForTaskArgs, reply *AskForTaskReply) error {
	// still have map tasks
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.nextMap != c.nMapTask {
		for task, state := range c.mapTasks {
			if state != NotYet {
				continue
			}
			reply.Task = task
			reply.NReduceTask = c.nReduceTask
			reply.NMapTask = c.nMapTask
			reply.Job = true
			c.mapTasks[task] = Running
			go c.waitSeconds(task, 10)
			//fmt.Printf("give a map task on %v\n", task.Filename)
			break
		}
	} else if c.nextReduce != c.nReduceTask {
		for task, state := range c.reduceTasks {
			if state != NotYet {
				continue
			}
			reply.Task = task
			reply.NReduceTask = c.nReduceTask
			reply.NMapTask = c.nMapTask
			reply.Job = true
			c.reduceTasks[task] = Running
			go c.waitSeconds(task, 10)
			//fmt.Printf("give #%v reduce task\n", task.Tasknum)
			break
		}
	} else {
		return fmt.Errorf("everything done. nothing for you")
	}
	return nil
}

// JobDone
// an RPC call to notify server
func (c *Coordinator) JobDone(args *JobFinishArgs, reply *JobFinishReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.Task.Map {
		c.mapTasks[args.Task] = Done
		c.nextMap += 1
		if c.nextMap > c.nMapTask {
			log.Fatalln("map tasks done more than total number")
		}
		//fmt.Printf("co:#%v map task done\n", args.Task.Tasknum)
		//fmt.Printf("nextMap:%v\n", c.nextMap)
	} else {
		c.reduceTasks[args.Task] = Done
		c.nextReduce += 1
		if c.nextReduce > c.nReduceTask {
			log.Fatalln("reduce tasks done more than total number")
		}
		//fmt.Printf("co:#%v reduce task done\n", args.Task.Tasknum)
		//fmt.Printf("nextReduce:%v\n", c.nextReduce)
	}
	return nil
}

// JobFail
// an RPC call to notify server
func (c *Coordinator) JobFail(args *JobFinishArgs, reply *JobFinishReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.Task.Map {
		c.mapTasks[args.Task] = NotYet
		//fmt.Printf("#%v map task failed", args.Task.Tasknum)
	} else {
		c.reduceTasks[args.Task] = NotYet
		//fmt.Printf("#%v reduce task failed", args.Task.Tasknum)
	}
	return nil
}

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	} else {
		//fmt.Println("cooridinator listening")
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nextMap == c.nMapTask && c.nextReduce == c.nReduceTask
}

// printMapTasks
// print current map tasks' state
func (c *Coordinator) printMapTasks() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for task, state := range c.mapTasks {
		switch state {
		case 0:
			fmt.Printf("#%v map task: %v is NotYet\n", task.Tasknum, task.Filename)
		case 1:
			fmt.Printf("#%v map task: %v is Running\n", task.Tasknum, task.Filename)
		case 2:
			fmt.Printf("#%v map task: %v is Done\n", task.Tasknum, task.Filename)
		}
	}
}

// printReduceTasks
// print current reduce tasks' state
func (c *Coordinator) printReduceTasks() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for task, state := range c.reduceTasks {
		switch state {
		case 0:
			fmt.Printf("#%v reduce task: %v is NotYet\n", task.Tasknum, task.Filename)
		case 1:
			fmt.Printf("#%v reduce task: %v is Running\n", task.Tasknum, task.Filename)
		case 2:
			fmt.Printf("#%v reduce task: %v is Done\n", task.Tasknum, task.Filename)
		}
	}
}

// WaitTenSeconds
// a go routine waiting for worker
func (c *Coordinator) waitSeconds(task Task, seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
	c.lock.Lock()
	defer c.lock.Unlock()
	if task.Map && c.mapTasks[task] == Running {
		c.mapTasks[task] = NotYet
		//fmt.Println("too slow")
	} else if !task.Map && c.reduceTasks[task] == Running {
		c.reduceTasks[task] = NotYet
		//fmt.Println("too slow")
	}
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMapTask:    len(files),
		nReduceTask: nReduce,
		mapTasks:    make(map[Task]TaskState, len(files)),
		reduceTasks: make(map[Task]TaskState, nReduce),
		nextMap:     0,
		nextReduce:  0}
	for i, filename := range files {
		task := Task{Map: true, Filename: filename, Tasknum: i}
		c.mapTasks[task] = NotYet
	}
	for i := 0; i < nReduce; i += 1 {
		task := Task{Map: false, Filename: "", Tasknum: i}
		c.reduceTasks[task] = NotYet
	}
	c.server()
	return &c
}
