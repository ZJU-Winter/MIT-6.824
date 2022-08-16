package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	for {
		AskForTask(mapf, reducef)
		time.Sleep(time.Duration(2) * time.Second)
	}
}

// AskForTask
// rpc function to ask a map or reduce task from coordinator.
func AskForTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := AskForTaskArgs{}
	reply := AskForTaskReply{}

	rst := call("Coordinator.Task", &args, &reply)
	if !rst {
		//fmt.Println("call coordinator failed. I am done")
		os.Exit(0)
	}
	if !reply.Job {
		//fmt.Println("currently no job for me.")
		return
	}

	if reply.Task.Map {
		go func() {
			err := Map(&reply, mapf)
			jobargs := JobFinishArgs{reply.Task}
			jobreply := JobFinishReply{}
			if err == nil {
				for i := 0; i < reply.NReduceTask; i += 1 {
					temp := fmt.Sprintf("mr-temp-%v-%v", reply.Task.Tasknum, i)
					final := fmt.Sprintf("mr-%v-%v", reply.Task.Tasknum, i)
					err := os.Rename(temp, final)
					if err != nil {
						log.Fatal("rename file failed\n")
					}
				}
				call("Coordinator.JobDone", &jobargs, &jobreply)
			} else {
				call("Coordinator.JobFail", &jobargs, &jobreply)
			}
		}()
	} else if !reply.Task.Map {
		go func() {
			err := Reduce(&reply, reducef)
			jobargs := JobFinishArgs{reply.Task}
			jobreply := JobFinishReply{}
			if err == nil {
				call("Coordinator.JobDone", &jobargs, &jobreply)
			} else {
				call("Coordinator.JobFail", &jobargs, &jobreply)
			}
		}()
	}
}

// Map
// A wrapper function for mapf
func Map(reply *AskForTaskReply, mapf func(string, string) []KeyValue) error {
	filename := reply.Task.Filename
	n := reply.NReduceTask
	//fmt.Printf("worker:map task on %v start\n", filename)
	//defer fmt.Printf("worker:map task on %v done\n", filename)
	maprst := make([][]KeyValue, n)
	for i := range maprst {
		maprst[i] = []KeyValue{}
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v\n", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	for _, kv := range kva {
		index := ihash(kv.Key) % n
		maprst[index] = append(maprst[index], kv)
	}
	tasknum := reply.Task.Tasknum
	for i := 0; i < n; i += 1 {
		//intermediateFileName := "mr-temp-" + strconv.Itoa(tasknum) + "-" + strconv.Itoa(i)
		intermediateFileName := fmt.Sprintf("mr-temp-%v-%v", tasknum, i)
		ofile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("create temp file %v failed\n", intermediateFileName)
		}
		ofile.Close()
		write(maprst[i], intermediateFileName)
	}
	return nil
}

// Reduce
// A wrapper function for recudef
func Reduce(reply *AskForTaskReply, reducef func(string, []string) string) error {
	//filename := reply.TaskInfo.Filename // filename is ""
	tasknum := reply.Task.Tasknum
	n := reply.NMapTask // total number of map tasks
	//n := 1
	//fmt.Printf("worker:#%v reduce task start\n", tasknum)
	//defer fmt.Printf("worker:#%v reduce task done\n", tasknum)

	data := []KeyValue{}
	prev := []KeyValue{}
	for i := 0; i < n; i += 1 {
		filename := fmt.Sprintf("mr-%v-%v", i, tasknum)
		read(filename, &prev)
		data = append(data, prev...)
	}
	sort.Sort(ByKey(data))

	ofilename := "mr-out-" + strconv.Itoa(tasknum)
	ofile, _ := os.Create(ofilename)
	defer ofile.Close()

	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := reducef(data[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)
		i = j
	}
	return nil
}

// write
// write key/value pairs in JSON format to an open file
func write(content []KeyValue, filename string) error {
	data, _ := json.Marshal(content)
	err := os.WriteFile(filename, data, 0644)
	if err != nil {
		log.Fatalf("Write file %v failed\n", filename)
	}
	return nil
}

// read
// read key/value pairs in JSON format from an open file
func read(filename string, content *[]KeyValue) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Read file %v failed\n", filename)
	}
	json.Unmarshal(data, content)
	return nil
}

// CallExample
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
		return false
		//log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	return err == nil
	//fmt.Println(err)
	//return false
}
