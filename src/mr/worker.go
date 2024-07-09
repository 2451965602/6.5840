package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

func GetTask() (string, int, []string, int) {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.TaskAssignment", &args, &reply)
	if ok {
		return reply.TaskSort, reply.TaskId, reply.Task, reply.NReduce
	}
	return "", -1, nil, -1
}

func MapWorker(mapf func(string, string) []KeyValue, TaskId int, Task []string, NReduce int) {

	intermediate := []KeyValue{}
	for _, filename := range Task {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	HashKV := make(map[int][]KeyValue)

	for _, v := range intermediate {
		index := ihash(v.Key) % NReduce
		HashKV[index] = append(HashKV[index], v)
	}

	for i := 0; i < NReduce; i++ {
		data, err := json.MarshalIndent(HashKV[i], "", "     ")
		if err != nil {
			return
		}

		err = os.WriteFile("mr-"+strconv.Itoa(TaskId)+"-"+strconv.Itoa(i), data, 0644)
		if err != nil {
			panic(err)
		}
	}
	ReportDone(TaskId)
}

func ReduceWorker(reducef func(string, []string) string, TaskId int, Task []string, NReduce int) {

	intermediate := []KeyValue{}

	for _, filename := range Task {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		var data []KeyValue
		json.Unmarshal(content, &data)
		intermediate = append(intermediate, data...)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(TaskId)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	ReportDone(TaskId)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		TaskSort, TaskId, Task, NReduce := GetTask()

		ctx, cancel := context.WithCancel(context.Background())

		fmt.Println("TaskSort", TaskSort, "Task", Task)

		if TaskSort == "Map" {

			go func() {
				defer cancel()
				HealthUpdata(ctx, TaskId)
			}()

			MapWorker(mapf, TaskId, Task, NReduce)
		} else if TaskSort == "Reduce" {
			go func() {
				defer cancel()
				HealthUpdata(ctx, TaskId)
			}()

			ReduceWorker(reducef, TaskId, Task, NReduce)
		} else if TaskSort == "Wait" {
			time.Sleep(time.Second)
		} else {
			break
		}

		// 确保在每次循环结束时取消协程
		cancel()
	}
}

func ReportDone(TaskId int) {

	args := DoneArgs{}
	args.TaskId = TaskId
	reply := DoneReply{}

	call("Coordinator.DoneHandle", &args, &reply)
}

func HealthUpdata(ctx context.Context, TaskId int) {

	for {
		select {
		case <-ctx.Done():
			return
		default:
			args := HealthArgs{}
			args.TaskId = TaskId
			reply := HealthReply{}

			call("Coordinator.HealthUpdata", &args, &reply)
			time.Sleep(800 * time.Millisecond)
		}
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
