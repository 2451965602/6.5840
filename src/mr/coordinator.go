package mr

import (
	"github.com/sasha-s/go-deadlock"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Task struct {
	TaskSlice     []string
	Statue        string
	LastCheckTime time.Time
}

type Coordinator struct {
	NReduce   int
	TotalTask int
	Tasks     []Task
	Status    string
	AwaitTask []int
	Donenum   int
	mu        deadlock.Mutex
}

func (c *Coordinator) TaskAssignment(args *TaskArgs, reply *TaskReply) error {

	if c.Status == "Reduce" && c.Donenum == c.TotalTask {
		c.Status = "Done"
		return nil
	}

	reply.NReduce = c.NReduce

	if len(c.AwaitTask) > 0 {
		reply.TaskSort = c.Status

		c.mu.Lock()
		index := c.AwaitTask[0]
		reply.TaskId = index
		reply.Task = c.Tasks[index].TaskSlice
		c.AwaitTask = c.AwaitTask[1:]
		c.Tasks[index].LastCheckTime = time.Now()
		c.Tasks[index].Statue = "Running"
		c.mu.Unlock()
	} else if c.Status == "Done" {
		reply.TaskSort = "Done"
	} else if len(c.AwaitTask) == 0 {
		reply.TaskSort = "Wait"
	}

	return nil
}

func (c *Coordinator) TaskDone() {
	c.mu.Lock()
	if c.Status == "Map" {

		Tasks := make([]Task, c.NReduce)

		for i := 0; i < c.NReduce; i++ {

			for j := 0; j < c.TotalTask; j++ {
				Tasks[i].TaskSlice = append(Tasks[i].TaskSlice, "mr-"+strconv.Itoa(j)+"-"+strconv.Itoa(i))
				Tasks[i].Statue = "Wait"
			}

		}
		c.Tasks = Tasks

		c.TotalTask = c.NReduce
		c.Status = "Reduce"
		c.AwaitTask = make([]int, c.TotalTask)
		for i := 0; i < c.TotalTask; i++ {
			c.AwaitTask[i] = i
		}
		c.Donenum = 0

	}
	c.mu.Unlock()
}

func (c *Coordinator) DoneHandle(args *DoneArgs, reply *DoneReply) error {
	c.mu.Lock()
	c.Donenum += 1
	c.Tasks[args.TaskId].Statue = "Done"
	c.mu.Unlock()
	if c.Donenum == c.TotalTask {
		c.TaskDone()
	}

	return nil
}

func (c *Coordinator) HealthUpdata(args *HealthArgs, reply *HealthReply) error {

	c.Tasks[args.TaskId].LastCheckTime = time.Now()
	return nil
}

func (c *Coordinator) HealthCheck() {

	for {

		for i := 0; i < c.TotalTask; i++ {

			if c.Tasks[i].Statue == "Running" && time.Now().Sub(c.Tasks[i].LastCheckTime) > 1*time.Second {
				c.AwaitTask = append(c.AwaitTask, i)
				c.Tasks[i].Statue = "Wait"
			}
		}

	}
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
func (c *Coordinator) Done() bool {
	ret := false

	if c.Status == "Done" {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce}
	deadlock.Opts.DeadlockTimeout = time.Second
	c.Status = "Map"
	sliceCapability := 3

	c.TotalTask = (len(files) + sliceCapability - 1) / sliceCapability

	c.Tasks = make([]Task, c.TotalTask)

	for i := 0; i < c.TotalTask; i += 1 {
		end := (i + 1) * sliceCapability
		if end > len(files) {
			end = len(files)
		}
		c.Tasks[i].TaskSlice = files[i*sliceCapability : end]
		c.Tasks[i].Statue = "Wait"
	}
	c.AwaitTask = make([]int, c.TotalTask)
	for i := 0; i < c.TotalTask; i++ {
		c.AwaitTask[i] = i
	}
	c.Donenum = 0

	go c.HealthCheck()

	c.server()
	return &c
}
