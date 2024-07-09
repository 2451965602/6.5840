package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct{}

type TaskReply struct {
	TaskSort string
	TaskId   int
	Task     []string
	NReduce  int
}

type DoneArgs struct {
	TaskId int
}

type DoneReply struct{}

type HealthArgs struct {
	TaskId int
}

type HealthReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
