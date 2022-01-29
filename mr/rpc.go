package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestTaskArgs struct{} //RPC for workers to requests task from Master

type RequestTaskReply struct { //RPC for Master to return task requests
	FileName []string
	NReduce  int
	Tasknum  int
	TaskPos  int
	TaskType string
}

type TaskDoneArgs struct {
	TaskNum   int
	TaskType  string
	FileNames []string
}

type TaskDoneReply struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
