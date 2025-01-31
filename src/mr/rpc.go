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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	Map    = 1
	Reduce = 2
	Sleep  = 3
	Exit   = 4
)

type FileInfo struct {
	Ip       string
	Filename string
}

type MapTaskInfo struct {
	WorkId   int
	Filename string
	NReduce  int
}

type ReduceTaskInfo struct {
	WorkId int
	Files  []FileInfo
}

type GetTaskArgs struct {
	WorkId   int
	Finished bool
}

type TaskReply struct {
	TaskType int
	Mtask    MapTaskInfo
	Rtask    ReduceTaskInfo
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
