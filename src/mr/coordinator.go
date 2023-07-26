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

type mapWork struct {
	finished int
	filename string
	ip       string
}

type reduceWork struct {
	finished int
}

type Coordinator struct {
	// Your definitions here.
	mapWorks    []mapWork
	reduceWorks []reduceWork
	mapCount    int
	reduceCount int
	nMap        int
	nReduce     int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) waitForFinish(taskType, idx int) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if taskType == Map {
		if c.mapWorks[idx].finished == -1 {
			c.mapWorks[idx].finished = 0
		}
	} else if taskType == Reduce {
		if c.reduceWorks[idx].finished == -1 {
			c.reduceWorks[idx].finished = 0
		}
	}
}

func (c *Coordinator) getRtaskFiles(id int) []FileInfo {
	files := []FileInfo{}

	for i := 0; i < c.nMap; i++ {
		file := FileInfo{"", fmt.Sprintf("mr-%d-%d", i, id)}
		files = append(files, file)
	}

	return files
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *TaskReply) error {
	workId := args.WorkId
	finished := args.Finished

	c.mu.Lock()
	defer c.mu.Unlock()

	if workId != -1 && finished {
		if c.mapCount != 0 {
			c.mapCount -= 1
			c.mapWorks[workId].finished = 1
			c.mapWorks[workId].ip = "" //不知道如何获取client ip, 暂时为空
		} else if c.reduceCount != 0 {
			c.reduceCount -= 1
			c.reduceWorks[workId].finished = 1
		}

	}

	reply.TaskType = Sleep
	if c.mapCount != 0 {
		for id := 0; id < c.nMap; id++ {
			if c.mapWorks[id].finished == 0 {
				c.mapWorks[id].finished = -1
				reply.TaskType = Map
				reply.Mtask.WorkId = id
				reply.Mtask.Filename = c.mapWorks[id].filename
				reply.Mtask.NReduce = c.nReduce
				go c.waitForFinish(Map, id)
				break
			}
		}
	} else if c.reduceCount != 0 {
		for id := 0; id < c.nReduce; id++ {
			if c.reduceWorks[id].finished == 0 {
				c.reduceWorks[id].finished = -1
				reply.TaskType = Reduce
				reply.Rtask.WorkId = id
				reply.Rtask.Files = c.getRtaskFiles(id)
				go c.waitForFinish(Reduce, id)
				break
			}
		}
	} else {
		reply.TaskType = Exit
	}

	return nil
}

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
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	if c.reduceCount == 0 {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapCount = len(files)
	c.nMap = len(files)
	c.mapWorks = make([]mapWork, c.mapCount)
	for i := 0; i < len(c.mapWorks); i++ {
		c.mapWorks[i].filename = files[i]
	}

	c.reduceCount = nReduce
	c.nReduce = nReduce
	c.reduceWorks = make([]reduceWork, c.reduceCount)

	c.server()
	return &c
}
