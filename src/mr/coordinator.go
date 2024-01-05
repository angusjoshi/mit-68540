package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
  finishedMaps map[int]bool
  finishedReds map[int]bool
  toMap []int
  files []string
  m sync.Mutex
  totalMap int
  nMap int
  nReduce int
  totalReduce int
  mapTasks chan int
  reduceTasks chan int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishReduce(args *FinishReduceArgs, reply *FinishReduceReply) error {
  c.m.Lock()
  c.nReduce--
  c.finishedReds[args.I] = true
  if c.nReduce == 0 {
    close(c.reduceTasks)
  }
  c.m.Unlock()
  return nil
}

func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
  c.m.Lock()
  c.nMap--
  c.finishedMaps[args.I] = true
  if c.nMap == 0 {
    close(c.mapTasks)
  }
  c.m.Unlock()
  return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
  reply.Done = false

  i, ok := <- c.mapTasks
  if ok {
    // got a map task
    reply.File = c.files[i]
    reply.TaskType = Map
    reply.WorkerI = i
    reply.NReduce = c.totalReduce

    c.finishedMaps[i] = false

    go func() {
      time.Sleep(10 * time.Second)
      if !c.finishedMaps[i] {
        c.mapTasks <- i
      }
    }()
    return nil
  }

  j, ok := <- c.reduceTasks
  if ok {
    reply.TaskType = Reduce
    reply.NMaps = c.totalMap
    reply.WorkerI = j

    c.finishedReds[j] = false

    go func() {
      time.Sleep(10 * time.Second)
      if !c.finishedReds[j] {
        c.reduceTasks <- j
      }
    }()

    return nil
  }

  // all tasks finished
  reply.Done = true
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
  return c.nMap == 0 && c.nReduce == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
  c.files = files
  c.nMap = len(files)
  c.totalMap = c.nMap
  c.nReduce = nReduce
  c.totalReduce = nReduce
  c.mapTasks = make(chan int)
  c.reduceTasks = make(chan int)


  go func() {
    for i := 0; i < c.totalMap; i++ {
      c.mapTasks <- i
    }
  }()

  go func() {
    for i := 0; i < c.totalReduce; i++ {
      c.reduceTasks <- i
    }
  }()

  c.finishedMaps = make(map[int]bool)
  c.finishedReds = make(map[int]bool)

	c.server()
	return &c
}
