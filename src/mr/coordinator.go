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

type stack []int

func (s *stack) pop() int {
  x := (*s)[len(*s) - 1]
  (*s)[len(*s) - 1] = 0
  *s = (*s)[:len(*s) - 1]
  return x 
}
func(s *stack) contains(i int) bool {
  for _, x := range(*s) {
    if(x == i) {
      return true
    }
  } 
  return false
}

type Coordinator struct {
	// Your definitions here.
  finishedMaps map[int]bool
  finishedReds map[int]bool
  mapQueue stack
  reduceQueue stack
  toMap []int
  files []string
  m sync.Mutex
  totalMap int
  nMap int
  nReduce int
  done bool
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
  c.m.Unlock()
  return nil
}

func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
  c.m.Lock()
  c.nMap--
  c.finishedMaps[args.I] = true
  c.m.Unlock()
  return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
  reply.Done = false

  c.m.Lock()
  for c.nMap > 0 {
    // map task
    if len(c.mapQueue) == 0 {
      c.m.Unlock()
      time.Sleep(200 * time.Millisecond)
      c.m.Lock()
      continue
    }

    i := c.mapQueue.pop()
    reply.File = c.files[i]
    reply.TaskType = Map
    reply.WorkerI = i
    reply.NReduce = c.nReduce

    c.finishedMaps[i] = false

    go func() {
      time.Sleep(10 * time.Second)
      c.m.Lock()
      if !c.finishedMaps[i] {
        c.mapQueue = append(c.mapQueue, i)
      }
      c.m.Unlock()
    }()

    c.m.Unlock()
    return nil
  }
  c.m.Unlock()

  c.m.Lock()
  for c.nReduce > 0 {
    if len(c.reduceQueue) == 0 {
      c.m.Unlock()
      time.Sleep(200 * time.Millisecond)
      c.m.Lock()
      continue
    }
    // reduce task
    j := c.reduceQueue.pop()
    reply.TaskType = Reduce
    reply.NMaps = c.totalMap
    reply.WorkerI = j

    c.finishedReds[j] = false
    go func() {
      time.Sleep(10 * time.Second)
      c.m.Lock()
      if !c.finishedReds[j] {
        c.reduceQueue = append(c.reduceQueue, j)
      }
      c.m.Unlock()
    }()

    c.m.Unlock()
    return nil
  }

  c.m.Unlock()

  // all tasks finished
  c.done = true
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
  return c.done
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
  c.done = false


  c.mapQueue = make([]int, c.nMap)
  c.reduceQueue = make([]int, nReduce)
  for i := 0; i < c.nMap; i++ {
    c.mapQueue[i] = i
  }

  for i := 0; i < nReduce; i++ {
    c.reduceQueue[i] = i
  }

  c.finishedMaps = make(map[int]bool)
  c.finishedReds = make(map[int]bool)

	c.server()
	return &c
}
