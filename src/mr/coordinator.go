package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"

type Coordinator struct {
	// Your definitions here.
  toMap []int
  i int
  j int
  files []string
  m sync.Mutex
  nMaps int
  nReduce int
  mapWg sync.WaitGroup
  redWg sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishMap(args *FinishMapArgs, reply *FinishMapReply) error {
  c.mapWg.Done()
  fmt.Println("asdfasdf")
  return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
  c.m.Lock()
  if c.i < c.nMaps {
    // map task
    reply.File = c.files[c.i]
    reply.TaskType = Map
    reply.WorkerI = c.i
    reply.NReduce = c.nReduce
    c.i++
    c.m.Unlock()
    return nil
  }

  c.m.Unlock()
  c.mapWg.Wait()

  reply.TaskType = Reduce
  reply.NMaps = c.nMaps

  c.m.Lock()
  reply.WorkerI = c.j
  c.j++
  c.m.Unlock()
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
	// ret := true

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
  c.files = files
  c.nMaps = len(files)
  c.mapWg.Add(c.nMaps)
  c.nReduce = nReduce
  fmt.Println(c.nMaps)

	for _, file := range files {
		fmt.Println(file)
	}

	// Your code here.

	c.server()
	return &c
}
