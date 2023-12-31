package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
  "encoding/json"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func doMap(filename string, workerI int, nReduce int, mapf func (string, string) []KeyValue) {
  file, err := os.Open(filename)
  if(err != nil) {
    log.Fatalf("cannot open %v", filename)
  }

  content, err := io.ReadAll(file)

  if(err != nil) {
    log.Fatalf("cannot read %v", filename)
  }

  kva := mapf(filename, string(content))

  splitKva := make([][]KeyValue, nReduce)
  
  for _, kv := range(kva) {
    reduceI := ihash(kv.Key) % nReduce
    splitKva[reduceI] = append(splitKva[reduceI], kv)
  }

  for i := 0; i < nReduce; i++ {
    // write the partitioned kvs.
    outname := fmt.Sprintf("mr-%v-%v", workerI, i)

    // os.
		file, err := os.Create(outname)
    if err != nil {
      fmt.Println(err)
      log.Fatalf("could not open %s", outname)
    }

    j, _ := json.Marshal(splitKva[i])
    file.Write(j)
  }
}

func doReduce(reply *GetTaskReply, reducef func(string, []string) string) {
  fmt.Println("reducing!!!")
}
//
// main/mrworker.go calls this function.
// this is just a function. (no struct or anything.)
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
  args  := GetTaskArgs{}
  reply := GetTaskReply{}
  call("Coordinator.GetTask", &args, &reply)

  if reply.TaskType == Map {
    doMap(reply.File, reply.WorkerI, reply.NReduce, mapf)
    time.Sleep(5 * time.Second)
    args := FinishMapArgs{}
    reply := FinishMapReply{}
    call("Coordinator.FinishMap", &args, &reply)
    return
  }

  if reply.TaskType == Reduce {
    doReduce(&reply, reducef)
  }


	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
