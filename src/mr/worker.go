package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
  "encoding/json"
  "sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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
    outname := fmt.Sprintf("mr-tmp/mr-%v-%v", workerI, i)

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
  // workerI is the reduce index
  // nMaps is the limit on map indides.
  workerI := reply.WorkerI // the index of the 

  all := make([]KeyValue, 0)
  for i := 0; i < reply.NMaps; i++ {
    filename  := fmt.Sprintf("mr-tmp/mr-%v-%v", i, workerI)
    f, err := os.Open(filename)

    if err != nil {
      log.Fatalf("could not open file %s", filename)
    }

    d, err := io.ReadAll(f)

    if err != nil {
      log.Fatalf("could not read file %s", filename)
    }

    var dm []KeyValue
    err = json.Unmarshal(d, &dm)
    all = append(all, dm...)
  }

	sort.Sort(ByKey(all))

	oname := fmt.Sprintf("mr-out-%v", workerI)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(all) {
		j := i + 1
		for j < len(all) && all[j].Key == all[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, all[k].Value)
		}
		output := reducef(all[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", all[i].Key, output)

		i = j
	}

	ofile.Close()
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
