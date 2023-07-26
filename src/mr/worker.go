package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getInterFile(fileInfo FileInfo) []KeyValue {
	kva := []KeyValue{}

	//	ip := fileInfo.Ip
	filename := fileInfo.Filename

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// declare an argument structure.
	args := GetTaskArgs{-1, false}

	// declare a reply structure.
	reply := TaskReply{}

	for call("Coordinator.GetTask", &args, &reply) {
		switch reply.TaskType {
		case Map:
			nReduce := reply.Mtask.NReduce
			mapId := reply.Mtask.WorkId

			fileName := reply.Mtask.Filename
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()

			interMediate := make([][]KeyValue, nReduce)
			for i := 0; i < nReduce; i++ {
				interMediate[i] = make([]KeyValue, 0)
			}

			kva := mapf(fileName, string(content))
			for _, keyValue := range kva {
				idx := ihash(keyValue.Key) % nReduce
				interMediate[idx] = append(interMediate[idx], keyValue)
			}

			for reduceId := 0; reduceId < nReduce; reduceId++ {
				tmpFile, err := ioutil.TempFile(".", "tmpfile")
				if err != nil {
					log.Fatalf("cannot create a tmpfile")
				}

				enc := json.NewEncoder(tmpFile)

				for _, keyValue := range interMediate[reduceId] {
					err := enc.Encode(&keyValue)
					if err != nil {
						log.Fatalf("cannot write kv in tmpfile")
					}
				}

				tmpName := tmpFile.Name()
				saveName := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
				err = os.Rename(tmpName, saveName)
				if err != nil {
					log.Fatalf("cannot rename the tmpfile")
				}
				os.Remove(tmpName)
				tmpFile.Close()
			}

			args = GetTaskArgs{mapId, true}
			reply = TaskReply{}
		case Reduce:
			reduceId := reply.Rtask.WorkId
			files := reply.Rtask.Files

			intermediate := []KeyValue{}
			for i := 0; i < len(files); i++ {
				kva := getInterFile(reply.Rtask.Files[i])
				intermediate = append(intermediate, kva...)
			}

			sort.Sort(ByKey(intermediate))

			tmpFile, err := ioutil.TempFile(".", "tmpfile")
			if err != nil {
				log.Fatalf("cannot create a tmpfile")
			}

			for i := 0; i < len(intermediate); {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			tmpName := tmpFile.Name()
			saveName := fmt.Sprintf("mr-out-%d", reduceId)
			err = os.Rename(tmpName, saveName)
			if err != nil {
				log.Fatalf("cannot rename the tmpfile")
			}
			os.Remove(tmpName)
			tmpFile.Close()

			args = GetTaskArgs{reduceId, true}
			reply = TaskReply{}
		case Sleep:
			time.Sleep(time.Second)
			args = GetTaskArgs{-1, false}
			reply = TaskReply{}
		case Exit:
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
