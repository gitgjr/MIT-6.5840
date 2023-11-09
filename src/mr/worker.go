package mr

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type intermediate struct {
	FileName string
	Kva      []KeyValue
}

type hashKva map[int][]KeyValue

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	workerID := rand.Int()
	//CallExample()
	//ok := CallSayHello(workerID)
	//if !ok {
	//	fmt.Println("failed to content to the RPC server, worker shut down")
	//}
	response := RequestTask(workerID)
	if response == nil {
		fmt.Println("failed to content to the RPC server, worker shut down")
	}
	//fmt.Println(response)
	if response.TaskType == "MAP" {
		// one worker can handle multiple mapTasks
		filename := response.TaskPair.Key
		maped_kva := mapf(filename, response.TaskPair.Value)
		hashedKva := make(hashKva)
		for _, kv := range maped_kva {
			hashed := ihash(kv.Key) % 10
			hashedKva[hashed] = append(hashedKva[hashed], kv)
		}
		//fmt.Println(hashedKva[0])
		err := writeInter(filename, hashedKva)
		if err != nil {
			panic(err)
		} else {
			TaskDone(response, workerID)
		}

	} else if response.TaskType == "REDUCE" {
		fmt.Println("mada")
		fmt.Println(response.TaskPair2)
		task := response.TaskPair2
		for index := range task {
			fmt.Println(index)
		}
		//inter, err := readInter(filename[0])
		//if err != nil {
		//	panic(err)
		//}
		//fmt.Println(inter)
	}

	//intermediate := mapf(string())

}

func RequestTask(ID int) *TaskResponse {
	request := strconv.Itoa(ID)
	response := TaskResponse{}
	ok := call("Coordinator.TaskHandler", request, &response)
	if ok {
		return &response
	} else {
		fmt.Println("call failed")
		return nil
	}

}

func TaskDone(finishedTask *TaskResponse, ID int) {
	request := finishedTask
	response := ""
	ok := call("Coordinator.DoneHandler", request, &response)
	if ok {
		fmt.Println("Task finished")
	} else {
		fmt.Println("call failed")
	}
}

func CallSayHello(ID int) bool {
	request := strconv.Itoa(ID)
	response := ""
	ok := call("Coordinator.SayHello", request, &response)
	if ok {
		fmt.Println("response is", response)
	} else {
		fmt.Println("call failed")
	}
	return ok
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

func writeInter(filename string, content hashKva) error {
	filename2 := strings.Split(filename, ".")
	for i := 0; i < 10; i++ {
		fmt.Println()
		outputName := "inter-" + strconv.Itoa(i) + "-" + filename2[0] + ".json"
		filePtr, err := os.Create(outputName)
		if err != nil {
			return err
		}
		defer filePtr.Close()

		encoder := json.NewEncoder(filePtr)
		err = encoder.Encode(content[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func readInter(filename string) (hashKva, error) {

	filePtr, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer filePtr.Close()

	decoder := json.NewDecoder(filePtr)

	inter := make(hashKva)
	err = decoder.Decode(&inter)
	if err != nil {
		return nil, err
	}
	return inter, nil
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
