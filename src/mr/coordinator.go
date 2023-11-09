package mr

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask //{FileName,fileContent}
	reduceTasks []ReduceTask
	nReduce     int
	nMap        int
	taskCounter int //used to judge if map mapTasks are all finished
	//workerList []string
}

// For single task
type Task struct {
	state    string // idle/assigned/finished
	taskType string // MAP/REDUCE
	timer    *time.Timer
}
type MapTask struct {
	Task
	taskPair KeyValue //{FileName,fileContent} for map phase
}

type ReduceTask struct {
	Task
	taskPair map[int][]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) SayHello(request string, response *string) error {
	*response = *response + request
	return nil
}

func (c *Coordinator) TaskHandler(request string, response *TaskResponse) error {
	if c.taskCounter == c.nMap {
		if c.reduceTasks == nil {
			err := c.scanForJSONFiles("./")
			if err != nil {
				return err
			}
		}
		for i, item := range c.reduceTasks {
			if item.state == "idle" {
				// assign task
				c.sendREDUCETask(&c.reduceTasks[i], response, true)
				return nil
			} else if item.state == "assigned" {
				select {
				case <-c.mapTasks[i].timer.C:
					fmt.Println("recycle task")
					c.sendREDUCETask(&c.reduceTasks[i], response, false)
					return nil
				}
			}
		}
	}
	for i, item := range c.mapTasks {
		if item.state == "idle" {
			// assign task
			c.sendMAPTask(&c.mapTasks[i], response, true)
			return nil
		} else if item.state == "assigned" {
			select {
			case <-c.mapTasks[i].timer.C:
				fmt.Println("recycle task")
				c.sendMAPTask(&c.mapTasks[i], response, false)
				return nil
			}
		}
	}
	//c.PrintTaskState()
	response = nil
	return nil
}

func (c *Coordinator) DoneHandler(request *TaskResponse, response *string) error {
	//c.PrintTaskState()
	for i, item := range c.mapTasks {
		if item.taskPair.Key == request.TaskPair.Key {
			c.taskCounter++
			c.mapTasks[i].state = "finished"
			*response = "GOOD"
			return nil
		}
	}
	*response = "Task not found"
	return errors.New("Task not found")

}

func (c *Coordinator) sendMAPTask(sendTask *MapTask, sendResponse *TaskResponse, startTimer bool) {
	fmt.Println("start scan")
	sendResponse.TaskType = sendTask.taskType
	if sendTask.taskType == "MAP" {
		sendResponse.NTask = c.nMap
	} else {
		sendResponse.NTask = c.nReduce
	}

	sendResponse.TaskPair = sendTask.taskPair
	sendTask.state = "assigned"
	if startTimer {
		sendTask.timer = time.NewTimer(10 * time.Second)
	} else {
		sendTask.timer.Reset(10 * time.Second)
	}

}
func (c *Coordinator) sendREDUCETask(sendTask *ReduceTask, sendResponse *TaskResponse, startTimer bool) {
	sendResponse.TaskType = sendTask.taskType
	if sendTask.taskType == "MAP" {
		sendResponse.NTask = c.nMap
	} else {
		sendResponse.NTask = c.nReduce
	}

	sendResponse.TaskPair2 = sendTask.taskPair
	sendTask.state = "assigned"
	if startTimer {
		sendTask.timer = time.NewTimer(10 * time.Second)
	} else {
		sendTask.timer.Reset(10 * time.Second)
	}

}

func (c *Coordinator) PrintTaskState() {
	for _, item := range c.mapTasks {
		fmt.Println("Name: ", item.taskPair.Key, " State: ", item.state, " Type: ", item.taskType)
	}
}

func (c *Coordinator) scanForJSONFiles(directoryPath string) error {

	files, err := os.ReadDir(directoryPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			fileName := file.Name()
			for i := 0; i < c.nReduce; i++ {
				newTask := ReduceTask{}
				newTask.state = "idle"
				newTask.taskType = "REDUCE"

				prefix := "inter" + strconv.Itoa(i)
				if strings.HasPrefix(fileName, prefix) && strings.HasSuffix(fileName, ".json") {
					newTask.taskPair[i] = append(newTask.taskPair[i], fileName)
				}
				c.reduceTasks = append(c.reduceTasks, newTask)
			}

		}
	}
	c.PrintTaskState()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//TODO 指针也可以传给服务器吗

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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nTask is the number of reduce mapTasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.taskCounter = 0

	for _, fileName := range files {
		content, err := os.ReadFile(fileName)
		if err != nil {
			log.Fatal(err)
		}
		contentPair := KeyValue{fileName, string(content)}
		newTask := MapTask{}
		newTask.taskPair = contentPair
		newTask.state = "idle"
		newTask.taskType = "MAP"
		c.mapTasks = append(c.mapTasks, newTask)
	}
	//---MAP PHASE---

	//---REDUCE PHASE---

	c.server()
	return &c
}
