package mr

import (
	"fmt"
	"log"
	"strings"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus string

const (
	Map    CoordinatorStatus = "map"
	Reduce CoordinatorStatus = "reduce"
	Done   CoordinatorStatus = "done"
)

type Coordinator struct {
	MapTasks    map[int]*Task
	ReduceTasks map[int]*Task
	Status      CoordinatorStatus
	Mutex       sync.Mutex
}

func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	switch c.Status {
	case Map:
		for taskId, task := range c.MapTasks {
			if task.Status == Pending {
				c.MapTasks[taskId].Status = Processing
				resp.TaskId = task.TaskId
				resp.Type = task.Type
				resp.Input = task.Input
				return nil
			}
		}
		return fmt.Errorf("no more spare map task")
	case Reduce:
		for taskId, task := range c.ReduceTasks {
			if task.Status == Pending {
				c.ReduceTasks[taskId].Status = Processing
				resp.TaskId = task.TaskId
				resp.Type = task.Type
				resp.Input = task.Input
				return nil
			}
		}
		return fmt.Errorf("no more spare reduce task")
	case Done:
		return fmt.Errorf("all tasks had done")
	}
	return fmt.Errorf("invalid coordinator status")
}

func (c *Coordinator) CommitTask(req *CommitTaskRequest, resp *CommitTaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	taskId := req.TaskId
	taskType := req.Type
	switch c.Status {
	case Map:
		if taskType != MapType {
			return fmt.Errorf("task type is %v, but not %v", taskType, MapType)
		}
		if _, ok := c.MapTasks[taskId]; !ok {
			return fmt.Errorf("task id is %v, but not found", taskId)
		}
		if !req.Done { // 任务没做完
			c.MapTasks[taskId].Status = Pending // 放回等待队列
			return nil
		}
		c.MapTasks[taskId].Status = Finished
		allFinished := true
		for _, t := range c.MapTasks {
			if t.Status != Finished {
				allFinished = false
				break
			}
		}
		if allFinished {
			fmt.Println("all map tasks finished!!!")
			c.Status = Reduce // mr任务进入Reduce阶段
		}
	case Reduce:
		if taskType != ReduceType {
			return fmt.Errorf("task type is %v, but not %v", taskType, ReduceType)
		}
		if _, ok := c.ReduceTasks[taskId]; !ok {
			return fmt.Errorf("task id is %v, but not found", taskId)
		}
		if !req.Done { // 任务没做完
			c.ReduceTasks[taskId].Status = Pending // 放回等待队列
			return nil
		}
		c.ReduceTasks[taskId].Status = Finished
		allFinished := true
		for _, t := range c.MapTasks {
			if t.Status != Finished {
				allFinished = false
				break
			}
		}
		if allFinished {
			fmt.Println("all reduce tasks finished!!!")
			c.Status = Done // mr任务完成
		}
	case Done:
		return fmt.Errorf("all tasks had done")
	}
	return fmt.Errorf("invalid coordinator status")
}

func (c *Coordinator) GetTaskNum(req *GetTaskNumRequest, resp *GetTaskNumResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	resp.NMap = len(c.MapTasks)
	resp.NReduce = len(c.ReduceTasks)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c) // 让 Coordinator 作为server
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

func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.Status == Done
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(map[int]*Task, len(files))
	reduceTasks := make(map[int]*Task, nReduce)

	for i, fileName := range files {
		taskId := i
		task := &Task{
			TaskId: taskId,
			Input:  fileName,
			Type:   MapType,
			Status: Pending,
		}
		mapTasks[taskId] = task
		task.start()
	}

	for i := 0; i < nReduce; i++ {
		taskId := i
		input := func() string {
			names := make([]string, 0, nReduce)
			for mapTaskId := range mapTasks {
				names = append(names, fmt.Sprintf(interFmt, mapTaskId, taskId))
			}
			return strings.Join(names, ":")
		}()
		task := &Task{
			TaskId: taskId,
			Input:  input,
			Type:   ReduceType,
			Status: Pending,
		}
		reduceTasks[taskId] = task
		task.start()
	}

	c := Coordinator{
		MapTasks:    mapTasks,
		ReduceTasks: reduceTasks,
		Status:      Map,
		Mutex:       sync.Mutex{},
	}

	c.server()
	return &c
}
