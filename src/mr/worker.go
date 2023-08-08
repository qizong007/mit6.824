package mr

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		taskResp, err := getTask()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		switch taskResp.Type {
		case MapType:
			err = handleMapTask(mapf, taskResp.TaskId, taskResp.Input)
			if err != nil {
				// commit fail
				fmt.Printf("handleMapTask failed. taskId:%v, err:%v\n", taskResp.TaskId, err)
				_, err = commitTask(&CommitTaskRequest{
					Type:   MapType,
					TaskId: taskResp.TaskId,
					Done:   false,
				})
				if err != nil {
					fmt.Printf("handleMapTask commitTask failed. taskId:%v, err:%v\n", taskResp.TaskId, err)
					time.Sleep(time.Second)
					continue
				}
			}
			// commit success
			_, err = commitTask(&CommitTaskRequest{
				Type:   MapType,
				TaskId: taskResp.TaskId,
				Done:   true,
			})
			if err != nil {
				fmt.Printf("handleMapTask commitTask failed. taskId:%v, err:%v\n", taskResp.TaskId, err)
				time.Sleep(time.Second)
				continue
			}
		case ReduceType:
			inputs := strings.Split(taskResp.Input, ":")
			err = handleReduceTask(reducef, taskResp.TaskId, inputs)
			if err != nil {
				// commit fail
				fmt.Printf("handleReduceTask failed. taskId:%v, err:%v\n", taskResp.TaskId, err)
				_, err = commitTask(&CommitTaskRequest{
					Type:   MapType,
					TaskId: taskResp.TaskId,
					Done:   false,
				})
				if err != nil {
					fmt.Printf("handleReduceTask commitTask failed. taskId:%v, err:%v\n", taskResp.TaskId, err)
					time.Sleep(time.Second)
					continue
				}
			}
			// commit success
			_, err = commitTask(&CommitTaskRequest{
				Type:   MapType,
				TaskId: taskResp.TaskId,
				Done:   true,
			})
			if err != nil {
				fmt.Printf("handleReduceTask commitTask failed. taskId:%v, err:%v\n", taskResp.TaskId, err)
				time.Sleep(time.Second)
				continue
			}
		}
	}
}

func handleMapTask(mapf func(string, string) []KeyValue, taskId int, filename string) error {
	start := time.Now()
	fmt.Printf("[task%v] start handleMapTask, filename=%v.\n", taskId, filename)
	defer func() {
		fmt.Printf("[task%v] finished handleMapTask, cost time: %v.\n", taskId, time.Since(start))
	}()
	content, err := readFile(filename)
	if err != nil {
		return err
	}
	getTaskNumResp, err := getTaskNum()
	if err != nil {
		return err
	}
	nReduce := getTaskNumResp.NReduce
	ofiles := make([]*os.File, 0)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf(interFmt, taskId, i)
		ofile, _ := os.Create(oname)
		ofiles = append(ofiles, ofile)
	}
	defer func() {
		for _, ofile := range ofiles {
			ofile.Close()
		}
	}()
	kvList := mapf(filename, content)
	for _, kv := range kvList {
		shardNum := ihash(kv.Key) % nReduce
		_, err = fmt.Fprintf(ofiles[shardNum], "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func handleReduceTask(reducef func(string, []string) string, taskId int, filenames []string) error {
	start := time.Now()
	fmt.Printf("[task%v] start handleMapTask.\n", taskId)
	defer func() {
		fmt.Printf("[task%v] finished handleMapTask, cost time: %v.\n", taskId, time.Since(start))
	}()
	res := make([]*KeyValue, 0)
	for _, filename := range filenames {
		content, err := readFile(filename)
		if err != nil {
			return err
		}
		lines := strings.Split(content, "\n")
		for _, line := range lines {
			l := strings.TrimSpace(line)
			if l != "" {
				params := strings.Split(l, " ")
				if len(params) != 2 {
					fmt.Printf("split params failed. l=%v\n", l)
					continue
				}
				res = append(res, &KeyValue{
					Key:   params[0],
					Value: params[1],
				})
			}
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})

	oname := fmt.Sprintf(outputFmt, taskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(res) {
		j := i + 1
		for j < len(res) && res[j].Key == res[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, res[k].Value)
		}
		output := reducef(res[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", res[i].Key, output)
		i = j
	}

	return nil
}

func getTask() (*GetTaskResponse, error) {
	resp := &GetTaskResponse{}
	err := call("Coordinator.GetTask", &GetTaskRequest{}, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func commitTask(req *CommitTaskRequest) (*CommitTaskResponse, error) {
	resp := &CommitTaskResponse{}
	err := call("Coordinator.CommitTask", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func getTaskNum() (*GetTaskNumResponse, error) {
	resp := &GetTaskNumResponse{}
	err := call("Coordinator.GetTaskNum", &GetTaskNumRequest{}, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
