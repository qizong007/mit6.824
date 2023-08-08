package mr

import (
	"fmt"
	"time"
)

type (
	TaskType   string
	TaskStatus string
)

const (
	MapType    TaskType = "map"
	ReduceType TaskType = "reduce"

	Pending    TaskStatus = "pending"    // 等待中
	Processing TaskStatus = "processing" // 处理中
	Finished   TaskStatus = "finished"   // 任务完成
)

type Task struct {
	TaskId int
	Input  string // map是1个，reduce是map个
	Type   TaskType
	Status TaskStatus
}

func (t *Task) start() {
	// 超时监听
	go func() {
		for {
			select {
			case <-time.After(10 * time.Second): // 10秒超时
				fmt.Printf("%v:%v 任务超时！", t.Type, t.TaskId)
				t.Status = Pending // 超时释放
				return
			}
		}
	}()
}
