package mr

import (
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
			time.Sleep(10 * time.Second)
			if t.Status == Processing {
				t.Status = Pending // 超时释放
			}
		}
	}()
}
