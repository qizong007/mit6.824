package mr

type GetTaskRequest struct {
}

type GetTaskResponse struct {
	TaskId int
	Input  string // map是1个，reduce是map个
	Type   TaskType
}

type CommitTaskRequest struct {
	Type   TaskType
	TaskId int
	Done   bool
}

type CommitTaskResponse struct {
}

type GetTaskNumRequest struct {
}

type GetTaskNumResponse struct {
	NMap    int
	NReduce int
}
