package runtime

import "context"

// TaskFunc 是任务的执行体。
type TaskFunc func(ctx context.Context) error

// Task 描述一个待调度任务。
type Task struct {
	ID       string
	Resource string
	Deps     []string
	Retries  int
	Run      TaskFunc
}

// Status 是任务的终态分类。
type Status int

const (
	StatusPending Status = iota
	StatusCompleted
	StatusFailed
	StatusCanceled
	StatusRejected
	StatusSkipped  // 依赖失败导致未执行
	StatusPanicked // 执行体 panic，被运行时隔离收敛
)

// Result 记录单个任务的最终结果。
type Result struct {
	TaskID        string
	Status        Status
	Err           error
	Attempts      int
	DurationNanos int64
}
