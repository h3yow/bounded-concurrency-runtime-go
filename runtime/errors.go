package runtime

import (
	"context"
	"errors"
	"fmt"
)

// Reason 区分错误的可归因类别。
type Reason int

const (
	ReasonUnknown Reason = iota
	// ReasonCanceled 上下文取消或超时。
	ReasonCanceled
	// ReasonRejected 因背压（队列满）或运行时关闭被拒绝。
	ReasonRejected
	// ReasonTaskFailed 任务执行体返回错误。
	ReasonTaskFailed
	// ReasonDependencyFailed 因依赖失败而被跳过。
	ReasonDependencyFailed
	// ReasonInvalidDependency 依赖未知或成环，提交被拒绝。
	ReasonInvalidDependency
)

func (r Reason) String() string {
	switch r {
	case ReasonCanceled:
		return "canceled"
	case ReasonRejected:
		return "rejected"
	case ReasonTaskFailed:
		return "task_failed"
	case ReasonDependencyFailed:
		return "dependency_failed"
	case ReasonInvalidDependency:
		return "invalid_dependency"
	default:
		return "unknown"
	}
}

// Error 是运行时统一错误类型，保留分类与原始原因。
type Error struct {
	Reason Reason
	TaskID string
	Cause  error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("runtime: task %q %s: %v", e.TaskID, e.Reason, e.Cause)
	}
	return fmt.Sprintf("runtime: task %q %s", e.TaskID, e.Reason)
}

func (e *Error) Unwrap() error { return e.Cause }

// Is 支持 errors.Is 按 Reason 判定：目标为 *Error 且 Reason 相同即匹配。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Reason == e.Reason
}

// ReasonOf 从错误中提取分类，无法识别时返回 ReasonUnknown。
// 上下文取消/超时错误归为 ReasonCanceled。
func ReasonOf(err error) Reason {
	if err == nil {
		return ReasonUnknown
	}
	var re *Error
	if errors.As(err, &re) {
		return re.Reason
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ReasonCanceled
	}
	return ReasonUnknown
}

var (
	// ErrQueueFull 在提交超过有界队列容量且不愿等待时返回。
	ErrQueueFull = errors.New("runtime: queue full")
	// ErrRuntimeClosed 在运行时关闭后继续提交时返回。
	ErrRuntimeClosed = errors.New("runtime: closed")
)

var (
	errEmptyID     = errors.New("runtime: empty task id")
	errNilRun      = errors.New("runtime: nil task run func")
	errDuplicateID = errors.New("runtime: duplicate task id")
	errUnknownDep  = errors.New("runtime: unknown dependency")
	errCycle       = errors.New("runtime: dependency cycle")
)
