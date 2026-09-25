package bcr

import (
	"context"
	"errors"
	"fmt"
)

// FailureReason 以可区分的原因归类任务的非成功终态。
// 所有非成功终态都必须携带明确原因，错误不会被静默吞掉。
type FailureReason int

const (
	// ReasonUnknown 未分类（不应出现在终态中，出现即说明实现缺陷）。
	ReasonUnknown FailureReason = iota
	// ReasonTaskError 任务体返回了普通错误。
	ReasonTaskError
	// ReasonPanic 任务体 panic，已被运行时捕获并保留原始值。
	ReasonPanic
	// ReasonContextCanceled 外部 context 被取消（含 FailFast 触发的级联取消）。
	ReasonContextCanceled
	// ReasonContextDeadline context 或任务自身 Timeout 超时。
	ReasonContextDeadline
	// ReasonAdmissionRejected 提交时排队容量已满，被明确拒绝。
	ReasonAdmissionRejected
	// ReasonQueueTimeout 有界等待入队超时。
	ReasonQueueTimeout
	// ReasonResourceTimeout 有界等待资源额度超时。
	ReasonResourceTimeout
	// ReasonDependencyFailed 前置任务未成功，本任务不再启动（失败隔离）。
	ReasonDependencyFailed
	// ReasonCircularDependency 依赖图存在环路。
	ReasonCircularDependency
	// ReasonUnreachableDependency 依赖引用了不存在的任务 ID。
	ReasonUnreachableDependency
	// ReasonDuplicateID 任务 ID 重复登记。
	ReasonDuplicateID
	// ReasonInvalidTask 任务定义非法（空 ID、空函数体等）。
	ReasonInvalidTask
	// ReasonRetryExhausted 有限重试次数耗尽（保留最后一次错误）。
	ReasonRetryExhausted
	// ReasonAborted 运行时因失败策略（FailFast）整体终止。
	ReasonAborted
)

func reasonString(r FailureReason) string {
	switch r {
	case ReasonTaskError:
		return "task_error"
	case ReasonPanic:
		return "panic"
	case ReasonContextCanceled:
		return "context_canceled"
	case ReasonContextDeadline:
		return "context_deadline"
	case ReasonAdmissionRejected:
		return "admission_rejected"
	case ReasonQueueTimeout:
		return "queue_timeout"
	case ReasonResourceTimeout:
		return "resource_timeout"
	case ReasonDependencyFailed:
		return "dependency_failed"
	case ReasonCircularDependency:
		return "circular_dependency"
	case ReasonUnreachableDependency:
		return "unreachable_dependency"
	case ReasonDuplicateID:
		return "duplicate_id"
	case ReasonInvalidTask:
		return "invalid_task"
	case ReasonRetryExhausted:
		return "retry_exhausted"
	case ReasonAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// TaskError 包装任务执行中的错误：保留原始原因（Unwrap）、
// 任务 ID、尝试次数与发生阶段，便于按原因区分并归因统计。
type TaskError struct {
	TaskID  string
	Reason  FailureReason
	Attempt int
	Op      string
	Err     error
}

func (e *TaskError) Error() string {
	if e == nil {
		return "<nil>"
	}
	base := ""
	if e.Err != nil {
		base = ": " + e.Err.Error()
	}
	return fmt.Sprintf("bcr: task %q attempt %d op=%s reason=%s%s",
		e.TaskID, e.Attempt, e.Op, reasonString(e.Reason), base)
}

func (e *TaskError) Unwrap() error { return e.Err }

// Is 支持 errors.Is(err, ErrXxx) 与原因哨兵的匹配。
func (e *TaskError) Is(target error) bool {
	switch target {
	case ErrQueueFull:
		return e.Reason == ReasonAdmissionRejected
	case ErrQueueTimeout:
		return e.Reason == ReasonQueueTimeout
	case ErrResourceTimeout:
		return e.Reason == ReasonResourceTimeout
	case ErrCircularDependency:
		return e.Reason == ReasonCircularDependency
	case ErrUnreachableDep:
		return e.Reason == ReasonUnreachableDependency
	case ErrDuplicateID:
		return e.Reason == ReasonDuplicateID
	case ErrInvalidTask:
		return e.Reason == ReasonInvalidTask
	default:
		return false
	}
}

// 哨兵错误：调用方可用 errors.Is 精确判定拒绝/取消原因。
var (
	ErrQueueFull          = errors.New("bcr: submission queue is full")
	ErrQueueTimeout       = errors.New("bcr: timed out waiting for queue admission")
	ErrResourceTimeout    = errors.New("bcr: timed out waiting for resource quota")
	ErrCircularDependency = errors.New("bcr: dependency graph contains a cycle")
	ErrUnreachableDep     = errors.New("bcr: depends on an unknown task id")
	ErrDuplicateID        = errors.New("bcr: duplicate task id")
	ErrInvalidTask        = errors.New("bcr: invalid task definition")
	ErrRuntimeClosed      = errors.New("bcr: runtime is closed")
	ErrAlreadySubmitted   = errors.New("bcr: task already submitted")
)

// RetryDecider 依据当前尝试次数（1-based，即本次失败是第几次尝试）
// 与错误判定是否可以重试。返回 false 即立即终态失败。
type RetryDecider func(attempt int, err error) bool

// DefaultRetryDecider 默认只对任务普通错误与 panic 重试；
// 取消/超时/资源等待失败等不可控原因不重试。
func DefaultRetryDecider(attempt int, err error) bool {
	var te *TaskError
	if errors.As(err, &te) {
		switch te.Reason {
		case ReasonTaskError, ReasonPanic:
			return true
		default:
			return false
		}
	}
	return false
}

// classifyContextError 区分 context.Canceled 与 DeadlineExceeded，
// 其他错误回落到 ReasonUnknown（调用方应自行进一步分类）。
func classifyContextError(err error) FailureReason {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return ReasonContextDeadline
	case errors.Is(err, context.Canceled):
		return ReasonContextCanceled
	default:
		return ReasonUnknown
	}
}

// fmtErr 构造一个纯文本原始错误，避免在 task.go 额外引入 fmt。
func fmtErr(format string, args ...any) error {
	return &plainError{msg: fmt.Sprintf(format, args...)}
}

type plainError struct{ msg string }

func (e *plainError) Error() string { return e.msg }
