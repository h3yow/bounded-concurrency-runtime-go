package bcr

import (
	"context"
	"time"
)

// Status 是任务在其生命周期中的状态。
// 终态（不再变化）：Succeeded/Failed/Canceled/Rejected/Skipped。
type Status int

const (
	// StatusPending 已登记，尚未参与调度（等待依赖图构建完成）。
	StatusPending Status = iota
	// StatusWaitingDeps 前置任务尚未全部完成。
	StatusWaitingDeps
	// StatusReady 依赖已满足，等待进入有界提交队列。
	StatusReady
	// StatusQueued 已被接纳，等待资源额度。
	StatusQueued
	// StatusRunning 已获取资源并开始执行用户函数。
	StatusRunning
	// StatusRetryWait 本次尝试失败，退避中；不占用任何额度。
	StatusRetryWait
	// StatusSucceeded 终态：成功。
	StatusSucceeded
	// StatusFailed 终态：执行失败且不再重试。
	StatusFailed
	// StatusCanceled 终态：因外部取消、超时或 FailFast 被取消。
	StatusCanceled
	// StatusRejected 终态：提交时被明确拒绝（队列满/等待超时）。
	StatusRejected
	// StatusSkipped 终态：前置失败（失败隔离），本任务不再启动。
	StatusSkipped
)

// Func 是任务体。panic 会被运行时捕获并归类为 ReasonPanic。
type Func func(ctx context.Context) error

// Task 描述一个可调度任务。
type Task struct {
	// ID 任务唯一标识，非空。
	ID string
	// Deps 前置任务 ID 列表，必须全部成功本任务才可启动；
	// 引用不存在的 ID 或形成环路会在构建时被拒绝。
	Deps []string
	// Resources 对各资源类别的需求量；缺省视为仅占用 1 个执行槽。
	Resources ResourceWeights
	// Fn 任务体，非空。
	Fn Func
	// Priority 调度优先级，值越大越优先；同优先级按登记顺序（FIFO）。
	Priority int
	// MaxAttempts 最大尝试次数（含首次）；<=0 时使用运行时默认值。
	MaxAttempts int
	// Timeout 单次尝试的执行超时；0 表示不设任务级超时。
	Timeout time.Duration
	// Retryable 可重试判定；nil 时使用运行时默认策略。
	Retryable RetryDecider
}

// effectiveResources 返回任务的资源需求，确保至少占用 1 个执行槽。
func (t *Task) effectiveResources() ResourceWeights {
	w := make(ResourceWeights, len(t.Resources)+1)
	for k, v := range t.Resources {
		if v > 0 {
			w[k] = v
		}
	}
	if w[ResourceSlots] <= 0 {
		w[ResourceSlots] = 1
	}
	return w
}

// validate 检查任务自身定义的合法性（跨任务的依赖合法性由 dag 校验）。
func (t *Task) validate() error {
	if t == nil {
		return &TaskError{Reason: ReasonInvalidTask, Op: "validate", Err: ErrInvalidTask}
	}
	if t.ID == "" {
		return &TaskError{Reason: ReasonInvalidTask, Op: "validate",
			Err: fmtErr("task id is empty")}
	}
	if t.Fn == nil {
		return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "validate",
			Err: fmtErr("task function is nil")}
	}
	if t.MaxAttempts < 0 {
		return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "validate",
			Err: fmtErr("MaxAttempts must be >= 0")}
	}
	if t.Timeout < 0 {
		return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "validate",
			Err: fmtErr("Timeout must be >= 0")}
	}
	for name, v := range t.Resources {
		if v < 0 {
			return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "validate",
				Err: fmtErr("resource weight for %q must be >= 0", name)}
		}
	}
	return nil
}

// TaskResult 是任务终态的不可变记录，用于统计与一致性校验。
type TaskResult struct {
	TaskID    string
	Status    Status
	Attempts  int
	Reason    FailureReason
	Err       error
	StartTime time.Time
	EndTime   time.Time
	WaitStart time.Time
	WaitDur   time.Duration
	RunDur    time.Duration
}

func statusString(s Status) string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusWaitingDeps:
		return "waiting_deps"
	case StatusReady:
		return "ready"
	case StatusQueued:
		return "queued"
	case StatusRunning:
		return "running"
	case StatusRetryWait:
		return "retry_wait"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusCanceled:
		return "canceled"
	case StatusRejected:
		return "rejected"
	case StatusSkipped:
		return "skipped"
	default:
		return "unknown"
	}
}
