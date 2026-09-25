package bcr

import (
	"math/rand"
	"time"
)

// FailurePolicy 决定某个任务到达失败终态后如何处理其余任务。
type FailurePolicy int

const (
	// FailFast：任一任务到达失败终态（Failed）后，运行级 context
	// 被取消，所有排队中、等待依赖/退避中与在途任务收到取消；
	// 尚未启动的任务记为 Canceled(reason=aborted)，在途任务按其
	// 观测到的取消原因归类。
	FailFast FailurePolicy = iota
	// Isolate：失败任务自身记为 Failed；仅把直接/间接依赖它的任务
	// 标记为 Skipped(reason=dependency_failed)，其余任务继续执行，
	// 不因单点失败而整体终止。
	Isolate
)

// Backoff 计算第 attempt 次失败后的退避时长（attempt 从 1 开始）。
type Backoff interface {
	Next(attempt int) time.Duration
}

// ExponentialBackoff 指数退避：min(Base*Factor^(attempt-1), Max)，
// 可选比例抖动 Jitter∈[0,1]：实际等待 = d*(1 + random[-Jitter,Jitter))。
// 抖动用于避免同批次失败任务重试时形成雷鸣群。
type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
	Jitter float64
}

func (b ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := b.Base
	if base <= 0 {
		base = time.Millisecond
	}
	factor := b.Factor
	if factor < 1 {
		factor = 2
	}
	d := base
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * factor)
		if b.Max > 0 && d >= b.Max {
			d = b.Max
			break
		}
	}
	if b.Max > 0 && d > b.Max {
		d = b.Max
	}
	if b.Jitter > 0 {
		j := b.Jitter
		if j > 1 {
			j = 1
		}
		// random∈[-1,1)
		mul := 1 + (rand.Float64()*2-1)*j
		if mul < 0 {
			mul = 0
		}
		d = time.Duration(float64(d) * mul)
	}
	return d
}

// RetryPolicy 有限重试策略：重试次数有界，且必须通过 Decider 判定
// 当前错误是否可重试；退避时长由 Backoff 给出。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次），必须 >=1。
	MaxAttempts int
	// Backoff 退避序列；nil 表示不退避立即重试。
	Backoff Backoff
	// Decider 可重试判定；nil 使用 DefaultRetryDecider。
	Decider RetryDecider
}

// Config 是运行时配置。请使用 NewConfig 获得带默认值的配置后再改写。
type Config struct {
	// MaxQueued 已就绪任务中等待资源的任务数上限（背压队列容量）。
	// 就绪任务超过该容量时，入队调用按 EnqueueTimeout 有界等待，
	// 仍无空位则明确拒绝（ReasonAdmissionRejected/QueueTimeout）。
	MaxQueued int
	// EnqueueTimeout 入队有界等待时长；0 表示不等待，满即拒绝。
	EnqueueTimeout time.Duration
	// AcquireTimeout 单个任务等待资源额度的有界时长；0 表示无限等待
	// （仅受外部 context 约束）。超时记为 ReasonResourceTimeout 并进入
	// 失败/重试判定流程。
	AcquireTimeout time.Duration
	// Resources 各资源类别总额度；内建 ResourceSlots 控制最大并发，
	// 缺省为 1。
	Resources ResourceLimits
	// FailurePolicy 失败链路策略，缺省 FailFast。
	FailurePolicy FailurePolicy
	// DefaultRetry 任务未单独指定 MaxAttempts/Retryable 时使用。
	DefaultRetry RetryPolicy
	// Logger 关键判定日志；nil 使用 NopLogger。
	Logger Logger
}

// NewConfig 返回带安全默认值的配置：
// 单执行槽、排队容量 1024、入队不等待、资源无限等待、FailFast、不重试。
func NewConfig() Config {
	return Config{
		MaxQueued:      1024,
		EnqueueTimeout: 0,
		AcquireTimeout: 0,
		Resources:      ResourceLimits{ResourceSlots: 1},
		FailurePolicy:  FailFast,
		DefaultRetry: RetryPolicy{
			MaxAttempts: 1,
			Decider:     DefaultRetryDecider,
		},
		Logger: NopLogger(),
	}
}

// effectiveMaxAttempts 解析任务的最大尝试次数。
func (cfg Config) effectiveMaxAttempts(t *Task) int {
	if t.MaxAttempts > 0 {
		return t.MaxAttempts
	}
	if cfg.DefaultRetry.MaxAttempts > 0 {
		return cfg.DefaultRetry.MaxAttempts
	}
	return 1
}

// effectiveDecider 解析任务的可重试判定函数。
func (cfg Config) effectiveDecider(t *Task) RetryDecider {
	if t.Retryable != nil {
		return t.Retryable
	}
	if cfg.DefaultRetry.Decider != nil {
		return cfg.DefaultRetry.Decider
	}
	return DefaultRetryDecider
}
