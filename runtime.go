package bcr

import (
	"context"
	"sync"
	"time"
)

// Runtime 是进程内、有界资源的并发任务执行运行时。
//
// 并发模型：单个调度器 goroutine 持有全部可变状态（任务状态、就绪队列、
// 额度池、统计），worker goroutine 只执行用户函数并通过事件回报结果。
// 因此调度判定无锁、无竞争；所有额度的获取/释放在同一点串行发生，
// 等待依赖、等待额度或退避中的任务都不持有任何额度，从结构上消除
// "持票等待"型死锁。
type Runtime struct {
	cfg Config
	log Logger

	mu      sync.Mutex
	pending []*Task // 启动前的有界登记缓冲，容量 MaxQueued
	sealed  bool    // Run 开始后密封，后续 Submit 明确拒绝
	notFull *sync.Cond

	stats *statsCollector
}

// New 构造运行时。最大并发度由 cfg.Resources[ResourceSlots] 控制。
func New(cfg Config) *Runtime {
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = 1024
	}
	if cfg.Resources == nil {
		cfg.Resources = ResourceLimits{ResourceSlots: 1}
	}
	if _, ok := cfg.Resources[ResourceSlots]; !ok {
		cfg.Resources[ResourceSlots] = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = NopLogger()
	}
	r := &Runtime{cfg: cfg, log: cfg.Logger}
	r.notFull = sync.NewCond(&r.mu)
	return r
}

// Submit 在 Run 启动前登记一个任务。
//
// 背压语义（有界、可预测）：
//   - 登记缓冲容量为 Config.MaxQueued，排队数量不会无界增长；
//   - 缓冲已满且 EnqueueTimeout==0：立即明确拒绝
//     （ReasonAdmissionRejected，errors.Is(_, ErrQueueFull)）；
//   - EnqueueTimeout>0：有界等待空位，超时拒绝
//     （ReasonQueueTimeout，errors.Is(_, ErrQueueTimeout)）；
//   - 单个任务的资源需求超过任一类别总额度：立即拒绝（永远无法满足，
//     不能让它静默挂死）；
//   - Run 启动后登记通道密封：返回 ErrRuntimeClosed。
//
// 跨任务的依赖合法性（重复 ID、不可达依赖、环路）在 Run/RunTasks
// 构建依赖图时统一拒绝。
func (r *Runtime) Submit(t *Task) error {
	if err := t.validate(); err != nil {
		return err
	}
	pool := newResourcePool(r.cfg.Resources)
	if name, ok := pool.fits(t.effectiveResources()); !ok {
		r.log.Logf("admission reject task=%s reason=exceeds_limit resource=%s limits=%v need=%v",
			t.ID, name, pool.limits, t.effectiveResources())
		return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "Submit",
			Err: fmtErr("task requires %d of resource %q which exceeds limit",
				t.effectiveResources()[name], name)}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "Submit",
			Err: ErrRuntimeClosed}
	}
	if len(r.pending) >= r.cfg.MaxQueued {
		if r.cfg.EnqueueTimeout <= 0 {
			r.log.Logf("admission reject task=%s reason=queue_full queued=%d cap=%d",
				t.ID, len(r.pending), r.cfg.MaxQueued)
			return &TaskError{TaskID: t.ID, Reason: ReasonAdmissionRejected, Op: "Submit",
				Err: ErrQueueFull}
		}
		if !r.waitNotFull(r.cfg.EnqueueTimeout) {
			r.log.Logf("admission reject task=%s reason=queue_timeout queued=%d cap=%d timeout=%s",
				t.ID, len(r.pending), r.cfg.MaxQueued, r.cfg.EnqueueTimeout)
			return &TaskError{TaskID: t.ID, Reason: ReasonQueueTimeout, Op: "Submit",
				Err: ErrQueueTimeout}
		}
		// 等待期间 Run 可能已经排空并密封：必须重新检查，
		// 否则会把任务追加进一个已经在执行的任务集之外。
		if r.sealed {
			return &TaskError{TaskID: t.ID, Reason: ReasonInvalidTask, Op: "Submit",
				Err: ErrRuntimeClosed}
		}
	}
	r.pending = append(r.pending, t)
	return nil
}

// waitNotFull 在锁内有界等待 pending 出现空位。
// 通过一个超时定时器唤醒并重新检查，避免无界等待。
func (r *Runtime) waitNotFull(timeout time.Duration) bool {
	timer := time.AfterFunc(timeout, func() {
		r.mu.Lock()
		r.notFull.Broadcast()
		r.mu.Unlock()
	})
	defer timer.Stop()
	deadline := time.Now().Add(timeout)
	for len(r.pending) >= r.cfg.MaxQueued {
		now := time.Now()
		if !now.Before(deadline) {
			return false
		}
		r.notFull.Wait()
		if time.Now().After(deadline) {
			// 可能因超时唤醒：仍满则失败。
			if len(r.pending) >= r.cfg.MaxQueued {
				return false
			}
		}
	}
	return true
}

func (r *Runtime) drainPending() []*Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	tasks := r.pending
	r.pending = nil
	r.sealed = true
	// 排空与密封后立即广播：等待空位的提交者醒来后会看到 sealed
	// 并收到明确拒绝，而不是追加进已在执行的任务集。
	r.notFull.Broadcast()
	return tasks
}

// RunTasks 是便捷方法：批量登记（同样执行依赖/容量校验）后立即执行。
// 任一定义非法或依赖图不合法时返回错误且不启动任何任务。
func (r *Runtime) RunTasks(ctx context.Context, tasks []*Task) (Stats, error) {
	for _, t := range tasks {
		if err := r.Submit(t); err != nil {
			return Stats{}, err
		}
	}
	return r.Run(ctx)
}

// Run 对已登记的任务集执行调度，阻塞至全部任务到达终态后返回最终统计。
func (r *Runtime) Run(ctx context.Context) (Stats, error) {
	tasks := r.drainPending()
	d, err := buildDAG(tasks)
	if err != nil {
		// 图不合法时不启动任何任务；运行时保持密封（一次 Run 已消费
		// 登记集），返回的错误原因可由 errors.Is/As 精确区分。
		r.log.Logf("dag rejected: %v", err)
		return Stats{}, err
	}
	return r.execute(ctx, d)
}

// execute 构造运行级 context（FailFast 时由调度器取消）并运行
// 单线程事件循环；该函数返回时所有任务都已到达终态。
func (r *Runtime) execute(ctx context.Context, d *dag) (Stats, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.stats = newStatsCollector()
	r.stats.submitted = len(d.nodes)
	sched := newScheduler(r, d, runCtx, cancel)
	return sched.run(), nil
}

// Stats 返回当前统计快照；运行结束后即为最终统计。
func (r *Runtime) Stats() Stats {
	if r.stats == nil {
		return Stats{}
	}
	return r.stats.snapshot()
}

// Close 释放运行时资源。可重复调用。
func (r *Runtime) Close() error { return nil }
