package runtime

import (
	"context"
	"errors"
	"sync"
	"time"
)

// FailurePolicy 决定任务失败后的整体行为。
type FailurePolicy int

const (
	// FailFast 任一任务失败即取消其余所有未完成任务。
	FailFast FailurePolicy = iota
	// Isolate 失败被隔离，仅其依赖者被跳过，其余任务继续。
	Isolate
)

// statusRunning 是内部状态：已派发、正在获取额度或执行中。
const statusRunning Status = -1

// Option 配置 Runtime。
type Option func(*Runtime)

// WithQuotas 设置各资源类别的额度；未配置的类别视为无限。
func WithQuotas(limits map[string]int) Option {
	return func(r *Runtime) { r.quotas = newQuotas(limits) }
}

// WithQueueCapacity 设置有界等待队列容量（在途+排队任务总数上限）。
func WithQueueCapacity(n int) Option {
	return func(r *Runtime) {
		if n > 0 {
			r.queueCap = n
		}
	}
}

// WithFailurePolicy 设置失败策略。
func WithFailurePolicy(p FailurePolicy) Option {
	return func(r *Runtime) { r.policy = p }
}

// WithAdmissionTimeout 设置阻塞式 Submit 等待准入名额的最坏收敛时间上界，
// 超时后整批拒绝并返回包装 ErrAdmissionTimeout 的错误。<=0 时回退默认值。
func WithAdmissionTimeout(d time.Duration) Option {
	return func(r *Runtime) {
		if d > 0 {
			r.admissionTimeout = d
		}
	}
}

// WithRetryPolicy 设置默认重试策略。
func WithRetryPolicy(p RetryPolicy) Option {
	return func(r *Runtime) { r.retry = p }
}

// Runtime 是有界资源的进程内并发任务执行运行时。
//
// 背压模型：运行时持有 queueCap 个准入名额，按批原子获取：一批任务要么
// 整批被接受，要么整批被拒绝并告知原因，不存在只进一部分的中间态；
// 等待期间不持有任何名额。Submit 在名额不足时有界等待（受
// WithAdmissionTimeout 与 ctx 双重约束，最坏有限时间内收敛），
// TrySubmit 在名额不足时立即整批拒绝。等待依赖的任务已计入准入，
// 但不持有任何资源额度；额度仅在实际执行尝试期间持有。
type Runtime struct {
	quotas           *quotas
	policy           FailurePolicy
	retry            RetryPolicy
	queueCap         int
	admissionTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	closed     bool
	tasks      map[string]*taskState
	order      []string
	dependents map[string][]*taskState
	pending    int // 未到达终态的任务数
	done       chan struct{}

	admission *admission
	ready     chan *taskState
	stop      chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup

	stats statsCounter
}

// taskState 是任务的内部调度状态。
type taskState struct {
	task     Task
	status   Status
	remain   int // 未完成的依赖数
	err      error
	attempts int
	durNanos int64
	batch    *batch
}

// batch 跟踪一次提交的上下文：提交方的 ctx 取消会传播到该批所有任务，
// 运行时 ctx 取消同样传播；批内任务全部到达终态后释放上下文资源。
type batch struct {
	ctx       context.Context
	cancel    context.CancelFunc
	stopMerge func() bool
	remaining int // 受 Runtime.mu 保护
}

// ctx 返回任务执行应使用的上下文。
func (ts *taskState) ctx() context.Context {
	if ts.batch != nil {
		return ts.batch.ctx
	}
	return context.Background()
}

// New 创建运行时。
func New(opts ...Option) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runtime{
		quotas:           newQuotas(nil),
		policy:           Isolate,
		retry:            RetryPolicy{MaxAttempts: 1},
		queueCap:         1024,
		admissionTimeout: DefaultAdmissionTimeout,
		ctx:              ctx,
		cancel:           cancel,
		tasks:            make(map[string]*taskState),
		dependents:       make(map[string][]*taskState),
		done:             make(chan struct{}),
		stop:             make(chan struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.admission = newAdmission(r.queueCap)
	r.ready = make(chan *taskState, r.queueCap)
	close(r.done) // 初始无任务，done 处于关闭态
	r.wg.Add(1)
	go r.dispatch()
	return r
}

// Submit 提交一批任务；依赖非法（未知或成环）时整批拒绝。
// 准入令牌不足时阻塞等待，直到有空位或 ctx 取消。
func (r *Runtime) Submit(ctx context.Context, tasks ...Task) error {
	return r.submit(ctx, tasks, false)
}

// TrySubmit 与 Submit 相同，但准入令牌不足时立即返回 ErrQueueFull。
func (r *Runtime) TrySubmit(ctx context.Context, tasks ...Task) error {
	return r.submit(ctx, tasks, true)
}

func (r *Runtime) submit(ctx context.Context, tasks []Task, tryOnly bool) error {
	if len(tasks) == 0 {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.stats.rejected.Add(int64(len(tasks)))
		return &Error{Reason: ReasonRejected, Cause: ErrRuntimeClosed}
	}
	if err := r.validateLocked(tasks); err != nil {
		r.mu.Unlock()
		r.stats.rejected.Add(int64(len(tasks)))
		return err
	}
	r.mu.Unlock()

	// 准入：整批原子获取名额——要么全部拿到，要么一个不占，
	// 等待期间不持有任何名额，最坏在 admissionTimeout 内收敛。
	waited, err := r.admission.acquire(ctx, len(tasks), r.admissionTimeout, tryOnly)
	if err != nil {
		r.stats.rejected.Add(int64(len(tasks)))
		if waited {
			r.stats.admWaits.Add(1)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &Error{Reason: ReasonCanceled, Cause: err}
		}
		return &Error{Reason: ReasonRejected, Cause: err}
	}
	if waited {
		r.stats.admWaits.Add(1)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.admission.release(len(tasks))
		r.stats.rejected.Add(int64(len(tasks)))
		return &Error{Reason: ReasonRejected, Cause: ErrRuntimeClosed}
	}
	if r.pending == 0 {
		r.done = make(chan struct{})
	}
	r.pending += len(tasks)
	r.stats.submitted.Add(int64(len(tasks)))
	b := &batch{remaining: len(tasks)}
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.stopMerge = context.AfterFunc(r.ctx, func() { b.cancel() })
	var skipList []*taskState
	for _, t := range tasks {
		ts := &taskState{task: t, status: StatusPending, batch: b}
		r.tasks[t.ID] = ts
		r.order = append(r.order, t.ID)
		for _, dep := range t.Deps {
			d := r.tasks[dep]
			switch {
			case d.status == StatusCompleted:
				// 已完成的依赖直接满足
			case terminal(d.status):
				// 依赖已失败/取消/跳过：本任务直接跳过
				skipList = append(skipList, ts)
			default:
				ts.remain++
				r.dependents[dep] = append(r.dependents[dep], ts)
			}
		}
	}
	r.mu.Unlock()

	for _, ts := range skipList {
		r.finish(ts, StatusSkipped, &Error{
			Reason: ReasonDependencyFailed,
			TaskID: ts.task.ID,
			Cause:  errDependencyTerminal,
		}, 0, 0)
	}

	// 就绪任务入队（remain==0 且未跳过）。
	r.mu.Lock()
	var readyList []*taskState
	for _, t := range tasks {
		ts := r.tasks[t.ID]
		if ts.status == StatusPending && ts.remain == 0 {
			readyList = append(readyList, ts)
		}
	}
	r.mu.Unlock()
	for _, ts := range readyList {
		r.ready <- ts
	}
	return nil
}

var errDependencyTerminal = errors.New("runtime: dependency reached terminal failure")

// validateLocked 校验批次：ID 非空唯一、Run 非空、依赖存在且无环。
func (r *Runtime) validateLocked(tasks []Task) error {
	batch := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		if t.ID == "" {
			return &Error{Reason: ReasonInvalidDependency, Cause: errEmptyID}
		}
		if t.Run == nil {
			return &Error{Reason: ReasonInvalidDependency, TaskID: t.ID, Cause: errNilRun}
		}
		if batch[t.ID] || r.tasks[t.ID] != nil {
			return &Error{Reason: ReasonInvalidDependency, TaskID: t.ID, Cause: errDuplicateID}
		}
		batch[t.ID] = true
	}
	for _, t := range tasks {
		for _, dep := range t.Deps {
			if !batch[dep] && r.tasks[dep] == nil {
				return &Error{Reason: ReasonInvalidDependency, TaskID: t.ID, Cause: errUnknownDep}
			}
		}
	}
	// 环只可能形成于批内（既有任务不会依赖新任务）。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(tasks))
	byID := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		byID[t.ID] = t
	}
	var visit func(id string) bool
	visit = func(id string) bool {
		color[id] = gray
		for _, dep := range byID[id].Deps {
			if !batch[dep] {
				continue
			}
			switch color[dep] {
			case gray:
				return true
			case white:
				if visit(dep) {
					return true
				}
			}
		}
		color[id] = black
		return false
	}
	for _, t := range tasks {
		if color[t.ID] == white && visit(t.ID) {
			return &Error{Reason: ReasonInvalidDependency, TaskID: t.ID, Cause: errCycle}
		}
	}
	return nil
}

// dispatch 消费就绪队列并派发执行。
func (r *Runtime) dispatch() {
	defer r.wg.Done()
	for {
		select {
		case ts := <-r.ready:
			r.mu.Lock()
			if ts.status != StatusPending {
				r.mu.Unlock()
				continue
			}
			ts.status = statusRunning
			r.mu.Unlock()
			r.wg.Add(1)
			go r.run(ts)
		case <-r.stop:
			return
		}
	}
}

// run 执行单个任务，含额度获取、重试与退避。
func (r *Runtime) run(ts *taskState) {
	defer r.wg.Done()
	maxAttempts := r.retry.MaxAttempts
	if ts.task.Retries > 0 {
		maxAttempts = ts.task.Retries + 1
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var totalDur time.Duration
	ctx := ts.ctx()
	for attempt := 1; ; attempt++ {
		release, err := r.quotas.acquire(ctx, ts.task.Resource)
		if err != nil {
			r.finish(ts, StatusCanceled, &Error{Reason: ReasonCanceled, TaskID: ts.task.ID, Cause: err}, totalDur, attempt-1)
			return
		}
		// 已取消的任务不得再被启动（同时检查运行时与批次上下文）。
		if err := ctx.Err(); err != nil {
			release()
			r.finish(ts, StatusCanceled, &Error{Reason: ReasonCanceled, TaskID: ts.task.ID, Cause: err}, totalDur, attempt-1)
			return
		}
		if err := r.ctx.Err(); err != nil {
			release()
			r.finish(ts, StatusCanceled, &Error{Reason: ReasonCanceled, TaskID: ts.task.ID, Cause: err}, totalDur, attempt-1)
			return
		}
		start := time.Now()
		runErr, panicErr := callIsolated(ctx, ts.task.Run)
		totalDur += time.Since(start)
		release()

		if panicErr != nil {
			// 崩溃被收敛为该任务一次可单独识别的失败终态，不重试，
			// 失败传播（Isolate 跳过下游 / FailFast 收敛全部）与普通失败一致。
			r.stats.panics.Add(1)
			r.finish(ts, StatusFailed, &Error{Reason: ReasonPanicked, TaskID: ts.task.ID, Cause: panicErr}, totalDur, attempt)
			return
		}
		if runErr == nil {
			r.finish(ts, StatusCompleted, nil, totalDur, attempt)
			return
		}
		if ReasonOf(runErr) == ReasonCanceled {
			r.finish(ts, StatusCanceled, &Error{Reason: ReasonCanceled, TaskID: ts.task.ID, Cause: runErr}, totalDur, attempt)
			return
		}
		taskErr := &Error{Reason: ReasonTaskFailed, TaskID: ts.task.ID, Cause: runErr}
		if attempt < maxAttempts && Retryable(taskErr) {
			r.stats.retried.Add(1)
			select {
			case <-time.After(r.retry.backoff(attempt)):
				continue
			case <-ctx.Done():
				r.finish(ts, StatusCanceled, &Error{Reason: ReasonCanceled, TaskID: ts.task.ID, Cause: ctx.Err()}, totalDur, attempt)
				return
			}
		}
		r.finish(ts, StatusFailed, taskErr, totalDur, attempt)
		return
	}
}

// finish 将任务置入终态，统计、归还准入并传播给依赖者。幂等。
func (r *Runtime) finish(ts *taskState, status Status, err error, dur time.Duration, attempts int) {
	r.mu.Lock()
	released := r.finishLocked(ts, status, err, dur, attempts)
	r.mu.Unlock()
	r.admission.release(released)
}

// finishLocked 返回本次状态转换中新到达终态的任务数（调用方据此归还准入令牌）。
func (r *Runtime) finishLocked(ts *taskState, status Status, err error, dur time.Duration, attempts int) int {
	if terminal(ts.status) {
		return 0
	}
	ts.status = status
	ts.err = err
	ts.attempts = attempts
	ts.durNanos = dur.Nanoseconds()
	released := 1
	switch status {
	case StatusCompleted:
		r.stats.completed.Add(1)
		r.stats.totalDur.Add(dur.Nanoseconds())
		for _, d := range r.dependents[ts.task.ID] {
			if d.status != StatusPending {
				continue
			}
			d.remain--
			if d.remain == 0 {
				r.ready <- d
			}
		}
	case StatusFailed:
		r.stats.failed.Add(1)
		r.stats.totalDur.Add(dur.Nanoseconds())
		released += r.skipDependentsLocked(ts.task.ID)
		if r.policy == FailFast {
			r.cancel()
			released += r.cancelPendingLocked()
		}
	case StatusCanceled:
		r.stats.canceled.Add(1)
		r.stats.totalDur.Add(dur.Nanoseconds())
		released += r.skipDependentsLocked(ts.task.ID)
	case StatusSkipped:
		r.stats.skipped.Add(1)
		released += r.skipDependentsLocked(ts.task.ID)
	}
	r.pending--
	if r.pending == 0 {
		close(r.done)
	}
	if ts.batch != nil {
		ts.batch.remaining--
		if ts.batch.remaining == 0 {
			ts.batch.stopMerge()
			ts.batch.cancel()
		}
	}
	return released
}

// skipDependentsLocked 将依赖者的未完成任务递归置为跳过。
func (r *Runtime) skipDependentsLocked(id string) int {
	released := 0
	for _, d := range r.dependents[id] {
		if d.status != StatusPending {
			continue
		}
		released += r.finishLocked(d, StatusSkipped, &Error{
			Reason: ReasonDependencyFailed,
			TaskID: d.task.ID,
			Cause:  errDependencyTerminal,
		}, 0, 0)
	}
	return released
}

// cancelPendingLocked 将所有未派发任务置为取消（FailFast / Close 用）。
func (r *Runtime) cancelPendingLocked() int {
	released := 0
	for _, ts := range r.tasks {
		if ts.status != StatusPending {
			continue
		}
		released += r.finishLocked(ts, StatusCanceled, &Error{
			Reason: ReasonCanceled,
			TaskID: ts.task.ID,
			Cause:  context.Canceled,
		}, 0, 0)
	}
	return released
}

// Wait 阻塞直到所有已提交任务到达终态或 ctx 取消，返回按提交顺序的结果。
func (r *Runtime) Wait(ctx context.Context) []Result {
	r.mu.Lock()
	done := r.done
	r.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	results := make([]Result, 0, len(r.order))
	for _, id := range r.order {
		ts := r.tasks[id]
		status := ts.status
		if status == statusRunning || status == StatusPending {
			status = StatusPending
		}
		results = append(results, Result{
			TaskID:        id,
			Status:        status,
			Err:           ts.err,
			Attempts:      ts.attempts,
			DurationNanos: ts.durNanos,
		})
	}
	return results
}

// Close 关闭运行时：取消排队任务、等待在途任务结束并停止调度器。
// 关闭后再提交将被拒绝。
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	r.closed = true
	r.mu.Unlock()

	r.cancel()
	r.mu.Lock()
	released := r.cancelPendingLocked()
	r.mu.Unlock()
	r.admission.release(released)

	r.mu.Lock()
	done := r.done
	r.mu.Unlock()
	<-done

	r.stopOnce.Do(func() { close(r.stop) })
	r.wg.Wait()
	return nil
}

// Stats 返回当前统计快照。
func (r *Runtime) Stats() Stats { return r.stats.snapshot() }

func terminal(s Status) bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCanceled, StatusRejected, StatusSkipped:
		return true
	}
	return false
}
