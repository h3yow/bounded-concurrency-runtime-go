package bcr

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// timerKind 标识调度器内部定时器到期的语义。
type timerKind int

const (
	timerAcquire timerKind = iota // 等待资源额度超时
	timerRetry                    // 退避结束，可以重新排队
)

// timerEvent 是定时器到期事件；若任务状态已迁移则视为过期事件忽略。
type timerEvent struct {
	taskID string
	kind   timerKind
}

// attemptResult 是一次尝试（worker goroutine）的回报。
type attemptResult struct {
	taskID  string
	attempt int
	runDur  time.Duration
	err     error // nil 表示成功
}

// scheduler 持有全部调度可变状态，只在其事件循环 goroutine 内访问，
// 因此所有判定无需加锁，天然无数据竞争。
type scheduler struct {
	rt     *Runtime
	dag    *dag
	pool   *resourcePool
	log    Logger
	stats  *statsCollector
	runCtx context.Context
	cancel context.CancelFunc

	st      map[string]*taskState
	ready   []string // 已就绪、等待资源的任务 ID（有限集合：≤已接纳任务总数）
	done    int
	total   int
	aborted bool

	results    chan attemptResult
	ticks      chan timerEvent
	retryUntil map[string]time.Time

	// 单一可复用唤醒定时器：只有最近截止时间发生变化时才重建，
	// 避免事件循环每轮重复堆积 AfterFunc。
	wakeTimer *time.Timer
	wakeDL    *deadline
}

type taskState struct {
	task              *Task
	status            Status
	seq               int
	indegree          int // 尚未成功的前置数
	attempts          int // 实际启动执行的次数
	weights           ResourceWeights
	waitStart         time.Time
	waitDur           time.Duration
	startTime         time.Time
	endTime           time.Time
	runDur            time.Duration
	reason            FailureReason
	lastErr           error
	firstWaitRecorded bool
}

func newScheduler(rt *Runtime, d *dag, runCtx context.Context, cancel context.CancelFunc) *scheduler {
	return &scheduler{
		rt:         rt,
		dag:        d,
		pool:       newResourcePool(rt.cfg.Resources),
		log:        rt.log,
		stats:      rt.stats,
		runCtx:     runCtx,
		cancel:     cancel,
		st:         make(map[string]*taskState, len(d.nodes)),
		results:    make(chan attemptResult, len(d.nodes)+1),
		ticks:      make(chan timerEvent, len(d.nodes)+1),
		retryUntil: make(map[string]time.Time),
	}
}

// run 是调度器事件循环：处理尝试回报、定时器到期与外部取消，
// 每次状态变化后尝试分派资源，直到全部任务到达终态。
func (s *scheduler) run() Stats {
	// 初始化状态表：入度来自已校验的依赖图。
	for _, id := range s.dag.order {
		n := s.dag.nodes[id]
		s.st[id] = &taskState{
			task:     n.task,
			status:   StatusWaitingDeps,
			seq:      n.seq,
			indegree: n.indegree,
			weights:  n.task.effectiveResources(),
		}
		s.total++
	}
	for _, id := range s.dag.order {
		if s.st[id].indegree == 0 {
			s.enqueueReady(id)
		}
	}
	s.dispatch()

	for s.done < s.total {
		// 维护单一唤醒定时器：最近截止时间变化时才重建。
		dl := s.nearestTimer()
		if !sameWake(s.wakeDL, dl) {
			if s.wakeTimer != nil {
				s.wakeTimer.Stop()
			}
			s.wakeDL = dl
			if dl != nil {
				wait := time.Until(dl.deadline)
				if wait < 0 {
					wait = 0
				}
				s.wakeTimer = time.AfterFunc(wait, func() {
					s.ticks <- timerEvent{taskID: dl.taskID, kind: dl.kind}
				})
			}
		}

		select {
		case res := <-s.results:
			s.handleResult(res)
		case ev := <-s.ticks:
			// 过期事件（任务状态已迁移）在 handler 内按状态校验忽略。
			s.handleTick(ev)
		case <-s.runCtx.Done():
			s.handleCancel()
		}
		s.dispatch()
	}
	if s.wakeTimer != nil {
		s.wakeTimer.Stop()
	}
	s.cancel()
	return s.stats.snapshot()
}

// sameWake 判定唤醒目标（任务+定时器类型+截止时刻）是否未变化。
func sameWake(a, b *deadline) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.taskID == b.taskID && a.kind == b.kind && a.deadline.Equal(b.deadline)
}

type deadline struct {
	taskID   string
	kind     timerKind
	deadline time.Time
}

// nearestTimer 返回当前最早到期的等待资源超时或退避截止时间。
func (s *scheduler) nearestTimer() *deadline {
	var best *deadline
	for id, st := range s.st {
		switch st.status {
		case StatusQueued:
			if s.rt.cfg.AcquireTimeout > 0 {
				dl := st.waitStart.Add(s.rt.cfg.AcquireTimeout)
				if best == nil || dl.Before(best.deadline) {
					best = &deadline{taskID: id, kind: timerAcquire, deadline: dl}
				}
			}
		}
	}
	for id, until := range s.retryUntil {
		// 已到期（含零退避）也必须包含：调用方会以 AfterFunc(0)
		// 立即唤醒，否则事件循环会无事件可等而挂死。
		if best == nil || until.Before(best.deadline) {
			best = &deadline{taskID: id, kind: timerRetry, deadline: until}
		}
	}
	return best
}

// enqueueReady 将任务放入等待资源的就绪队列，并记录首次等待起点。
func (s *scheduler) enqueueReady(id string) {
	st := s.st[id]
	st.status = StatusQueued
	if st.waitStart.IsZero() {
		st.waitStart = time.Now()
	}
	s.ready = append(s.ready, id)
}

// dispatch 在已就绪任务中按"优先级高者优先、同优先级 FIFO"挑选资源
// 可满足的任务启动。允许跳过队头暂时不可满足的任务（队头不会被永久
// 饿死：一次 Run 的任务集是封闭有限集，且每个任务在登记时已验证
// needs≤capacity，资源池排空时它必然可被满足）。
func (s *scheduler) dispatch() {
	if s.aborted || s.runCtx.Err() != nil {
		return
	}
	for len(s.ready) > 0 {
		// 一轮只做一次真实扣减：先在不改动额度的前提下找出
		// "资源可满足的最优候选"（优先级最高，同优先级 FIFO），
		// 再对它做唯一一次 tryAcquire，杜绝试探性重复扣减。
		idx := -1
		for i, id := range s.ready {
			if !s.canAcquire(s.st[id].weights) {
				continue
			}
			if idx == -1 {
				idx = i
				continue
			}
			cand, best := s.st[id], s.st[s.ready[idx]]
			if cand.task.Priority > best.task.Priority ||
				(cand.task.Priority == best.task.Priority && cand.seq < best.seq) {
				idx = i
			}
		}
		if idx == -1 {
			return // 当前无任何任务可被满足，等待事件后再试
		}
		id := s.ready[idx]
		s.ready = append(s.ready[:idx], s.ready[idx+1:]...)
		if !s.pool.tryAcquire(s.st[id].weights) {
			// 与 canAcquire 判定竞态不可能发生（同 goroutine），
			// 保守处理：放回队首等待下次分派。
			s.ready = append([]string{id}, s.ready...)
			return
		}
		s.startAttempt(id)
	}
}

// canAcquire 是只读判定：现有可用额度是否足以覆盖需求，不扣减。
func (s *scheduler) canAcquire(w ResourceWeights) bool {
	for name, need := range w {
		if need > s.pool.available[name] {
			return false
		}
	}
	return true
}

// startAttempt 在独立 goroutine 中启动一次尝试。等待/退避中的任务
// 从不持有 goroutine；只有真正执行的尝试占用一个 worker goroutine，
// 其数量受 slots 等资源额度严格约束。
func (s *scheduler) startAttempt(id string) {
	st := s.st[id]
	now := time.Now()
	if !st.firstWaitRecorded {
		st.waitDur = now.Sub(st.waitStart)
		st.firstWaitRecorded = true
		st.startTime = now
	}
	st.attempts++
	attempt := st.attempts
	st.status = StatusRunning

	actx := s.runCtx
	var cancel context.CancelFunc = func() {}
	if st.task.Timeout > 0 {
		actx, cancel = context.WithTimeout(s.runCtx, st.task.Timeout)
	}
	fn := st.task.Fn
	taskID := st.task.ID
	weights := st.weights

	s.log.Logf("launch task=%s attempt=%d/%d resources=%v available=%v",
		taskID, attempt, s.rt.cfg.effectiveMaxAttempts(st.task), weights, s.pool.snapshot())

	go func() {
		defer cancel()
		start := time.Now()
		err := runOnce(actx, fn, taskID, attempt)
		res := attemptResult{
			taskID:  taskID,
			attempt: attempt,
			runDur:  time.Since(start),
			err:     err,
		}
		// 结果通道按任务总数预留容量，投递不会阻塞；再以 runCtx
		// 兜底，保证 worker goroutine 不会因调度器退出而泄漏。
		select {
		case s.results <- res:
		case <-s.runCtx.Done():
			select {
			case s.results <- res:
			default:
			}
		}
	}()
}

// runOnce 执行用户函数并把结果归类为 *TaskError（成功返回 nil）。
// panic 在此处被捕获并保留原始值，绝不允许击穿 worker goroutine。
func runOnce(ctx context.Context, fn Func, taskID string, attempt int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &TaskError{
				TaskID: taskID, Reason: ReasonPanic, Attempt: attempt, Op: "run",
				Err: fmt.Errorf("%v", r),
			}
		}
	}()
	ferr := fn(ctx)
	if ferr == nil {
		return nil
	}
	reason := ReasonTaskError
	if cr := classifyContextError(ferr); cr != ReasonUnknown {
		reason = cr
	} else if ctx.Err() != nil {
		// 函数未透传 context 错误，但尝试 context 已到期/取消。
		if classifyContextError(ctx.Err()) == ReasonContextDeadline {
			reason = ReasonContextDeadline
		} else {
			reason = ReasonContextCanceled
		}
	}
	return &TaskError{TaskID: taskID, Reason: reason, Attempt: attempt, Op: "run", Err: ferr}
}

// handleResult 处理一次尝试回报，是唯一的成功/失败分类入口。
func (s *scheduler) handleResult(res attemptResult) {
	st := s.st[res.taskID]
	if isTerminal(st.status) {
		// 终态后到达的回报（理论上不会发生）：释放额度后忽略，
		// 防止额度泄漏。
		s.pool.release(st.weights)
		return
	}
	st.runDur += res.runDur

	if res.err == nil {
		s.pool.release(st.weights)
		s.succeed(res.taskID)
		return
	}

	te := asTaskError(res.err)
	st.lastErr = te

	// 取消类错误：无论失败策略如何，归类为 Canceled（任务确实观测到
	// 取消）。该判定只依赖任务自身返回值，不依赖调度时序，因此不会
	// 因并发度不同而改变归类。
	if te.Reason == ReasonContextCanceled || te.Reason == ReasonContextDeadline {
		s.pool.release(st.weights)
		s.finalize(res.taskID, StatusCanceled, te.Reason, te)
		return
	}

	// 判定是否可有限重试。
	maxAttempts := s.rt.cfg.effectiveMaxAttempts(st.task)
	decider := s.rt.cfg.effectiveDecider(st.task)
	canRetry := st.attempts < maxAttempts && decider(st.attempts, te)
	s.log.Logf("attempt-end task=%s attempt=%d/%d reason=%s retry=%v err=%v",
		res.taskID, st.attempts, maxAttempts, reasonString(te.Reason), canRetry, te.Err)

	if canRetry {
		// 退避期间释放全部额度：等待重试不占用资源，避免饥饿/死锁。
		s.pool.release(st.weights)
		st.status = StatusRetryWait
		d := time.Duration(0)
		if b := s.backoffFor(st.task); b != nil {
			d = b.Next(st.attempts)
		}
		until := time.Now().Add(d)
		s.setRetryUntil(res.taskID, until)
		s.log.Logf("retry-wait task=%s next_attempt=%d backoff=%s",
			res.taskID, st.attempts+1, d)
		return
	}

	s.pool.release(st.weights)
	finalReason := te.Reason
	finalErr := error(te)
	if st.attempts >= maxAttempts && maxAttempts > 1 &&
		(te.Reason == ReasonTaskError || te.Reason == ReasonPanic) {
		// 重试次数耗尽：外层标注 RetryExhausted，原始原因经 Unwrap 保留。
		finalReason = ReasonRetryExhausted
		finalErr = &TaskError{TaskID: res.taskID, Reason: ReasonRetryExhausted,
			Attempt: st.attempts, Op: "retry-exhausted", Err: te}
	}
	s.finalize(res.taskID, StatusFailed, finalReason, finalErr)
}

// succeed 把任务置为成功终态，并向下游传播就绪信号。
func (s *scheduler) succeed(id string) {
	s.finalize(id, StatusSucceeded, ReasonUnknown, nil)
	for _, down := range s.dag.nodes[id].dependents {
		dst := s.st[down]
		if isTerminal(dst.status) || dst.status == StatusQueued ||
			dst.status == StatusRunning || dst.status == StatusRetryWait {
			continue
		}
		dst.indegree--
		if dst.indegree == 0 {
			s.enqueueReady(down)
		}
	}
}

// finalize 冻结任务终态并写入统计；幂等。
func (s *scheduler) finalize(id string, status Status, reason FailureReason, err error) {
	st := s.st[id]
	if isTerminal(st.status) {
		return
	}
	st.status = status
	st.reason = reason
	st.lastErr = err
	st.endTime = time.Now()
	s.stats.record(TaskResult{
		TaskID:    id,
		Status:    status,
		Attempts:  st.attempts,
		Reason:    reason,
		Err:       err,
		StartTime: st.startTime,
		EndTime:   st.endTime,
		WaitStart: st.waitStart,
		WaitDur:   st.waitDur,
		RunDur:    st.runDur,
	})
	s.done++
	s.log.Logf("terminal task=%s status=%s reason=%s attempts=%d",
		id, statusString(status), reasonString(reason), st.attempts)

	if status == StatusFailed {
		switch s.rt.cfg.FailurePolicy {
		case FailFast:
			if !s.aborted {
				s.aborted = true
				s.log.Logf("failure-policy=failfast trigger=%s -> cancel all non-terminal tasks", id)
				s.cancel()
			}
		case Isolate:
			s.log.Logf("failure-policy=isolate trigger=%s -> skip dependents", id)
			s.skipDependents(id)
		}
	}
}

// skipDependents 在失败隔离策略下，把失败/跳过任务的全部下游
// （传递闭包）标记为 Skipped，且不再启动。
func (s *scheduler) skipDependents(id string) {
	queue := append([]string(nil), s.dag.nodes[id].dependents...)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		st := s.st[cur]
		if st.status == StatusRunning {
			// 已在运行：让它跑完（其结果保留），但它的下游此刻必然
			// 仍是 WaitingDeps（本任务尚未完成），必须继续向下传播
			// 跳过——否则它"侥幸成功"会错误放行传递依赖了失败任务
			// 的下游。待它回报成功时，下游已是终态，传播被幂等忽略。
			queue = append(queue, s.dag.nodes[cur].dependents...)
			continue
		}
		if isTerminal(st.status) || st.status == StatusQueued || st.status == StatusRetryWait {
			// 已排队/退避：前置失败已意味着它不可能成功，直接跳过。
			if st.status == StatusQueued || st.status == StatusRetryWait {
				s.removeReady(cur)
				s.clearRetryUntil(cur)
				s.finalize(cur, StatusSkipped, ReasonDependencyFailed,
					&TaskError{TaskID: cur, Reason: ReasonDependencyFailed, Op: "skip",
						Err: fmtErr("upstream task %q did not succeed", id)})
				queue = append(queue, s.dag.nodes[cur].dependents...)
			}
			continue
		}
		if st.status == StatusWaitingDeps {
			s.finalize(cur, StatusSkipped, ReasonDependencyFailed,
				&TaskError{TaskID: cur, Reason: ReasonDependencyFailed, Op: "skip",
					Err: fmtErr("upstream task %q did not succeed", id)})
			queue = append(queue, s.dag.nodes[cur].dependents...)
		}
	}
}

// handleTick 处理定时器事件（过期事件按当前状态校验后忽略）。
func (s *scheduler) handleTick(ev timerEvent) {
	st := s.st[ev.taskID]
	switch ev.kind {
	case timerAcquire:
		if st.status != StatusQueued {
			return
		}
		s.removeReady(ev.taskID)
		err := &TaskError{TaskID: ev.taskID, Reason: ReasonResourceTimeout,
			Attempt: st.attempts, Op: "acquire", Err: ErrResourceTimeout}
		st.lastErr = err
		s.log.Logf("resource-timeout task=%s waited=%s resources=%v",
			ev.taskID, s.rt.cfg.AcquireTimeout, st.weights)
		// 不增加 attempts（函数体未执行）；仍走可重试判定流程。
		decider := s.rt.cfg.effectiveDecider(st.task)
		maxAttempts := s.rt.cfg.effectiveMaxAttempts(st.task)
		if decider(st.attempts, err) && st.attempts < maxAttempts {
			st.status = StatusRetryWait
			d := time.Duration(0)
			if b := s.backoffFor(st.task); b != nil {
				d = b.Next(st.attempts + 1)
			}
			s.setRetryUntil(ev.taskID, time.Now().Add(d))
		} else {
			s.finalize(ev.taskID, StatusFailed, ReasonResourceTimeout, err)
		}
	case timerRetry:
		if st.status != StatusRetryWait {
			return
		}
		s.clearRetryUntil(ev.taskID)
		s.enqueueReady(ev.taskID)
	}
}

// handleCancel 在外部 context 取消或 FailFast 级联取消时，
// 立即终止所有未启动/等待中的任务；在途任务通过 context 传播取消，
// 已取消的任务不会再被启动。
func (s *scheduler) handleCancel() {
	reason := classifyContextError(s.runCtx.Err())
	if reason == ReasonUnknown {
		reason = ReasonContextCanceled
	}
	// FailFast 触发的取消：未启动任务标注 Aborted，区别于外部取消。
	policyAbort := s.aborted
	for id, st := range s.st {
		if isTerminal(st.status) {
			continue
		}
		switch st.status {
		case StatusQueued, StatusWaitingDeps, StatusRetryWait:
			s.removeReady(id)
			s.clearRetryUntil(id)
			r := reason
			if policyAbort {
				r = ReasonAborted
			}
			s.finalize(id, StatusCanceled, r,
				&TaskError{TaskID: id, Reason: r, Attempt: st.attempts, Op: "cancel",
					Err: s.runCtx.Err()})
		case StatusRunning:
			// 在途：取消信号已通过 runCtx 传播，等待其回报后归类。
		}
	}
}

func (s *scheduler) removeReady(id string) {
	for i, rid := range s.ready {
		if rid == id {
			s.ready = append(s.ready[:i], s.ready[i+1:]...)
			return
		}
	}
}

func isTerminal(st Status) bool {
	switch st {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusRejected, StatusSkipped:
		return true
	}
	return false
}

func asTaskError(err error) *TaskError {
	var te *TaskError
	if errors.As(err, &te) {
		return te
	}
	return &TaskError{Reason: ReasonTaskError, Op: "run", Err: err}
}

// backoffFor 返回任务生效的退避器（任务级 Backoff 暂不单独配置，
// 统一使用运行时默认重试策略中的 Backoff）；nil 表示不退避。
func (s *scheduler) backoffFor(t *Task) Backoff {
	return s.rt.cfg.DefaultRetry.Backoff
}

func (s *scheduler) setRetryUntil(id string, until time.Time) {
	s.retryUntil[id] = until
}

func (s *scheduler) clearRetryUntil(id string) {
	delete(s.retryUntil, id)
}
