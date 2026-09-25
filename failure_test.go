package bcr

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// FailFast：任一任务失败，在途收到取消，未启动任务不再执行。
func TestFailureFailFastAborts(t *testing.T) {
	var independentRan int32
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 1}
	rt := New(cfg)
	tasks := []*Task{
		{ID: "fails", Fn: func(ctx context.Context) error { return errBoom }},
		{ID: "later", Fn: func(ctx context.Context) error {
			atomic.StoreInt32(&independentRan, 1)
			return nil
		}},
	}
	st, err := rt.RunTasks(context.Background(), tasks)
	if err != nil {
		t.Fatal(err)
	}
	if independentRan != 0 {
		t.Fatal("later task must not run after failfast")
	}
	if st.Failed != 1 || st.Canceled != 1 || st.Completed != 0 {
		t.Fatalf("failed=%d canceled=%d completed=%d", st.Failed, st.Canceled, st.Completed)
	}
	f := st.ByTask["fails"]
	if f.Reason != ReasonTaskError || !errors.Is(f.Err, errBoom) {
		t.Fatalf("reason=%s err=%v", reasonString(f.Reason), f.Err)
	}
}

// Isolate：失败任务单独失败；无依赖关系的任务照常完成；
// 依赖它的任务（含传递依赖）标记 Skipped 且不执行。
func TestFailureIsolateSkipsDependents(t *testing.T) {
	var indRan, downRan, farRan int32
	cfg := NewConfig()
	cfg.FailurePolicy = Isolate
	cfg.Resources = ResourceLimits{ResourceSlots: 4}
	rt := New(cfg)
	tasks := []*Task{
		{ID: "bad", Fn: func(ctx context.Context) error { return errBoom }},
		{ID: "ind", Fn: func(ctx context.Context) error { atomic.StoreInt32(&indRan, 1); return nil }},
		{ID: "down", Deps: []string{"bad"}, Fn: func(ctx context.Context) error {
			atomic.StoreInt32(&downRan, 1)
			return nil
		}},
		{ID: "far", Deps: []string{"down"}, Fn: func(ctx context.Context) error {
			atomic.StoreInt32(&farRan, 1)
			return nil
		}},
	}
	st, err := rt.RunTasks(context.Background(), tasks)
	if err != nil {
		t.Fatal(err)
	}
	if indRan != 1 || downRan != 0 || farRan != 0 {
		t.Fatalf("ran ind=%d down=%d far=%d", indRan, downRan, farRan)
	}
	if st.Failed != 1 || st.Skipped != 2 || st.Completed != 1 {
		t.Fatalf("failed=%d skipped=%d completed=%d", st.Failed, st.Skipped, st.Completed)
	}
	for _, id := range []string{"down", "far"} {
		r := st.ByTask[id]
		if r.Status != StatusSkipped || r.Reason != ReasonDependencyFailed {
			t.Fatalf("%s status=%s reason=%s", id, statusString(r.Status), reasonString(r.Reason))
		}
	}
}

// 有限重试：前几次失败、最终成功；Attempts 与 RetryAttempts 口径正确，
// 退避时长被遵守。
func TestRetryThenSuccess(t *testing.T) {
	var tries int32
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 3,
		Backoff:     ExponentialBackoff{Base: 20 * time.Millisecond, Factor: 2},
		Decider:     DefaultRetryDecider,
	}
	rt := New(cfg)
	start := time.Now()
	st, err := rt.RunTasks(context.Background(), []*Task{{
		ID: "r",
		Fn: func(ctx context.Context) error {
			n := atomic.AddInt32(&tries, 1)
			if n < 3 {
				return errBoom
			}
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if tries != 3 {
		t.Fatalf("tries=%d", tries)
	}
	if st.Completed != 1 || st.Failed != 0 || st.RetryAttempts != 2 {
		t.Fatalf("%+v", st)
	}
	// 退避至少 Base(20ms) + Base*Factor(40ms) = 60ms
	if time.Since(start) < 55*time.Millisecond {
		t.Fatal("backoff was not observed")
	}
}

// 重试耗尽：最终失败，原因为 RetryExhausted，但最后一次原始错误
// 经 Unwrap/errors.Is 仍可判定，且不重复计数。
func TestRetryExhaustedPreservesCause(t *testing.T) {
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 2,
		Backoff:     ExponentialBackoff{Base: time.Millisecond, Factor: 2},
		Decider:     DefaultRetryDecider,
	}
	rt := New(cfg)
	st, err := rt.RunTasks(context.Background(), []*Task{{
		ID: "r",
		Fn: func(ctx context.Context) error { return errBoom },
	}})
	if err != nil {
		t.Fatal(err)
	}
	r := st.ByTask["r"]
	if r.Status != StatusFailed || r.Reason != ReasonRetryExhausted || r.Attempts != 2 {
		t.Fatalf("status=%s reason=%s attempts=%d",
			statusString(r.Status), reasonString(r.Reason), r.Attempts)
	}
	if !errors.Is(r.Err, errBoom) {
		t.Fatalf("original cause lost: %v", r.Err)
	}
	if st.Failed != 1 || st.RetryAttempts != 1 {
		t.Fatalf("failed=%d retries=%d (must not double count)", st.Failed, st.RetryAttempts)
	}
}

// Decider 返回 false 时即使还有次数也不重试。
func TestRetryDeciderVeto(t *testing.T) {
	var tries int32
	nonRetryable := fmt.Errorf("permanent")
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 5,
		Decider: func(attempt int, err error) bool {
			return !errors.Is(err, nonRetryable)
		},
	}
	rt := New(cfg)
	st, _ := rt.RunTasks(context.Background(), []*Task{{
		ID: "r",
		Fn: func(ctx context.Context) error {
			atomic.AddInt32(&tries, 1)
			return nonRetryable
		},
	}})
	if tries != 1 || st.Failed != 1 || st.RetryAttempts != 0 {
		t.Fatalf("tries=%d failed=%d retries=%d", tries, st.Failed, st.RetryAttempts)
	}
}

// 取消类错误不参与重试：即使 MaxAttempts>1，取消即终态取消。
func TestCanceledIsNotRetried(t *testing.T) {
	var tries int32
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{MaxAttempts: 5, Decider: DefaultRetryDecider}
	ctx, cancel := context.WithCancel(context.Background())
	rt := New(cfg)
	done := make(chan struct{})
	go func() {
		rt.RunTasks(ctx, []*Task{{ID: "r", Fn: func(ctx context.Context) error {
			atomic.AddInt32(&tries, 1)
			<-ctx.Done()
			return ctx.Err()
		}}})
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
	st := rt.Stats()
	if tries != 1 || st.Canceled != 1 || st.RetryAttempts != 0 {
		t.Fatalf("tries=%d canceled=%d retries=%d", tries, st.Canceled, st.RetryAttempts)
	}
}

// panic 被捕获归类，可按策略重试，恢复后成功。
func TestPanicRecoveredAndClassified(t *testing.T) {
	var tries int32
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 3,
		Backoff:     ExponentialBackoff{Base: time.Millisecond, Factor: 2},
		Decider:     DefaultRetryDecider,
	}
	rt := New(cfg)
	st, _ := rt.RunTasks(context.Background(), []*Task{{
		ID: "p",
		Fn: func(ctx context.Context) error {
			if atomic.AddInt32(&tries, 1) < 3 {
				panic("kaboom")
			}
			return nil
		},
	}})
	if tries != 3 || st.Completed != 1 || st.RetryAttempts != 2 {
		t.Fatalf("tries=%d %+v", tries, st)
	}
}

// 零退避重试不得挂死（回归：到期的重试唤醒必须被事件循环处理）。
func TestZeroBackoffRetryDoesNotHang(t *testing.T) {
	var tries int32
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{MaxAttempts: 3, Decider: DefaultRetryDecider}
	rt := New(cfg)
	done := make(chan Stats, 1)
	go func() {
		st, _ := rt.RunTasks(context.Background(), []*Task{{
			ID: "z",
			Fn: func(ctx context.Context) error {
				if atomic.AddInt32(&tries, 1) < 3 {
					return errBoom
				}
				return nil
			},
		}})
		done <- st
	}()
	select {
	case st := <-done:
		if tries != 3 || st.Completed != 1 || st.RetryAttempts != 2 {
			t.Fatalf("tries=%d %+v", tries, st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("zero-backoff retry hung")
	}
}

// Isolate 边界：F 失败瞬间 M 已在运行（让 M 跑完），但 M 的下游 G
// 传递依赖 F，必须 Skipped，而不是被 M 的成功错误放行。
func TestIsolateRunningMiddleStillSkipsDownstream(t *testing.T) {
	mRunning := make(chan struct{})
	releaseM := make(chan struct{})
	var gRan int32
	cfg := NewConfig()
	cfg.FailurePolicy = Isolate
	cfg.Resources = ResourceLimits{ResourceSlots: 4}
	rt := New(cfg)
	tasks := []*Task{
		{ID: "f", Fn: func(ctx context.Context) error { return errBoom }},
		{ID: "m", Fn: func(ctx context.Context) error {
			close(mRunning)
			<-releaseM
			return nil // 即使 M 成功，G 仍必须跳过
		}},
		{ID: "g", Deps: []string{"m", "f"}, Fn: func(ctx context.Context) error {
			atomic.StoreInt32(&gRan, 1)
			return nil
		}},
	}
	done := make(chan struct{})
	go func() { rt.RunTasks(context.Background(), tasks); close(done) }()
	<-mRunning
	// 等 f 完成失败、skip 传播发生，再放行 m。
	time.Sleep(50 * time.Millisecond)
	close(releaseM)
	<-done
	st := rt.Stats()
	if gRan != 0 {
		t.Fatal("g must never run")
	}
	if st.ByTask["g"].Status != StatusSkipped {
		t.Fatalf("g status=%s", statusString(st.ByTask["g"].Status))
	}
	if st.ByTask["m"].Status != StatusSucceeded {
		t.Fatalf("m should finish running and succeed, got %s",
			statusString(st.ByTask["m"].Status))
	}
	if st.Failed != 1 || st.Skipped != 1 || st.Completed != 1 {
		t.Fatalf("failed=%d skipped=%d completed=%d", st.Failed, st.Skipped, st.Completed)
	}
}
