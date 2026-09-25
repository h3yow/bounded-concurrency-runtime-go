package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// TestIsolateFailure 验证失败隔离：失败任务的依赖者被跳过，其余任务继续。
func TestIsolateFailure(t *testing.T) {
	rt := New(WithFailurePolicy(Isolate), WithQueueCapacity(16))
	defer rt.Close()

	mk := func(id string, run TaskFunc, deps ...string) Task {
		return Task{ID: id, Deps: deps, Run: run}
	}
	ok := func(context.Context) error { return nil }
	fail := func(context.Context) error { return errBoom }

	err := rt.Submit(context.Background(),
		mk("bad", fail),
		mk("child", ok, "bad"),
		mk("grandchild", ok, "child"),
		mk("independent", ok),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())

	want := map[string]Status{
		"bad":         StatusFailed,
		"child":       StatusSkipped,
		"grandchild":  StatusSkipped,
		"independent": StatusCompleted,
	}
	t.Logf("判定依据: Isolate 策略, bad 失败 -> child/grandchild 跳过, independent 不受影响")
	for _, res := range results {
		t.Logf("任务 %s -> %v (err=%v)", res.TaskID, res.Status, res.Err)
		if res.Status != want[res.TaskID] {
			t.Fatalf("任务 %s 期望 %v, 实际 %v", res.TaskID, want[res.TaskID], res.Status)
		}
		if res.Status == StatusSkipped && ReasonOf(res.Err) != ReasonDependencyFailed {
			t.Fatalf("任务 %s 跳过原因期望 ReasonDependencyFailed, 实际 %v", res.TaskID, ReasonOf(res.Err))
		}
		if res.TaskID == "bad" {
			if !errors.Is(res.Err, errBoom) {
				t.Fatalf("原始原因未保留: %v", res.Err)
			}
			if ReasonOf(res.Err) != ReasonTaskFailed {
				t.Fatalf("期望 ReasonTaskFailed, 实际 %v", ReasonOf(res.Err))
			}
		}
	}
	stats := rt.Stats()
	t.Logf("统计: %+v", stats)
	if stats.Failed != 1 || stats.Skipped != 2 || stats.Completed != 1 {
		t.Fatalf("统计口径错误: %+v", stats)
	}
}

// TestFailFast 验证整体终止：任一失败后未完成任务被取消。
func TestFailFast(t *testing.T) {
	rt := New(WithFailurePolicy(FailFast), WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(16))
	defer rt.Close()

	slowStarted := make(chan struct{}, 1)
	mk := func(id string, run TaskFunc, deps ...string) Task {
		return Task{ID: id, Resource: "cpu", Deps: deps, Run: run}
	}
	err := rt.Submit(context.Background(),
		mk("bad", func(context.Context) error { return errBoom }),
		mk("slow", func(ctx context.Context) error {
			slowStarted <- struct{}{}
			<-ctx.Done() // 响应取消
			return ctx.Err()
		}, "bad"),
		mk("queued", func(context.Context) error { return nil }, "slow"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())

	t.Logf("判定依据: FailFast 策略, bad 失败 -> slow 收到取消, queued 不得启动")
	for _, res := range results {
		t.Logf("任务 %s -> %v (err=%v)", res.TaskID, res.Status, res.Err)
	}
	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
	}
	if byID["bad"].Status != StatusFailed {
		t.Fatalf("bad 期望 Failed, 实际 %v", byID["bad"].Status)
	}
	if byID["queued"].Status == StatusCompleted {
		t.Fatal("queued 不应被执行")
	}
	stats := rt.Stats()
	t.Logf("统计: %+v", stats)
	if stats.Completed > 0 {
		t.Fatalf("FailFast 下不应有任务完成: %+v", stats)
	}
	if stats.Terminal() != 3 {
		t.Fatalf("终态总数应为 3: %+v", stats)
	}
}

// TestRetryWithBackoff 验证有限重试、退避与可重试判定。
func TestRetryWithBackoff(t *testing.T) {
	rt := New(
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 20 * time.Millisecond, MaxDelay: 50 * time.Millisecond}),
		WithQueueCapacity(16),
	)
	defer rt.Close()

	var calls atomic.Int32
	var times []time.Time
	start := time.Now()
	err := rt.Submit(context.Background(), Task{
		ID: "flaky",
		Run: func(context.Context) error {
			times = append(times, time.Now())
			if calls.Add(1) < 3 {
				return errBoom
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())

	res := results[0]
	t.Logf("判定依据: MaxAttempts=3, 前 2 次失败可重试 -> 期望第 3 次成功, attempts=%d", res.Attempts)
	if res.Status != StatusCompleted || res.Attempts != 3 {
		t.Fatalf("期望第 3 次尝试成功, 实际 status=%v attempts=%d", res.Status, res.Attempts)
	}
	if len(times) == 3 {
		gap1, gap2 := times[1].Sub(times[0]), times[2].Sub(times[1])
		t.Logf("退避间隔: 第1次=%v 第2次=%v (BaseDelay=20ms, 期望指数增长)", gap1, gap2)
		if gap1 < 15*time.Millisecond || gap2 < gap1 {
			t.Fatalf("退避不符合指数增长: gap1=%v gap2=%v", gap1, gap2)
		}
	}
	if got := rt.Stats().Retried; got != 2 {
		t.Fatalf("Retried 期望 2, 实际 %d", got)
	}
	_ = start
}

// TestRetryExhausted 验证重试次数耗尽后记为失败且只计一次。
func TestRetryExhausted(t *testing.T) {
	rt := New(
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond}),
		WithQueueCapacity(16),
	)
	defer rt.Close()

	var calls atomic.Int32
	err := rt.Submit(context.Background(), Task{
		ID:  "always-fail",
		Run: func(context.Context) error { calls.Add(1); return errBoom },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	res := results[0]
	t.Logf("判定依据: MaxAttempts=2 且始终失败 -> Failed, attempts=%d calls=%d", res.Attempts, calls.Load())
	if res.Status != StatusFailed || res.Attempts != 2 || calls.Load() != 2 {
		t.Fatalf("期望 2 次尝试后失败, 实际 status=%v attempts=%d calls=%d", res.Status, res.Attempts, calls.Load())
	}
	stats := rt.Stats()
	if stats.Failed != 1 || stats.Retried != 1 {
		t.Fatalf("失败不得重复计数: %+v", stats)
	}
}

// TestCancelNotRetryable 验证取消类错误不可重试。
func TestCancelNotRetryable(t *testing.T) {
	rt := New(
		WithRetryPolicy(RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond}),
		WithQueueCapacity(16),
	)
	defer rt.Close()

	var calls atomic.Int32
	err := rt.Submit(context.Background(), Task{
		ID:  "cancel-task",
		Run: func(context.Context) error { calls.Add(1); return context.Canceled },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	res := results[0]
	t.Logf("判定依据: 任务返回 context.Canceled -> 不可重试, 期望 Canceled 且仅 1 次调用, 实际 calls=%d", calls.Load())
	if res.Status != StatusCanceled || calls.Load() != 1 {
		t.Fatalf("期望取消不重试, 实际 status=%v calls=%d", res.Status, calls.Load())
	}
	if got := rt.Stats().Retried; got != 0 {
		t.Fatalf("取消不应计入重试, Retried=%d", got)
	}
}
