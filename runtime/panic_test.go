package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// explode 是命名函数，便于验证崩溃栈能定位到现场。
func explode(ctx context.Context) error { panic("plugin exploded") }

// TestPanicIsolated 验证隔离模式下崩溃只影响自身下游，不波及其它任务与进程。
func TestPanicIsolated(t *testing.T) {
	rt := New(
		WithQuotas(map[string]int{"cpu": 2}),
		WithQueueCapacity(8),
		WithFailurePolicy(Isolate),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond}),
	)
	defer rt.Close()

	var otherRan atomic.Int64
	err := rt.Submit(context.Background(),
		Task{ID: "bad", Resource: "cpu", Retries: 2, Run: explode},
		Task{ID: "child", Resource: "cpu", Deps: []string{"bad"}, Run: func(ctx context.Context) error { return nil }},
		Task{ID: "ok1", Resource: "cpu", Run: func(ctx context.Context) error { otherRan.Add(1); return nil }},
		Task{ID: "ok2", Resource: "cpu", Run: func(ctx context.Context) error { otherRan.Add(1); return nil }},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())

	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
		t.Logf("任务 %s -> status=%v attempts=%d err=%v", res.TaskID, res.Status, res.Attempts, res.Err)
	}

	bad := byID["bad"]
	t.Logf("判定依据: bad 的 Run 直接 panic, 期望 StatusFailed + ReasonPanicked, 实际 status=%v", bad.Status)
	if bad.Status != StatusFailed {
		t.Fatalf("崩溃任务绝不能记成成功, 实际 %v", bad.Status)
	}
	if ReasonOf(bad.Err) != ReasonPanicked {
		t.Fatalf("期望 ReasonPanicked, 实际 %v (err=%v)", ReasonOf(bad.Err), bad.Err)
	}
	var pe *PanicError
	if !errors.As(bad.Err, &pe) {
		t.Fatalf("期望 Cause 为 *PanicError, 实际 %T", bad.Err)
	}
	t.Logf("崩溃现场: value=%v, stack 前 80 字节=%q", pe.Value, string(pe.Stack[:80]))
	if pe.Value != "plugin exploded" {
		t.Fatalf("panic 原始值丢失: %v", pe.Value)
	}
	if !strings.Contains(string(pe.Stack), "explode") {
		t.Fatalf("栈信息应能定位到崩溃现场(explode), 实际:\n%s", pe.Stack)
	}
	t.Logf("判定依据: 崩溃不可重试, 配置 MaxAttempts=3/Retries=2 下 attempts 期望 1, 实际 %d", bad.Attempts)
	if bad.Attempts != 1 {
		t.Fatalf("崩溃不应重试, attempts 期望 1, 实际 %d", bad.Attempts)
	}

	if got := byID["child"].Status; got != StatusSkipped {
		t.Fatalf("隔离模式下崩溃任务的下游应被跳过, 实际 %v", got)
	}
	if ReasonOf(byID["child"].Err) != ReasonDependencyFailed {
		t.Fatalf("下游错误分类期望 ReasonDependencyFailed, 实际 %v", ReasonOf(byID["child"].Err))
	}
	t.Logf("判定依据: 输入含 2 个无依赖正常任务, 隔离模式下应照常完成, 实际执行 %d 个", otherRan.Load())
	if otherRan.Load() != 2 {
		t.Fatalf("无关任务被崩溃波及, 期望执行 2 个, 实际 %d", otherRan.Load())
	}

	stats := rt.Stats()
	t.Logf("统计: %+v", stats)
	if stats.Panics != 1 {
		t.Fatalf("Panics 期望 1, 实际 %d", stats.Panics)
	}
	if stats.Completed != 2 || stats.Failed != 1 || stats.Skipped != 1 {
		t.Fatalf("终态口径错误: %+v", stats)
	}
	if stats.Terminal() != 4 {
		t.Fatalf("Terminal 期望 4, 实际 %d", stats.Terminal())
	}

	// 运行时仍然可用：崩溃后还能继续接受并执行新任务。
	if err := rt.Submit(context.Background(), Task{ID: "after", Resource: "cpu", Run: func(ctx context.Context) error { return nil }}); err != nil {
		t.Fatalf("崩溃后运行时不可用: %v", err)
	}
	for _, res := range rt.Wait(context.Background()) {
		if res.TaskID == "after" && res.Status != StatusCompleted {
			t.Fatalf("崩溃后新任务未正常完成: %v", res.Status)
		}
	}
}

// TestPanicFailFast 验证整体终止模式下崩溃正常触发收敛。
func TestPanicFailFast(t *testing.T) {
	rt := New(
		WithQuotas(map[string]int{"cpu": 2}),
		WithQueueCapacity(8),
		WithFailurePolicy(FailFast),
	)
	defer rt.Close()

	release := make(chan struct{})
	slowStarted := make(chan struct{}, 1)
	err := rt.Submit(context.Background(),
		Task{ID: "bad", Resource: "cpu", Run: func(ctx context.Context) error { panic("boom") }},
		Task{ID: "slow", Resource: "cpu", Run: func(ctx context.Context) error {
			slowStarted <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}},
		Task{ID: "queued", Resource: "cpu", Deps: []string{"slow"}, Run: func(ctx context.Context) error { return nil }},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	close(release)

	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
		t.Logf("任务 %s -> status=%v err=%v", res.TaskID, res.Status, res.Err)
	}
	t.Logf("判定依据: FailFast 下 bad 崩溃 -> 期望 bad=Failed(Panicked), 其余收敛为 Canceled/Skipped")
	if byID["bad"].Status != StatusFailed || ReasonOf(byID["bad"].Err) != ReasonPanicked {
		t.Fatalf("bad 期望 Failed+Panicked, 实际 %v (%v)", byID["bad"].Status, byID["bad"].Err)
	}
	for _, id := range []string{"slow", "queued"} {
		s := byID[id].Status
		if s != StatusCanceled && s != StatusSkipped {
			t.Fatalf("FailFast 下 %s 期望 Canceled/Skipped, 实际 %v", id, s)
		}
	}
	stats := rt.Stats()
	t.Logf("统计: %+v", stats)
	if stats.Panics != 1 || stats.Terminal() != 3 {
		t.Fatalf("统计口径错误: %+v", stats)
	}
}

// TestPanicConsistencyAcrossConcurrency 验证崩溃终态口径与并发度、容量无关。
func TestPanicConsistencyAcrossConcurrency(t *testing.T) {
	configs := []struct {
		quota    int
		queueCap int
	}{{1, 8}, {4, 16}, {16, 64}}
	type want struct{ completed, failed, skipped, panics int64 }
	expected := want{completed: 3, failed: 1, skipped: 1, panics: 1}

	for _, cfg := range configs {
		rt := New(
			WithQuotas(map[string]int{"cpu": cfg.quota}),
			WithQueueCapacity(cfg.queueCap),
			WithFailurePolicy(Isolate),
		)
		tasks := []Task{
			{ID: "bad", Resource: "cpu", Run: func(ctx context.Context) error { panic("x") }},
			{ID: "child", Resource: "cpu", Deps: []string{"bad"}, Run: func(ctx context.Context) error { return nil }},
		}
		for _, id := range []string{"a", "b", "c"} {
			tasks = append(tasks, Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error { return nil }})
		}
		if err := rt.Submit(context.Background(), tasks...); err != nil {
			t.Fatalf("submit: %v", err)
		}
		rt.Wait(context.Background())
		s := rt.Stats()
		t.Logf("quota=%d queueCap=%d -> Completed=%d Failed=%d Skipped=%d Panics=%d (判定依据: 期望恒定 %+v)",
			cfg.quota, cfg.queueCap, s.Completed, s.Failed, s.Skipped, s.Panics, expected)
		if s.Completed != expected.completed || s.Failed != expected.failed ||
			s.Skipped != expected.skipped || s.Panics != expected.panics {
			t.Fatalf("quota=%d queueCap=%d 口径不一致: %+v", cfg.quota, cfg.queueCap, s)
		}
		rt.Close()
	}
}
