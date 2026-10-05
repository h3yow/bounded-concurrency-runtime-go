package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestPanicIsolated 验证 Isolate 策略下：panic 只影响自身与其下游，
// 独立任务与运行时进程均不受连带，崩溃被记为可识别的独立终态。
func TestPanicIsolated(t *testing.T) {
	rt := New(WithFailurePolicy(Isolate))
	defer rt.Close()

	sentinel := errors.New("plugin boom")
	err := rt.Submit(context.Background(),
		Task{ID: "boom", Run: func(context.Context) error { panic(sentinel) }},
		Task{ID: "downstream", Deps: []string{"boom"}, Run: func(context.Context) error { return nil }},
		Task{ID: "independent", Run: func(context.Context) error { return nil }},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
	}

	boom := byID["boom"]
	t.Logf("boom: status=%v attempts=%d err=%v", boom.Status, boom.Attempts, boom.Err)
	if boom.Status != StatusPanicked {
		t.Fatalf("boom status = %v, want StatusPanicked", boom.Status)
	}
	if ReasonOf(boom.Err) != ReasonPanicked {
		t.Fatalf("boom reason = %v, want ReasonPanicked", ReasonOf(boom.Err))
	}
	var pe *PanicError
	if !errors.As(boom.Err, &pe) {
		t.Fatalf("boom cause is not *PanicError: %T", boom.Err)
	}
	if !errors.Is(boom.Err, sentinel) {
		t.Fatalf("panic value not preserved/unwrapable: %v", boom.Err)
	}
	if len(pe.Stack) == 0 || !strings.Contains(strings.ToLower(string(pe.Stack)), "panic") {
		t.Fatalf("stack missing or lacks panic site, got %d bytes", len(pe.Stack))
	}
	t.Logf("panic 现场: value=%v stack_bytes=%d", pe.Value, len(pe.Stack))

	if got := byID["downstream"].Status; got != StatusSkipped {
		t.Fatalf("downstream status = %v, want StatusSkipped", got)
	}
	if got := byID["independent"].Status; got != StatusCompleted {
		t.Fatalf("independent status = %v, want StatusCompleted（进程与无关任务未被带崩）", got)
	}

	s := rt.Stats()
	t.Logf("stats: %+v", s)
	if s.Panicked != 1 || s.Failed != 0 || s.Skipped != 1 || s.Completed != 1 {
		t.Fatalf("stats mismatch: %+v", s)
	}
	if s.Terminal() != s.Submitted {
		t.Fatalf("terminal %d != submitted %d", s.Terminal(), s.Submitted)
	}
}

// TestPanicFailFast 验证 FailFast 策略下 panic 正常触发整体收敛。
func TestPanicFailFast(t *testing.T) {
	rt := New(WithFailurePolicy(FailFast))
	defer rt.Close()

	started := make(chan struct{}, 1)
	err := rt.Submit(context.Background(),
		Task{ID: "boom", Run: func(context.Context) error {
			<-started // 等 slow 在途后再崩，确保存在可被收敛的任务
			panic("kaboom")
		}},
		Task{ID: "slow", Run: func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return nil
			}
		}},
		Task{ID: "queued", Deps: []string{"slow"}, Run: func(context.Context) error { return nil }},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
		t.Logf("result: id=%s status=%v", res.TaskID, res.Status)
	}
	if byID["boom"].Status != StatusPanicked {
		t.Fatalf("boom status = %v, want StatusPanicked", byID["boom"].Status)
	}
	if byID["slow"].Status != StatusCanceled {
		t.Fatalf("slow status = %v, want StatusCanceled（FailFast 收敛）", byID["slow"].Status)
	}
	if got := byID["queued"].Status; got != StatusCanceled && got != StatusSkipped {
		t.Fatalf("queued status = %v, want Canceled 或 Skipped", got)
	}
	s := rt.Stats()
	if s.Panicked != 1 || s.Terminal() != s.Submitted {
		t.Fatalf("stats mismatch: %+v", s)
	}
}

// TestPanicNotRetried 验证 panic 不进入重试：崩溃是确定性现场，重试无意义。
func TestPanicNotRetried(t *testing.T) {
	rt := New(WithRetryPolicy(RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond}))
	defer rt.Close()

	calls := 0
	err := rt.Submit(context.Background(), Task{
		ID:      "boom",
		Retries: 3,
		Run:     func(context.Context) error { calls++; panic("always") },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	t.Logf("calls=%d attempts=%d status=%v", calls, results[0].Attempts, results[0].Status)
	if results[0].Status != StatusPanicked {
		t.Fatalf("status = %v, want StatusPanicked", results[0].Status)
	}
	if calls != 1 || results[0].Attempts != 1 {
		t.Fatalf("panic 被重试: calls=%d attempts=%d, want 1/1", calls, results[0].Attempts)
	}
	if s := rt.Stats(); s.Retried != 0 {
		t.Fatalf("retried = %d, want 0", s.Retried)
	}
}

// TestPanicMixedWithFailures 验证 panic 与普通业务失败、成功混合提交时
// 各自计入独立口径，互不影响。
func TestPanicMixedWithFailures(t *testing.T) {
	rt := New()
	defer rt.Close()

	err := rt.Submit(context.Background(),
		Task{ID: "ok", Run: func(context.Context) error { return nil }},
		Task{ID: "biz-fail", Run: func(context.Context) error { return errors.New("biz") }},
		Task{ID: "boom", Run: func(context.Context) error { panic("x") }},
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rt.Wait(context.Background())
	s := rt.Stats()
	t.Logf("stats: %+v", s)
	if s.Completed != 1 || s.Failed != 1 || s.Panicked != 1 {
		t.Fatalf("want completed=1 failed=1 panicked=1, got %+v", s)
	}
	if s.Terminal() != 3 {
		t.Fatalf("terminal = %d, want 3", s.Terminal())
	}
}
