package bcr

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmptyRun(t *testing.T) {
	rt := New(NewConfig())
	st, err := rt.RunTasks(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Submitted != 0 || st.Completed+st.Failed+st.Canceled+st.Rejected+st.Skipped != 0 {
		t.Fatalf("empty run stats not zero: %+v", st)
	}
}

// 资源等待超时进入重试判定：decider 放行 + 额度及时释放后，
// 第二次排队应成功。
func TestResourceTimeoutRetryThenSuccess(t *testing.T) {
	release := make(chan struct{})
	var started int32
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 1}
	cfg.AcquireTimeout = 60 * time.Millisecond
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 3,
		Backoff:     ExponentialBackoff{Base: time.Millisecond, Factor: 2},
		Decider: func(attempt int, err error) bool {
			return errors.Is(err, ErrResourceTimeout)
		},
	}
	rt := New(cfg)
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(release)
	}()
	st, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "holder", Fn: func(ctx context.Context) error { <-release; return nil }},
		{ID: "w", Fn: func(ctx context.Context) error {
			atomic.AddInt32(&started, 1)
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := st.ByTask["w"]
	if r.Status != StatusSucceeded {
		t.Fatalf("w status=%s reason=%s attempts=%d",
			statusString(r.Status), reasonString(r.Reason), r.Attempts)
	}
	if started != 1 {
		t.Fatalf("started=%d", started)
	}
	if st.Completed != 2 {
		t.Fatalf("completed=%d", st.Completed)
	}
}

// 隔离策略下，任务重试后成功：其下游正常执行，不应被误跳过。
func TestIsolateRetrySuccessUnblocksDownstream(t *testing.T) {
	var n int32
	cfg := NewConfig()
	cfg.FailurePolicy = Isolate
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 2,
		Backoff:     ExponentialBackoff{Base: time.Millisecond, Factor: 2},
		Decider:     DefaultRetryDecider,
	}
	rt := New(cfg)
	st, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "p", Fn: func(ctx context.Context) error {
			if atomic.AddInt32(&n, 1) == 1 {
				return errBoom
			}
			return nil
		}},
		{ID: "child", Deps: []string{"p"}, Fn: noopFn()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Completed != 2 || st.Failed != 0 || st.Skipped != 0 {
		t.Fatalf("%+v", st)
	}
}

// 在途任务 panic 不影响进程：FailFast 下其余任务取消，panic 归类正确。
func TestRunningPanicFailsFast(t *testing.T) {
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 2}
	rt := New(cfg)
	st, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "boom", MaxAttempts: 1, Fn: func(ctx context.Context) error {
			panic("boom value")
		}},
		{ID: "other", Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	b := st.ByTask["boom"]
	if b.Status != StatusFailed || b.Reason != ReasonPanic ||
		b.Err == nil || !containsSub(b.Err.Error(), "boom value") {
		t.Fatalf("boom status=%s reason=%s err=%v",
			statusString(b.Status), reasonString(b.Reason), b.Err)
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
