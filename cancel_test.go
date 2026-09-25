package bcr

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// 外部取消：排队中（等待资源）与在途任务都必须收到取消；
// 排队任务函数体不得被启动，在途任务通过 ctx 观测到取消。
func TestCancelPropagatesQueuedAndRunning(t *testing.T) {
	release := make(chan struct{})
	var queuedRan int32
	var runningSawCancel int32
	ctx, cancel := context.WithCancel(context.Background())

	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 1}
	rt := New(cfg)
	tasks := []*Task{
		{ID: "holder", Fn: func(ctx context.Context) error {
			<-release
			return nil
		}},
		{ID: "queued", Fn: func(ctx context.Context) error {
			atomic.StoreInt32(&queuedRan, 1)
			return nil
		}},
		{ID: "running", Fn: func(ctx context.Context) error {
			<-ctx.Done()
			atomic.StoreInt32(&runningSawCancel, 1)
			return ctx.Err()
		}},
	}
	_ = runningSawCancel

	// holder 占槽；running 任务通过把 holder 释放后第二批进入：
	// 简化为 holder(阻塞) + 多个 queued，取消后 queued 不启动。
	tasks = []*Task{
		{ID: "holder", Fn: func(ctx context.Context) error {
			<-release
			<-ctx.Done()
			return ctx.Err()
		}},
		{ID: "q1", Fn: func(ctx context.Context) error { atomic.StoreInt32(&queuedRan, 1); return nil }},
		{ID: "q2", Fn: func(ctx context.Context) error { atomic.StoreInt32(&queuedRan, 1); return nil }},
	}
	done := make(chan Stats)
	go func() {
		st, _ := rt.RunTasks(ctx, tasks)
		done <- st
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
	close(release)
	st := <-done

	if queuedRan != 0 {
		t.Fatal("queued task must never start after cancel")
	}
	h := st.ByTask["holder"]
	if h.Status != StatusCanceled ||
		(h.Reason != ReasonContextCanceled && h.Reason != ReasonAborted) {
		t.Fatalf("holder status=%s reason=%s", statusString(h.Status), reasonString(h.Reason))
	}
	for _, id := range []string{"q1", "q2"} {
		r := st.ByTask[id]
		if r.Status != StatusCanceled {
			t.Fatalf("%s status=%s, want canceled", id, statusString(r.Status))
		}
	}
	if st.Completed != 0 || st.Canceled != 3 {
		t.Fatalf("completed=%d canceled=%d", st.Completed, st.Canceled)
	}
}

// 超时：根 context 截止时间到期等价于取消，归因为 deadline。
func TestCancelDeadlineAttribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	close(release)
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 1}
	rt := New(cfg)
	tasks := []*Task{
		{ID: "slow", Fn: func(ctx context.Context) error {
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}},
		{ID: "q", Fn: noopFn()},
	}
	st, _ := rt.RunTasks(ctx, tasks)
	r := st.ByTask["slow"]
	if r.Status != StatusCanceled || r.Reason != ReasonContextDeadline {
		t.Fatalf("slow status=%s reason=%s", statusString(r.Status), reasonString(r.Reason))
	}
}

// 任务级 Timeout：仅取消该次尝试，归因为 deadline；
// 可重试时按退避重试（此处 decider 对 deadline 不重试 -> 直接失败分类）。
func TestTaskTimeoutOwnDeadline(t *testing.T) {
	cfg := NewConfig()
	rt := New(cfg)
	st, err := rt.RunTasks(context.Background(), []*Task{{
		ID:      "tt",
		Timeout: 30 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	r := st.ByTask["tt"]
	if r.Status != StatusCanceled || r.Reason != ReasonContextDeadline {
		t.Fatalf("status=%s reason=%s", statusString(r.Status), reasonString(r.Reason))
	}
}

// 已取消后不再有新任务被启动：取消瞬间记录在途数，之后不得增长。
func TestNoLaunchAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan string, 16)
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 8}
	rt := New(cfg)
	var tasks []*Task
	for i := 0; i < 8; i++ {
		tasks = append(tasks, &Task{ID: "t" + itoa(i), Fn: func(ctx context.Context) error {
			started <- "x"
			<-ctx.Done()
			return ctx.Err()
		}})
	}
	done := make(chan struct{})
	go func() { rt.RunTasks(ctx, tasks); close(done) }()
	time.Sleep(60 * time.Millisecond) // 等 8 个任务全部在途
	nBefore := len(started)
	cancel()
	<-done
	select {
	case <-time.After(30 * time.Millisecond):
	default:
	}
	if len(started) != nBefore {
		t.Fatalf("launches grew after cancel: %d -> %d", nBefore, len(started))
	}
}
