package bcr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// 排队容量满：EnqueueTimeout=0 时必须立即明确拒绝，
// 且原因可由 errors.Is(_, ErrQueueFull) 区分。
func TestAdmissionQueueFullImmediateReject(t *testing.T) {
	cfg := NewConfig()
	cfg.MaxQueued = 2
	cfg.EnqueueTimeout = 0
	rt := New(cfg)
	ok := noopFn()
	if err := rt.Submit(&Task{ID: "a", Fn: ok}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Submit(&Task{ID: "b", Fn: ok}); err != nil {
		t.Fatal(err)
	}
	err := rt.Submit(&Task{ID: "c", Fn: ok})
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	var te *TaskError
	if !errors.As(err, &te) || te.Reason != ReasonAdmissionRejected {
		t.Fatalf("want reason admission_rejected, got %v", err)
	}
}

// EnqueueTimeout>0：缓冲满时有界等待，超时返回 ErrQueueTimeout，
// 等待时长应接近超时值。
func TestAdmissionBoundedWaitTimeout(t *testing.T) {
	cfg := NewConfig()
	cfg.MaxQueued = 1
	cfg.EnqueueTimeout = 150 * time.Millisecond
	rt := New(cfg)
	if err := rt.Submit(&Task{ID: "a", Fn: noopFn()}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := rt.Submit(&Task{ID: "b", Fn: noopFn()})
	if !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("want ErrQueueTimeout, got %v", err)
	}
	if d := time.Since(start); d < 140*time.Millisecond {
		t.Fatalf("returned too early after %s", d)
	}
}

// 有界等待的正向路径：缓冲满时提交者阻塞等待，另一个 goroutine 通过
// Run 排空缓冲（Run 与并发 Submit 的密封竞争：先等待的提交在密封前
// 已进入等待），等待者应被唤醒成功而不是超时。
func TestAdmissionBoundedWaitWakesOnDrain(t *testing.T) {
	cfg := NewConfig()
	cfg.MaxQueued = 1
	cfg.EnqueueTimeout = time.Second
	rt := New(cfg)
	if err := rt.Submit(&Task{ID: "a", Fn: noopFn()}); err != nil {
		t.Fatal(err)
	}
	// 在 Run 排空后重新登记的路径无法与密封竞争；这里直接驱动
	// notFull 条件：另起 goroutine 排空 pending 并广播。
	type result struct{ err error }
	resCh := make(chan result, 1)
	go func() {
		resCh <- result{err: rt.Submit(&Task{ID: "b", Fn: noopFn()})}
	}()
	time.Sleep(30 * time.Millisecond)
	rt.mu.Lock()
	rt.pending = rt.pending[:0]
	rt.notFull.Broadcast()
	rt.mu.Unlock()
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("waiting submit should succeed after drain, got %v", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting submit was not woken after drain")
	}
}

// 任务需求超过资源类别额度：必须在登记时立即拒绝，不能静默挂死。
func TestAdmissionExceedsLimitRejected(t *testing.T) {
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 4, "mem": 10}
	rt := New(cfg)
	err := rt.Submit(&Task{
		ID:        "big",
		Fn:        noopFn(),
		Resources: ResourceWeights{"mem": 11},
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("want exceeds-limit rejection, got %v", err)
	}
	// slots 也不允许超过
	err = rt.Submit(&Task{
		ID:        "toomanyslots",
		Fn:        noopFn(),
		Resources: ResourceWeights{ResourceSlots: 5},
	})
	if err == nil {
		t.Fatal("expected exceeds-limit rejection for slots")
	}
}

// Run 之后提交通道密封：明确拒绝。
func TestAdmissionAfterRunRejected(t *testing.T) {
	rt := New(NewConfig())
	if _, err := rt.RunTasks(context.Background(), []*Task{{ID: "a", Fn: noopFn()}}); err != nil {
		t.Fatal(err)
	}
	err := rt.Submit(&Task{ID: "late", Fn: noopFn()})
	if !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("want ErrRuntimeClosed, got %v", err)
	}
}

// 非法任务定义（空 ID / nil Fn）立即拒绝。
func TestAdmissionInvalidTask(t *testing.T) {
	rt := New(NewConfig())
	if err := rt.Submit(&Task{ID: "", Fn: noopFn()}); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("got %v", err)
	}
	if err := rt.Submit(&Task{ID: "x"}); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("got %v", err)
	}
}

func noopFn() Func {
	return func(ctx context.Context) error { return nil }
}

// 并发竞争：缓冲满时 Submit 正在有界等待，Run 排空并密封，
// 等待者必须收到明确拒绝，且不 panic、不把任务追加到执行集之外。
func TestConcurrentSubmitVsRunSeal(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		cfg := NewConfig()
		cfg.MaxQueued = 1
		cfg.EnqueueTimeout = 500 * time.Millisecond
		rt := New(cfg)
		if err := rt.Submit(&Task{ID: "seed", Fn: noopFn()}); err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() {
			errCh <- rt.Submit(&Task{ID: "w" + itoa(iter), Fn: noopFn()})
		}()
		time.Sleep(time.Millisecond)
		st, err := rt.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Submitted != 1 || st.Completed != 1 {
			t.Fatalf("iter=%d unexpected batch: %+v", iter, st)
		}
		select {
		case serr := <-errCh:
			if serr == nil || !errors.Is(serr, ErrRuntimeClosed) {
				if !errors.Is(serr, ErrQueueTimeout) {
					t.Fatalf("iter=%d unexpected submit result: %v", iter, serr)
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("iter=%d submitter never returned", iter)
		}
	}
}
