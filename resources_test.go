package bcr

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 多资源类别同时受限：观测到的任意资源占用都不得超过额度。
func TestMultipleResourcesNeverExceed(t *testing.T) {
	var memPeak, connPeak, slotPeak int64
	var memCur, connCur, slotCur int64
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 3, "mem": 2, "conn": 1}
	rt := New(cfg)
	var tasks []*Task
	for i := 0; i < 12; i++ {
		tasks = append(tasks, &Task{
			ID:        idFor(i),
			Fn:        holdAndObserve(&memCur, &memPeak, &connCur, &connPeak, &slotCur, &slotPeak),
			Resources: ResourceWeights{"mem": 1, "conn": 1},
		})
	}
	st, err := rt.RunTasks(context.Background(), tasks)
	if err != nil {
		t.Fatal(err)
	}
	if st.Completed != 12 {
		t.Fatalf("completed=%d", st.Completed)
	}
	if memPeak > 2 || connPeak > 1 || slotPeak > 3 {
		t.Fatalf("peaks mem=%d conn=%d slot=%d", memPeak, connPeak, slotPeak)
	}
}

// 资源等待超时：大任务占满额度，等待者在 AcquireTimeout 后以
// ReasonResourceTimeout 失败（函数体从未执行）。
func TestResourceAcquireTimeout(t *testing.T) {
	release := make(chan struct{})
	var waiterStarted int64
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 1}
	cfg.AcquireTimeout = 80 * time.Millisecond
	rt := New(cfg)
	tasks := []*Task{
		{ID: "holder", Fn: func(ctx context.Context) error {
			<-release
			return nil
		}},
		{ID: "waiter", Fn: func(ctx context.Context) error {
			atomic.StoreInt64(&waiterStarted, 1)
			return nil
		}},
	}
	go func() {
		time.Sleep(400 * time.Millisecond)
		close(release)
	}()
	st, err := rt.RunTasks(context.Background(), tasks)
	if err != nil {
		t.Fatal(err)
	}
	res := st.ByTask["waiter"]
	if res.Status != StatusFailed || res.Reason != ReasonResourceTimeout {
		t.Fatalf("waiter status=%s reason=%s", statusString(res.Status), reasonString(res.Reason))
	}
	if waiterStarted != 0 {
		t.Fatal("waiter function must never start")
	}
	if res.Attempts != 0 {
		t.Fatalf("attempts=%d, want 0", res.Attempts)
	}
	if !errors.Is(res.Err, ErrResourceTimeout) {
		t.Fatalf("err=%v", res.Err)
	}
}

// 等待依赖的任务不得占用额度：depWaiter 的前置被阻塞期间，
// 3 个独立任务可以同时用满 slots=3，证明等待依赖不持票。
func TestWaitingOnDepsHoldsNoQuota(t *testing.T) {
	var cur, peak int64
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 3}
	rt := New(cfg)
	// 依赖链 ind0 -> root -> depWaiter：链上等待的任务都不得持有槽位，
	// 因此 ind0/ind1/ind2 三个无依赖任务必须能同时跑满 3 槽。
	tasks := []*Task{
		{ID: "root", Deps: []string{"ind0"}, Fn: func(ctx context.Context) error {
			return nil
		}},
		{ID: "depWaiter", Deps: []string{"root"}, Fn: func(ctx context.Context) error {
			return nil
		}},
	}
	for i := 0; i < 3; i++ {
		tasks = append(tasks, &Task{ID: "ind" + itoa(i), Fn: func(ctx context.Context) error {
			n := atomic.AddInt64(&cur, 1)
			updatePeak(&peak, n)
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt64(&cur, -1)
			return nil
		}})
	}
	st, err := rt.RunTasks(context.Background(), tasks)
	if err != nil {
		t.Fatal(err)
	}
	if st.Completed != 5 {
		t.Fatalf("completed=%d", st.Completed)
	}
	if peak != 3 {
		t.Fatalf("peak concurrency=%d, want 3 (dep waiter must not hold a slot)", peak)
	}
}

func idFor(i int) string {
	return "t" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	b := []byte{}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func holdAndObserve(memCur, memPeak, connCur, connPeak, slotCur, slotPeak *int64) Func {
	return func(ctx context.Context) error {
		m := atomic.AddInt64(memCur, 1)
		updatePeak(memPeak, m)
		c := atomic.AddInt64(connCur, 1)
		updatePeak(connPeak, c)
		s := atomic.AddInt64(slotCur, 1)
		updatePeak(slotPeak, s)
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt64(memCur, -1)
		atomic.AddInt64(connCur, -1)
		atomic.AddInt64(slotCur, -1)
		return nil
	}
}

func updatePeak(peak *int64, v int64) {
	for {
		old := atomic.LoadInt64(peak)
		if v <= old || atomic.CompareAndSwapInt64(peak, old, v) {
			return
		}
	}
}
