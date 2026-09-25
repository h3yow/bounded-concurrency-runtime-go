package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestTrySubmitRejectedWhenFull 验证队列满时 TrySubmit 明确拒绝。
func TestTrySubmitRejectedWhenFull(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(2))
	defer rt.Close()

	block := make(chan struct{})
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			<-block // 占住额度与准入
			return nil
		}}
	}
	if err := rt.Submit(context.Background(), mk("t1"), mk("t2")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// 等待 t1 进入执行，t2 占住另一个准入位
	time.Sleep(50 * time.Millisecond)

	err := rt.TrySubmit(context.Background(), mk("t3"))
	t.Logf("判定依据: queueCap=2 已占满, TrySubmit 输入 1 个任务 -> 期望 ErrQueueFull, 实际 err=%v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull, 实际 %v", err)
	}
	if ReasonOf(err) != ReasonRejected {
		t.Fatalf("期望 ReasonRejected, 实际 %v", ReasonOf(err))
	}
	if got := rt.Stats().Rejected; got != 1 {
		t.Fatalf("Rejected 统计期望 1, 实际 %d", got)
	}
	close(block)
	rt.Wait(context.Background())
}

// TestSubmitBoundedWait 验证 Submit 在队列满时有界等待，空位释放后成功。
func TestSubmitBoundedWait(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(1))
	defer rt.Close()

	block := make(chan struct{})
	started := make(chan struct{}, 1)
	if err := rt.Submit(context.Background(), Task{
		ID: "t1", Resource: "cpu",
		Run: func(ctx context.Context) error {
			started <- struct{}{}
			<-block
			return nil
		},
	}); err != nil {
		t.Fatalf("submit t1: %v", err)
	}
	<-started

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- rt.Submit(context.Background(), Task{
			ID: "t2", Resource: "cpu", Run: func(ctx context.Context) error { return nil },
		})
	}()

	select {
	case err := <-submitDone:
		t.Fatalf("Submit 应阻塞等待空位, 却提前返回: %v", err)
	case <-time.After(50 * time.Millisecond):
		t.Logf("判定依据: queueCap=1 被 t1 占用, Submit(t2) 阻塞等待 -> 符合有界等待")
	}
	close(block) // 释放空位
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatalf("空位释放后 Submit 应成功, 实际 %v", err)
		}
		t.Logf("空位释放后 Submit 成功")
	case <-time.After(2 * time.Second):
		t.Fatal("空位释放后 Submit 仍未返回")
	}
	rt.Wait(context.Background())
}

// TestSubmitWaitCanceled 验证有界等待可被 ctx 超时取消。
func TestSubmitWaitCanceled(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(1))
	defer rt.Close()

	block := make(chan struct{})
	if err := rt.Submit(context.Background(), Task{
		ID: "t1", Resource: "cpu", Run: func(ctx context.Context) error { <-block; return nil },
	}); err != nil {
		t.Fatalf("submit t1: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := rt.Submit(ctx, Task{ID: "t2", Resource: "cpu", Run: func(context.Context) error { return nil }})
	t.Logf("判定依据: 队列满且 ctx 50ms 超时 -> 期望 ReasonCanceled, 实际 err=%v", err)
	if ReasonOf(err) != ReasonCanceled {
		t.Fatalf("期望 ReasonCanceled, 实际 %v (err=%v)", ReasonOf(err), err)
	}
	close(block)
	rt.Wait(context.Background())
}

// TestQuotaBound 验证在途并发不超过资源额度。
func TestQuotaBound(t *testing.T) {
	const quota = 3
	rt := New(WithQuotas(map[string]int{"cpu": quota}), WithQueueCapacity(64))
	defer rt.Close()

	var inFlight, maxSeen atomic.Int64
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return nil
		}}
	}
	tasks := make([]Task, 0, 20)
	for i := 0; i < 20; i++ {
		tasks = append(tasks, mk(string(rune('a'+i))))
	}
	if err := rt.Submit(context.Background(), tasks...); err != nil {
		t.Fatalf("submit: %v", err)
	}
	rt.Wait(context.Background())
	t.Logf("判定依据: quota=%d, 观测最大在途=%d", quota, maxSeen.Load())
	if maxSeen.Load() > quota {
		t.Fatalf("在途并发 %d 超过额度 %d", maxSeen.Load(), quota)
	}
}
