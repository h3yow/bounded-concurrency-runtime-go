package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatchTrySubmitAllOrNothing 验证 TrySubmit 整批判定：
// 容量不足时整批拒绝（一个都不进），且不攥着已占令牌不放。
func TestBatchTrySubmitAllOrNothing(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(3))
	defer rt.Close()

	block := make(chan struct{})
	occupant := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error { <-block; return nil }}
	}
	if err := rt.Submit(context.Background(), occupant("o1"), occupant("o2")); err != nil {
		t.Fatalf("submit occupants: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // 等 o1/o2 占住 2 个准入位

	// 剩余容量 1，提交 2 个一批：必须整批拒绝
	err := rt.TrySubmit(context.Background(),
		Task{ID: "b1", Run: func(context.Context) error { return nil }},
		Task{ID: "b2", Run: func(context.Context) error { return nil }},
	)
	t.Logf("判定依据: queueCap=3 已占 2, TrySubmit 批量=2 -> 期望整批拒绝 ErrQueueFull, 实际 err=%v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull, 实际 %v", err)
	}
	if got := rt.Stats().Rejected; got != 2 {
		t.Fatalf("Rejected 期望整批计 2, 实际 %d", got)
	}
	if got := rt.Stats().Submitted; got != 2 {
		t.Fatalf("Submitted 期望仍占位的 2, 实际 %d（拒绝批不得部分准入）", got)
	}

	// 被拒批不得攥住令牌：剩余 1 个空位应立即可用
	if err := rt.TrySubmit(context.Background(), Task{ID: "b3", Run: func(context.Context) error { return nil }}); err != nil {
		t.Fatalf("被拒批后剩余空位应可用, 实际 %v", err)
	}
	close(block)
	rt.Wait(context.Background())

	// 全部终态后令牌必须全部归还：整批 queueCap=3 应能立即进入
	err = rt.TrySubmit(context.Background(),
		Task{ID: "c1", Run: func(context.Context) error { return nil }},
		Task{ID: "c2", Run: func(context.Context) error { return nil }},
		Task{ID: "c3", Run: func(context.Context) error { return nil }},
	)
	t.Logf("判定依据: 全部终态后 TrySubmit 批量=queueCap=3 -> 期望立即成功, 实际 err=%v", err)
	if err != nil {
		t.Fatalf("令牌未全部归还（被拒批泄漏）: %v", err)
	}
	rt.Wait(context.Background())
}

// TestBatchSubmitTimeout 验证 Submit 在调用方 ctx 无截止时仍受
// WithSubmitTimeout 约束，有限时间内整批拒绝并说明原因。
func TestBatchSubmitTimeout(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(1),
		WithSubmitTimeout(50*time.Millisecond))
	defer rt.Close()

	block := make(chan struct{})
	if err := rt.Submit(context.Background(), Task{
		ID: "o1", Resource: "cpu", Run: func(ctx context.Context) error { <-block; return nil },
	}); err != nil {
		t.Fatalf("submit occupant: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	err := rt.Submit(context.Background(), // 无截止时间，仅靠 WithSubmitTimeout 收敛
		Task{ID: "w1", Run: func(context.Context) error { return nil }},
		Task{ID: "w2", Run: func(context.Context) error { return nil }},
	)
	elapsed := time.Since(start)
	t.Logf("判定依据: queueCap=1 被占满, Submit 批量=2, submitTimeout=50ms -> 期望 ErrAdmissionTimeout, 实际 err=%v 耗时=%v", err, elapsed)
	if !errors.Is(err, ErrAdmissionTimeout) {
		t.Fatalf("期望 ErrAdmissionTimeout, 实际 %v", err)
	}
	if ReasonOf(err) != ReasonRejected {
		t.Fatalf("期望 ReasonRejected, 实际 %v", ReasonOf(err))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("未在有限时间收敛: %v", elapsed)
	}
	if got := rt.Stats().Rejected; got != 2 {
		t.Fatalf("Rejected 期望整批计 2, 实际 %d", got)
	}
	close(block)
	rt.Wait(context.Background())
	// 超时拒绝的批不得残留令牌
	if err := rt.TrySubmit(context.Background(), Task{ID: "after", Run: func(context.Context) error { return nil }}); err != nil {
		t.Fatalf("超时拒绝后空位应可用, 实际 %v", err)
	}
	rt.Wait(context.Background())
}

// TestBatchConcurrentWholeResults 验证并发批量提交下整体结果确定：
// 每批要么全进要么全拒，Submitted+Rejected 恒等于尝试总数，
// 且终态口径与提交口径一致。
func TestBatchConcurrentWholeResults(t *testing.T) {
	const (
		queueCap   = 8
		submitters = 6
		batches    = 20
		batchSize  = 3
	)
	rt := New(WithQuotas(map[string]int{"cpu": 2}), WithQueueCapacity(queueCap))
	defer rt.Close()

	var acceptedBatches, rejectedBatches atomic.Int64
	var wg sync.WaitGroup
	for s := 0; s < submitters; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				tasks := make([]Task, 0, batchSize)
				for i := 0; i < batchSize; i++ {
					id := fmt.Sprintf("s%d-b%d-t%d", s, b, i)
					tasks = append(tasks, Task{ID: id, Resource: "cpu",
						Run: func(ctx context.Context) error {
							time.Sleep(time.Millisecond)
							return nil
						}})
				}
				err := rt.TrySubmit(context.Background(), tasks...)
				if err == nil {
					acceptedBatches.Add(1)
				} else if errors.Is(err, ErrQueueFull) {
					rejectedBatches.Add(1)
				} else {
					t.Errorf("意外错误: %v", err)
				}
			}
		}(s)
	}
	wg.Wait()
	rt.Wait(context.Background())

	stats := rt.Stats()
	total := int64(submitters * batches * batchSize)
	t.Logf("判定依据: submitters=%d batches=%d batchSize=%d queueCap=%d -> accepted=%d rejected=%d",
		submitters, batches, batchSize, queueCap, acceptedBatches.Load(), rejectedBatches.Load())
	t.Logf("stats: %+v", stats)
	if want := acceptedBatches.Load() * batchSize; stats.Submitted != want {
		t.Fatalf("Submitted=%d, 期望=接受整批数*批量=%d（不得部分准入）", stats.Submitted, want)
	}
	if want := rejectedBatches.Load() * batchSize; stats.Rejected != want {
		t.Fatalf("Rejected=%d, 期望=拒绝整批数*批量=%d（整批拒绝）", stats.Rejected, want)
	}
	if stats.Submitted+stats.Rejected != total {
		t.Fatalf("Submitted+Rejected=%d, 期望=尝试总数 %d", stats.Submitted+stats.Rejected, total)
	}
	if stats.Terminal() != stats.Submitted+stats.Rejected {
		t.Fatalf("Terminal=%d 与 Submitted+Rejected=%d 不一致", stats.Terminal(), stats.Submitted+stats.Rejected)
	}
	if stats.Completed != stats.Submitted {
		t.Fatalf("Completed=%d, 期望=Submitted=%d（本场景无失败）", stats.Completed, stats.Submitted)
	}
}
