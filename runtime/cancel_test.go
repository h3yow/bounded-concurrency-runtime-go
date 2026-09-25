package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelPropagatesToInFlight 验证取消传播到在途任务。
func TestCancelPropagatesToInFlight(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 2}), WithQueueCapacity(16))

	ctx, cancel := context.WithCancel(context.Background())
	var observed atomic.Int32
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			<-ctx.Done() // 等待取消
			observed.Add(1)
			return ctx.Err()
		}}
	}
	if err := rt.Submit(ctx, mk("a"), mk("b")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // 等待两个任务进入执行
	cancel()

	results := rt.Wait(context.Background())
	t.Logf("判定依据: 运行时 ctx 被取消 -> 在途任务应收到取消并归为 Canceled, 观测到 %d 个", observed.Load())
	for _, res := range results {
		t.Logf("任务 %s -> %v (err=%v)", res.TaskID, res.Status, res.Err)
		if res.Status != StatusCanceled {
			t.Fatalf("任务 %s 期望 Canceled, 实际 %v", res.TaskID, res.Status)
		}
		if ReasonOf(res.Err) != ReasonCanceled {
			t.Fatalf("任务 %s 原因期望 ReasonCanceled, 实际 %v", res.TaskID, ReasonOf(res.Err))
		}
	}
	if observed.Load() != 2 {
		t.Fatalf("在途任务未收到取消: %d/2", observed.Load())
	}
	rt.Close()
}

// TestCanceledTaskNeverStarts 验证已取消的排队任务不再被启动。
func TestCanceledTaskNeverStarts(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(16))

	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	firstEntered := make(chan struct{})
	var started atomic.Int32

	first := Task{ID: "first", Resource: "cpu", Run: func(ctx context.Context) error {
		close(firstEntered)
		<-release
		return nil
	}}
	second := Task{ID: "second", Resource: "cpu", Run: func(ctx context.Context) error {
		started.Add(1)
		return nil
	}}
	// 先确认 first 占住唯一额度，再提交 second 使其排队等额度。
	if err := rt.Submit(ctx, first); err != nil {
		t.Fatalf("submit first: %v", err)
	}
	<-firstEntered
	if err := rt.Submit(ctx, second); err != nil {
		t.Fatalf("submit second: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // second 在等额度
	cancel()                          // 取消时 second 尚未启动
	close(release)

	results := rt.Wait(context.Background())
	t.Logf("判定依据: 取消发生在 second 获取额度之前 -> second 不得启动, started=%d", started.Load())
	if started.Load() != 0 {
		t.Fatal("已取消的任务被启动")
	}
	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
		t.Logf("任务 %s -> %v", res.TaskID, res.Status)
	}
	if byID["second"].Status != StatusCanceled {
		t.Fatalf("second 期望 Canceled, 实际 %v", byID["second"].Status)
	}
	rt.Close()
}

// TestCloseCancelsQueued 验证 Close 取消排队任务并等待在途任务。
func TestCloseCancelsQueued(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(16))

	var started atomic.Int32
	slowEntered := make(chan struct{})
	slow := Task{ID: "slow", Resource: "cpu", Run: func(ctx context.Context) error {
		started.Add(1)
		close(slowEntered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}}
	queued := Task{ID: "queued", Resource: "cpu", Run: func(context.Context) error {
		started.Add(1)
		return nil
	}}
	// 先提交 slow 并确认其已占住唯一额度，再提交 queued 使其排队等额度。
	if err := rt.Submit(context.Background(), slow); err != nil {
		t.Fatalf("submit slow: %v", err)
	}
	<-slowEntered
	if err := rt.Submit(context.Background(), queued); err != nil {
		t.Fatalf("submit queued: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := rt.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	t.Logf("判定依据: Close 后 queued 不得启动, started=%d (期望 1, 仅 slow)", started.Load())
	if started.Load() != 1 {
		t.Fatalf("Close 后排队任务被启动: started=%d", started.Load())
	}
	stats := rt.Stats()
	t.Logf("统计: %+v", stats)
	if stats.Terminal() != 2 {
		t.Fatalf("终态总数应为 2: %+v", stats)
	}

	// 关闭后提交应被拒绝
	err := rt.Submit(context.Background(), Task{ID: "late", Run: func(context.Context) error { return nil }})
	t.Logf("判定依据: 已关闭 -> 提交应被拒绝, err=%v", err)
	if ReasonOf(err) != ReasonRejected {
		t.Fatalf("期望 ReasonRejected, 实际 %v", ReasonOf(err))
	}
}
