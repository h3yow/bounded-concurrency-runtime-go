package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestBatchAllOrNothingTrySubmit 验证 TrySubmit 下整批要么全进要么全拒，
// 拒绝后不攥住任何名额。
func TestBatchAllOrNothingTrySubmit(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(3))
	defer rt.Close()

	block := make(chan struct{})
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			<-block
			return nil
		}}
	}
	// 占住 2 个名额，仅剩 1 个空位。
	if err := rt.Submit(context.Background(), mk("h1"), mk("h2")); err != nil {
		t.Fatalf("submit holders: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 提交 2 个一批，空位只有 1 个：必须整批拒绝。
	err := rt.TrySubmit(context.Background(), mk("b1"), mk("b2"))
	t.Logf("判定依据: queueCap=3 已占 2, TrySubmit 输入 2 个 -> 期望整批拒绝 ErrQueueFull, 实际 err=%v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull, 实际 %v", err)
	}
	if got := rt.Stats().Rejected; got != 2 {
		t.Fatalf("整批拒绝应按批内任务数计入 Rejected, 期望 2, 实际 %d", got)
	}

	// 拒绝后名额未被攥住：单个任务仍能立即进入。
	if err := rt.TrySubmit(context.Background(), mk("solo")); err != nil {
		t.Fatalf("整批拒绝后名额应仍可用, 实际 %v", err)
	}
	// 此时 3 个名额全满，再试单个也应立即拒。
	if err := rt.TrySubmit(context.Background(), mk("overflow")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("满员后 TrySubmit 期望 ErrQueueFull, 实际 %v", err)
	}
	close(block)
	results := rt.Wait(context.Background())
	var completed int
	for _, res := range results {
		if res.Status == StatusCompleted {
			completed++
		}
	}
	t.Logf("判定依据: 仅 h1/h2/solo 被接受, 期望 3 个 Completed, 实际 %d", completed)
	if completed != 3 {
		t.Fatalf("Completed 期望 3, 实际 %d", completed)
	}
}

// TestBatchWaitThenAdmitted 验证阻塞口味下整批等待、空位凑齐后整批进入，
// 且等待期间不攥住名额拖住别人。
func TestBatchWaitThenAdmitted(t *testing.T) {
	rt := New(
		WithQuotas(map[string]int{"cpu": 1}),
		WithQueueCapacity(2),
		WithAdmissionTimeout(5*time.Second),
	)
	defer rt.Close()

	block := make(chan struct{})
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			<-block
			return nil
		}}
	}
	if err := rt.Submit(context.Background(), mk("h1"), mk("h2")); err != nil {
		t.Fatalf("submit holders: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 后台提交 2 个一批：名额不足，进入等待。
	batchErr := make(chan error, 1)
	go func() {
		batchErr <- rt.Submit(context.Background(), mk("w1"), mk("w2"))
	}()
	time.Sleep(100 * time.Millisecond)

	// 等待期间不攥名额：此时没有空位，别的 TrySubmit 被立即拒而非被拖住。
	err := rt.TrySubmit(context.Background(), mk("probe"))
	t.Logf("判定依据: 大批等待中不持有名额, 但容量仍满 -> probe 期望 ErrQueueFull, 实际 %v", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull, 实际 %v", err)
	}

	// 释放占住的任务，等待中的批次应整批进入。
	close(block)
	if err := <-batchErr; err != nil {
		t.Fatalf("等待批次应整批被接受, 实际 %v", err)
	}
	rt.Wait(context.Background())
	stats := rt.Stats()
	t.Logf("统计: %+v (判定依据: 等待批次 2 个 + 占位 2 个 = Submitted 4, 等待批次计 1 次 AdmissionWaits)", stats)
	if stats.Submitted != 4 || stats.Completed != 4 {
		t.Fatalf("统计口径错误: %+v", stats)
	}
	if stats.AdmissionWaits != 1 {
		t.Fatalf("AdmissionWaits 期望 1, 实际 %d", stats.AdmissionWaits)
	}
}

// TestBatchNeverFitsBoundedReject 验证批次大于总容量时在有限时间内整批拒绝。
func TestBatchNeverFitsBoundedReject(t *testing.T) {
	rt := New(
		WithQuotas(map[string]int{"cpu": 1}),
		WithQueueCapacity(2),
		WithAdmissionTimeout(100*time.Millisecond),
	)
	defer rt.Close()

	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error { return nil }}
	}
	start := time.Now()
	err := rt.Submit(context.Background(), mk("x1"), mk("x2"), mk("x3"))
	elapse := time.Since(start)
	t.Logf("判定依据: queueCap=2 而批次=3 永远凑不齐, 期望 ~100ms 内整批拒绝 ErrAdmissionTimeout, 实际 err=%v 耗时=%v", err, elapse)
	if !errors.Is(err, ErrAdmissionTimeout) {
		t.Fatalf("期望 ErrAdmissionTimeout, 实际 %v", err)
	}
	if ReasonOf(err) != ReasonRejected {
		t.Fatalf("期望 ReasonRejected, 实际 %v", ReasonOf(err))
	}
	if elapse > 2*time.Second {
		t.Fatalf("未在有限时间内收敛, 耗时 %v", elapse)
	}
	if got := rt.Stats().Rejected; got != 3 {
		t.Fatalf("Rejected 期望 3, 实际 %d", got)
	}
	// 超时拒绝后名额未被攥住：后续小批可正常进入。
	if err := rt.Submit(context.Background(), mk("y1")); err != nil {
		t.Fatalf("超时拒绝后应能继续提交, 实际 %v", err)
	}
	rt.Wait(context.Background())
	if got := rt.Stats().Completed; got != 1 {
		t.Fatalf("Completed 期望 1, 实际 %d", got)
	}
}

// TestBatchSubmitCtxCancelReleasesNothing 验证阻塞等待被 ctx 取消时不留下任何名额占用。
func TestBatchSubmitCtxCancelReleasesNothing(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(1))
	defer rt.Close()

	block := make(chan struct{})
	if err := rt.Submit(context.Background(), Task{ID: "h", Resource: "cpu", Run: func(ctx context.Context) error {
		<-block
		return nil
	}}); err != nil {
		t.Fatalf("submit holder: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := rt.Submit(ctx, Task{ID: "w", Resource: "cpu", Run: func(context.Context) error { return nil }})
	t.Logf("判定依据: 容量满且 ctx 100ms 超时 -> 期望 ReasonCanceled, 实际 err=%v", err)
	if ReasonOf(err) != ReasonCanceled {
		t.Fatalf("期望 ReasonCanceled, 实际 %v (%v)", ReasonOf(err), err)
	}

	close(block)
	rt.Wait(context.Background())
	// 名额全部归还：新任务可立即进入。
	if err := rt.TrySubmit(context.Background(), Task{ID: "after", Resource: "cpu", Run: func(context.Context) error { return nil }}); err != nil {
		t.Fatalf("取消后名额应可用, 实际 %v", err)
	}
	rt.Wait(context.Background())
}
