package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestMetricsRunningWaiting 验证实时水位：能看到某资源类别当下
// 在执行多少、在等待多少，且等待事件被累计。
func TestMetricsRunningWaiting(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(4))
	defer rt.Close()

	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			entered <- struct{}{}
			<-block
			return nil
		}}
	}
	if err := rt.Submit(context.Background(), mk("a"), mk("b")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-entered // a 已持有额度执行中；b 应阻塞在等额度

	deadline := time.Now().Add(2 * time.Second)
	for {
		m := rt.Metrics()
		cpu := m.Resources["cpu"]
		if cpu.Running == 1 && cpu.Waiting == 1 {
			t.Logf("判定依据: quota=1, a 在执行 b 在等 -> Running=%d Waiting=%d QuotaWaits=%d",
				cpu.Running, cpu.Waiting, m.QuotaWaits)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("水位未达预期: %+v", cpu)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := rt.Metrics().QuotaWaits; got < 1 {
		t.Fatalf("QuotaWaits 期望 >=1, 实际 %d", got)
	}

	close(block)
	rt.Wait(context.Background())
	m := rt.Metrics()
	cpu := m.Resources["cpu"]
	t.Logf("收敛后: Running=%d Waiting=%d", cpu.Running, cpu.Waiting)
	if cpu.Running != 0 || cpu.Waiting != 0 {
		t.Fatalf("全部终态后水位应归零, 实际 %+v", cpu)
	}
}

// TestMetricsConsistencyAcrossConcurrency 验证同一混合输入（成功/业务失败/
// panic/依赖跳过/拒绝）在不同并发与容量下，终态口径与 Stats 完全一致。
func TestMetricsConsistencyAcrossConcurrency(t *testing.T) {
	build := func() []Task {
		return []Task{
			{ID: "ok1", Resource: "cpu", Run: func(context.Context) error { return nil }},
			{ID: "ok2", Resource: "io", Run: func(context.Context) error {
				time.Sleep(time.Millisecond)
				return nil
			}},
			{ID: "biz-fail", Resource: "cpu", Run: func(context.Context) error { return errors.New("biz") }},
			{ID: "boom", Resource: "io", Run: func(context.Context) error { panic("plugin") }},
			{ID: "dep-of-boom", Deps: []string{"boom"}, Run: func(context.Context) error { return nil }},
			{ID: "dep-of-fail", Deps: []string{"biz-fail"}, Run: func(context.Context) error { return nil }},
		}
	}

	type cfg struct {
		cpu, io, queueCap int
	}
	var baseline *Stats
	for _, c := range []cfg{{1, 1, 8}, {2, 4, 16}, {8, 8, 64}} {
		rt := New(WithQuotas(map[string]int{"cpu": c.cpu, "io": c.io}), WithQueueCapacity(c.queueCap))
		if err := rt.Submit(context.Background(), build()...); err != nil {
			t.Fatalf("cfg=%+v submit: %v", c, err)
		}
		rt.Wait(context.Background())
		s := rt.Stats()
		m := rt.Metrics()
		rt.Close()

		t.Logf("cfg=%+v: stats=%+v", c, s)
		t.Logf("cfg=%+v: metrics rejected=%d panicked=%d admissionWaits=%d quotaWaits=%d",
			c, m.Rejected, m.Panicked, m.AdmissionWaits, m.QuotaWaits)

		// 观察口径与统计口径必须对得上
		if m.Panicked != s.Panicked {
			t.Fatalf("cfg=%+v Metrics.Panicked=%d != Stats.Panicked=%d", c, m.Panicked, s.Panicked)
		}
		if m.Rejected != s.Rejected {
			t.Fatalf("cfg=%+v Metrics.Rejected=%d != Stats.Rejected=%d", c, m.Rejected, s.Rejected)
		}
		if s.Terminal() != s.Submitted {
			t.Fatalf("cfg=%+v Terminal=%d != Submitted=%d", c, s.Terminal(), s.Submitted)
		}
		// 收敛后水位归零
		for name, rm := range m.Resources {
			if rm.Running != 0 || rm.Waiting != 0 {
				t.Fatalf("cfg=%+v 资源 %s 水位未归零: %+v", c, name, rm)
			}
		}

		cur := Stats{
			Submitted: s.Submitted, Completed: s.Completed, Failed: s.Failed,
			Canceled: s.Canceled, Rejected: s.Rejected, Skipped: s.Skipped, Panicked: s.Panicked,
		}
		if baseline == nil {
			base := cur
			baseline = &base
			continue
		}
		if cur != *baseline {
			t.Fatalf("cfg=%+v 终态口径 %+v 与基线 %+v 不一致（语义不得随并发度变化）", c, cur, *baseline)
		}
	}
	if baseline.Panicked != 1 || baseline.Failed != 1 || baseline.Skipped != 2 || baseline.Completed != 2 {
		t.Fatalf("基线终态不符合输入预期: %+v", *baseline)
	}
}

// TestMetricsRejectedAndAdmissionWaits 验证容量不足导致的拒绝与准入等待
// 被分别累计，且与 Stats.Rejected 同口径。
func TestMetricsRejectedAndAdmissionWaits(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(1),
		WithSubmitTimeout(30*time.Millisecond))
	defer rt.Close()

	block := make(chan struct{})
	if err := rt.Submit(context.Background(), Task{
		ID: "o1", Resource: "cpu", Run: func(ctx context.Context) error { <-block; return nil },
	}); err != nil {
		t.Fatalf("submit occupant: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	tryErr := rt.TrySubmit(context.Background(), Task{ID: "r1", Run: func(context.Context) error { return nil }})
	waitErr := rt.Submit(context.Background(), Task{ID: "w1", Run: func(context.Context) error { return nil }})
	t.Logf("判定依据: queueCap=1 被占满 -> TrySubmit err=%v, Submit(超时) err=%v", tryErr, waitErr)
	if !errors.Is(tryErr, ErrQueueFull) || !errors.Is(waitErr, ErrAdmissionTimeout) {
		t.Fatalf("拒绝原因不符合预期: try=%v wait=%v", tryErr, waitErr)
	}

	m := rt.Metrics()
	t.Logf("metrics: %+v", m)
	if m.Rejected != 2 || rt.Stats().Rejected != 2 {
		t.Fatalf("Rejected 期望 2, metrics=%d stats=%d", m.Rejected, rt.Stats().Rejected)
	}
	if m.AdmissionWaits != 1 {
		t.Fatalf("AdmissionWaits 期望 1（仅 Submit 阻塞等待过）, 实际 %d", m.AdmissionWaits)
	}
	close(block)
	rt.Wait(context.Background())
}
