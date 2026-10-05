package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestObserveLiveGauges 验证运行中能观察到各类别的在执行/在等待数量。
func TestObserveLiveGauges(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 1}), WithQueueCapacity(8))
	defer rt.Close()

	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(ctx context.Context) error {
			entered <- struct{}{}
			<-release
			return nil
		}}
	}
	if err := rt.Submit(context.Background(), mk("t1"), mk("t2"), mk("t3")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-entered // t1 进入执行，t2/t3 排队等额度
	deadline := time.Now().Add(2 * time.Second)
	var obs Observation
	for {
		obs = rt.Observe()
		if len(obs.Resources) == 1 && obs.Resources[0].Running == 1 && obs.Resources[0].Waiting >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待仪表收敛超时, 最后快照: %+v", obs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	res := obs.Resources[0]
	t.Logf("实时快照: resource=%s limit=%d running=%d waiting=%d pending=%d (判定依据: cpu 额度=1, 3 个任务抢 -> running=1, waiting>=1, pending=3)",
		res.Resource, res.Limit, res.Running, res.Waiting, obs.Pending)
	if res.Resource != "cpu" || res.Limit != 1 {
		t.Fatalf("资源口径错误: %+v", res)
	}
	if res.Running != 1 {
		t.Fatalf("Running 期望 1, 实际 %d", res.Running)
	}
	if obs.Pending != 3 {
		t.Fatalf("Pending 期望 3, 实际 %d", obs.Pending)
	}

	close(release)
	rt.Wait(context.Background())
	obs = rt.Observe()
	t.Logf("收敛后快照: running=%d waiting=%d pending=%d", obs.Resources[0].Running, obs.Resources[0].Waiting, obs.Pending)
	if obs.Resources[0].Running != 0 || obs.Resources[0].Waiting != 0 || obs.Pending != 0 {
		t.Fatalf("收敛后仪表应归零: %+v", obs)
	}
	if obs.Stats.Completed != 3 {
		t.Fatalf("Completed 期望 3, 实际 %d", obs.Stats.Completed)
	}
}

// TestObserveConsistencyAcrossConcurrency 验证同一份输入在不同并发与容量下，
// 观察口径（终态统计）完全一致。
func TestObserveConsistencyAcrossConcurrency(t *testing.T) {
	const n = 40
	configs := []struct {
		quota    int
		queueCap int
	}{{1, n}, {4, n}, {16, n}, {n, n}}

	type snapshot struct {
		completed, failed, skipped, canceled, panics, retried int64
	}
	var want *snapshot

	for _, cfg := range configs {
		rt := New(
			WithQuotas(map[string]int{"cpu": cfg.quota}),
			WithQueueCapacity(cfg.queueCap),
			WithFailurePolicy(Isolate),
		)
		var tasks []Task
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("t%02d", i)
			var deps []string
			if i >= 4 {
				deps = []string{fmt.Sprintf("t%02d", i-4)}
			}
			run := func(ctx context.Context) error { return nil }
			switch id {
			case "t10":
				run = func(ctx context.Context) error { return fmt.Errorf("biz fail") }
			case "t20":
				run = func(ctx context.Context) error { panic("boom") }
			}
			tasks = append(tasks, Task{ID: id, Resource: "cpu", Deps: deps, Run: run})
		}
		if err := rt.Submit(context.Background(), tasks...); err != nil {
			t.Fatalf("submit: %v", err)
		}
		rt.Wait(context.Background())
		s := rt.Observe().Stats
		got := snapshot{s.Completed, s.Failed, s.Skipped, s.Canceled, s.Panics, s.Retried}
		t.Logf("quota=%2d queueCap=%2d -> %+v", cfg.quota, cfg.queueCap, got)
		if want == nil {
			want = &got
			// 判定依据: t10 业务失败 + t20 崩溃 = 2 个失败; t20 计入 Panics;
			// t10/t20 各自下游链(间隔 4: t10->t14->...->t38 共 8 个, t20->...->t36 共 5 个)被跳过。
			if got.failed != 2 || got.panics != 1 || got.canceled != 0 {
				t.Fatalf("基准口径错误: %+v", got)
			}
			if got.completed+got.failed+got.skipped != n {
				t.Fatalf("终态总数应等于任务数 %d: %+v", n, got)
			}
		} else if got != *want {
			t.Fatalf("并发/容量变化导致口径漂移: 期望 %+v, 实际 %+v", *want, got)
		}
		rt.Close()
	}
}

// TestObserveRejectionAndWaitCounts 验证容量不足导致的拒绝与等待被累计。
func TestObserveRejectionAndWaitCounts(t *testing.T) {
	rt := New(
		WithQuotas(map[string]int{"cpu": 1}),
		WithQueueCapacity(1),
		WithAdmissionTimeout(80*time.Millisecond),
	)
	defer rt.Close()

	block := make(chan struct{})
	if err := rt.Submit(context.Background(), Task{ID: "h", Resource: "cpu", Run: func(ctx context.Context) error {
		<-block
		return nil
	}}); err != nil {
		t.Fatalf("submit holder: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	mk := func(id string) Task {
		return Task{ID: id, Resource: "cpu", Run: func(context.Context) error { return nil }}
	}
	// 立即拒：1 个任务。
	if err := rt.TrySubmit(context.Background(), mk("r1")); err == nil {
		t.Fatal("期望拒绝")
	}
	// 等满再拒：2 个一批，超时整批拒绝。
	if err := rt.Submit(context.Background(), mk("w1"), mk("w2")); err == nil {
		t.Fatal("期望超时拒绝")
	}
	obs := rt.Observe()
	t.Logf("判定依据: TrySubmit 拒 1 + 超时拒 2 -> Rejected=3; 等待发生 1 次 -> AdmissionWaits=1, 实际 %+v", obs.Stats)
	if obs.Stats.Rejected != 3 {
		t.Fatalf("Rejected 期望 3, 实际 %d", obs.Stats.Rejected)
	}
	if obs.Stats.AdmissionWaits != 1 {
		t.Fatalf("AdmissionWaits 期望 1, 实际 %d", obs.Stats.AdmissionWaits)
	}
	close(block)
	rt.Wait(context.Background())
}
