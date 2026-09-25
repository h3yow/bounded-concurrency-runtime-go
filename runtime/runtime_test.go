package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestBasicDAG 验证依赖顺序执行与结果统计。
func TestBasicDAG(t *testing.T) {
	rt := New(WithQuotas(map[string]int{"cpu": 2}), WithQueueCapacity(16))
	defer rt.Close()

	var order atomic.Int64
	ch := make(chan string, 4)

	mk := func(id string, deps ...string) Task {
		return Task{
			ID:       id,
			Resource: "cpu",
			Deps:     deps,
			Run: func(ctx context.Context) error {
				order.Add(1)
				ch <- id
				return nil
			},
		}
	}
	err := rt.Submit(context.Background(),
		mk("a"), mk("b", "a"), mk("c", "a"), mk("d", "b", "c"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	results := rt.Wait(context.Background())
	close(ch)
	seq := make([]string, 0, 4)
	for id := range ch {
		seq = append(seq, id)
	}
	pos := map[string]int{}
	for i, id := range seq {
		pos[id] = i
	}
	t.Logf("执行顺序: %v (判定依据: a 必须先于 b/c, b/c 必须先于 d)", seq)
	if pos["a"] > pos["b"] || pos["a"] > pos["c"] || pos["b"] > pos["d"] || pos["c"] > pos["d"] {
		t.Fatalf("依赖顺序被违反: %v", seq)
	}

	t.Logf("判定依据: 输入为 4 任务 DAG(a<-b,a<-c,b+c<-d), 期望全部 Completed")
	for _, res := range results {
		t.Logf("任务 %s -> status=%v attempts=%d err=%v", res.TaskID, res.Status, res.Attempts, res.Err)
		if res.Status != StatusCompleted {
			t.Fatalf("任务 %s 期望 Completed, 实际 %v (err=%v)", res.TaskID, res.Status, res.Err)
		}
	}
	stats := rt.Stats()
	t.Logf("统计: %+v", stats)
	if stats.Completed != 4 || stats.Terminal() != 4 {
		t.Fatalf("统计口径错误: %+v", stats)
	}
}

// TestConsistencyAcrossConcurrency 验证同一输入在不同并发度下结果与统计一致。
func TestConsistencyAcrossConcurrency(t *testing.T) {
	const n = 60
	build := func() []Task {
		tasks := make([]Task, 0, n)
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("t%02d", i)
			var deps []string
			if i >= 3 {
				deps = []string{fmt.Sprintf("t%02d", i-3)}
			}
			tasks = append(tasks, Task{
				ID:       id,
				Resource: "cpu",
				Deps:     deps,
				Run: func(ctx context.Context) error {
					time.Sleep(time.Millisecond)
					return nil
				},
			})
		}
		return tasks
	}

	var baseline []Result
	var baselineStats Stats
	for _, quota := range []int{1, 2, 4, 16} {
		rt := New(WithQuotas(map[string]int{"cpu": quota}), WithQueueCapacity(n))
		if err := rt.Submit(context.Background(), build()...); err != nil {
			t.Fatalf("quota=%d submit: %v", quota, err)
		}
		results := rt.Wait(context.Background())
		stats := rt.Stats()
		rt.Close()

		t.Logf("quota=%d: completed=%d failed=%d terminal=%d (判定依据: 与并发度无关)",
			quota, stats.Completed, stats.Failed, stats.Terminal())
		if baseline == nil {
			baseline, baselineStats = results, stats
			continue
		}
		if stats.Completed != baselineStats.Completed ||
			stats.Failed != baselineStats.Failed ||
			stats.Terminal() != baselineStats.Terminal() {
			t.Fatalf("quota=%d 统计与基线不一致: got %+v want %+v", quota, stats, baselineStats)
		}
		for i := range results {
			if results[i].Status != baseline[i].Status {
				t.Fatalf("quota=%d 任务 %s 状态 %v, 基线 %v",
					quota, results[i].TaskID, results[i].Status, baseline[i].Status)
			}
		}
	}
}
