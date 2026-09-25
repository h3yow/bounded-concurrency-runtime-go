package bcr

import (
	"context"
	"sync/atomic"
	"testing"
)

// BenchmarkIndependentTasks 测量无依赖任务在不同 slots 并发度下的
// 调度吞吐（任务体极短，主要反映调度/额度开销）。
//
// 复现：go test -run='^$' -bench=BenchmarkIndependentTasks -benchmem ./...
func BenchmarkIndependentTasks(b *testing.B) {
	for _, slots := range []int{1, 4, 16, 64} {
		b.Run("slots-"+itoa(slots), func(b *testing.B) {
			cfg := NewConfig()
			cfg.Resources = ResourceLimits{ResourceSlots: int64(slots)}
			fn := func(ctx context.Context) error { return nil }
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rt := New(cfg)
				tasks := make([]*Task, 64)
				for j := range tasks {
					tasks[j] = &Task{ID: "t" + itoa(i) + "-" + itoa(j), Fn: fn}
				}
				st, err := rt.RunTasks(context.Background(), tasks)
				if err != nil {
					b.Fatal(err)
				}
				if st.Completed != 64 {
					b.Fatalf("completed=%d", st.Completed)
				}
			}
		})
	}
}

// BenchmarkDAGChain 测量链式依赖（极端：全程并行度 1）调度开销。
func BenchmarkDAGChain(b *testing.B) {
	const n = 128
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 8}
	tasks := make([]*Task, n)
	for j := 0; j < n; j++ {
		id := itoa(j)
		if j == 0 {
			tasks[j] = &Task{ID: id, Fn: noopFn()}
		} else {
			tasks[j] = &Task{ID: id, Deps: []string{itoa(j - 1)}, Fn: noopFn()}
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rt := New(cfg)
		// RunTasks 会消费同一批 ID，但每次运行使用新 Runtime 即可复用定义。
		if _, err := rt.RunTasks(context.Background(), tasks); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRetryBackoff 测量重试路径（短退避）的额外开销。
func BenchmarkRetryBackoff(b *testing.B) {
	cfg := NewConfig()
	cfg.DefaultRetry = RetryPolicy{
		MaxAttempts: 2,
		Decider:     DefaultRetryDecider,
	}
	var calls int64
	fn := func(ctx context.Context) error {
		if atomic.AddInt64(&calls, 1)%2 == 1 {
			return errBoom
		}
		return nil
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rt := New(cfg)
		if _, err := rt.RunTasks(context.Background(), []*Task{
			{ID: "r" + itoa(i), Fn: fn},
		}); err != nil {
			b.Fatal(err)
		}
	}
}
