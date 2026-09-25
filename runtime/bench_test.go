package runtime

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkSchedule 测量不同并发度下 DAG 调度吞吐。
// 复现: go test ./runtime/ -bench BenchmarkSchedule -benchmem
func BenchmarkSchedule(b *testing.B) {
	for _, quota := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("quota=%d", quota), func(b *testing.B) {
			rt := New(WithQuotas(map[string]int{"cpu": quota}), WithQueueCapacity(4096))
			defer rt.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				prefix := fmt.Sprintf("r%d-", i)
				tasks := make([]Task, 0, 256)
				for j := 0; j < 256; j++ {
					id := fmt.Sprintf("%st%03d", prefix, j)
					var deps []string
					if j >= 4 {
						deps = []string{fmt.Sprintf("%st%03d", prefix, j-4)}
					}
					tasks = append(tasks, Task{
						ID: id, Resource: "cpu", Deps: deps,
						Run: func(context.Context) error { return nil },
					})
				}
				if err := rt.Submit(context.Background(), tasks...); err != nil {
					b.Fatalf("submit: %v", err)
				}
				rt.Wait(context.Background())
			}
		})
	}
}
