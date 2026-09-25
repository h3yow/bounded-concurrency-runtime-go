package bcr

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// 大量任务在全部在途时取消：Run 返回后 worker goroutine 必须全部退出，
// 调度循环不得残留（等待资源/退避的任务本就没有 goroutine）。
func TestNoGoroutineLeakAfterCancel(t *testing.T) {
	base := runtime.NumGoroutine()
	for iter := 0; iter < 3; iter++ {
		ctx, cancel := context.WithCancel(context.Background())
		cfg := NewConfig()
		cfg.Resources = ResourceLimits{ResourceSlots: 16}
		rt := New(cfg)
		var tasks []*Task
		for i := 0; i < 32; i++ {
			tasks = append(tasks, &Task{ID: "t" + itoa(iter) + "-" + itoa(i), Fn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}})
		}
		done := make(chan struct{})
		go func() { rt.RunTasks(ctx, tasks); close(done) }()
		time.Sleep(40 * time.Millisecond)
		cancel()
		<-done
	}
	// 给调度器/worker 的收尾留出时间。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= base+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: base=%d now=%d", base, runtime.NumGoroutine())
}
