package bcr

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestDAGUnreachableDependency(t *testing.T) {
	rt := New(NewConfig())
	_, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "a", Deps: []string{"ghost"}, Fn: noopFn()},
	})
	if !errors.Is(err, ErrUnreachableDep) {
		t.Fatalf("want ErrUnreachableDep, got %v", err)
	}
}

func TestDAGDuplicateID(t *testing.T) {
	rt := New(NewConfig())
	_, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "a", Fn: noopFn()},
		{ID: "a", Fn: noopFn()},
	})
	if !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("want ErrDuplicateID, got %v", err)
	}
}

func TestDAGSelfDependency(t *testing.T) {
	rt := New(NewConfig())
	_, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "a", Deps: []string{"a"}, Fn: noopFn()},
	})
	if !errors.Is(err, ErrCircularDependency) {
		t.Fatalf("want cycle, got %v", err)
	}
}

func TestDAGLongCycle(t *testing.T) {
	rt := New(NewConfig())
	_, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "a", Deps: []string{"c"}, Fn: noopFn()},
		{ID: "b", Deps: []string{"a"}, Fn: noopFn()},
		{ID: "c", Deps: []string{"b"}, Fn: noopFn()},
		{ID: "d", Fn: noopFn()},
	})
	if !errors.Is(err, ErrCircularDependency) {
		t.Fatalf("want cycle, got %v", err)
	}
}

// 非法依赖时任何任务都不应被启动。
func TestDAGRejectedRunsNothing(t *testing.T) {
	var ran int32
	rt := New(NewConfig())
	_, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "ok", Fn: func(ctx context.Context) error { ran++; return nil }},
		{ID: "bad", Deps: []string{"missing"}, Fn: noopFn()},
	})
	if err == nil {
		t.Fatal("expected rejection")
	}
	if ran != 0 {
		t.Fatalf("ran=%d, no task should run when DAG invalid", ran)
	}
}

// 菱形拓扑 a->b,a->c,b&c->d：d 必须在 b、c 都成功后只执行一次。
func TestDAGDiamondRunsOnceAfterAllDeps(t *testing.T) {
	var mu sync.Mutex
	var dRuns int
	bDone := make(chan struct{})
	cDone := make(chan struct{})
	cfg := NewConfig()
	cfg.Resources = ResourceLimits{ResourceSlots: 4}
	rt := New(cfg)
	tasks := []*Task{
		{ID: "a", Fn: noopFn()},
		{ID: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) error {
			close(bDone)
			return nil
		}},
		{ID: "c", Deps: []string{"a"}, Fn: func(ctx context.Context) error {
			<-bDone // 强制 b 先于 c 完成，d 必须等两者
			close(cDone)
			return nil
		}},
		{ID: "d", Deps: []string{"b", "c"}, Fn: func(ctx context.Context) error {
			mu.Lock()
			dRuns++
			mu.Unlock()
			select {
			case <-cDone:
			default:
				t.Error("d started before c finished")
			}
			return nil
		}},
	}
	st, err := rt.RunTasks(context.Background(), tasks)
	if err != nil {
		t.Fatal(err)
	}
	if dRuns != 1 || st.Completed != 4 {
		t.Fatalf("dRuns=%d completed=%d", dRuns, st.Completed)
	}
}

// 重复列出同一依赖应去重，不导致计数错误。
func TestDAGDuplicateDepEdgeDedup(t *testing.T) {
	rt := New(NewConfig())
	st, err := rt.RunTasks(context.Background(), []*Task{
		{ID: "a", Fn: noopFn()},
		{ID: "b", Deps: []string{"a", "a"}, Fn: noopFn()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Completed != 2 {
		t.Fatalf("completed=%d", st.Completed)
	}
}
