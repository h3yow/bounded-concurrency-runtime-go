package runtime

import (
	"context"
	"testing"
)

// TestRejectInvalidDependencies 验证非法依赖被明确拒绝。
func TestRejectInvalidDependencies(t *testing.T) {
	ok := func(context.Context) error { return nil }
	cases := []struct {
		name  string
		tasks []Task
	}{
		{
			name: "未知依赖",
			tasks: []Task{
				{ID: "a", Deps: []string{"ghost"}, Run: ok},
			},
		},
		{
			name: "直接自环",
			tasks: []Task{
				{ID: "a", Deps: []string{"a"}, Run: ok},
			},
		},
		{
			name: "间接环",
			tasks: []Task{
				{ID: "a", Deps: []string{"c"}, Run: ok},
				{ID: "b", Deps: []string{"a"}, Run: ok},
				{ID: "c", Deps: []string{"b"}, Run: ok},
			},
		},
		{
			name: "重复ID",
			tasks: []Task{
				{ID: "a", Run: ok},
				{ID: "a", Run: ok},
			},
		},
		{
			name:  "空ID",
			tasks: []Task{{ID: "", Run: ok}},
		},
		{
			name:  "空Run",
			tasks: []Task{{ID: "a"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := New(WithQueueCapacity(8))
			defer rt.Close()
			err := rt.Submit(context.Background(), tc.tasks...)
			t.Logf("输入: %s (%d 个任务) -> 判定: err=%v", tc.name, len(tc.tasks), err)
			if err == nil {
				t.Fatalf("期望拒绝, 实际成功")
			}
			if ReasonOf(err) != ReasonInvalidDependency {
				t.Fatalf("期望 ReasonInvalidDependency, 实际 %v", ReasonOf(err))
			}
			// 整批拒绝：不得有任务被接受
			if got := rt.Stats().Submitted; got != 0 {
				t.Fatalf("非法批次不得有任务被接受, Submitted=%d", got)
			}
		})
	}
}

// TestCrossBatchDependency 验证跨批次依赖：后续批次可依赖已提交任务。
func TestCrossBatchDependency(t *testing.T) {
	rt := New(WithQueueCapacity(8))
	defer rt.Close()
	ok := func(context.Context) error { return nil }

	if err := rt.Submit(context.Background(), Task{ID: "base", Run: ok}); err != nil {
		t.Fatalf("submit base: %v", err)
	}
	if err := rt.Submit(context.Background(), Task{ID: "top", Deps: []string{"base"}, Run: ok}); err != nil {
		t.Fatalf("submit top: %v", err)
	}
	results := rt.Wait(context.Background())
	for _, res := range results {
		t.Logf("任务 %s -> %v", res.TaskID, res.Status)
		if res.Status != StatusCompleted {
			t.Fatalf("任务 %s 期望 Completed, 实际 %v", res.TaskID, res.Status)
		}
	}

	// 依赖已失败的任务：新任务应立即被跳过
	rt2 := New(WithQueueCapacity(8))
	defer rt2.Close()
	if err := rt2.Submit(context.Background(), Task{ID: "bad", Run: func(context.Context) error { return errBoom }}); err != nil {
		t.Fatalf("submit bad: %v", err)
	}
	rt2.Wait(context.Background())
	if err := rt2.Submit(context.Background(), Task{ID: "after-bad", Deps: []string{"bad"}, Run: ok}); err != nil {
		t.Fatalf("submit after-bad: %v", err)
	}
	results = rt2.Wait(context.Background())
	byID := map[string]Result{}
	for _, res := range results {
		byID[res.TaskID] = res
	}
	t.Logf("判定依据: after-bad 依赖已失败的 bad -> 期望 Skipped, 实际 %v", byID["after-bad"].Status)
	if byID["after-bad"].Status != StatusSkipped {
		t.Fatalf("期望 Skipped, 实际 %v", byID["after-bad"].Status)
	}
	if ReasonOf(byID["after-bad"].Err) != ReasonDependencyFailed {
		t.Fatalf("期望 ReasonDependencyFailed, 实际 %v", ReasonOf(byID["after-bad"].Err))
	}
}
