package bcr

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// 关键判定日志：接纳拒绝、资源等待超时、重试判定、失败策略级联、
// 终态归类都必须带上输入（任务 ID、尝试数）与判定依据（reason）。
func TestKeyDecisionLogging(t *testing.T) {
	var buf bytes.Buffer
	cfg := NewConfig()
	cfg.Logger = NewLogger(&buf)
	rt := New(cfg)

	// 1) 非法依赖拒绝
	_, _ = rt.RunTasks(context.Background(), []*Task{
		{ID: "a", Deps: []string{"ghost"}, Fn: noopFn()},
	})
	if s := buf.String(); !strings.Contains(s, "dag rejected") ||
		!strings.Contains(s, "unreachable_dependency") {
		t.Fatalf("dag rejection log missing:\n%s", s)
	}

	buf.Reset()

	// 2) 入队拒绝：输入（task id、容量）与依据（queue_full）
	cfg2 := NewConfig()
	cfg2.MaxQueued = 1
	cfg2.Logger = NewLogger(&buf)
	rt2 := New(cfg2)
	_ = rt2.Submit(&Task{ID: "a", Fn: noopFn()})
	_ = rt2.Submit(&Task{ID: "b", Fn: noopFn()})
	if s := buf.String(); !strings.Contains(s, "admission reject task=b") ||
		!strings.Contains(s, "reason=queue_full") || !strings.Contains(s, "cap=1") {
		t.Fatalf("admission log missing input/basis:\n%s", s)
	}

	buf.Reset()

	// 3) 重试判定与终态日志
	cfg3 := NewConfig()
	cfg3.Logger = NewLogger(&buf)
	cfg3.DefaultRetry = RetryPolicy{
		MaxAttempts: 2,
		Backoff:     ExponentialBackoff{Base: time.Millisecond, Factor: 2},
		Decider:     DefaultRetryDecider,
	}
	rt3 := New(cfg3)
	var n int32
	_, _ = rt3.RunTasks(context.Background(), []*Task{{ID: "r", Fn: func(ctx context.Context) error {
		if atomic.AddInt32(&n, 1) == 1 {
			return errors.New("transient")
		}
		return nil
	}}})
	s := buf.String()
	for _, want := range []string{
		"attempt-end task=r attempt=1/2", // 输入：任务与尝试号
		"retry=true",                     // 判定依据
		"retry-wait task=r",
		"terminal task=r status=succeeded",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in:\n%s", want, s)
		}
	}

	buf.Reset()

	// 4) FailFast 级联取消依据
	cfg4 := NewConfig()
	cfg4.Resources = ResourceLimits{ResourceSlots: 1}
	cfg4.Logger = NewLogger(&buf)
	rt4 := New(cfg4)
	_, _ = rt4.RunTasks(context.Background(), []*Task{
		{ID: "bad", Fn: func(ctx context.Context) error { return errors.New("x") }},
		{ID: "rest", Fn: noopFn()},
	})
	s = buf.String()
	if !strings.Contains(s, "failure-policy=failfast") ||
		!strings.Contains(s, "status=failed") ||
		!strings.Contains(s, "status=canceled") {
		t.Fatalf("failfast cascade log missing:\n%s", s)
	}
}
