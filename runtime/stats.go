package runtime

import "sync/atomic"

// Stats 汇总运行统计，口径与并发度无关：
// 每个任务恰好计入一个终态（Completed/Failed/Canceled/Rejected/Skipped 之一），
// Retried 记录重试次数（不含首次尝试）。
type Stats struct {
	Submitted int64
	Completed int64
	Failed    int64
	Retried   int64
	Canceled  int64
	Rejected  int64
	Skipped   int64
	// TotalDurationNanos 为所有已执行任务（含重试各次尝试）的累计耗时。
	TotalDurationNanos int64
}

// Terminal 返回到达终态的任务总数。
func (s Stats) Terminal() int64 {
	return s.Completed + s.Failed + s.Canceled + s.Rejected + s.Skipped
}

type statsCounter struct {
	submitted atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	retried   atomic.Int64
	canceled  atomic.Int64
	rejected  atomic.Int64
	skipped   atomic.Int64
	totalDur  atomic.Int64
}

func (c *statsCounter) snapshot() Stats {
	return Stats{
		Submitted:          c.submitted.Load(),
		Completed:          c.completed.Load(),
		Failed:             c.failed.Load(),
		Retried:            c.retried.Load(),
		Canceled:           c.canceled.Load(),
		Rejected:           c.rejected.Load(),
		Skipped:            c.skipped.Load(),
		TotalDurationNanos: c.totalDur.Load(),
	}
}
