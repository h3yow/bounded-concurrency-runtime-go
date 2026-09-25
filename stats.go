package bcr

import (
	"sort"
	"time"
)

// Stats 是一次运行（或截至当前）的聚合统计。
//
// 口径约定（不随并发度改变）：
//   - 终态互斥：Completed/Failed/Canceled/Rejected/Skipped 每个任务
//     恰好计入其中一项，五者之和 == Submitted；
//   - RetryAttempts 是"额外尝试次数"（sum(Attempts-1)），不是任务数，
//     因此不会与终态计数重复计数；
//   - TotalWaitDur：任务从就绪（依赖全部满足）到开始首次执行的排队/等
//     资源时长；只在任务实际被启动过或等待过资源时累计；
//   - TotalRunDur：所有尝试的执行时长之和（含失败尝试）。并发度改变的
//     是墙钟完成时间与单任务等待时长的分布，不改变这些口径的归类。
type Stats struct {
	Submitted int
	Completed int
	Failed    int
	Canceled  int
	Rejected  int
	Skipped   int

	RetryAttempts int

	TotalWaitDur time.Duration
	TotalRunDur  time.Duration

	ByTask map[string]TaskResult
}

// Results 返回按任务 ID 排序的终态明细，便于确定性断言。
func (s Stats) Results() []TaskResult {
	out := make([]TaskResult, 0, len(s.ByTask))
	for _, r := range s.ByTask {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}

// statsCollector 只在调度器所在的单一 goroutine 写入，
// 终态记录一次即冻结，杜绝重复计数与数据竞争。
type statsCollector struct {
	submitted int
	terminal  map[string]TaskResult
}

func newStatsCollector() *statsCollector {
	return &statsCollector{terminal: make(map[string]TaskResult)}
}

// record 写入任务终态；重复记录同一任务会被忽略（幂等），
// 返回最终保存的记录。
func (c *statsCollector) record(res TaskResult) TaskResult {
	if old, ok := c.terminal[res.TaskID]; ok {
		return old
	}
	c.terminal[res.TaskID] = res
	return res
}

// snapshot 生成聚合统计：所有计数均从终态明细汇总，
// 保证与 ByTask 永远自洽，不存在两套记账漂移。
func (c *statsCollector) snapshot() Stats {
	s := Stats{
		Submitted: c.submitted,
		ByTask:    make(map[string]TaskResult, len(c.terminal)),
	}
	for id, r := range c.terminal {
		switch r.Status {
		case StatusSucceeded:
			s.Completed++
		case StatusFailed:
			s.Failed++
		case StatusCanceled:
			s.Canceled++
		case StatusRejected:
			s.Rejected++
		case StatusSkipped:
			s.Skipped++
		}
		if r.Attempts > 1 {
			s.RetryAttempts += r.Attempts - 1
		}
		s.TotalWaitDur += r.WaitDur
		s.TotalRunDur += r.RunDur
		s.ByTask[id] = r
	}
	return s
}
