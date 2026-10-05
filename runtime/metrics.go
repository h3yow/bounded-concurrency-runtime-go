package runtime

// ResourceMetrics 是单个资源类别的实时观察口径。
type ResourceMetrics struct {
	// Running 为当前持有该类别额度、正在执行的任务数。
	Running int64
	// Waiting 为当前阻塞在等待该类别额度的任务数。
	Waiting int64
}

// Metrics 是运行时的运维观察快照：实时水位 + 累计计数。
// 其中 Rejected 与 Panicked 和 Stats 同名字段同口径，
// 与并发度、容量配置无关。
type Metrics struct {
	// Resources 按资源类别给出实时水位（仅含配置了额度的类别）。
	Resources map[string]ResourceMetrics
	// Rejected 累计因容量不足（队列满/准入超时）或已关闭被拒绝的任务数。
	Rejected int64
	// AdmissionWaits 累计发生准入等待的提交批次数（未立即获得全部令牌）。
	AdmissionWaits int64
	// QuotaWaits 累计发生阻塞等待的额度获取次数（一次获取只计一次）。
	QuotaWaits int64
	// Panicked 累计被隔离收敛的 panic 任务数（与 Stats.Panicked 同口径）。
	Panicked int64
}

// Metrics 返回当前观察快照。实时水位为瞬时值，累计计数单调不减。
func (r *Runtime) Metrics() Metrics {
	m := Metrics{
		Resources:      make(map[string]ResourceMetrics, len(r.quotas.sems)),
		Rejected:       r.stats.rejected.Load(),
		AdmissionWaits: r.admissionWaits.Load(),
		QuotaWaits:     r.quotas.waits.Load(),
		Panicked:       r.stats.panicked.Load(),
	}
	for name, sem := range r.quotas.sems {
		m.Resources[name] = ResourceMetrics{
			Running: sem.running.Load(),
			Waiting: sem.waiting.Load(),
		}
	}
	return m
}
