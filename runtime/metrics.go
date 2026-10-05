package runtime

// ResourceMetrics 是单个资源类别的实时观察口径。
type ResourceMetrics struct {
	// Running 为当前持有该类别额度、正在执行的任务数。
	Running int64
	// Waiting 为当前阻塞在等待该类别额度的任务数。
	Waiting int64
}

// Metrics 是运行时的运维观察快照：实时水位 + 累计计数。
type Metrics struct {
	// Resources 按资源类别给出实时水位（仅含配置了额度的类别）。
	Resources map[string]ResourceMetrics
	// Rejected 累计因容量不足（队列满/准入超时/已关闭）被拒绝的任务数。
	Rejected int64
	// AdmissionWaits 累计发生准入等待的提交批次数（非立即获得全部令牌）。
	AdmissionWaits int64
	// QuotaWaits 累计发生额度等待的执行尝试次数（获取额度时曾阻塞）。
	QuotaWaits int64
	// Panicked 累计被隔离收敛的 panic 任务数（与 Stats.Panicked 同口径）。
	Panicked int64
}

// Metrics 返回当前观察快照。
func (r *Runtime) Metrics() Metrics {
	// TODO: 实现快照采集
	return Metrics{}
}
