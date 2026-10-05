package runtime

// ResourceObservation 是单个资源类别的实时观察口径。
type ResourceObservation struct {
	// Resource 为资源类别名。
	Resource string
	// Limit 为配置的额度上限。
	Limit int
	// Running 为当前持有该类别额度、正在执行的任务数。
	Running int64
	// Waiting 为当前阻塞在等待该类别额度的任务数。
	Waiting int64
}

// Observation 是运行时的实时观察快照。
// 终态汇总口径（Stats）与并发度、容量配置无关：
// 同一份输入无论用多大并发和容量去跑，各终态计数一致。
type Observation struct {
	// Pending 为已准入但尚未到达终态的任务数（含等待依赖与执行中）。
	Pending int64
	// Resources 为各资源类别的实时仪表，按类别名排序。
	Resources []ResourceObservation
	// Stats 为累计统计：Rejected/AdmissionWaits 记录因容量不足
	// 被拒绝或等待的次数，Panics 记录已收敛的崩溃数量。
	Stats Stats
}

// Observe 返回当前观察快照，可随时调用。
func (r *Runtime) Observe() Observation {
	r.mu.Lock()
	pending := int64(r.pending)
	r.mu.Unlock()

	names := r.quotas.resourceNames()
	obs := Observation{
		Pending:   pending,
		Resources: make([]ResourceObservation, 0, len(names)),
		Stats:     r.stats.snapshot(),
	}
	for _, name := range names {
		sem := r.quotas.sems[name]
		obs.Resources = append(obs.Resources, ResourceObservation{
			Resource: name,
			Limit:    sem.limit,
			Running:  sem.running.Load(),
			Waiting:  sem.waiting.Load(),
		})
	}
	return obs
}
