package bcr

// ResourceWeights 是任务对各资源类别的需求量（权重）。
type ResourceWeights map[string]int64

// ResourceLimits 是各资源类别的总额度（容量）。
type ResourceLimits map[string]int64

// ResourceSlots 是内建资源类别：每个任务默认占用 1 个执行槽，
// 从而把"最大并发度（MaxInFlight）"统一纳入有界资源模型。
const ResourceSlots = "slots"

// resourcePool 是固定额度、多资源类别的计数信号量集合。
//
// 关键设计：只提供非阻塞 tryAcquire。是否等待、等待多久完全由中央
// 调度器以事件方式决定，因此等待资源的任务不持有任何额度、不占用
// goroutine，也就不会出现"持票等依赖"导致的死锁或额度饥饿。
type resourcePool struct {
	limits    map[string]int64
	available map[string]int64
}

// newResourcePool 构造额度池；slots 类别缺省为 1（严格串行）。
func newResourcePool(limits ResourceLimits) *resourcePool {
	p := &resourcePool{
		limits:    make(map[string]int64, len(limits)+1),
		available: make(map[string]int64, len(limits)+1),
	}
	for k, v := range limits {
		if v < 0 {
			v = 0
		}
		p.limits[k] = v
		p.available[k] = v
	}
	if _, ok := p.limits[ResourceSlots]; !ok {
		p.limits[ResourceSlots] = 1
		p.available[ResourceSlots] = 1
	}
	return p
}

// fits 判断需求是否在静态容量范围内。需求超过容量的任务永远无法被
// 调度，必须在登记阶段被拒绝，而不是静默挂死。
func (p *resourcePool) fits(w ResourceWeights) (string, bool) {
	for name, need := range w {
		limit, ok := p.limits[name]
		if !ok || need > limit {
			return name, false
		}
	}
	return "", true
}

// tryAcquire 尝试一次性获取全部所需资源。成功返回 true 并扣减额度；
// 失败时不产生任何部分扣减（all-or-nothing），返回 false。
func (p *resourcePool) tryAcquire(w ResourceWeights) bool {
	for name, need := range w {
		avail, ok := p.available[name]
		if !ok || need > avail {
			return false
		}
	}
	for name, need := range w {
		p.available[name] -= need
	}
	return true
}

// release 归还资源。只在任务终态或尝试结束需要退避时调用。
func (p *resourcePool) release(w ResourceWeights) {
	for name, need := range w {
		p.available[name] += need
		if cur, limit := p.available[name], p.limits[name]; cur > limit {
			// 不应发生：归还超过容量说明记账错误，钳制并暴露问题。
			p.available[name] = limit
		}
	}
}

// snapshot 返回各类别当前可用额度（用于日志与测试断言）。
func (p *resourcePool) snapshot() map[string]int64 {
	out := make(map[string]int64, len(p.available))
	for k, v := range p.available {
		out[k] = v
	}
	return out
}
