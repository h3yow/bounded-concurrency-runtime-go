package runtime

import "time"

// RetryPolicy 控制有限重试与指数退避。
type RetryPolicy struct {
	// MaxAttempts 为最大尝试次数（含首次），<=1 表示不重试。
	MaxAttempts int
	// BaseDelay 为首次重试前的退避基数。
	BaseDelay time.Duration
	// MaxDelay 为退避上限，<=0 表示不封顶。
	MaxDelay time.Duration
}

// Retryable 判定错误是否可重试。
// 仅任务执行失败（ReasonTaskFailed）可重试；
// 取消、拒绝、依赖失败等结构性错误不可重试。
func Retryable(err error) bool {
	return ReasonOf(err) == ReasonTaskFailed
}

// backoff 计算第 attempt 次重试前的退避时长（attempt 从 1 开始），
// 按 BaseDelay * 2^(attempt-1) 指数增长并封顶于 MaxDelay。
func (p RetryPolicy) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxDelay > 0 && d >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}
