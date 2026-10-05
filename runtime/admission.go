package runtime

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrAdmissionTimeout 在等待准入名额超过上限后被整体拒绝时返回。
var ErrAdmissionTimeout = errors.New("runtime: admission wait timeout")

// DefaultAdmissionTimeout 是阻塞式提交等待准入名额的最坏收敛时间上界。
const DefaultAdmissionTimeout = 30 * time.Second

// admission 以计数方式管理准入名额，支持整批原子获取：
// 一批任务要么一次性拿到全部名额，要么一个都不拿，
// 等待期间不攥住任何名额，避免拖住其它提交方。
type admission struct {
	mu      sync.Mutex
	used    int
	cap     int
	changed chan struct{} // 容量 1，释放名额时触发，唤醒等待者重试
}

func newAdmission(capacity int) *admission {
	if capacity < 1 {
		capacity = 1
	}
	return &admission{cap: capacity, changed: make(chan struct{}, 1)}
}

// acquire 原子获取 n 个名额。
// tryOnly 为 true 时名额不足立即返回 ErrQueueFull；
// 否则等待名额释放，但最坏在 maxWait 内收敛，超时返回 ErrAdmissionTimeout；
// ctx 取消时返回 ctx.Err()。返回的 waited 表示是否发生过等待。
func (a *admission) acquire(ctx context.Context, n int, maxWait time.Duration, tryOnly bool) (waited bool, err error) {
	deadline := time.Now().Add(maxWait)
	for {
		a.mu.Lock()
		if a.cap-a.used >= n {
			a.used += n
			a.mu.Unlock()
			return waited, nil
		}
		a.mu.Unlock()

		if tryOnly {
			return false, ErrQueueFull
		}
		waited = true
		remain := time.Until(deadline)
		if remain <= 0 {
			return true, ErrAdmissionTimeout
		}
		timer := time.NewTimer(remain)
		select {
		case <-a.changed:
			timer.Stop()
		case <-timer.C:
			return true, ErrAdmissionTimeout
		case <-ctx.Done():
			timer.Stop()
			return true, ctx.Err()
		}
	}
}

// release 归还 n 个名额并唤醒等待者。
func (a *admission) release(n int) {
	if n <= 0 {
		return
	}
	a.mu.Lock()
	a.used -= n
	if a.used < 0 {
		a.used = 0
	}
	a.mu.Unlock()
	select {
	case a.changed <- struct{}{}:
	default:
	}
}
