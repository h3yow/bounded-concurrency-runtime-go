package runtime

import (
	"context"
	"sync/atomic"
)

// semaphore 是单资源类别的有界额度信号量，附带观察口径计数。
type semaphore struct {
	ch      chan struct{}
	running atomic.Int64 // 当前持有额度执行中的任务数
	waiting atomic.Int64 // 当前阻塞等待额度的任务数
}

func newSemaphore(capacity int) *semaphore {
	return &semaphore{ch: make(chan struct{}, capacity)}
}

// acquire 获取一个额度，可被 ctx 取消。
func (s *semaphore) acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release 归还一个额度。
func (s *semaphore) release() { <-s.ch }

// quotas 管理多个资源类别的额度。
type quotas struct {
	sems map[string]*semaphore
	// waits 累计发生阻塞等待的获取次数（一次获取只计一次）。
	waits atomic.Int64
}

func newQuotas(limits map[string]int) *quotas {
	q := &quotas{sems: make(map[string]*semaphore, len(limits))}
	for resource, limit := range limits {
		if limit <= 0 {
			limit = 1
		}
		q.sems[resource] = newSemaphore(limit)
	}
	return q
}

// acquire 获取某资源类别的一个额度；未配置的类别视为无限。
// 返回的 release 必须且只能调用一次。
func (q *quotas) acquire(ctx context.Context, resource string) (release func(), err error) {
	sem, ok := q.sems[resource]
	if !ok {
		return func() {}, nil
	}
	// 立即可得则不计等待；否则进入等待口径并累计一次等待事件。
	select {
	case sem.ch <- struct{}{}:
		sem.running.Add(1)
	default:
		// 先累计等待事件再抬水位：观察到 Waiting>0 时 QuotaWaits 必然已计入。
		q.waits.Add(1)
		sem.waiting.Add(1)
		if err := sem.acquire(ctx); err != nil {
			sem.waiting.Add(-1)
			return nil, err
		}
		sem.waiting.Add(-1)
		sem.running.Add(1)
	}
	var once bool
	return func() {
		if !once {
			once = true
			sem.running.Add(-1)
			sem.release()
		}
	}, nil
}
