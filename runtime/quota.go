package runtime

import "context"

// semaphore 是单资源类别的有界额度信号量。
type semaphore struct {
	ch chan struct{}
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
	if err := sem.acquire(ctx); err != nil {
		return nil, err
	}
	var once bool
	return func() {
		if !once {
			once = true
			sem.release()
		}
	}, nil
}
