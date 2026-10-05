package runtime

import (
	"context"
	"sort"
	"sync/atomic"
)

// semaphore 是单资源类别的有界额度信号量，附带在执行/等待仪表。
type semaphore struct {
	ch      chan struct{}
	limit   int
	running atomic.Int64 // 当前持有额度执行中的任务数
	waiting atomic.Int64 // 当前阻塞在等待额度的任务数
}

func newSemaphore(capacity int) *semaphore {
	return &semaphore{ch: make(chan struct{}, capacity), limit: capacity}
}

// acquire 获取一个额度，可被 ctx 取消。
func (s *semaphore) acquire(ctx context.Context) error {
	s.waiting.Add(1)
	defer s.waiting.Add(-1)
	select {
	case s.ch <- struct{}{}:
		s.running.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release 归还一个额度。
func (s *semaphore) release() {
	s.running.Add(-1)
	<-s.ch
}

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

// resourceNames 返回已配置额度的资源类别名（排序后，保证输出确定）。
func (q *quotas) resourceNames() []string {
	names := make([]string, 0, len(q.sems))
	for name := range q.sems {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
