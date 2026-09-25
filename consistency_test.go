package bcr

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// 构建一个含多种形态的确定性任务集：菱形、链、扇出/扇入、
// 固定次数失败后成功的重试任务、永久失败任务（Isolate）及其下游。
// 每个任务把确定性结果写入共享表（下游在依赖完成后读取上游结果，
// 同时也验证了 happens-before）。
func buildDeterministicSet(out map[string]int, failCounts map[string]*int32, mu *sync.Mutex) []*Task {
	put := func(id string, v int) {
		mu.Lock()
		out[id] = v
		mu.Unlock()
	}
	get := func(id string) int {
		mu.Lock()
		defer mu.Unlock()
		return out[id]
	}
	mk := func(id string, deps []string, f func() error) *Task {
		return &Task{ID: id, Deps: deps, Fn: func(ctx context.Context) error { return f() }}
	}
	return []*Task{
		mk("root", nil, func() error { put("root", 1); return nil }),
		mk("l1", []string{"root"}, func() error { put("l1", get("root")+1); return nil }),
		mk("r1", []string{"root"}, func() error { put("r1", get("root")+10); return nil }),
		mk("join", []string{"l1", "r1"}, func() error { put("join", get("l1")+get("r1")); return nil }),
		mk("chain", []string{"join"}, func() error { put("chain", get("join")*2); return nil }),
		// 重试 2 次后成功（确定性，与并发度无关）
		mk("flaky", nil, func() error {
			n := incFail(failCounts, "flaky")
			if n < 3 {
				return errors.New("transient")
			}
			put("flaky", 42)
			return nil
		}),
		// 永久失败
		mk("dead", nil, func() error { return errors.New("permanent") }),
		mk("dead-child", []string{"dead"}, func() error { put("dead-child", 1); return nil }),
		mk("dead-grand", []string{"dead-child"}, func() error { put("dead-grand", 1); return nil }),
		// 独立扇出
		mk("fan0", nil, func() error { put("fan0", 3); return nil }),
		mk("fan1", nil, func() error { put("fan1", 5); return nil }),
		mk("fanin", []string{"fan0", "fan1", "flaky"}, func() error {
			put("fanin", get("fan0")+get("fan1")+get("flaky"))
			return nil
		}),
	}
}

func incFail(m map[string]*int32, id string) int32 {
	mu := m[id]
	if mu == nil {
		return 0
	}
	return addI32(mu, 1)
}

func addI32(p *int32, d int32) int32 {
	for {
		old := loadI32(p)
		if casI32(p, old, old+d) {
			return old + d
		}
	}
}

// 用原子包直接实现，避免多余封装。
func loadI32(p *int32) int32               { return atomicLoad(p) }
func casI32(p *int32, old, new int32) bool { return atomicCAS(p, old, new) }

// fingerprint 提取与并发度无关的口径：每个任务的终态/原因/尝试次数，
// 以及聚合计数；不含墙钟耗时（耗时分布本来就会随并发度变化）。
type fingerprint struct {
	tasks map[string][3]int // status, reason, attempts
	agg   [5]int
}

func fingerprintStats(st Stats) fingerprint {
	fp := fingerprint{tasks: map[string][3]int{}}
	ids := make([]string, 0, len(st.ByTask))
	for id := range st.ByTask {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := st.ByTask[id]
		fp.tasks[id] = [3]int{int(r.Status), int(r.Reason), r.Attempts}
	}
	fp.agg = [5]int{st.Completed, st.Failed, st.Canceled, st.Rejected, st.Skipped}
	return fp
}

func TestSameInputConsistentAcrossConcurrency(t *testing.T) {
	slotsLevels := []int{1, 2, 4, 32}
	var ref *fingerprint
	var refOut map[string]int
	for _, slots := range slotsLevels {
		out := map[string]int{}
		var mu sync.Mutex
		fc := map[string]*int32{"flaky": new(int32)}
		cfg := NewConfig()
		cfg.Resources = ResourceLimits{ResourceSlots: int64(slots)}
		cfg.FailurePolicy = Isolate
		cfg.DefaultRetry = RetryPolicy{
			MaxAttempts: 4,
			Backoff:     ExponentialBackoff{Base: time.Millisecond, Factor: 2},
			Decider: func(attempt int, err error) bool {
				return err.Error() == "transient"
			},
		}
		rt := New(cfg)
		st, err := rt.RunTasks(context.Background(), buildDeterministicSet(out, fc, &mu))
		if err != nil {
			t.Fatal(err)
		}
		fp := fingerprintStats(st)
		if ref == nil {
			ref = &fp
			refOut = out
			continue
		}
		if fmt.Sprint(fp.agg) != fmt.Sprint(ref.agg) {
			t.Fatalf("slots=%d agg=%v want %v", slots, fp.agg, ref.agg)
		}
		if len(fp.tasks) != len(ref.tasks) {
			t.Fatalf("slots=%d task count differs", slots)
		}
		for id, v := range ref.tasks {
			if got := fp.tasks[id]; got != v {
				t.Fatalf("slots=%d task=%s got=(%s,%s,%d) want=(%s,%s,%d)",
					slots, id,
					statusString(Status(got[0])), reasonString(FailureReason(got[1])), got[2],
					statusString(Status(v[0])), reasonString(FailureReason(v[1])), v[2])
			}
		}
		if fmt.Sprint(out) != fmt.Sprint(refOut) {
			t.Fatalf("slots=%d outputs=%v want %v", slots, out, refOut)
		}
	}
}

// 重复运行同一构造多次（相同并发度），统计口径同样必须稳定。
func TestRepeatedRunsStable(t *testing.T) {
	var ref fingerprint
	for i := 0; i < 5; i++ {
		out := map[string]int{}
		var mu sync.Mutex
		fc := map[string]*int32{"flaky": new(int32)}
		cfg := NewConfig()
		cfg.Resources = ResourceLimits{ResourceSlots: 3}
		cfg.FailurePolicy = Isolate
		cfg.DefaultRetry = RetryPolicy{MaxAttempts: 4,
			Backoff: ExponentialBackoff{Base: time.Millisecond, Factor: 2},
			Decider: func(attempt int, err error) bool { return err.Error() == "transient" }}
		rt := New(cfg)
		st, err := rt.RunTasks(context.Background(), buildDeterministicSet(out, fc, &mu))
		if err != nil {
			t.Fatal(err)
		}
		fp := fingerprintStats(st)
		if i == 0 {
			ref = fp
		} else if fmt.Sprint(fp) != fmt.Sprint(ref) {
			t.Fatalf("run %d fingerprint drift:\n%v\n%v", i, fp, ref)
		}
	}
}
