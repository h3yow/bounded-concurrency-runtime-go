package bcr

import "sort"

// node 是依赖图内部节点。
type node struct {
	task       *Task
	seq        int      // 登记顺序，用于稳定排序
	indegree   int      // 尚未完成的前置数量
	dependents []string // 以本节点为前置的任务 ID
}

// dag 是经过校验的依赖图。
type dag struct {
	nodes map[string]*node
	// order 是确定性的调度优先级提示序：拓扑可行且按
	// (seq) 稳定，保证任意并发度下"就绪顺序"不改变结果语义。
	order []string
}

// depError 构造依赖类错误，保留具体任务 ID 作为原始原因。
func depError(reason FailureReason, sentinel error, id, detail string) *TaskError {
	return &TaskError{TaskID: id, Reason: reason, Op: "buildDAG",
		Err: &plainError{msg: detail + ": " + sentinel.Error()}}
}

// buildDAG 校验任务集并生成拓扑序：
//   - 空 ID / nil 函数体由 Task.validate 拒绝；
//   - 重复 ID：拒绝；
//   - 不可达依赖：依赖引用了不存在的任务 ID，拒绝；
//   - 自依赖 / 环路：拒绝（Kahn 算法后仍有剩余节点即有环）。
//
// 拓扑序生成规则：每轮在入度为 0 的候选中取登记顺序最小者，
// 输出确定，与并发度无关。
func buildDAG(tasks []*Task) (*dag, error) {
	d := &dag{nodes: make(map[string]*node, len(tasks))}

	for i, t := range tasks {
		if err := t.validate(); err != nil {
			return nil, err
		}
		if _, exists := d.nodes[t.ID]; exists {
			return nil, depError(ReasonDuplicateID, ErrDuplicateID, t.ID,
				"task id declared more than once")
		}
		d.nodes[t.ID] = &node{task: t, seq: i}
	}

	// 依赖解析：引用必须可达（存在对应节点）。
	for _, t := range tasks {
		n := d.nodes[t.ID]
		seen := make(map[string]struct{}, len(t.Deps))
		for _, dep := range t.Deps {
			if dep == t.ID {
				return nil, depError(ReasonCircularDependency, ErrCircularDependency, t.ID,
					"task depends on itself")
			}
			if _, ok := d.nodes[dep]; !ok {
				return nil, depError(ReasonUnreachableDependency, ErrUnreachableDep, t.ID,
					"dependency "+dep+" is not defined")
			}
			// 同一依赖重复列出：去重而非拒绝（语义等价于一次）。
			if _, dup := seen[dep]; dup {
				continue
			}
			seen[dep] = struct{}{}
			n.indegree++
			d.nodes[dep].dependents = append(d.nodes[dep].dependents, t.ID)
		}
	}

	// Kahn 算法 + 登记顺序稳定选择。
	indeg := make(map[string]int, len(d.nodes))
	candidates := make([]string, 0, len(d.nodes))
	for id, n := range d.nodes {
		indeg[id] = n.indegree
		if n.indegree == 0 {
			candidates = append(candidates, id)
		}
	}
	d.order = make([]string, 0, len(d.nodes))
	for len(candidates) > 0 {
		// 取 seq 最小者，保证输出确定。
		sort.Slice(candidates, func(i, j int) bool {
			return d.nodes[candidates[i]].seq < d.nodes[candidates[j]].seq
		})
		id := candidates[0]
		candidates = candidates[1:]
		d.order = append(d.order, id)
		for _, down := range d.nodes[id].dependents {
			indeg[down]--
			if indeg[down] == 0 {
				candidates = append(candidates, down)
			}
		}
	}

	if len(d.order) != len(d.nodes) {
		// 剩余节点必然处于环上（其前置互相依赖，入度无法归零）。
		remaining := make([]string, 0)
		for id := range d.nodes {
			if indeg[id] > 0 {
				remaining = append(remaining, id)
			}
		}
		sort.Strings(remaining)
		id := ""
		if len(remaining) > 0 {
			id = remaining[0]
		}
		return nil, depError(ReasonCircularDependency, ErrCircularDependency, id,
			"cycle detected among tasks")
	}

	return d, nil
}
