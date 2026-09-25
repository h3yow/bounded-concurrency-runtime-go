# Bounded Concurrency Runtime (Go)

一个**进程内、有界资源**的并发任务执行运行时：以可预测的方式调度并执行大量
相互独立或存在依赖关系的任务，并在**资源受限、取消与失败**场景下保持语义一致。

- 多资源类别固定额度（内建 `slots` 控制最大并发），等待者不持票，结构上无
  "持额度等依赖/子任务"型死锁；
- 提交背压有界：队列满立即拒绝或有界等待超时拒绝，排队/在途数量均有上界；
- 取消与超时传播到排队中、退避中与在途任务，已取消任务不会再被启动；
- 失败策略区分**整体终止（FailFast）**与**失败隔离（Isolate）**；有限重试
  遵循指数退避与显式可重试判定；
- 依赖图在执行前校验：重复 ID、不可达依赖、自依赖与环路一律拒绝；
- 终态互斥计数、重试单独计、耗时口径固定，**同一输入在任意并发度下结果与
  统计一致**。

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "time"

    bcr "github.com/h3yow/bounded-concurrency-runtime-go"
)

func main() {
    cfg := bcr.NewConfig()
    // 最多 8 个任务同时执行；另设两个自定义资源类别。
    cfg.Resources = bcr.ResourceLimits{bcr.ResourceSlots: 8, "mem": 4, "conns": 2}
    cfg.MaxQueued = 256            // 就绪等待队列容量（背压上界）
    cfg.AcquireTimeout = 5 * time.Second
    cfg.FailurePolicy = bcr.Isolate
    cfg.DefaultRetry = bcr.RetryPolicy{
        MaxAttempts: 3,
        Backoff:     bcr.ExponentialBackoff{Base: 10 * time.Millisecond, Factor: 2, Jitter: 0.2},
        Decider:     bcr.DefaultRetryDecider,
    }

    rt := bcr.New(cfg)
    stats, err := rt.RunTasks(context.Background(), []*bcr.Task{
        {ID: "fetch-users", Fn: fetchUsers,
         Resources: bcr.ResourceWeights{"conns": 1, "mem": 1}},
        {ID: "fetch-orders", Fn: fetchOrders,
         Resources: bcr.ResourceWeights{"conns": 1, "mem": 1}},
        {ID: "build-report", Deps: []string{"fetch-users", "fetch-orders"}, Fn: buildReport,
         Resources: bcr.ResourceWeights{"mem": 2}},
    })
    if err != nil {
        panic(err) // 依赖图非法（环/未知 ID/重复 ID）
    }
    fmt.Printf("completed=%d failed=%d skipped=%d retries=%d\n",
        stats.Completed, stats.Failed, stats.Skipped, stats.RetryAttempts)
}
```

## 1. 资源与背压模型

### 1.1 多资源类别额度

- 每个资源类别有**固定总额度**（`Config.Resources`），任务通过
  `Task.Resources` 声明需求量；任务只有在**全部类别**同时可用时才被启动
  （all-or-nothing，无部分扣减）。
- 内建类别 `slots`（`ResourceSlots`）表示执行槽，每个任务默认占 1，
  因而"最大并发度"与其它资源共用同一套机制。
- 任务需求量超过某类别总额度时，`Submit` **立即拒绝**（永远无法满足，不能
  静默挂死）。

### 1.2 等待者不持票（无死锁的结构性保证）

调度集中在**单个调度器 goroutine**，只有真正执行用户函数的尝试才占用额度：

- 等待依赖满足、等待资源额度、退避等待重试的任务，**不持有任何额度**，也不
  占用 goroutine（仅表现为调度器内的状态）；
- 因此不会出现"任务 A 持有资源等待任务 B，而 B 需要 A 持有的资源"这类
  死锁，也不会因为等待链长期占用额度而饥饿。

资源等待队列为就绪任务集合，调度允许越过队头暂不可满足的任务（一次运行的
任务集是封闭有限集，且每任务需求已验证不超过容量，资源排空时队头必然可满足，
故不会被永久饿死）。同资源可满足时按 `Priority` 降序、同优先级按登记顺序
（FIFO）启动。

### 1.3 有界提交与拒绝语义

| 情形 | 行为 | 可判定错误 | 原因 |
|---|---|---|---|
| 队列满，`EnqueueTimeout==0` | 立即拒绝 | `errors.Is(err, ErrQueueFull)` | `ReasonAdmissionRejected` |
| 队列满，`EnqueueTimeout>0` | 有界等待，超时拒绝 | `errors.Is(err, ErrQueueTimeout)` | `ReasonQueueTimeout` |
| 需求超总额度 | 立即拒绝 | 错误消息含 `exceeds limit` | `ReasonInvalidTask` |
| `Run` 之后再提交 | 密封拒绝 | `errors.Is(err, ErrRuntimeClosed)` | `ReasonInvalidTask` |
| 等待资源超时（`AcquireTimeout`） | 终态失败或按策略重试 | `errors.Is(err, ErrResourceTimeout)` | `ReasonResourceTimeout` |

在途 goroutine 数受资源额度严格约束，就绪等待数受 `MaxQueued` 约束，两者都
不会无界增长。

## 2. 取消与错误分类语义

### 2.1 取消传播

运行级 context 来自 `Run(ctx)`。取消来源有两种，任务的归类只依赖**任务自身
观测到的事实**，与调度时序/并发度无关：

- **外部取消 / 超时**：根 context 取消或截止；
  - 排队中 / 等待依赖 / 退避中的任务：立即终态 `Canceled`，函数体不再启动，
    原因为 `ReasonContextCanceled` / `ReasonContextDeadline`；
  - 在途任务：取消经 context 传播，任务返回 context 错误后归为
    `Canceled`（透传 `ctx.Err()` 即自动归类；即使返回自定义错误，只要尝试
    context 已取消也会按取消归类）。
- **FailFast 级联取消**：任一任务到达失败终态后取消全运行时尚未完成的任务；
  未启动者归因为 `ReasonAborted`，在途者按其观测到的取消归类。
- 任务级 `Task.Timeout` 为单次尝试设置截止，归类为 `ReasonContextDeadline`。

已取消的任务不会再被启动；取消是终态，幂等。

### 2.2 错误分类（原因可区分、原始原因保留）

所有非成功终态携带 `FailureReason`，并包装为 `*TaskError`
（`errors.As` 可取，`Unwrap()` 保留原始错误，`errors.Is` 可匹配哨兵）：

| Reason | 含义 |
|---|---|
| `task_error` | 任务体返回普通错误 |
| `panic` | 任务体 panic（已恢复，原始值保留在错误文本中） |
| `context_canceled` / `context_deadline` | 取消 / 超时 |
| `admission_rejected` / `queue_timeout` | 入队拒绝 / 入队等待超时 |
| `resource_timeout` | 等待资源额度超时 |
| `dependency_failed` | 前置未成功（失败隔离下跳过） |
| `circular_dependency` / `unreachable_dependency` | 依赖图非法 |
| `duplicate_id` / `invalid_task` | 登记非法 |
| `retry_exhausted` | 有限重试耗尽（外层原因；原始错误仍可 `errors.Is`） |
| `aborted` | FailFast 整体终止 |

错误不会被静默吞掉：每个任务终态在 `Stats.ByTask` 中都有记录与原因。

### 2.3 失败策略

- `FailFast`（默认）：首个失败终态触发整体终止，其余未完成任务取消。
- `Isolate`：失败任务单独失败；仅其（传递）下游标记
  `Skipped/dependency_failed` 且不启动，无关节点继续完成。边界规则：失败
  瞬间**已经在途**的下游任务允许跑完（其结果保留、不被抢占），但它自己的
  下游仍按传递依赖跳过——即使该在途任务"侥幸成功"也不会错误放行。

### 2.4 有限重试

- `RetryPolicy.MaxAttempts` 为含首次的尝试上界；每次重试前调用
  `Decider(attempt, err)` 显式判定**是否可重试**；
- 退避由 `Backoff` 给出，`ExponentialBackoff` 支持 `Base/Factor/Max/Jitter`；
- **退避期间释放全部额度**；
- 取消/超时类错误默认不重试（`DefaultRetryDecider` 只对 `task_error`、
  `panic` 放行），任务可提供自己的 `Task.Retryable`；
- 次数耗尽后终态原因为 `retry_exhausted`，最后一次原始错误经 `Unwrap`
  保留，不重复计数。

## 3. 依赖调度规则

- 依赖在 `Run`/`RunTasks` 时构建为图并**先校验后执行**，非法则不启动任何
  任务：
  - 重复 ID → `ErrDuplicateID`；
  - 引用不存在的 ID（不可达依赖）→ `ErrUnreachableDep`；
  - 自依赖或任意环路（Kahn 算法后仍有节点入度未归零）→
    `ErrCircularDependency`；
  - 同一依赖重复列出按一条边处理（去重）。
- 全部前置**成功**后任务才就绪；前置失败时按失败策略处理
  （FailFast 取消 / Isolate 跳过下游）。
- 拓扑序确定：每轮在入度为 0 的候选中取登记顺序最早者，与并发度无关。

## 4. 统计口径（并发度无关）

`Stats` 的终态分类互斥且自洽（计数从终态明细汇总，不存在两套记账）：

```
Submitted == Completed + Failed + Canceled + Rejected + Skipped
RetryAttempts == Σ(Attempts-1)        // 额外尝试次数，不与终态重复计数
TotalRunDur   == Σ 各尝试执行时长（含失败尝试）
TotalWaitDur  == Σ 就绪到首次执行的等待时长
```

改变并发度只改变墙钟完成时间与等待时长的**分布**，不改变每个任务的终态、
原因、尝试次数，以及上述聚合计数与任务输出。验证见
`TestSameInputConsistentAcrossConcurrency`（slots=1/2/4/32）。

## 5. 可复现的验证与基准方法

```bash
# 常规测试
go test ./...

# 竞态检测（并发路径必须通过），并重复 10 次以暴露时序问题
go test -race -count=10 ./...

# 只跑某类语义
go test -run 'TestAdmission|TestResource' -race ./...   # 超限拒绝/有界等待
go test -run 'TestCancel|TestNoLaunch'  -race ./...     # 取消传播
go test -run 'TestFailure|TestRetry|TestPanic' -race ./...  # 隔离/重试/panic
go test -run 'TestDAG' -race ./...                      # 非法依赖拒绝
go test -run 'TestSameInput|TestRepeatedRuns' -race ./...   # 一致性
go test -run TestKeyDecisionLogging ./...               # 判定日志依据

# 基准
go test -run '^$' -bench=. -benchmem ./...
go test -run '^$' -bench=BenchmarkIndependentTasks -benchmem -cpuprofile cpu.out ./...
go tool pprof -top cpu.out
```

开启判定日志（接纳拒绝、资源超时、取消传播、重试判定、失败级联、终态归类，
均带任务 ID、尝试数、额度快照与判定依据）：

```go
cfg.Logger = bcr.DefaultLogger()       // os.Stderr
// 或写入任意 io.Writer：
cfg.Logger = bcr.NewLogger(&buf)
```

## 6. 设计要点小结

- **单调度器事件循环 + worker 池**：调度判定无锁无竞争；worker 只执行函数
  并通过有界 channel 回报，取消/结果/timer 都是事件。
- **有界性**：goroutine 数 ≤ 各资源额度允许的在途尝试数；就绪等待集合 ≤
  接纳任务数；提交缓冲 ≤ `MaxQueued`；timer 单一复用，不随循环轮数堆积。
- **确定性**：依赖图校验、拓扑序、调度选择规则与统计汇总都不依赖 goroutine
  交错时序。

## 源码结构

| 文件 | 职责 |
|---|---|
| `errors.go` | 失败原因分类、`TaskError`、哨兵错误、可重试判定 |
| `task.go` | 任务/状态/结果定义与任务自校验 |
| `resources.go` | 多类别固定额度池（非阻塞获取/释放、超需判定） |
| `dag.go` | 依赖图校验与确定性拓扑序 |
| `scheduler.go` | 失败策略、指数退避、重试策略与配置 |
| `stats.go` | 终态互斥统计与并发度无关口径 |
| `logger.go` | 关键判定日志接口与标准实现 |
| `runtime.go` | 运行时、有界提交（拒绝/有界等待）、密封语义 |
| `execute.go` | 调度事件循环、取消传播、重试与失败链路 |
