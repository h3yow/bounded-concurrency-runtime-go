# bounded-concurrency-runtime-go

进程内、有界资源的并发任务执行运行时。以可预测的方式调度并执行大量相互独立或存在依赖关系的任务，在资源受限、取消与失败场景下保持语义一致。

## 快速开始

```go
rt := runtime.New(
    runtime.WithQuotas(map[string]int{"cpu": 4, "io": 8}), // 各资源类别额度
    runtime.WithQueueCapacity(1024),                       // 排队+在途总数上限
    runtime.WithSubmitTimeout(5*time.Second),              // Submit 准入等待上限
    runtime.WithFailurePolicy(runtime.Isolate),            // 失败隔离
    runtime.WithRetryPolicy(runtime.RetryPolicy{
        MaxAttempts: 3, BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second,
    }),
)
defer rt.Close()

err := rt.Submit(ctx,
    runtime.Task{ID: "extract", Resource: "io", Run: extract},
    runtime.Task{ID: "transform", Resource: "cpu", Deps: []string{"extract"}, Run: transform},
    runtime.Task{ID: "load", Resource: "io", Deps: []string{"transform"}, Retries: 2, Run: load},
)
results := rt.Wait(ctx)   // 按提交顺序返回每个任务的终态
stats := rt.Stats()       // 与并发度无关的一致统计口径
metrics := rt.Metrics()   // 运维观察快照：资源水位 + 累计计数
```

## 资源与背压模型

- **资源额度**：`WithQuotas` 按资源类别设置信号量额度，任务仅在每次执行尝试期间持有额度，未配置的类别视为无限。在途并发严格不超过额度（见 `TestQuotaBound`）。
- **有界准入**：运行时持有 `queueCap` 枚准入令牌，每个被接受的任务持有一枚，到达终态后归还。排队与在途任务总数因此有界。
  - `Submit`：令牌不足时**有界等待**，可被调用方 `ctx` 取消（归为 `ReasonCanceled`）；`WithSubmitTimeout` 为其设置与 ctx 无关的最长等待时限，超时整批拒绝（`ErrAdmissionTimeout`，归为 `ReasonRejected`）。两种约束任一先触发即收敛，**最坏情况下有限时间内必有结果**。
  - `TrySubmit`：令牌不足时**立即拒绝**，返回包装 `ErrQueueFull` 的错误（归为 `ReasonRejected`）。
- **整批准入（all-or-nothing）**：一次 `Submit`/`TrySubmit` 调用是一个工作单元——要么整批被接受，要么整批被拒绝并携带原因，**绝不部分准入**。准入失败时已占令牌立即全部归还，不会攥着名额拖住其它提交方；校验失败（非法依赖）同样不占用任何令牌（见 `TestBatchTrySubmitAllOrNothing`、`TestBatchConcurrentWholeResults`）。
- **无饥饿/死锁**：等待依赖的任务只占用准入令牌，**不持有任何资源额度**；额度在实际执行前才获取，执行结束（含每次重试尝试结束）即释放。重试退避期间同样不持有额度。

## 取消与错误分类语义

- **取消传播**：`Submit` 的 `ctx` 与运行时内部 `ctx` 任一取消，都会传播到该批排队中（等待额度）与在途的任务；已取消的任务在获取额度后、启动前会再次检查，**不会被启动**。`Close` 取消所有排队任务并等待在途任务响应取消后退出。
- **错误分类**：所有错误包装为 `*runtime.Error`，携带 `Reason`、`TaskID` 与原始 `Cause`（`errors.Is/As` 可穿透）：

  | Reason | 含义 | 可重试 |
  |---|---|---|
  | `ReasonCanceled` | 上下文取消/超时 | 否 |
  | `ReasonRejected` | 背压拒绝、准入超时或运行时已关闭 | 否 |
  | `ReasonTaskFailed` | 任务执行体返回错误 | 是 |
  | `ReasonPanicked` | 任务执行体 panic，被隔离收敛 | 否 |
  | `ReasonDependencyFailed` | 依赖失败导致跳过 | 否 |
  | `ReasonInvalidDependency` | 依赖未知/成环/ID 非法，整批拒绝 | 否 |

  用 `runtime.ReasonOf(err)` 提取分类；原始原因通过 `Unwrap` 保留，不会被静默吞掉。
- **失败策略**：
  - `Isolate`（默认）：失败被隔离，仅其（传递）依赖者被标记 `Skipped`，其余任务继续。
  - `FailFast`：任一任务失败即取消运行时上下文，所有未完成任务归为 `Canceled`。
- **重试**：仅 `ReasonTaskFailed` 可重试（`Retryable`）。退避为 `BaseDelay * 2^(n-1)` 并封顶 `MaxDelay`；次数受 `RetryPolicy.MaxAttempts` 与任务级 `Task.Retries` 控制。重试次数计入 `Stats.Retried`，失败任务在终态只计数一次。

## 崩溃隔离语义

任务执行体可能来自第三方插件，其中未被接住的 panic 属于正常情况。运行时在每个任务的执行 goroutine 内 recover：

- **崩溃不扩散**：panic 被就地收敛，进程与其它任务不受影响，正在等待的依赖、其它资源类别的任务照常调度。
- **独立终态**：崩溃任务记为 `StatusPanicked`，错误为 `*runtime.Error{Reason: ReasonPanicked}`，与业务失败（`StatusFailed`/`ReasonTaskFailed`）明确区分，**绝不会记成成功**。
- **现场保留**：`Cause` 为 `*runtime.PanicError`，携带原始 panic 值（`Value`，若为 error 可经 `errors.Is/As` 穿透）与捕获时的完整 goroutine 栈（`Stack`），可直接定位现场。
- **不重试**：panic 是确定性现场，不参与重试（`Retryable` 为 false），`Attempts` 恒为 1。
- **遵循失败策略**：`Isolate` 下仅其（传递）依赖者被 `Skipped`；`FailFast` 下正常触发整体收敛，其余未完成任务归为 `Canceled`。
- **独立口径**：计入 `Stats.Panicked` 与 `Metrics.Panicked`，与 `Failed` 分开统计（见 `TestPanicIsolated`、`TestPanicFailFast`）。

## 运维观察口径

`rt.Metrics()` 返回随时可读的观察快照，与 `Stats` 同口径的部分严格相等：

| 字段 | 含义 |
|---|---|
| `Resources[name].Running` | 该资源类别当前持有额度、正在执行的任务数（实时水位） |
| `Resources[name].Waiting` | 该资源类别当前阻塞等额度的任务数（实时水位） |
| `Rejected` | 累计因容量不足（队列满/准入超时）或已关闭被拒绝的任务数，== `Stats.Rejected` |
| `AdmissionWaits` | 累计发生准入等待的提交批次数（未立即拿到全部令牌） |
| `QuotaWaits` | 累计发生阻塞等待的额度获取次数 |
| `Panicked` | 累计被隔离收敛的 panic 任务数，== `Stats.Panicked` |

实时水位为瞬时值，全部任务收敛后恒归零；累计计数单调不减。终态口径（`Rejected`/`Panicked` 及各 `Stats` 终态）与并发度、容量配置无关：同一输入在任意额度与队列容量下结果一致（见 `TestMetricsConsistencyAcrossConcurrency`）。

## 依赖调度规则

- 任务通过 `Deps` 声明依赖，构成 DAG；依赖就绪（全部 `Completed`）后任务才会被派发。
- 依赖可指向同批次或此前批次已提交的任务；依赖已失败/取消/跳过的任务会立即被 `Skipped` 并向下传播。
- 提交时校验：ID 非空且全局唯一、`Run` 非空、依赖必须存在、批内不得成环（DFS 三色标记）。任一校验失败**整批拒绝**，返回 `ReasonInvalidDependency`，不占用准入令牌。
- **统计一致性**：每个任务恰好计入一个终态（`Completed/Failed/Canceled/Skipped/Panicked`，提交期拒绝计 `Rejected`），`Stats.Terminal()` 恒等于已接受任务数；同一输入在任意并发度下终态分类与统计完全一致（见 `TestConsistencyAcrossConcurrency`、`TestMetricsConsistencyAcrossConcurrency`）。

## 可复现的验证与基准

```bash
# 全部测试（含竞态检测）
go test ./runtime/ -race -v -count=1

# 关键场景
go test ./runtime/ -race -run 'TestTrySubmit|TestSubmitBoundedWait'   # 超限拒绝/有界等待
go test ./runtime/ -race -run 'TestBatch'                             # 整批准入：全进或全拒
go test ./runtime/ -race -run 'TestCancel|TestClose'                  # 取消传播
go test ./runtime/ -race -run 'TestIsolate|TestFailFast|TestRetry'    # 失败隔离与重试
go test ./runtime/ -race -run 'TestPanic'                             # 崩溃隔离与收敛
go test ./runtime/ -race -run 'TestRejectInvalidDependencies'         # 非法依赖拒绝
go test ./runtime/ -race -run 'TestConsistencyAcrossConcurrency'      # 并发度一致性
go test ./runtime/ -race -run 'TestMetrics'                           # 观察口径与一致性

# 基准：调度吞吐 / 整批准入开销 / panic 隔离开销
go test ./runtime/ -run xxx -bench BenchmarkSchedule -benchmem
go test ./runtime/ -run xxx -bench BenchmarkBatchAdmission -benchmem
go test ./runtime/ -run xxx -bench BenchmarkPanicIsolation -benchmem
```

测试在关键判定处通过 `t.Logf` 打印输入与判定依据（`go test -v` 可见）。
