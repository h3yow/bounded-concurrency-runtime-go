# bounded-concurrency-runtime-go

进程内、有界资源的并发任务执行运行时。以可预测的方式调度并执行大量相互独立或存在依赖关系的任务，在资源受限、取消与失败场景下保持语义一致。

## 快速开始

```go
rt := runtime.New(
    runtime.WithQuotas(map[string]int{"cpu": 4, "io": 8}), // 各资源类别额度
    runtime.WithQueueCapacity(1024),                       // 排队+在途总数上限
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
```

## 资源与背压模型

- **资源额度**：`WithQuotas` 按资源类别设置信号量额度，任务仅在每次执行尝试期间持有额度，未配置的类别视为无限。在途并发严格不超过额度（见 `TestQuotaBound`）。
- **有界准入（整批原子）**：运行时持有 `queueCap` 个准入名额，按批原子获取——一批任务**要么整批被接受，要么整批被拒绝并告知原因**，不存在只进一部分的中间态；等待期间不攥住任何名额，不会拖住其它提交方。名额在任务到达终态后归还，排队与在途任务总数因此有界。
  - `Submit`：名额不足时**有界等待**，受 `WithAdmissionTimeout`（默认 30s）与调用方 `ctx` 双重约束，最坏情况下在有限时间内收敛：超时整批拒绝并返回包装 `ErrAdmissionTimeout` 的错误（归为 `ReasonRejected`），`ctx` 取消归为 `ReasonCanceled`。
  - `TrySubmit`：名额不足时**立即整批拒绝**，返回包装 `ErrQueueFull` 的错误（归为 `ReasonRejected`）。
  - 批次大于 `queueCap` 时永远凑不齐名额，阻塞口味会在超时上界内确定性地整批拒绝，不会无限等待。
- **无饥饿/死锁**：等待依赖的任务只占用准入令牌，**不持有任何资源额度**；额度在实际执行前才获取，执行结束（含每次重试尝试结束）即释放。重试退避期间同样不持有额度。

## 取消与错误分类语义

- **取消传播**：`Submit` 的 `ctx` 与运行时内部 `ctx` 任一取消，都会传播到该批排队中（等待额度）与在途的任务；已取消的任务在获取额度后、启动前会再次检查，**不会被启动**。`Close` 取消所有排队任务并等待在途任务响应取消后退出。
- **错误分类**：所有错误包装为 `*runtime.Error`，携带 `Reason`、`TaskID` 与原始 `Cause`（`errors.Is/As` 可穿透）：

  | Reason | 含义 | 可重试 |
  |---|---|---|
  | `ReasonCanceled` | 上下文取消/超时 | 否 |
  | `ReasonRejected` | 背压拒绝或运行时已关闭 | 否 |
  | `ReasonTaskFailed` | 任务执行体返回错误 | 是 |
  | `ReasonDependencyFailed` | 依赖失败导致跳过 | 否 |
  | `ReasonInvalidDependency` | 依赖未知/成环/ID 非法，整批拒绝 | 否 |
  | `ReasonPanicked` | 任务执行体未捕获 panic，被收敛为失败终态 | 否 |

  用 `runtime.ReasonOf(err)` 提取分类；原始原因通过 `Unwrap` 保留，不会被静默吞掉。
- **崩溃隔离**：任务执行体（含第三方插件）发生未捕获 panic 时，运行时在执行 goroutine 内将其收敛——进程与其它任务不受波及，该任务记为 `StatusFailed` + `ReasonPanicked` 的失败终态（**绝不记成成功**），`Cause` 为 `*PanicError`，保留 panic 原始值与崩溃时刻的完整 goroutine 栈（`errors.As` 可提取）。崩溃不可重试（通常可复现），但遵循既有失败策略：`Isolate` 下仅其下游被 `Skipped`，`FailFast` 下正常触发整体收敛。已收敛的崩溃累计入 `Stats.Panics`（同时计入 `Failed`）。注意：`runtime` 包不可恢复的致命错误（如并发 map 写、OOM）与 `os.Exit` 无法被任何进程内机制拦截，不在此语义范围内。
- **失败策略**：
  - `Isolate`（默认）：失败被隔离，仅其（传递）依赖者被标记 `Skipped`，其余任务继续。
  - `FailFast`：任一任务失败即取消运行时上下文，所有未完成任务归为 `Canceled`。
- **重试**：仅 `ReasonTaskFailed` 可重试（`Retryable`）。退避为 `BaseDelay * 2^(n-1)` 并封顶 `MaxDelay`；次数受 `RetryPolicy.MaxAttempts` 与任务级 `Task.Retries` 控制。重试次数计入 `Stats.Retried`，失败任务在终态只计数一次。

## 依赖调度规则

- 任务通过 `Deps` 声明依赖，构成 DAG；依赖就绪（全部 `Completed`）后任务才会被派发。
- 依赖可指向同批次或此前批次已提交的任务；依赖已失败/取消/跳过的任务会立即被 `Skipped` 并向下传播。
- 提交时校验：ID 非空且全局唯一、`Run` 非空、依赖必须存在、批内不得成环（DFS 三色标记）。任一校验失败**整批拒绝**，返回 `ReasonInvalidDependency`，不占用准入令牌。
- **统计一致性**：每个任务恰好计入一个终态（`Completed/Failed/Canceled/Skipped`，提交期拒绝计 `Rejected`），`Stats.Terminal()` 恒等于已接受任务数；同一输入在任意并发度下终态分类与统计完全一致（见 `TestConsistencyAcrossConcurrency`）。

## 运行观察

- `rt.Observe()` 返回实时快照 `Observation`，可随时调用：
  - `Resources`：每个资源类别（按名排序）的 `Limit`、当下在执行数 `Running`、当下在等待数 `Waiting`；
  - `Pending`：已准入但未达终态的任务数；
  - `Stats`：累计口径——`Rejected`/`AdmissionWaits` 记录因容量不足被拒绝的任务数与发生过等待的提交批次数，`Panics` 记录已收敛的崩溃数量。
- 终态汇总口径与并发度、容量配置无关：同一份输入无论用多大并发和容量去跑，各终态计数一致（见 `TestObserveConsistencyAcrossConcurrency`）。

## 可复现的验证与基准

```bash
# 全部测试（含竞态检测）
go test ./runtime/ -race -v -count=1

# 关键场景
go test ./runtime/ -race -run 'TestTrySubmit|TestSubmitBoundedWait'   # 超限拒绝/有界等待
go test ./runtime/ -race -run 'TestCancel|TestClose'                  # 取消传播
go test ./runtime/ -race -run 'TestIsolate|TestFailFast|TestRetry'    # 失败隔离与重试
go test ./runtime/ -race -run 'TestRejectInvalidDependencies'         # 非法依赖拒绝
go test ./runtime/ -race -run 'TestConsistencyAcrossConcurrency'      # 并发度一致性
go test ./runtime/ -race -run 'TestPanic'                             # 崩溃隔离与收敛
go test ./runtime/ -race -run 'TestBatch'                             # 批量整体准入（等满再拒/立即拒）
go test ./runtime/ -race -run 'TestObserve'                           # 观察口径一致性

# 基准：不同资源额度下 256 任务 DAG 的调度吞吐
go test ./runtime/ -run xxx -bench BenchmarkSchedule -benchmem
```

测试在关键判定处通过 `t.Logf` 打印输入与判定依据（`go test -v` 可见）。
