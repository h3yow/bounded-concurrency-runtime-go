package runtime

import (
	"context"
	"fmt"
	"runtime/debug"
)

// PanicError 记录任务执行体未被接住的 panic：保留 panic 值与捕获时的
// 完整调用栈，用于事后定位现场。它作为 Cause 包在 *Error（ReasonPanicked）中，
// 任务终态为 StatusPanicked，与普通业务失败（StatusFailed）区分。
type PanicError struct {
	TaskID string
	Value  any    // recover() 捕获的原始 panic 值
	Stack  []byte // panic 现场的完整 goroutine 栈
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("runtime: task %q panicked: %v", e.TaskID, e.Value)
}

// Unwrap 在 panic 值本身是 error 时将其透出，便于 errors.Is/As 定位。
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// runProtected 执行任务体并把 panic 收敛为 *PanicError（含栈现场）；
// 正常返回的错误原样透传。panic 不会向运行时其它部分传播。
func runProtected(taskID string, fn TaskFunc, ctx context.Context) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{TaskID: taskID, Value: v, Stack: debug.Stack()}
		}
	}()
	return fn(ctx)
}
