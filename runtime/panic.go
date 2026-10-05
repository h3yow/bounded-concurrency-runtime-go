package runtime

import (
	"context"
	"fmt"
	"runtime/debug"
)

// PanicError 记录任务执行体崩溃（未捕获 panic）的现场信息，
// 作为 *Error 的 Cause 保留，可用 errors.As 提取。
type PanicError struct {
	// Value 为 panic 的原始值。
	Value any
	// Stack 为崩溃时刻的 goroutine 栈。
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// callIsolated 执行 fn，把未捕获的 panic 收敛为 *PanicError 返回，
// 不让其传播到运行时其它部分。
func callIsolated(ctx context.Context, fn TaskFunc) (err error, panicErr *PanicError) {
	defer func() {
		if v := recover(); v != nil {
			panicErr = &PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	err = fn(ctx)
	return err, nil
}
