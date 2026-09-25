package bcr

import (
	"fmt"
	"io"
	"log"
	"os"
)

// Logger 记录关键判定点：入队拒绝/超时、资源等待超时、
// 取消传播、重试判定、依赖非法拒绝、失败策略级联等。
// 判定日志必须带上输入（任务 ID、尝试数、额度快照）与判定依据。
type Logger interface {
	Logf(format string, args ...any)
}

// nopLogger 静默丢弃所有日志（用于对性能敏感且不关心判定轨迹的场景）。
type nopLogger struct{}

func (nopLogger) Logf(format string, args ...any) {}

// NopLogger 返回空实现日志器。
func NopLogger() Logger { return nopLogger{} }

// stdLogger 使用标准库 log 输出，默认带日期与微秒时间戳。
type stdLogger struct {
	l *log.Logger
}

func (s stdLogger) Logf(format string, args ...any) {
	s.l.Output(2, "bcr: "+fmt.Sprintf(format, args...))
}

// DefaultLogger 输出到标准错误，关键判定处可被测试与基准复现观察。
func DefaultLogger() Logger {
	return stdLogger{l: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)}
}

// NewLogger 构造输出到任意 writer 的日志器。
func NewLogger(w io.Writer) Logger {
	if w == nil {
		return nopLogger{}
	}
	return stdLogger{l: log.New(w, "", log.LstdFlags|log.Lmicroseconds)}
}
