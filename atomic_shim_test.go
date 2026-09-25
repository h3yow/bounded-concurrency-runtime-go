package bcr

import "sync/atomic"

func atomicLoad(p *int32) int32             { return atomic.LoadInt32(p) }
func atomicCAS(p *int32, old, n int32) bool { return atomic.CompareAndSwapInt32(p, old, n) }
