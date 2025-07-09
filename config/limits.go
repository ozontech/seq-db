package config

import (
	"runtime"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"go.uber.org/automaxprocs/maxprocs"
)

var (
	NumCPU      int
	TotalMemory uint64
)

func init() {
	_, _ = maxprocs.Set()

	NumCPU = runtime.GOMAXPROCS(0)
	TotalMemory = getTotalMemory()

	IndexWorkers = NumCPU
	FetchWorkers = NumCPU
	ReaderWorkers = NumCPU
}

func getTotalMemory() uint64 {
	if mem, err := memlimit.FromCgroup(); err == nil {
		return mem
	}
	mem, _ := memlimit.FromSystem()
	return mem
}
