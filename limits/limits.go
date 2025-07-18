package limits

import (
	"fmt"
	"runtime"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/ozontech/seq-db/logger"
)

var (
	NumCPU      int
	TotalMemory uint64
)

func init() {
	_, _ = maxprocs.Set(maxprocs.Logger(func(tpl string, args ...any) { logger.Info(fmt.Sprintf(tpl, args...)) }))

	NumCPU = runtime.GOMAXPROCS(0)
	TotalMemory = getTotalMemory()
}

func getTotalMemory() uint64 {
	if mem, err := memlimit.FromCgroup(); err == nil {
		return mem
	}
	mem, _ := memlimit.FromSystem()
	return mem
}
