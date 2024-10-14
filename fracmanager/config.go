package fracmanager

import (
	"time"

	"github.com/ozontech/seq-db/consts"
)

type Config struct {
	DataDir string

	FracSize          uint64
	TotalSize         uint64
	CacheSize         uint64
	DocBlockCacheSize uint64

	ComputeN int

	MaxFractionHits uint64 // the maximum number of fractions used in the search

	FracLoadLimit    uint64 // how many sealed fractions should fracmanager load, if 0 then loads all
	ShouldReplay     bool
	ShouldRemoveMeta bool
	EvalDelay        time.Duration
	FieldCacheDelay  time.Duration
	MaintenanceDelay time.Duration
	CacheDelay       time.Duration
}

func FillConfigWithDefault(config *Config) *Config {
	if config.ComputeN == 0 {
		config.ComputeN = consts.DefaultComputeN
	}

	if config.MaintenanceDelay == 0 {
		config.MaintenanceDelay = consts.DefaultMaintenanceDelay
	}

	if config.CacheDelay == 0 {
		config.CacheDelay = consts.DefaultCacheDelay
	}

	return config
}
