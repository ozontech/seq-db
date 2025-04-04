package fracmanager

import (
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
)

type Config struct {
	DataDir string

	FracSize  uint64
	TotalSize uint64
	CacheSize uint64

	FracLoadLimit     uint64 // how many sealed fractions should fracmanager load, if 0 then loads all
	ShouldReplay      bool
	ShouldRemoveMeta  bool
	MaintenanceDelay  time.Duration
	CacheCleanupDelay time.Duration
	CacheGCDelay      time.Duration
	SealParams        frac.SealParams
	Fraction          frac.Config
}

func FillConfigWithDefault(config *Config) *Config {
	if config.MaintenanceDelay == 0 {
		config.MaintenanceDelay = consts.DefaultMaintenanceDelay
	}

	if config.CacheCleanupDelay == 0 {
		config.CacheCleanupDelay = consts.DefaultCacheCleanupDelay
	}
	if config.CacheGCDelay == 0 {
		config.CacheGCDelay = consts.DefaultCacheGCDelay
	}

	// Default zstd compression level, see: https://facebook.github.io/zstd/zstd_manual.html
	const zstdDefaultLevel = 3
	if config.SealParams.IDsZstdLevel == 0 {
		config.SealParams.IDsZstdLevel = zstdDefaultLevel
	}
	if config.SealParams.LIDsZstdLevel == 0 {
		config.SealParams.LIDsZstdLevel = zstdDefaultLevel
	}
	if config.SealParams.TokenListZstdLevel == 0 {
		config.SealParams.TokenListZstdLevel = zstdDefaultLevel
	}
	if config.SealParams.DocsPositionsZstdLevel == 0 {
		config.SealParams.DocsPositionsZstdLevel = zstdDefaultLevel
	}
	if config.SealParams.TokenTableZstdLevel == 0 {
		config.SealParams.TokenTableZstdLevel = zstdDefaultLevel
	}

	return config
}
