package fracmanager

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

type Config struct {
	DataDir string

	FracSize  uint64
	TotalSize uint64
	CacheSize uint64

	suspendThreshold uint64
	SealingQueueLen  uint64

	ReplayWorkers     int
	MaintenanceDelay  time.Duration
	CacheCleanupDelay time.Duration
	CacheGCDelay      time.Duration
	SealParams        common.SealParams
	SortCacheSize     uint64 // size for docs cache for active fraction
	Fraction          frac.Config
	MinSealFracSize   uint64

	OffloadingEnabled    bool
	OffloadingQueueSize  uint64
	OffloadingRetention  time.Duration
	OffloadingRetryDelay time.Duration

	CompactionEnabled bool
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
	if config.SealParams.LIDBlockSize == 0 {
		config.SealParams.LIDBlockSize = consts.DefaultLIDBlockCap
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
	if config.SealParams.LidsBitmapThreshold == 0 {
		config.SealParams.LidsBitmapThreshold = consts.DefaultLIDBlockCap
	}
	if config.ReplayWorkers == 0 {
		config.ReplayWorkers = consts.DefaultReplayWorkers
	}

	if config.SortCacheSize == 0 {
		const (
			SdocsCacheSizeMultiplier = 8
			SdocsCacheSizeMaxRatio   = 0.8
		)
		config.SortCacheSize = config.FracSize * SdocsCacheSizeMultiplier
		if config.SortCacheSize > config.CacheSize {
			config.SortCacheSize = uint64(float64(config.CacheSize) * 0.8)
		}
	} else if config.SortCacheSize > config.CacheSize {
		logger.Fatal("cache size misconfiguration",
			zap.Float64("total_cache_size_mb", util.SizeToUnit(config.CacheSize, "mb")),
			zap.Float64("sort_cache_size_mb", util.SizeToUnit(config.SortCacheSize, "mb")))
	}

	return config
}

func (cfg *Config) SuspendThreshold() uint64 {
	if cfg.suspendThreshold == 0 {
		cfg.suspendThreshold = cfg.TotalSize
		cfg.suspendThreshold += cfg.TotalSize / 100 // small buffer
		if cfg.OffloadingEnabled {
			cfg.suspendThreshold += cfg.OffloadingQueueSize // offloading queue size
		}
	}
	return cfg.suspendThreshold
}
