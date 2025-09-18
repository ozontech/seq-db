package fracmanager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	fractionLoadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "main",
		Name:      "fraction_load_errors",
		Help:      "Doc file load errors (missing or invalid doc file)",
	})
)

// Loader is responsible for loading and initializing fractions from filesystem
// Coordinates the process of discovering, validating, and loading all fraction types
type Loader struct {
	config    *Config           // loader configuration
	provider  *fractionProvider // provider for creating fraction objects
	infoCache *fracInfoCache    // fraction metadata cache

	cacheStat struct {
		hits   int // counter of fractions loaded from cache
		misses int // counter of fractions loaded without using cache
	}
}

// NewLoader creates a new fraction loader
// Initialized at system startup to prepare data
func NewLoader(config *Config, provider *fractionProvider, infoCache *fracInfoCache) *Loader {
	return &Loader{
		config:    config,
		provider:  provider,
		infoCache: infoCache,
	}
}

// Load is the main method for loading all fractions
// Coordinates the entire process: discovery, validation, recovery, and ordering
func (l *Loader) Load(ctx context.Context) (*fractionRegistry, error) {
	// Stage 1: Discover all fractions in filesystem
	actives, locals, remotes, err := l.discover(ctx)
	if err != nil {
		return nil, err
	}

	// Stage 2: Replay active fractions and seal them
	active, sealed, err := l.replayAndSeal(ctx, actives)
	if err != nil {
		return nil, err
	}

	// Stage 3: Create new active fraction if no existing ones
	if active == nil {
		active = l.provider.GenerateActive()
	}

	// Stage 4: Combine all local fractions
	locals = append(locals, sealed...)
	return NewFractionRegistry(remotes, locals, active), nil
}

// replayAndSeal replays active fractions and seals old ones
// Key method for ensuring data consistency during restart
func (l *Loader) replayAndSeal(ctx context.Context, actives []*frac.Active) (*frac.Active, []*frac.Sealed, error) {
	if len(actives) == 0 {
		return nil, nil, nil
	}
	var last *frac.Active // last active fraction (remains active)
	sealedFracs := make([]*frac.Sealed, 0, len(actives)-1)

	for i, active := range actives {
		// Replay operations from WAL to restore state
		if err := active.Replay(ctx); err != nil {
			return nil, nil, err
		}

		// Clean up empty fractions
		if active.Info().DocsTotal == 0 {
			active.Release()
			removeAllFiles(active.BaseFileName)
			continue
		}

		// Last fraction remains active
		if i == len(actives)-1 {
			last = active
			continue
		}

		// Seal previous fractions
		sealed, err := l.provider.Seal(active)
		if err != nil {
			return nil, nil, err
		}
		sealedFracs = append(sealedFracs, sealed)
	}

	return last, sealedFracs, nil
}

// discover discovers all fractions in filesystem
// Returns fractions separated by type: active, local, remote
func (l *Loader) discover(ctx context.Context) ([]*frac.Active, []*frac.Sealed, []*frac.Remote, error) {
	// Scan and analyze fraction files
	ids, manifests := l.analyzeFiles(l.scanFiles())
	sort.Strings(ids) // sort by identifiers

	// Apply load limit if specified in configuration
	if l.config.FracLoadLimit > 0 {
		logger.Info("preloading fractions", zap.Uint64("limit", l.config.FracLoadLimit))
		if len(ids) > int(l.config.FracLoadLimit) {
			ids = ids[len(ids)-int(l.config.FracLoadLimit):] // take the newest ones
		}
	}

	// Filter valid fractions
	filtered, err := l.filterValid(ids, manifests)
	if err != nil {
		return nil, nil, nil, err
	}

	// Load fractions according to their stage
	return l.loadByStage(ctx, filtered)
}

// loadByStage loads fractions according to their stage
// Separates fractions into active, sealed, and remote
func (l *Loader) loadByStage(ctx context.Context, manifests []*fracManifest) ([]*frac.Active, []*frac.Sealed, []*frac.Remote, error) {
	start := time.Now()
	total := len(manifests)

	actives := make([]*frac.Active, 0)
	locals := make([]*frac.Sealed, 0, total)
	remotes := make([]*frac.Remote, 0, total)

	// Iterate through all manifests and load corresponding fraction types
	for i, manifest := range manifests {
		switch manifest.Stage() {
		case fracStageActive:
			actives = append(actives, l.provider.NewActive(manifest.basePath))
		case fracStageSealed:
			locals = append(locals, l.loadSealed(manifest.basePath))
		case fracStageRemote:
			remotes = append(remotes, l.loadRemote(ctx, manifest.basePath))
		default:
			logger.Error("unexpected fraction stage", zap.Any("manifest", manifest))
		}
		logLoadingProgress(start, i, total) // log progress
	}

	logger.Info("fractions initialization completed",
		zap.Int("cached", l.cacheStat.hits),
		zap.Int("uncached", l.cacheStat.misses))

	return actives, locals, remotes, nil
}

// loadSealed loads a sealed fraction using cache
// Optimizes loading through pre-saved metadata
func (l *Loader) loadSealed(basePath string) *frac.Sealed {
	info, found := l.infoCache.Get(filepath.Base(basePath))
	l.updateStats(found)

	frac := l.provider.NewSealed(basePath, info)
	l.infoCache.Add(frac.Info()) // update cache
	return frac
}

// loadRemote loads a remote fraction
// Works with external storages through context
func (l *Loader) loadRemote(ctx context.Context, basePath string) *frac.Remote {
	info, found := l.infoCache.Get(filepath.Base(basePath))
	l.updateStats(found)

	frac := l.provider.NewRemote(ctx, basePath, info)
	l.infoCache.Add(frac.Info())
	return frac
}

// updateCacheStats updates cache usage statistics
// For monitoring caching effectiveness
func (l *Loader) updateStats(found bool) {
	if found {
		l.cacheStat.hits++
	} else {
		l.cacheStat.misses++
	}
}

// scanFiles scans filesystem for fraction files
// Uses glob pattern to find all matching files
func (l *Loader) scanFiles() []string {
	fullPattern := filepath.Join(l.config.DataDir, fileBasePattern+"*")
	files, err := filepath.Glob(fullPattern)
	if err != nil {
		logger.Panic("failed to scan fraction files", zap.Error(err))
	}
	return files
}

// filterValid filters valid fractions and handles invalid ones
// Removes partially deleted and unknown fractions
func (l *Loader) filterValid(ids []string, manifests map[string]*fracManifest) ([]*fracManifest, error) {
	validated := make([]*fracManifest, 0, len(manifests))
	for _, id := range ids {
		manifest := manifests[id]
		if manifest == nil {
			return nil, errors.New("inconsistent fraction file analysis")
		}

		switch manifest.Stage() {
		case fracStageUnknown:
			logger.Error("unknown fraction stage", zap.String("fraction", id), zap.Any("manifest", manifest))
			fractionLoadErrors.Inc()
			continue
		case fracStagePartiallyDeleted:
			logger.Warn("cleaning up partially deleted fraction files", zap.String("base_path", manifest.basePath))
			removeAllFiles(manifest.basePath)
			continue
		}

		manifest.Cleanup()
		validated = append(validated, manifest)
	}
	return validated, nil
}

// analyzeFiles analyzes fraction files and groups them by fraction ID
// Creates manifests that represent the complete state of each fraction
func (l *Loader) analyzeFiles(files []string) ([]string, map[string]*fracManifest) {
	ids := make([]string, 0, len(files))
	manifests := make(map[string]*fracManifest)

	for _, file := range files {
		basePath, ext, id, err := parseFilePath(file)
		if err != nil {
			logger.Fatal("file analysis error", zap.String("file", file), zap.Error(err))
		}

		manifest, exists := manifests[id]
		if !exists {
			manifest = &fracManifest{basePath: basePath}
			manifests[id] = manifest
			ids = append(ids, id)
		}

		if err := manifest.AddExtension(ext); err != nil {
			logger.Fatal("invalid file type", zap.String("file", file), zap.Error(err))
		}
	}
	return ids, manifests
}

// logLoadingProgress logs loading progress at regular intervals
// Provides visibility into the fraction loading process
func logLoadingProgress(startTime time.Time, currentIndex int, totalCount int) {
	if time.Since(startTime) >= time.Second || currentIndex == totalCount-1 {
		progressPercent := 100 * (currentIndex + 1) / totalCount
		logger.Info(
			"fraction loading progress",
			zap.String("progress", fmt.Sprintf("%d%%", progressPercent)),
			zap.Int("total", totalCount),
			zap.Int("loaded", currentIndex+1),
		)
	}
}
