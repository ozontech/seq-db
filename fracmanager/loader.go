package fracmanager

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
)

// Loader is responsible for loading and initializing fractions from filesystem
// Coordinates the process of discovering, validating, and loading all fraction types
type Loader struct {
	config    *Config           // loader configuration
	provider  *fractionProvider // provider for creating fraction objects
	infoCache *fracInfoCache    // new empty info cache

	cacheStat struct {
		hits   int // counter of fractions loaded from frac info cache
		misses int // counter of fractions loaded without using frac info cache
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
func (l *Loader) Load(ctx context.Context) (*frac.Active, []*frac.Sealed, []*frac.Remote, error) {
	// Stage 1: Discover all fractions in filesystem
	actives, locals, remotes, err := l.discover(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// Stage 2: Replay active fractions and seal them
	active, sealed, err := l.replayAndSeal(ctx, actives)
	if err != nil {
		return nil, nil, nil, err
	}

	// Stage 3: Create new active fraction if no existing ones
	if active == nil {
		active = l.provider.CreateActive()
	}

	// Stage 4: Combine all local fractions
	locals = append(locals, sealed...)
	return active, locals, remotes, nil
}

// replayAndSeal replays active fractions and seals old ones
// Key method for ensuring data consistency during restart
func (l *Loader) replayAndSeal(ctx context.Context, actives []*frac.Active) (*frac.Active, []*frac.Sealed, error) {
	if len(actives) == 0 {
		return nil, nil, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(l.config.ReplayWorkers)

	sealed := make([]*frac.Sealed, len(actives)-1)

	for i, a := range actives[:len(actives)-1] {
		g.Go(func() error {
			// Replay operations from WAL to restore state
			if err := a.Replay(ctx); err != nil {
				return err
			}

			if a.Info().DocsTotal == 0 {
				a.Suicide() // can't seal empty, skip it
				return nil
			}

			s, err := l.provider.Seal(a)
			if err != nil {
				return err
			}
			sealed[i] = s

			return nil
		})
	}

	last := actives[len(actives)-1]
	g.Go(func() error { return last.Replay(ctx) }) // last frac stays active and is not sealed just replayed

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	n := 0
	for i, s := range sealed { // skip empty
		if s != nil {
			sealed[n] = sealed[i]
			n++
		}
	}

	return last, sealed[:n], nil
}

// discover discovers all fractions in filesystem
// Returns fractions separated by type: active, local, remote
func (l *Loader) discover(ctx context.Context) ([]*frac.Active, []*frac.Sealed, []*frac.Remote, error) {
	// Scan and analyze fraction files. Filter valid fractions
	manifests, err := analyzeFiles(l.scanFiles())
	if err != nil {
		return nil, nil, nil, err
	}

	total := len(manifests)
	logProgress := progressLogger(time.Millisecond * 500)

	actives := make([]*frac.Active, 0)
	locals := make([]*frac.Sealed, 0, total)
	remotes := make([]*frac.Remote, 0, total)

	loadedInfoCache := NewFracInfoCacheFromDisk(l.infoCache.fullPath)

	for i, manifest := range manifests {
		switch manifest.Stage() {
		case fracStageActive:
			actives = append(actives, l.provider.NewActive(manifest.basePath))
		case fracStageSealed:
			locals = append(locals, l.loadSealed(manifest.basePath, loadedInfoCache))
		case fracStageRemote:
			remotes = append(remotes, l.loadRemote(ctx, manifest.basePath, loadedInfoCache))
		default:
			logger.Error("unexpected fraction stage", zap.Any("manifest", manifest))
		}
		logProgress(i, total)
	}

	logger.Info("fractions initialization completed",
		zap.Int("cached", l.cacheStat.hits),
		zap.Int("uncached", l.cacheStat.misses))

	return actives, locals, remotes, nil
}

// loadSealed loads a sealed fraction using cache
func (l *Loader) loadSealed(basePath string, loadedInfoCache *fracInfoCache) *frac.Sealed {
	info, found := loadedInfoCache.Get(filepath.Base(basePath))
	l.updateStats(found)

	f := l.provider.NewSealed(basePath, info)
	l.infoCache.Add(f.Info())
	return f
}

// loadRemote loads a remote fraction
func (l *Loader) loadRemote(ctx context.Context, basePath string, loadedInfoCache *fracInfoCache) *frac.Remote {
	info, found := loadedInfoCache.Get(filepath.Base(basePath))
	l.updateStats(found)

	f := l.provider.NewRemote(ctx, basePath, info)
	l.infoCache.Add(f.Info())
	return f
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
func (l *Loader) scanFiles() []string {
	fullPattern := filepath.Join(l.config.DataDir, fileBasePattern+"*")
	files, err := filepath.Glob(fullPattern)
	if err != nil {
		logger.Panic("failed to scan fraction files", zap.Error(err))
	}
	return files
}

// progressLogger returns function that logs discovering progress no more frequently than the specified interval
// Provides visibility into the fraction loading process
func progressLogger(interval time.Duration) func(currentIndex, totalCount int) {
	ts := time.Now()
	return func(currentIndex, totalCount int) {
		if time.Since(ts) >= interval || currentIndex == totalCount-1 || currentIndex == 0 {
			logger.Info(
				"fraction list discovering",
				zap.String("progress", fmt.Sprintf("%d%%", 100*(currentIndex+1)/totalCount)),
				zap.Int("total", totalCount),
				zap.Int("loaded", currentIndex+1),
			)
			ts = time.Now()
		}
	}
}
