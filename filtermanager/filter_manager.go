package filtermanager

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

const (
	fracInQueueExt = ".queue"
	fracDoneExt    = ".filter"
	tmpExt         = ".tmp"

	tmpDirSuffix = "_tmp"
)

const (
	defaultMaintenanceInterval = 30 * time.Second
	defaultCacheCleanInterval  = 10 * time.Millisecond
	defaultCacheGCDelay        = 1 * time.Second
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type Config struct {
	DataDir        string
	Workers        int
	CacheSizeLimit uint64
}

type FilterManager struct {
	ctx context.Context

	config  Config
	filters map[string]*Filter

	fracs   map[string][]string
	fracsMu *sync.RWMutex

	mp MappingProvider

	rateLimit chan struct{}

	maintenanceWG       *sync.WaitGroup
	maintenanceInterval time.Duration
	maintenanceStop     context.CancelFunc

	cacheCleanInterval time.Duration
	cacheGCDelay       time.Duration

	headersCache        *cache.Cache[[]lidsBlockHeader]
	headersCacheCleaner *cache.Cleaner
}

func New(
	ctx context.Context,
	cfg Config,
	params []Params,
	mp MappingProvider,
) *FilterManager {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	filtersMap := make(map[string]*Filter, len(params))

	for _, p := range params {
		f := NewFilter(p)
		filtersMap[f.Hash()] = f
	}

	cacheCleaner := cache.NewCleaner(cfg.CacheSizeLimit, nil)

	return &FilterManager{
		ctx:                 ctx,
		config:              cfg,
		filters:             filtersMap,
		fracs:               make(map[string][]string),
		fracsMu:             &sync.RWMutex{},
		mp:                  mp,
		rateLimit:           make(chan struct{}, workers),
		maintenanceInterval: defaultMaintenanceInterval,
		cacheCleanInterval:  defaultCacheCleanInterval,
		cacheGCDelay:        defaultCacheGCDelay,
		headersCache:        cache.NewCache[[]lidsBlockHeader](cacheCleaner, nil),
		headersCacheCleaner: cacheCleaner,
	}
}

func (fm *FilterManager) Start(ctx context.Context, fracs fracmanager.List) {
	fm.createDataDir()

	err := fm.loadFilters()
	if err != nil {
		logger.Fatal("failed to load previous docs filters", zap.Error(err))
	}

	err = fm.buildQueue(fracs)
	if err != nil {
		logger.Fatal("failed to build docs filters queue", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(ctx)
	fm.maintenanceStop = cancel
	fm.startMaintenance(ctx)

	go fm.cacheCleanLoop()

	mapping := fm.mp.GetMapping()

	go func() {
		for _, f := range fm.filters {
			ast, err := parser.ParseSeqQL(f.params.Query, mapping)
			if err != nil {
				panic(fmt.Errorf("BUG: search query must be valid: %s", err))
			}
			f.ast = ast

			fm.processFilter(ctx, f, fracs.FilterInRange(f.params.From, f.params.To))
		}
	}()
}

func (fm *FilterManager) Stop() {
	fm.maintenanceStop()
	fm.maintenanceWG.Wait()
}

func (fm *FilterManager) GetTombstonesIteratorByFrac(fracName string, minLID, maxLID uint32, reverse bool) (node.Node, error) {
	fm.fracsMu.RLock()
	defer fm.fracsMu.RUnlock()

	fracFiles, has := fm.fracs[fracName]
	if !has {
		return &EmptyIterator{}, nil
	}

	iterators := make([]node.Node, 0, len(fracFiles))
	for _, f := range fracFiles {
		loader, err := newLoader(f, fm.headersCache)
		if err != nil {
			logger.Error("can't open filtered lids file", zap.String("path", f), zap.Error(err))
			return nil, err
		}
		if reverse {
			iterators = append(iterators, (*IteratorAsc)(NewIterator(loader, minLID, maxLID)))
		} else {
			iterators = append(iterators, (*IteratorDesc)(NewIterator(loader, minLID, maxLID)))
		}
	}

	return NewNMergedIterators(iterators), nil
}

// RefreshFrac replaces frac's filter files with newly found results. Used after active frac is sealed.
func (fm *FilterManager) RefreshFrac(fraction frac.Fraction) {
	fm.fracsMu.RLock()
	fracsFiles, has := fm.fracs[fraction.Info().Name()]
	fm.fracsMu.RUnlock()

	if !has {
		return
	}

	for _, fileName := range fracsFiles {
		filter := fm.filters[filterNameFromPath(fileName)]

		queueFilePath := path.Join(filter.dirPath, makeFileName(fraction.Info().Name(), fracInQueueExt))
		util.MustWriteFileAtomic(queueFilePath, []byte{}, 0o666, tmpExt)

		fm.rateLimit <- struct{}{}
		go func() {
			defer func() { <-fm.rateLimit }()
			if err := fm.processFrac(fraction, filter, false); err != nil {
				panic(fmt.Errorf("docs filter refresh frac err: %s", err))
			}
		}()
	}
}

// RemoveFrac removes fraction's filter files. Used after frac is deleted
func (fm *FilterManager) RemoveFrac(fracName string) {
	fm.fracsMu.RLock()
	fracsFiles, has := fm.fracs[fracName]
	fm.fracsMu.RUnlock()

	if !has {
		return
	}

	fm.fracsMu.Lock()
	delete(fm.fracs, fracName)
	fm.fracsMu.Unlock()

	for _, fileName := range fracsFiles {
		util.RemoveFile(fileName)
	}
}

func filterNameFromPath(p string) string {
	return path.Base(path.Dir(p))
}

func (fm *FilterManager) addDoneFrac(fracName, fracPath string) {
	fm.fracsMu.Lock()
	defer fm.fracsMu.Unlock()

	fm.fracs[fracName] = append(fm.fracs[fracName], fracPath)
}

// loadFilters loads existing filters
func (fm *FilterManager) loadFilters() error {
	des, err := os.ReadDir(fm.config.DataDir)
	if err != nil {
		return err
	}

	var anyRemove bool

	for _, de := range des {
		if !de.IsDir() {
			continue
		}

		if _, ok := fm.filters[de.Name()]; !ok {
			logger.Info("there is filter folder on disk, but not in config. need to delete it.")
			err := os.RemoveAll(path.Join(fm.config.DataDir, de.Name()))
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			anyRemove = true
			continue
		}

		f := fm.filters[de.Name()]
		f.status = StatusInProgress
		f.dirPath = path.Join(fm.config.DataDir, de.Name())

		filterDes, err := os.ReadDir(f.dirPath)
		if err != nil {
			return fmt.Errorf("reading directory: %s", err)
		}

		var hasFracsInQueue bool

		for _, fde := range filterDes {
			if fde.IsDir() {
				continue
			}
			name := fde.Name()

			switch path.Ext(name) {
			case fracInQueueExt:
				hasFracsInQueue = true
			case fracDoneExt:
				fm.addDoneFrac(fracNameFromFilePath(name), path.Join(f.dirPath, name))
			}
		}

		if !hasFracsInQueue {
			f.status = StatusDone
		}
	}

	if anyRemove {
		util.MustFsyncFile(fm.config.DataDir)
	}

	return nil
}

// buildQueue creates a directory for each of unprocessed filters and creates .queue files
func (fm *FilterManager) buildQueue(fracs fracmanager.List) error {
	for _, filter := range fm.filters {
		if filter.status != StatusCreated {
			continue
		}

		// create tmp dir
		tmpDir := path.Join(fm.config.DataDir, fmt.Sprintf("%s%s", filter.Hash(), tmpDirSuffix))
		util.MustCreateDir(tmpDir)

		filterFracs := fracs.FilterInRange(seq.MID(filter.params.From), seq.MID(filter.params.To))
		for _, f := range filterFracs {
			queueFilePath := path.Join(tmpDir, makeFileName(f.Info().Name(), fracInQueueExt))
			util.MustWriteFileAtomic(queueFilePath, []byte{}, 0o666, tmpExt)
		}

		// rename tmp dir
		dir := path.Join(fm.config.DataDir, filter.Hash())
		if err := os.Rename(tmpDir, dir); err != nil {
			return err
		}
		util.MustFsyncFile(fm.config.DataDir)
		filter.dirPath = dir
	}

	return nil
}

// handleFilter finds docs and writes to fs
func (fm *FilterManager) processFilter(ctx context.Context, filter *Filter, fracs fracmanager.List) {
	if len(fracs) == 0 {
		return
	}

	fracsByName := make(map[string]frac.Fraction)
	for _, f := range fracs {
		fracsByName[f.Info().Name()] = f
	}

	filterDes, err := os.ReadDir(filter.dirPath)
	if err != nil {
		panic(fmt.Errorf("BUG: reading directory must be successful: %s", err))
	}

	inProgressFilters.Add(1)

	processFracInQueue := func(name string) error {
		f, ok := fracsByName[fracNameFromFilePath(name)]
		if !ok { // skip missing fracs
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case fm.rateLimit <- struct{}{}:
			filter.processWg.Go(func() {
				defer func() { <-fm.rateLimit }()
				if err := fm.processFrac(f, filter, false); err != nil {
					panic(fmt.Errorf("docs filter process frac err: %s", err))
				}
			})
		}
		return nil
	}
	_ = util.VisitFilesWithExt(filterDes, fracInQueueExt, processFracInQueue)

	go func() {
		filter.processWg.Wait()
		filter.markAsDone()
		inProgressFilters.Add(-1)
	}()
}

func (fm *FilterManager) processFrac(f frac.Fraction, filter *Filter, refresh bool) error {
	qpr, err := f.Search(fm.ctx, processor.SearchParams{
		AST:   filter.ast.Root,
		From:  seq.MID(filter.params.From),
		To:    seq.MID(filter.params.To),
		Limit: math.MaxInt64,
	})
	if err != nil {
		return err
	}

	queueFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracInQueueExt))
	doneFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracDoneExt))

	if len(qpr.IDs) == 0 {
		util.RemoveFile(queueFilePath)
		return nil
	}

	// TODO: here we doing part of the work twice:
	// first time we find LIDs inside f.Search() and then find IDs by these LIDs.
	// Then we again find LIDs by earlier found IDs in f.FindLIDs().
	// We did it like this because otherwise we had to do serious f.Search() rewrite.
	// For now we're ok with some performance penalty.
	lids, err := f.FindLIDs(fm.ctx, qpr.IDs.IDs())
	if err != nil {
		return err
	}

	docsFilterBin := DocsFilterBinIn{LIDs: lids}
	if err := writeDocsFilter(&docsFilterBin, queueFilePath, doneFilePath); err != nil {
		return err
	}

	if !refresh {
		fm.addDoneFrac(f.Info().Name(), doneFilePath)
	}

	return nil
}

func (fm *FilterManager) startMaintenance(ctx context.Context) {
	fm.maintenanceWG.Go(func() {
		logger.Info("start docs filter maintenance")
		util.RunEvery(ctx.Done(), fm.maintenanceInterval, func() {
			logger.Info("docs filter maintenance iteration")
			fm.checkDiskUsage()
		})
	})
}

func (fm *FilterManager) cacheCleanLoop() {
	runs := 0
	gcRunsCount := int(fm.cacheGCDelay / fm.cacheCleanInterval)

	for {
		runs++
		fm.headersCacheCleaner.Cleanup(&cache.CleanStat{})
		fm.headersCacheCleaner.Rotate()

		if runs >= gcRunsCount {
			runs = 0
			fm.headersCacheCleaner.CleanEmptyGenerations()
			fm.headersCacheCleaner.ReleaseBuckets()
		}

		time.Sleep(fm.cacheCleanInterval)
	}
}

func (fm *FilterManager) checkDiskUsage() {
	du := int64(0)

	for _, f := range fm.filters {
		des, err := os.ReadDir(f.dirPath)
		if err != nil {
			logger.Error("docs filter: can't read filter's dir",
				zap.String("filter", f.String()), zap.Error(err))
			return
		}

		for _, fde := range des {
			if fde.IsDir() {
				continue
			}
			info, err := fde.Info()
			if err != nil {
				logger.Error("docs filter: can't read filter file info",
					zap.String("filter", f.String()), zap.Error(err))
				return
			}
			du += info.Size()
		}
	}

	diskUsage.Set(float64(du))
	storedFilters.Set(float64(len(fm.filters)))
}

func makeFileName(name, ext string) string {
	return name + ext
}

func fracNameFromFilePath(filterFilePath string) string {
	return strings.Split(path.Base(filterFilePath), ".")[0]
}

var marshalBufferPool util.BufferPool

func writeDocsFilter(df *DocsFilterBinIn, queueFilePath, doneFilePath string) error {
	rawDocsFilter := marshalBufferPool.Get()
	defer marshalBufferPool.Put(rawDocsFilter)

	rawDocsFilter.B = marshalDocsFilter(rawDocsFilter.B, df)
	util.MustWriteFileAtomic(doneFilePath, rawDocsFilter.B, 0o666, tmpExt)
	util.RemoveFile(queueFilePath)

	return nil
}

// createDataDir creates data dir.
func (fm *FilterManager) createDataDir() {
	if err := os.MkdirAll(fm.config.DataDir, 0o777); err != nil {
		panic(err)
	}
}
