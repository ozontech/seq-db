package skipmaskmanager

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
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
	fracDoneExt    = ".skipmask"
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

type SkipMaskManager struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	config    Config
	skipMasks map[string]*SkipMask

	fracs   map[string][]string
	fracsMu *sync.RWMutex

	mp MappingProvider

	rateLimit chan struct{}

	bgWG                *sync.WaitGroup
	maintenanceInterval time.Duration

	cacheCleanInterval time.Duration
	cacheGCDelay       time.Duration

	headersCache        *cache.Cache[[]lidsBlockHeader]
	headersCacheCleaner *cache.Cleaner
}

func New(
	ctx context.Context,
	cfg Config,
	params []SkipMaskParams,
	mp MappingProvider,
) *SkipMaskManager {
	fmCtx, ctxCancel := context.WithCancel(ctx)

	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	skipMasksMap := make(map[string]*SkipMask, len(params))

	for _, p := range params {
		sm := NewSkipMask(p)
		skipMasksMap[sm.Hash()] = sm
	}

	cacheCleaner := cache.NewCleaner(cfg.CacheSizeLimit, nil)

	return &SkipMaskManager{
		ctx:                 fmCtx,
		ctxCancel:           ctxCancel,
		config:              cfg,
		skipMasks:           skipMasksMap,
		fracs:               make(map[string][]string),
		fracsMu:             &sync.RWMutex{},
		mp:                  mp,
		rateLimit:           make(chan struct{}, workers),
		bgWG:                &sync.WaitGroup{},
		maintenanceInterval: defaultMaintenanceInterval,
		cacheCleanInterval:  defaultCacheCleanInterval,
		cacheGCDelay:        defaultCacheGCDelay,
		headersCache:        cache.NewCache[[]lidsBlockHeader](cacheCleaner, nil),
		headersCacheCleaner: cacheCleaner,
	}
}

func (smm *SkipMaskManager) Start(fracs fracmanager.List) {
	smm.createDataDir()

	err := smm.loadSkipMasks()
	if err != nil {
		logger.Fatal("failed to load previous skip masks", zap.Error(err))
	}

	err = smm.buildQueue(fracs)
	if err != nil {
		logger.Fatal("failed to build skip mask manager queue", zap.Error(err))
	}

	smm.startMaintenance()
	smm.cacheCleanLoop()

	mapping := smm.mp.GetMapping()

	smm.bgWG.Add(1)
	go func() {
		defer smm.bgWG.Done()

		for _, sm := range smm.skipMasks {
			ast, err := parser.ParseSeqQL(sm.params.Query, mapping)
			if err != nil {
				panic(fmt.Errorf("BUG: search query must be valid: %s", err))
			}
			sm.ast = ast

			smm.processSkipMask(sm, fracs.FilterInRange(sm.params.From, sm.params.To))
		}
	}()
}

func (smm *SkipMaskManager) Stop() {
	smm.ctxCancel()
	smm.bgWG.Wait()
	logger.Info("skip mask manager stopped")
}

func (smm *SkipMaskManager) GetIDsIteratorByFrac(
	fracName string,
	minLID, maxLID uint32,
	reverse bool,
) (node.Node, bool, error) {
	smm.fracsMu.RLock()
	defer smm.fracsMu.RUnlock()

	fracFiles, has := smm.fracs[fracName]
	if !has {
		return &EmptyIterator{}, has, nil
	}

	iterators := make([]node.Node, 0, len(fracFiles))
	for _, f := range fracFiles {
		loader, err := newLoader(f, smm.headersCache)
		if err != nil {
			logger.Error("can't open skip mask file", zap.String("path", f), zap.Error(err))
			return nil, has, err
		}
		if reverse {
			iterators = append(iterators, (*IteratorAsc)(NewIterator(loader, minLID, maxLID)))
		} else {
			iterators = append(iterators, (*IteratorDesc)(NewIterator(loader, minLID, maxLID)))
		}
	}

	return NewNMergedIterators(iterators), has, nil
}

// RefreshFrac replaces frac's skip mask files with newly found results. Used after active frac is sealed.
func (smm *SkipMaskManager) RefreshFrac(fraction frac.Fraction) {
	smm.fracsMu.Lock()
	fracsFiles, has := smm.fracs[fraction.Info().Name()]
	delete(smm.fracs, fraction.Info().Name())
	smm.fracsMu.Unlock()

	if !has {
		return
	}

	// mark skip masks as InProgress
	for _, fileName := range fracsFiles {
		smm.skipMasks[skipMaskNameFromPath(fileName)].setStatus(StatusInProgress)
	}

	smm.bgWG.Add(1)
	go func() {
		defer smm.bgWG.Done()

		for _, fileName := range fracsFiles {
			util.RemoveFile(fileName)
			smm.headersCache.Evict(hashFilePath(fileName))

			skipMask := smm.skipMasks[skipMaskNameFromPath(fileName)]

			queueFilePath := path.Join(skipMask.dirPath, makeFileName(fraction.Info().Name(), fracInQueueExt))
			util.MustWriteFileAtomic(queueFilePath, []byte{}, 0o666, tmpExt)

			select {
			case <-smm.ctx.Done():
				// do not return because we have to create a .queue file for each of frac files to handle it on startup
				continue
			case smm.rateLimit <- struct{}{}:
				go func() {
					defer func() { <-smm.rateLimit }()
					if err := smm.processFrac(fraction, skipMask); err != nil {
						if errors.Is(err, context.Canceled) {
							logger.Info("skip mask manager refresh frac context cancelled")
							return
						}
						panic(fmt.Errorf("skip mask manager refresh frac err: %s", err))
					}
					skipMask.setStatus(StatusDone)
				}()
			}
		}
	}()
}

// RemoveFrac removes fraction's skip mask files. Used after frac is deleted
func (smm *SkipMaskManager) RemoveFrac(fracName string) {
	// TODO: we might want to have some kind of GC on startup to clean up missed files
	smm.bgWG.Go(func() {
		smm.fracsMu.RLock()
		fracsFiles, has := smm.fracs[fracName]
		smm.fracsMu.RUnlock()

		if !has {
			return
		}

		smm.fracsMu.Lock()
		delete(smm.fracs, fracName)
		smm.fracsMu.Unlock()

		for _, fileName := range fracsFiles {
			util.RemoveFile(fileName)
			smm.headersCache.Evict(hashFilePath(fileName))
		}
	})
}

func (smm *SkipMaskManager) IsDone() bool {
	for _, sm := range smm.skipMasks {
		if sm.getStatus() != StatusDone {
			return false
		}
	}
	return true
}

func skipMaskNameFromPath(p string) string {
	return path.Base(path.Dir(p))
}

func (smm *SkipMaskManager) addDoneFrac(fracName, fracPath string) {
	smm.fracsMu.Lock()
	defer smm.fracsMu.Unlock()

	smm.fracs[fracName] = append(smm.fracs[fracName], fracPath)
}

// loadSkipMasks loads existing skip masks
func (smm *SkipMaskManager) loadSkipMasks() error {
	des, err := os.ReadDir(smm.config.DataDir)
	if err != nil {
		return err
	}

	var anyRemove bool

	for _, de := range des {
		if !de.IsDir() {
			continue
		}

		if _, ok := smm.skipMasks[de.Name()]; !ok {
			logger.Info("there is skip mask folder on disk, but not in config. need to delete it.")
			err := os.RemoveAll(path.Join(smm.config.DataDir, de.Name()))
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			anyRemove = true
			continue
		}

		sm := smm.skipMasks[de.Name()]
		sm.setStatus(StatusInProgress)
		sm.dirPath = path.Join(smm.config.DataDir, de.Name())

		skipMaskDes, err := os.ReadDir(sm.dirPath)
		if err != nil {
			return fmt.Errorf("reading directory: %s", err)
		}

		var hasFracsInQueue bool

		for _, smde := range skipMaskDes {
			if smde.IsDir() {
				continue
			}
			name := smde.Name()

			switch path.Ext(name) {
			case fracInQueueExt:
				hasFracsInQueue = true
			case fracDoneExt:
				smm.addDoneFrac(fracNameFromFilePath(name), path.Join(sm.dirPath, name))
			}
		}

		if !hasFracsInQueue {
			sm.setStatus(StatusDone)
		}
	}

	if anyRemove {
		util.MustFsyncFile(smm.config.DataDir)
	}

	return nil
}

// buildQueue creates a directory for each of unprocessed skip masks and creates .queue files
func (smm *SkipMaskManager) buildQueue(fracs fracmanager.List) error {
	for _, skipMask := range smm.skipMasks {
		if skipMask.getStatus() != StatusCreated {
			continue
		}

		// create tmp dir
		tmpDir := path.Join(smm.config.DataDir, fmt.Sprintf("%s%s", skipMask.Hash(), tmpDirSuffix))
		util.MustCreateDir(tmpDir)

		skipMaskFracs := fracs.FilterInRange(skipMask.params.From, skipMask.params.To)
		for _, f := range skipMaskFracs {
			queueFilePath := path.Join(tmpDir, makeFileName(f.Info().Name(), fracInQueueExt))
			util.MustWriteFileAtomic(queueFilePath, []byte{}, 0o666, tmpExt)
		}

		// rename tmp dir
		dir := path.Join(smm.config.DataDir, skipMask.Hash())
		if err := os.Rename(tmpDir, dir); err != nil {
			return err
		}
		util.MustFsyncFile(smm.config.DataDir)
		skipMask.dirPath = dir
	}

	return nil
}

// processSkipMask finds docs and writes to fs
func (smm *SkipMaskManager) processSkipMask(skipMask *SkipMask, fracs fracmanager.List) {
	if len(fracs) == 0 {
		skipMask.setStatus(StatusDone)
		return
	}

	fracsByName := make(map[string]frac.Fraction)
	for _, f := range fracs {
		fracsByName[f.Info().Name()] = f
	}

	skipMaskDes, err := os.ReadDir(skipMask.dirPath)
	if err != nil {
		panic(fmt.Errorf("BUG: reading directory must be successful: %s", err))
	}

	inProgress.Add(1)

	processFracInQueue := func(name string) error {
		f, ok := fracsByName[fracNameFromFilePath(name)]
		if !ok { // skip missing fracs
			return nil
		}

		select {
		case <-smm.ctx.Done():
			return nil
		case smm.rateLimit <- struct{}{}:
			skipMask.processWg.Add(1)
			go func() {
				defer skipMask.processWg.Done()
				defer func() { <-smm.rateLimit }()
				if err := smm.processFrac(f, skipMask); err != nil {
					if errors.Is(err, context.Canceled) {
						logger.Info("skip mask manager refresh frac context cancelled")
						return
					}
					panic(fmt.Errorf("skip mask manager process frac err: %s", err))
				}
			}()
		}
		return nil
	}
	_ = util.VisitFilesWithExt(skipMaskDes, fracInQueueExt, processFracInQueue)

	go func() {
		skipMask.processWg.Wait()
		skipMask.setStatus(StatusDone)
		inProgress.Add(-1)
	}()
}

func (smm *SkipMaskManager) processFrac(f frac.Fraction, skipMask *SkipMask) error {
	qpr, err := f.Search(smm.ctx, processor.SearchParams{
		AST:   skipMask.ast.Root,
		From:  skipMask.params.From,
		To:    skipMask.params.To,
		Limit: math.MaxInt64,
	})
	if err != nil {
		return err
	}

	queueFilePath := path.Join(skipMask.dirPath, makeFileName(f.Info().Name(), fracInQueueExt))
	doneFilePath := path.Join(skipMask.dirPath, makeFileName(f.Info().Name(), fracDoneExt))

	if len(qpr.IDs) == 0 {
		util.RemoveFile(queueFilePath)
		return nil
	}

	// TODO: here we doing part of the work twice:
	// first time we find LIDs inside f.Search() and then find IDs by these LIDs.
	// Then we again find LIDs by earlier found IDs in f.FindLIDs().
	// We did it like this because otherwise we had to do serious f.Search() rewrite.
	// For now we're ok with some performance penalty.
	lids, err := f.FindLIDs(smm.ctx, qpr.IDs.IDs())
	if err != nil {
		return err
	}

	skipMaskBin := SkipMaskBinIn{LIDs: lids}
	if err := writeSkipMask(&skipMaskBin, queueFilePath, doneFilePath); err != nil {
		return err
	}

	smm.addDoneFrac(f.Info().Name(), doneFilePath)

	return nil
}

func (smm *SkipMaskManager) startMaintenance() {
	smm.bgWG.Go(func() {
		logger.Info("start skip mask manager maintenance")
		util.RunEvery(smm.ctx.Done(), smm.maintenanceInterval, func() {
			logger.Info("skip mask manager maintenance iteration")
			smm.checkDiskUsage()
		})
	})
}

func (smm *SkipMaskManager) cacheCleanLoop() {
	smm.bgWG.Go(func() {
		runs := 0
		gcRunsCount := int(smm.cacheGCDelay / smm.cacheCleanInterval)

		util.RunEvery(smm.ctx.Done(), smm.cacheCleanInterval, func() {
			runs++
			smm.headersCacheCleaner.Cleanup(&cache.CleanStat{})
			smm.headersCacheCleaner.Rotate()

			if runs >= gcRunsCount {
				runs = 0
				smm.headersCacheCleaner.CleanEmptyGenerations()
				smm.headersCacheCleaner.ReleaseBuckets()
			}
		})
	})
}

func (smm *SkipMaskManager) checkDiskUsage() {
	du := int64(0)

	for _, sm := range smm.skipMasks {
		des, err := os.ReadDir(sm.dirPath)
		if err != nil {
			logger.Error("skip mask manager: can't read skip mask's dir",
				zap.String("skip mask", sm.String()), zap.Error(err))
			return
		}

		for _, smde := range des {
			if smde.IsDir() {
				continue
			}
			info, err := smde.Info()
			if err != nil {
				logger.Error("skip mask manager: can't read skip mask file info",
					zap.String("skip mask", sm.String()), zap.Error(err))
				return
			}
			du += info.Size()
		}
	}

	diskUsage.Set(float64(du))
	stored.Set(float64(len(smm.skipMasks)))
}

func makeFileName(name, ext string) string {
	return name + ext
}

func fracNameFromFilePath(skipMaskFilePath string) string {
	return strings.Split(path.Base(skipMaskFilePath), ".")[0]
}

func hashFilePath(filePath string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(skipMaskNameFromPath(filePath) + fracNameFromFilePath(filePath)))
	return hash.Sum32()
}

var marshalBufferPool util.BufferPool

func writeSkipMask(df *SkipMaskBinIn, queueFilePath, doneFilePath string) error {
	rawSkipMask := marshalBufferPool.Get()
	defer marshalBufferPool.Put(rawSkipMask)

	rawSkipMask.B = marshalSkipMask(rawSkipMask.B, df)
	util.MustWriteFileAtomic(doneFilePath, rawSkipMask.B, 0o666, tmpExt)
	util.RemoveFile(queueFilePath)

	return nil
}

// createDataDir creates data dir.
func (smm *SkipMaskManager) createDataDir() {
	if err := os.MkdirAll(smm.config.DataDir, 0o777); err != nil {
		panic(err)
	}
}
