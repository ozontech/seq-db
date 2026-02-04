package docsfilter

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	fracInQueueExt = ".queue"
	fracDoneExt    = ".filter"
	tmpExt         = ".tmp"
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type Config struct {
	DataDir string
	Workers int
}

type DocsFilter struct {
	ctx context.Context

	config  Config
	filters map[string]*Filter

	fracs   map[string][]string
	fracsMu *sync.RWMutex

	mp MappingProvider

	rateLimit     chan struct{}
	createDirOnce *sync.Once
}

func New(
	ctx context.Context,
	cfg Config,
	params []Params,
	mp MappingProvider,
) *DocsFilter {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	filtersMap := make(map[string]*Filter, len(params))

	for _, p := range params {
		f := NewFilter(p)
		filtersMap[string(f.Hash())] = f
	}

	return &DocsFilter{
		ctx:           ctx,
		config:        cfg,
		filters:       filtersMap,
		fracs:         make(map[string][]string),
		fracsMu:       &sync.RWMutex{},
		mp:            mp,
		rateLimit:     make(chan struct{}, workers),
		createDirOnce: &sync.Once{},
	}
}

func (df *DocsFilter) Start(fracs fracmanager.List) {
	df.createDataDir()

	err := df.loadFilters()
	if err != nil {
		logger.Fatal("failed to load previous docs filters", zap.Error(err))
	}

	err = df.buildQueue(fracs)
	if err != nil {
		logger.Fatal("failed to build docs filters queue", zap.Error(err))
	}

	mapping := df.mp.GetMapping()

	for _, f := range df.filters {
		ast, err := parser.ParseSeqQL(f.params.Query, mapping)
		if err != nil {
			panic(fmt.Errorf("BUG: search query must be valid: %s", err))
		}
		f.ast = ast

		df.processFilter(f, fracs.FilterInRange(seq.MID(f.params.From), seq.MID(f.params.To)))
	}
}

func (df *DocsFilter) GetFilteredLIDsByFrac(fracName string) ([]seq.LID, error) {
	df.fracsMu.RLock()
	defer df.fracsMu.RUnlock()

	fracFiles, has := df.fracs[fracName]
	if !has {
		return nil, nil
	}

	var lids []seq.LID

	for _, f := range fracFiles {
		compressedLIDs, err := os.ReadFile(f)
		if err != nil {
			logger.Error("can't read filtered lids from file", zap.String("path", f), zap.Error(err))
			return nil, err
		}

		rawLIDs, err := zstd.Decompress(compressedLIDs, nil)
		if err != nil {
			logger.Error("can't decompress filtered lids from file", zap.String("path", f), zap.Error(err))
			return nil, err
		}

		dst := DocsFilterBin{}
		tail, err := unmarshalDocsFilter(&dst, rawLIDs)
		if err != nil {
			logger.Error("can't unmarshal filtered lids file", zap.String("path", f), zap.Error(err))
			return nil, err
		}
		if len(tail) > 0 {
			logger.Error("unexpected tail when unmarshaling filtered lids file", zap.String("path", f), zap.Error(err))
			return nil, err
		}

		lids = append(lids, dst.LIDs...)
	}

	// LIDs need to be sorted in case of multiple files
	if len(fracFiles) > 1 {
		slices.Sort(lids)
	}

	return lids, nil
}

// RefreshFrac replaces frac's tombstone files with newly found results. Used after active frac is sealed.
func (df *DocsFilter) RefreshFrac(fraction frac.Fraction) {
	df.fracsMu.RLock()
	fracsFiles, has := df.fracs[fraction.Info().Name()]
	df.fracsMu.RUnlock()

	if !has {
		return
	}

	for _, fileName := range fracsFiles {
		filter := df.filters[filterNameFromTombstonesPath(fileName)]

		queueFilePath := path.Join(filter.dirPath, makeFileName(fraction.Info().Name(), fracInQueueExt))
		util.MustWriteFileAtomic(queueFilePath, []byte{}, tmpExt)

		filter.processWg.Add(1)
		go func() {
			if err := df.processFrac(fraction, filter, false); err != nil {
				panic(fmt.Errorf("docs filter refresh frac err: %s", err))
			}
		}()
	}
}

// TODO: method to remove frac's tombstone files after frac is deleted

func filterNameFromTombstonesPath(p string) string {
	return path.Base(path.Dir(p))
}

func (df *DocsFilter) addDoneFrac(fracName, fracPath string) {
	df.fracsMu.Lock()
	defer df.fracsMu.Unlock()

	df.fracs[fracName] = append(df.fracs[fracName], fracPath)
}

// loadFilters loads existing filters
func (df *DocsFilter) loadFilters() error {
	des, err := os.ReadDir(df.config.DataDir)
	if err != nil {
		return err
	}

	var anyRemove bool

	for _, de := range des {
		if !de.IsDir() {
			continue
		}

		if _, ok := df.filters[de.Name()]; !ok {
			logger.Info("there is filter folder on disk, but not in config. need to delete it.")
			err := os.RemoveAll(path.Join(df.config.DataDir, de.Name()))
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			anyRemove = true
			continue
		}

		f := df.filters[de.Name()]
		f.status = StatusInProgress
		f.dirPath = path.Join(df.config.DataDir, de.Name())

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
				df.addDoneFrac(fracNameFromFilePath(name), path.Join(f.dirPath, name))
			}
		}

		if !hasFracsInQueue {
			f.status = StatusDone
		}
	}

	if anyRemove {
		util.MustFsyncFile(df.config.DataDir)
	}

	return nil
}

// buildQueue creates a directory for each of unprocessed filters and creates .queue files
func (df *DocsFilter) buildQueue(fracs fracmanager.List) error {
	for _, filter := range df.filters {
		if filter.status != StatusCreated {
			continue
		}
		filter.dirPath = path.Join(df.config.DataDir, filter.Hash())
		util.MustCreateDir(filter.dirPath)

		filterFracs := fracs.FilterInRange(seq.MID(filter.params.From), seq.MID(filter.params.To))
		for _, f := range filterFracs {
			queueFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracInQueueExt))
			util.MustWriteFileAtomic(queueFilePath, []byte{}, tmpExt)
		}
	}

	return nil
}

// handleFilter finds docs and writes to fs
func (df *DocsFilter) processFilter(filter *Filter, fracs fracmanager.List) {
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

	processFracInQueue := func(name string) error {
		f := fracsByName[fracNameFromFilePath(name)]
		filter.processWg.Add(1)
		go func() {
			if err := df.processFrac(f, filter, false); err != nil {
				panic(fmt.Errorf("docs filter process frac err: %s", err))
			}
		}()

		return nil
	}
	_ = util.VisitFilesWithExt(filterDes, fracInQueueExt, processFracInQueue)

	go func() {
		filter.processWg.Wait()
		filter.markAsDone()
	}()
}

func (df *DocsFilter) processFrac(f frac.Fraction, filter *Filter, refresh bool) error {
	defer filter.processWg.Done()

	df.rateLimit <- struct{}{}
	defer func() { <-df.rateLimit }()

	qpr, err := f.Search(df.ctx, processor.SearchParams{
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

	storeDocsFilter := func(rawDocsFilter []byte) error {
		util.MustWriteFileAtomic(doneFilePath, rawDocsFilter, tmpExt)
		util.RemoveFile(queueFilePath)
		return nil
	}

	// TODO: here we doing part of the work twice:
	// first time we find LIDs inside f.Search() and then find IDs by these LIDs.
	// Then we again find LIDs by earlier found IDs in f.FindLIDs().
	// We did it like this because otherwise we had to do serious f.Search() rewrite.
	// For now we're ok with some performance penalty.
	lids, err := f.FindLIDs(df.ctx, qpr.IDs.IDs())
	if err != nil {
		return err
	}

	docsFilterBin := DocsFilterBin{LIDs: lids}
	if err := compressDocsFilter(&docsFilterBin, storeDocsFilter); err != nil {
		return err
	}

	if !refresh {
		df.addDoneFrac(f.Info().Name(), doneFilePath)
	}

	return nil
}

func makeFileName(name, ext string) string {
	return name + ext
}

func fracNameFromFilePath(filterFilePath string) string {
	return strings.Split(path.Base(filterFilePath), ".")[0]
}

var marshalBufferPool util.BufferPool

func compressDocsFilter(df *DocsFilterBin, cb func(compressed []byte) error) error {
	rawDocsFilter := marshalBufferPool.Get()
	defer marshalBufferPool.Put(rawDocsFilter)

	rawDocsFilter.B = marshalDocsFilter(rawDocsFilter.B, df)

	compressed := bytespool.Acquire(len(rawDocsFilter.B))
	defer bytespool.Release(compressed)

	level := getCompressLevel(len(rawDocsFilter.B))
	compressed.B = zstd.CompressLevel(rawDocsFilter.B, compressed.B, level)
	if err := cb(compressed.B); err != nil {
		return err
	}
	return nil
}

// createDataDir creates dir data lazily to avoid creating extra folders.
func (df *DocsFilter) createDataDir() {
	df.createDirOnce.Do(func() {
		if err := os.MkdirAll(df.config.DataDir, 0o777); err != nil {
			panic(err)
		}
	})
}
