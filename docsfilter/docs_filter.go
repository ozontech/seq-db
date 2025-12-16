package docsfilter

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
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

func Start(
	ctx context.Context,
	cfg Config,
	params []Params,
	mp MappingProvider,
	fracs fracmanager.List,
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

	df := &DocsFilter{
		ctx:           ctx,
		config:        cfg,
		filters:       filtersMap,
		fracs:         make(map[string][]string),
		fracsMu:       &sync.RWMutex{},
		mp:            mp,
		rateLimit:     make(chan struct{}, workers),
		createDirOnce: &sync.Once{},
	}

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

	return df
}

func (df *DocsFilter) addDoneFrac(fracName string, fracPath string) {
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
		go df.processFrac(f, filter) // nolint:errcheck // in progress

		return nil
	}
	_ = util.VisitFilesWithExt(filterDes, fracInQueueExt, processFracInQueue)

	go func() {
		filter.processWg.Wait()
		filter.markAsDone()
	}()
}

func (df *DocsFilter) processFrac(f frac.Fraction, filter *Filter) error {
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

	if len(qpr.IDs) == 0 {
		return nil
	}

	doneFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracDoneExt))

	storeDocsFilter := func(rawDocsFilter []byte) error {
		util.MustWriteFileAtomic(doneFilePath, rawDocsFilter, tmpExt)
		tmpFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracInQueueExt))
		util.RemoveFile(tmpFilePath)
		return nil
	}

	docsFilterBin := DocsFilterBin{LIDs: f.FindLIDs(df.ctx, qpr.IDs.IDs())}
	if err := compressDocsFilter(&docsFilterBin, storeDocsFilter); err != nil {
		return err
	}

	df.addDoneFrac(f.Info().Name(), doneFilePath)

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

func getCompressLevel(size int) int {
	level := 3
	if size <= 512 {
		level = 1
	} else if size <= 4*1024 {
		level = 2
	}
	return level
}

// createDataDir creates dir data lazily to avoid creating extra folders.
func (df *DocsFilter) createDataDir() {
	df.createDirOnce.Do(func() {
		if err := os.MkdirAll(df.config.DataDir, 0o777); err != nil {
			panic(err)
		}
	})
}
