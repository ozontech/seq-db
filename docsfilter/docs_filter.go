package docsfilter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type FilterStatus byte

const (
	StatusCreated FilterStatus = iota
	StatusInProgress
	StatusDone
	StatusError
)

type Config struct {
	DataDir string
	Workers int
}

type DocsFilterBin struct {
	LIDs []seq.LID
}

type Filter struct {
	Query string
	From  int64
	To    int64

	status FilterStatus

	ast parser.SeqQLQuery

	hash    string
	dirPath string

	processWg *sync.WaitGroup
}

func NewFilter(
	query string,
	from int64,
	to int64,
) *Filter {
	return &Filter{
		Query:     query,
		From:      from,
		To:        to,
		status:    StatusCreated,
		processWg: &sync.WaitGroup{},
	}
}

func (f *Filter) String() string {
	return fmt.Sprintf("%s_%d_%d", f.Query, f.From, f.To)
}

func (f *Filter) Hash() string {
	if f.hash == "" {
		h := sha256.New()
		h.Write([]byte(f.String()))
		bs := h.Sum(nil)
		f.hash = hex.EncodeToString(bs)
	}
	return f.hash
}

func (f *Filter) markAsDone() {
	f.status = StatusDone
}

type DocsFilter struct {
	ctx context.Context

	config  Config
	filters map[string]*Filter

	mp MappingProvider

	rateLimit     chan struct{}
	createDirOnce *sync.Once
}

func Start(
	ctx context.Context,
	cfg Config,
	filters []*Filter,
	mp MappingProvider,
	fracs fracmanager.List,
) *DocsFilter {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	filtersMap := make(map[string]*Filter, len(filters))

	for _, f := range filters {
		filtersMap[string(f.Hash())] = f
	}

	df := &DocsFilter{
		ctx:           ctx,
		config:        cfg,
		filters:       filtersMap,
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
		ast, err := parser.ParseSeqQL(f.Query, mapping)
		if err != nil {
			panic(fmt.Errorf("BUG: search query must be valid: %s", err))
		}
		f.ast = ast

		df.processFilter(f, fracs.FilterInRange(seq.MID(f.From), seq.MID(f.To)))
	}

	return df
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
		findFracsInQueue := func(name string) error {
			hasFracsInQueue = true
			return nil
		}
		if err := util.VisitFilesWithExt(filterDes, fracInQueueExt, findFracsInQueue); err != nil {
			return err
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

		filterFracs := fracs.FilterInRange(seq.MID(filter.From), seq.MID(filter.To))
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
		parts := strings.Split(name, ".")
		if len(parts) != 2 {
			return fmt.Errorf("unknown mqpr filename format: %s", name)
		}

		f := fracsByName[parts[0]]
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
		From:  seq.MID(filter.From),
		To:    seq.MID(filter.To),
		Limit: math.MaxInt64,
	})
	if err != nil {
		return err
	}

	if len(qpr.IDs) == 0 {
		return nil
	}

	storeDocsFilter := func(rawDocsFilter []byte) error {
		doneFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracDoneExt))
		util.MustWriteFileAtomic(doneFilePath, rawDocsFilter, tmpExt)
		tmpFilePath := path.Join(filter.dirPath, makeFileName(f.Info().Name(), fracInQueueExt))
		util.RemoveFile(tmpFilePath)
		return nil
	}
	docsFilterBin := DocsFilterBin{LIDs: f.FindLIDs(df.ctx, qpr.IDs.IDs())}
	if err := compressDocsFilter(&docsFilterBin, storeDocsFilter); err != nil {
		return err
	}

	return nil
}

func makeFileName(name, ext string) string {
	return name + ext
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
