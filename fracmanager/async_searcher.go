package fracmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/zstd"
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type AsyncSearcher struct {
	config AsyncSearcherConfig

	mp MappingProvider

	fracManager *FracManager

	requestsMu sync.RWMutex
	requests   map[string]asyncSearchInfo

	rateLimit chan struct{}

	createDirOnce *sync.Once
}

type AsyncSearcherConfig struct {
	DataDir     string
	Parallelism int
}

func MustStartAsync(config AsyncSearcherConfig, mp MappingProvider, fm *FracManager) *AsyncSearcher {
	if config.DataDir == "" {
		logger.Fatal("can't start async searcher: DataDir is empty")
	}

	asyncSearches, err := loadAsyncSearches(config.DataDir)
	if err != nil {
		logger.Fatal("failed to load previous async searches", zap.Error(err))
	}

	parallelism := config.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}

	as := &AsyncSearcher{
		config:        config,
		mp:            mp,
		fracManager:   fm,
		requestsMu:    sync.RWMutex{},
		requests:      asyncSearches,
		rateLimit:     make(chan struct{}, parallelism),
		createDirOnce: &sync.Once{},
	}

	notProcessedIDs := notProcessedTasks(asyncSearches)
	for _, id := range notProcessedIDs {
		go as.processRequest(id)
	}

	return as
}

type AsyncSearchRequest struct {
	ID        string
	Params    processor.SearchParams
	Query     string
	Retention time.Duration
}

type fracSearchState struct {
	Name string
}

type asyncSearchInfo struct {
	Done       bool
	Request    AsyncSearchRequest
	Fractions  []fracSearchState
	Expiration time.Time
	StartTime  time.Time
}

func (as *AsyncSearcher) StartSearch(r AsyncSearchRequest) error {
	as.requestsMu.RLock()
	_, ok := as.requests[r.ID]
	as.requestsMu.RUnlock()
	if ok {
		logger.Warn("async search already started", zap.String("id", r.ID))
		return nil
	}

	ast, err := parser.ParseSeqQL(r.Query, as.mp.GetMapping())
	if err != nil {
		return err
	}
	r.Params.AST = ast.Root

	fracs := as.fracManager.GetFracs(FracTypeLocal|FracTypeRemote).FilterInRange(r.Params.From, r.Params.To)
	fracsToSearch := make([]fracSearchState, 0, len(fracs))
	for _, f := range fracs {
		fracsToSearch = append(fracsToSearch, fracSearchState{Name: f.Info().Name()})
	}

	// It can be empty if the replica does not contain the required data.
	// In this case, it is necessary to consider that the asynchronous request is completed.
	requestDone := len(fracs) == 0

	now := time.Now()
	info := asyncSearchInfo{
		Done:       requestDone,
		Request:    r,
		Fractions:  fracsToSearch,
		Expiration: now.Add(r.Retention),
		StartTime:  now,
	}
	as.updateSearchInfo(r.ID, info)

	if !requestDone {
		go as.processRequest(r.ID)
	}

	return nil
}

func (as *AsyncSearcher) updateSearchInfo(id string, info asyncSearchInfo) {
	as.requestsMu.Lock()
	defer as.requestsMu.Unlock()

	as.mustWriteSearchInfo(id, info)
	as.requests[id] = info
}

const asyncSearchFileExtension = ".info"

func (as *AsyncSearcher) mustWriteSearchInfo(id string, info asyncSearchInfo) {
	as.createDataDir()

	infoRaw, err := json.Marshal(info)
	if err != nil {
		logger.Fatal("can't encode async request", zap.Error(err))
	}

	fpath := path.Join(as.config.DataDir, id+asyncSearchFileExtension)
	mustWriteFileAtomic(fpath, infoRaw)
}

func (as *AsyncSearcher) processRequest(asyncSearchID string) {
	as.rateLimit <- struct{}{}
	defer func() { <-as.rateLimit }()

	logger.Info("start to process async search request", zap.String("id", asyncSearchID))
	if err := as.doSearch(asyncSearchID); err != nil {
		logger.Fatal("async search failed", zap.Error(err))
	}
}

func (as *AsyncSearcher) doSearch(id string) error {
	qprPaths, err := as.loadQPRPaths(id)
	if err != nil {
		return fmt.Errorf("loading processed fracs: %s", err)
	}

	processedFracs := make(map[string]struct{})
	for _, qprPath := range qprPaths {
		fracName, err := fracNameFromQPRPath(qprPath)
		if err != nil {
			return err
		}
		processedFracs[fracName] = struct{}{}
	}

	as.requestsMu.RLock()
	state, ok := as.requests[id]
	astParsed := state.Request.Params.AST != nil
	as.requestsMu.RUnlock()
	if !ok {
		panic(fmt.Errorf("BUG: can't find async search request for id %s", id))
	}

	// AST can be nil in case of restarts.
	if !astParsed {
		as.requestsMu.Lock()
		// Check if another worker has parsed the AST.
		if state.Request.Params.AST == nil {
			ast, err := parser.ParseSeqQL(state.Request.Query, as.mp.GetMapping())
			if err != nil {
				panic(fmt.Errorf("BUG: search query must be valid: %s", err))
			}
			state.Request.Params.AST = ast.Root
		}
		as.requestsMu.Unlock()
	}

	r := state.Request
	fracsInRange := as.fracManager.GetFracs(FracTypeLocal|FracTypeRemote).FilterInRange(r.Params.From, r.Params.To)
	fracsByName := make(map[string]frac.Fraction)
	for _, f := range fracsInRange {
		fracsByName[f.Info().Name()] = f
	}

	for _, fracInfo := range state.Fractions {
		if _, ok := processedFracs[fracInfo.Name]; ok {
			continue
		}
		f := fracsByName[fracInfo.Name]

		if err := as.processFrac(f, state.Request); err != nil {
			return fmt.Errorf("processing fraction %s: %s", fracInfo.Name, err)
		}
	}

	state.Done = true

	as.updateSearchInfo(id, state)

	return nil
}

func (as *AsyncSearcher) processFrac(f frac.Fraction, r AsyncSearchRequest) error {
	dp, release := f.DataProvider(context.Background())
	qpr, err := dp.Search(r.Params)
	release()

	if err != nil {
		return err
	}

	qprRaw, err := json.Marshal(qpr)
	if err != nil {
		panic(fmt.Errorf("BUG: can't encode async search request: %s", err))
	}

	buf := bytespool.Acquire(len(qprRaw))
	defer bytespool.Release(buf)

	// zstd uses level 3 as the default value, which has optimal compression ratio and speed.
	const compressionLevel = 3
	buf.B = zstd.CompressLevel(qprRaw, buf.B, compressionLevel)

	fpath := path.Join(as.config.DataDir, r.ID+"."+f.Info().Name()+qprExtension) // <data-dir>/<request_id>.<frac_name>.qpr
	mustWriteFileAtomic(fpath, buf.B)

	return nil
}

func fracNameFromQPRPath(qprPath string) (string, error) {
	filename := path.Base(qprPath)
	parts := strings.Split(filename, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("unknown qpr filename format")
	}
	// example path: a087f43f-40f6-49ae-8743-2fd7bef1dfd5.seq-db-01JQRFHKSBZTY5NSZ937WV987B.qpr
	// parts[0] is request ID
	// parts[1] is fraction name
	// parts[2] is extension
	return parts[1], nil
}

const qprExtension = ".qpr"

func (as *AsyncSearcher) loadQPRPaths(id string) ([]string, error) {
	pattern := path.Join(as.config.DataDir, id+"*"+qprExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func loadAsyncSearches(dataDir string) (map[string]asyncSearchInfo, error) {
	requests := make(map[string]asyncSearchInfo)

	pattern := path.Join(dataDir, "*"+asyncSearchFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	for _, fpath := range files {
		filename := path.Base(fpath)

		ext := path.Ext(filename)
		if ext != asyncSearchFileExtension {
			logger.Fatal("unknown file", zap.String("filename", filename))
		}

		requestID := filename[:len(filename)-len(ext)]
		b, err := os.ReadFile(fpath)
		if err != nil {
			return nil, err
		}
		var req asyncSearchInfo
		if err := json.Unmarshal(b, &req); err != nil {
			logger.Error("can't load async search request", zap.String("filename", filename), zap.Error(err))
			continue
		}
		// It is difficult to marshal/unmarshal AST, so set it to nil and parse it later.
		req.Request.Params.AST = nil
		requests[requestID] = req
	}

	return requests, nil
}

func notProcessedTasks(tasks map[string]asyncSearchInfo) []string {
	var toProcess []string

	// nolint:gocritic // rangeValCopy (each iteration copies 216 bytes) – it's ok here
	for id, task := range tasks {
		if !task.Done {
			toProcess = append(toProcess, id)
		}
	}

	// Sort tasks by start time.
	slices.SortFunc(toProcess, func(a, b string) int {
		left := tasks[a].StartTime
		right := tasks[b].StartTime
		if left.After(right) {
			return -1
		}
		if left.Equal(right) {
			return 0
		}
		return 1
	})

	return toProcess
}

type FetchSearchResultRequest struct {
	ID string
}

type FetchSearchResultResponse struct {
	QPR        seq.QPR
	Expiration time.Time
	Done       bool

	// Stuff that needed seq-db proxy to complete async search response.
	AggQueries   []processor.AggQuery
	HistInterval uint64
	Order        seq.DocsOrder
}

func (as *AsyncSearcher) FetchSearchResult(r FetchSearchResultRequest) (FetchSearchResultResponse, bool) {
	as.requestsMu.RLock()
	info, ok := as.requests[r.ID]
	as.requestsMu.RUnlock()
	if !ok {
		return FetchSearchResultResponse{}, false
	}

	qprPaths, err := as.loadQPRPaths(r.ID)
	if err != nil {
		logger.Error("can't load async search result", zap.String("id", r.ID), zap.Error(err))
	}

	qpr := seq.QPR{}
	for _, qprPath := range qprPaths {
		compressedQPR, err := os.ReadFile(qprPath)
		if err != nil {
			logger.Error("can't load async search result", zap.String("id", r.ID), zap.Error(err))
		}
		qprRaw, err := zstd.Decompress(compressedQPR, nil)
		if err != nil {
			logger.Error("can't load decompress search result", zap.String("id", r.ID), zap.Error(err))
		}

		var tmp seq.QPR
		if err := json.Unmarshal(qprRaw, &tmp); err != nil {
			logger.Error("can't unmarshal search result", zap.String("id", r.ID), zap.Error(err))
		}

		seq.MergeQPRs(&qpr, []*seq.QPR{&tmp}, math.MaxInt, 1, info.Request.Params.Order)
	}

	return FetchSearchResultResponse{
		QPR:          qpr,
		Expiration:   info.Expiration,
		Done:         info.Done,
		AggQueries:   info.Request.Params.AggQ,
		HistInterval: info.Request.Params.HistInterval,
		Order:        info.Request.Params.Order,
	}, true
}

// createDataDir creates dir data lazily to avoid creating extra folders.
func (as *AsyncSearcher) createDataDir() {
	as.createDirOnce.Do(func() {
		if err := os.MkdirAll(as.config.DataDir, 0o777); err != nil {
			panic(err)
		}
	})
}

func mustWriteFileAtomic(fpath string, data []byte) {
	fpathTmp := fpath + ".tmp"

	f, err := os.Create(fpathTmp)
	if err != nil {
		logger.Fatal("can't create file", zap.Error(err))
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Fatal("can't close file", zap.Error(err))
		}
	}()

	if _, err := f.Write(data); err != nil {
		logger.Fatal("can't write to file", zap.Error(err))
	}

	if err := f.Sync(); err != nil {
		logger.Fatal("can't sync file", zap.Error(err))
	}

	if err := os.Rename(fpathTmp, fpath); err != nil {
		logger.Fatal("can't rename file", zap.Error(err))
	}

	absFpath, err := filepath.Abs(fpath)
	if err != nil {
		logger.Fatal("can't get absolute path", zap.String("path", fpath), zap.Error(err))
	}
	dir := path.Dir(absFpath)
	mustFsyncFile(dir)
}

func mustFsyncFile(fpath string) {
	dirFile, err := os.Open(fpath)
	if err != nil {
		logger.Fatal("can't open dir", zap.Error(err))
	}
	if err := dirFile.Sync(); err != nil {
		logger.Fatal("can't sync dir", zap.Error(err))
	}
	if err := dirFile.Close(); err != nil {
		logger.Fatal("can't close dir", zap.Error(err))
	}
}
