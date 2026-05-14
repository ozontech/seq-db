package asyncsearcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	asyncSearchExtInfo      = ".info"
	asyncSearchExtQPR       = ".qpr"
	asyncSearchExtMergedQPR = ".mqpr"
	asyncSearchTmpFile      = ".tmp"

	minRetention = 5 * time.Minute
	maxRetention = 30 * 24 * time.Hour // 30 days
)

type infoVersion uint8

const (
	infoVersion1 infoVersion = iota + 1 // MIDs stored in milliseconds
	infoVersion2                        // MIDs stored in nanoseconds
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type AsyncSearcher struct {
	config        AsyncSearcherConfig
	createDirOnce *sync.Once

	mp MappingProvider

	requestsMu *sync.RWMutex
	requests   map[string]asyncSearchInfo
	rateLimit  chan struct{}
	readOnly   atomic.Bool

	processWg *sync.WaitGroup
}

type AsyncSearcherConfig struct {
	DataDir string
	Workers int

	MaxSize           int
	MaxSizePerRequest int
}

type fractionAcquirer interface {
	AcquireFraction(name string) (_ frac.Fraction, release func(), ok bool)
}

func MustStartAsync(config AsyncSearcherConfig, mp MappingProvider, fracs fractionAcquirer) *AsyncSearcher {
	if config.DataDir == "" {
		logger.Fatal("can't start async searcher: DataDir is empty")
	}

	asyncSearches, err := loadAsyncRequests(config.DataDir)
	if err != nil {
		logger.Fatal("failed to load previous async searches", zap.Error(err))
	}

	workers := config.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	as := &AsyncSearcher{
		config:        config,
		mp:            mp,
		requestsMu:    &sync.RWMutex{},
		requests:      asyncSearches,
		rateLimit:     make(chan struct{}, workers),
		createDirOnce: &sync.Once{},
		processWg:     &sync.WaitGroup{},
	}

	notProcessedIDs := notProcessedTasks(asyncSearches)
	for _, id := range notProcessedIDs {
		asyncSearchActiveSearches.Add(1)
		as.processWg.Add(1)
		go as.processRequest(id, fracs)
	}

	// set limit metrics that allow us to calculate alerts' thresholds
	asyncSearchDiskUsageLimit.Set(float64(config.MaxSize))
	asyncSearchConcurrencyLimit.Set(float64(config.Workers))

	go as.startMaintenance()

	return as
}

type AsyncSearchRequest struct {
	ID        string `json:"-"`
	Params    processor.SearchParams
	Query     string
	Retention time.Duration
	WithDocs  bool
}

type fracSearchState struct {
	Name string
}

type asyncSearchInfo struct {
	Version infoVersion

	// Finished is true if there are no fracs waiting to be processed.
	//
	// An async search request is considered complete only when all fracs are processed,
	// or an error occurs during processing,
	// or the request is cancelled and processing has stopped.
	Finished bool
	// Error can be non-empty only if Finished is true
	Error string `json:",omitempty"`

	CanceledAt time.Time `json:",omitzero"`
	ctx        context.Context
	cancel     func()

	Request   AsyncSearchRequest
	Fractions []fracSearchState `json:",omitempty"`
	StartedAt time.Time

	// merged is true if QPRs have been merged into a single one.
	merged *atomic.Bool
	// qprsSize is the total size of mqpr or qpr files on disk.
	qprsSize *atomic.Int64
	// infoSize is the total size of the info file on disk.
	infoSize *atomic.Int64
}

func newAsyncSearchInfo(r AsyncSearchRequest, fracNames []string) asyncSearchInfo {
	fracsToSearch := make([]fracSearchState, 0, len(fracNames))
	for _, name := range fracNames {
		fracsToSearch = append(fracsToSearch, fracSearchState{Name: name})
	}
	ctx, cancel := context.WithCancel(context.Background())
	return asyncSearchInfo{
		Version:    infoVersion2,
		Finished:   false,
		Error:      "",
		CanceledAt: time.Time{},
		ctx:        ctx,
		cancel:     cancel,
		Request:    r,
		Fractions:  fracsToSearch,
		StartedAt:  time.Now(),
		merged:     &atomic.Bool{},
		qprsSize:   &atomic.Int64{},
		infoSize:   &atomic.Int64{},
	}
}

func (i *asyncSearchInfo) Canceled() bool {
	return !i.CanceledAt.IsZero()
}

func (i *asyncSearchInfo) Expired() bool {
	expiration := i.Expiration()
	return expiration.Before(time.Now())
}

func (i *asyncSearchInfo) Expiration() time.Time {
	return i.StartedAt.Add(i.Request.Retention)
}

func (i *asyncSearchInfo) Status() AsyncSearchStatus {
	status := AsyncSearchStatusInProgress
	if i.Finished {
		if i.Canceled() {
			status = AsyncSearchStatusCanceled
		} else if i.Error != "" {
			status = AsyncSearchStatusError
		} else {
			status = AsyncSearchStatusDone
		}
	}

	return status
}

func (as *AsyncSearcher) StartSearch(r AsyncSearchRequest, fracNames []string, fracs fractionAcquirer) error {
	if as.readOnly.Load() {
		return fmt.Errorf("cannot start search on read-only mode")
	}
	as.requestsMu.RLock()
	_, ok := as.requests[r.ID]
	as.requestsMu.RUnlock()
	if ok {
		logger.Warn("async search already started", zap.String("id", r.ID))
		return nil
	}
	if err := uuid.Validate(r.ID); err != nil {
		return fmt.Errorf("invalid id %q: %s", r.ID, err)
	}

	ast, err := parser.ParseSeqQL(r.Query, as.mp.GetMapping())
	if err != nil {
		return err
	}
	r.Params.AST = ast.Root

	now := timeNow()
	if r.Retention < minRetention {
		return fmt.Errorf("retention time should be at least %s, got %s", minRetention, r.Retention)
	}
	if now.Add(r.Retention).After(now.Add(maxRetention)) {
		// Just check Retention is correct. Retention more than 90 days is not expected.
		// More fine-grained validation by specific user/tenant/environment shouldn't be implemented in seq-db.
		return fmt.Errorf("retention time should be less than %s, got %s", maxRetention, r.Retention)
	}

	if ok := as.saveSearchInfo(r, fracNames); !ok {
		// Request was saved previously, skip it
		return nil
	}
	asyncSearchActiveSearches.Add(1)
	as.processWg.Add(1)
	go as.processRequest(r.ID, fracs)
	return nil
}

func (as *AsyncSearcher) saveSearchInfo(r AsyncSearchRequest, fracNames []string) bool {
	as.requestsMu.Lock()
	defer as.requestsMu.Unlock()
	if _, ok := as.requests[r.ID]; ok {
		return false
	}
	info := newAsyncSearchInfo(r, fracNames)
	as.storeSearchInfoLocked(r.ID, info)
	return true
}

func (as *AsyncSearcher) updateSearchInfo(id string, update func(info *asyncSearchInfo)) {
	as.requestsMu.Lock()
	defer as.requestsMu.Unlock()
	info, ok := as.requests[id]
	if !ok {
		return
	}
	update(&info)
	as.storeSearchInfoLocked(id, info)
}

func (as *AsyncSearcher) getSearchInfo(id string) (asyncSearchInfo, bool) {
	as.requestsMu.RLock()
	defer as.requestsMu.RUnlock()
	v, ok := as.requests[id]
	return v, ok
}

func (as *AsyncSearcher) storeSearchInfoLocked(id string, info asyncSearchInfo) {
	as.createDataDir()
	b, err := json.Marshal(info)
	if err != nil {
		panic(err)
	}
	fpath := path.Join(as.config.DataDir, id+asyncSearchExtInfo)
	util.MustWriteFileAtomic(fpath, b, 0o666, asyncSearchTmpFile)
	info.infoSize.Store(int64(len(b)))
	as.requests[id] = info
}

// createDataDir creates dir data lazily to avoid creating extra folders.
func (as *AsyncSearcher) createDataDir() {
	as.createDirOnce.Do(func() {
		if err := os.MkdirAll(as.config.DataDir, 0o777); err != nil {
			panic(err)
		}
	})
}

func (as *AsyncSearcher) processRequest(asyncSearchID string, fracs fractionAcquirer) {
	defer as.processWg.Done()

	as.rateLimit <- struct{}{}
	defer func() { <-as.rateLimit }()

	as.doSearch(asyncSearchID, fracs)
	asyncSearchActiveSearches.Add(-1)
}

func (as *AsyncSearcher) doSearch(id string, fracs fractionAcquirer) {
	qprPaths, err := as.findQPRs(id)
	if err != nil {
		panic(fmt.Errorf("can't find QPRs for id %q: %s", id, err))
	}

	processedFracs := make(map[string]struct{})
	for _, qprPath := range qprPaths {
		fracName, err := fracNameFromQPRPath(qprPath)
		if err != nil {
			logger.Fatal("cannot find previous QPRs", zap.Error(err))
		}
		processedFracs[fracName] = struct{}{}
	}

	info, ok := as.getSearchInfo(id)
	if !ok {
		panic(fmt.Errorf("BUG: can't find async search request for id %s", id))
	}

	logger.Info("starting async search request",
		zap.String("id", id),
		zap.Any("query", info.Request.Query),
		zap.Object("params", info.Request.Params),
	)

	// AST can be nil in case of restarts.
	if info.Request.Params.AST == nil {
		ast, err := parser.ParseSeqQL(info.Request.Query, as.mp.GetMapping())
		if err != nil {
			panic(fmt.Errorf("BUG: search query must be valid: %s", err))
		}
		info.Request.Params.AST = ast.Root
	}

	for _, fracInfo := range info.Fractions {
		if _, ok := processedFracs[fracInfo.Name]; ok {
			continue
		}
		if as.shouldStopSearch(id) {
			break
		}
		if err := as.acquireAndProcessFrac(fracInfo, info, fracs); err != nil {
			as.updateSearchInfo(id, func(info *asyncSearchInfo) {
				info.Error = err.Error()
			})
			break
		}
	}
	as.updateSearchInfo(id, func(info *asyncSearchInfo) {
		info.Finished = true
	})
}

func (as *AsyncSearcher) shouldStopSearch(id string) bool {
	info, ok := as.getSearchInfo(id)
	if !ok {
		return true
	}
	if info.Canceled() || info.Expired() {
		return true
	}
	return false
}

var qprMarshalBufPool util.BufferPool

func compressQPR(qpr *seq.QPR, cb func(compressed []byte) error) error {
	rawQPR := qprMarshalBufPool.Get()
	defer qprMarshalBufPool.Put(rawQPR)

	rawQPR.B = marshalQPR(qpr, rawQPR.B)

	compressed := bytespool.Acquire(len(rawQPR.B))
	defer bytespool.Release(compressed)

	level := getCompressLevel(len(rawQPR.B))
	compressed.B = zstd.CompressLevel(rawQPR.B, compressed.B, level)
	if err := cb(compressed.B); err != nil {
		return err
	}
	return nil
}

func (as *AsyncSearcher) acquireAndProcessFrac(fracInfo fracSearchState, searchInfo asyncSearchInfo, fracs fractionAcquirer) (err error) {
	f, release, ok := fracs.AcquireFraction(fracInfo.Name)
	defer release()

	if !ok { // oldest fracs may already be removed
		logger.Info(
			"async search: skip missing fraction",
			zap.String("id", searchInfo.Request.ID),
			zap.String("frac", fracInfo.Name),
		)
		return
	}
	return as.processFrac(f, searchInfo)
}

func (as *AsyncSearcher) processFrac(f frac.Fraction, info asyncSearchInfo) (err error) {
	defer func() {
		if panicData := util.RecoverToError(recover(), asyncSearchPanics); panicData != nil {
			err = fmt.Errorf("async search panic during processFrac: %w", panicData)
		}
	}()

	qpr, err := f.Search(info.ctx, info.Request.Params)
	if err != nil {
		return err
	}

	storeQPR := func(rawQPR []byte) error {
		du := int(info.qprsSize.Load() + info.infoSize.Load())
		if as.config.MaxSizePerRequest != 0 && du+len(rawQPR) > as.config.MaxSizePerRequest {
			return fmt.Errorf("cannot complete async search request since it requires more than %dMiB of memory", as.config.MaxSizePerRequest)
		}

		name := getQPRFilename(info.Request.ID, f.Info().Name())
		fpath := path.Join(as.config.DataDir, name)
		util.MustWriteFileAtomic(fpath, rawQPR, 0o666, asyncSearchTmpFile)

		info.qprsSize.Add(int64(len(rawQPR)))
		return nil
	}
	if err := compressQPR(qpr, storeQPR); err != nil {
		return err
	}
	return nil
}

func getQPRFilename(id, fracName string) string {
	// <request_id>.<frac_name>.qpr
	return id + "." + fracName + asyncSearchExtQPR
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

func (as *AsyncSearcher) findQPRs(id string) ([]string, error) {
	des, err := os.ReadDir(as.config.DataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading directory: %s", err)
	}
	var files []string
	appendQPRInfoFile := func(name string) error {
		if !strings.HasPrefix(name, id) {
			return nil
		}
		files = append(files, path.Join(as.config.DataDir, name))
		return nil
	}
	if err := util.VisitFilesWithExt(des, asyncSearchExtQPR, appendQPRInfoFile); err != nil {
		return nil, err
	}
	return files, nil
}

func loadAsyncRequests(dataDir string) (map[string]asyncSearchInfo, error) {
	des, err := os.ReadDir(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]asyncSearchInfo{}, nil
		}
		return nil, err
	}

	areQPRsMerged := make(map[string]bool)
	loadMergedQPRsInfo := func(name string) error {
		parts := strings.Split(name, ".")
		if len(parts) != 2 {
			return fmt.Errorf("unknown mqpr filename format: %s", name)
		}
		requestID := parts[0]
		areQPRsMerged[requestID] = true
		return nil
	}
	if err := util.VisitFilesWithExt(des, asyncSearchExtMergedQPR, loadMergedQPRsInfo); err != nil {
		return nil, err
	}

	anyRemove := false
	removeMergedQPRs := func(name string) error {
		parts := strings.Split(name, ".")
		if len(parts) != 3 {
			return fmt.Errorf("unknown qpr filename format: %s", name)
		}
		requestID := parts[0]
		_, merged := areQPRsMerged[requestID]
		if !merged {
			return nil
		}
		fpath := path.Join(dataDir, name)
		util.RemoveFile(fpath)
		anyRemove = true
		return nil
	}
	if err := util.VisitFilesWithExt(des, asyncSearchExtQPR, removeMergedQPRs); err != nil {
		return nil, err
	}

	removeTmpFiles := func(name string) error {
		p := filepath.Join(dataDir, name)
		if err := os.Remove(p); err != nil {
			return fmt.Errorf("error removing temporary file: %s", err)
		}
		anyRemove = true
		return nil
	}
	if err := util.VisitFilesWithExt(des, asyncSearchTmpFile, removeTmpFiles); err != nil {
		return nil, err
	}
	if anyRemove {
		util.MustFsyncFile(dataDir)
	}

	qprsDuByID := make(map[string]int)
	infoDuByID := make(map[string]int)
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		ext := path.Ext(name)
		if ext != asyncSearchExtQPR && ext != asyncSearchExtMergedQPR && ext != asyncSearchExtInfo {
			continue
		}
		n := strings.IndexByte(name, '.')
		if n <= 0 {
			continue
		}
		id := name[:n]
		info, err := de.Info()
		if err != nil {
			return nil, fmt.Errorf("cannot get disk usage: %s", err)
		}
		size := int(info.Size())
		switch ext {
		case asyncSearchExtQPR, asyncSearchExtMergedQPR:
			qprsDuByID[id] += size
		case asyncSearchExtInfo:
			infoDuByID[id] = size
		default:
			panic("unreachable")
		}
	}

	requests := make(map[string]asyncSearchInfo)
	loadInfos := func(name string) error {
		parts := strings.Split(name, ".")
		if len(parts) != 2 {
			return fmt.Errorf("unknown info filename format: %s", name)
		}
		requestID := parts[0]
		b, err := os.ReadFile(path.Join(dataDir, name))
		if err != nil {
			return err
		}
		info := newAsyncSearchInfo(AsyncSearchRequest{}, nil)
		if err := json.Unmarshal(b, &info); err != nil {
			return fmt.Errorf("malformed async search info %q: %s", name, err)
		}

		if info.Version == 0 {
			info.Version = infoVersion1
		}
		if info.Version == infoVersion1 {
			info.Request.Params.From = seq.MillisToMID(uint64(info.Request.Params.From))
			info.Request.Params.To = seq.MillisToMID(uint64(info.Request.Params.To))
			info.Version = infoVersion2
		}

		info.merged.Store(areQPRsMerged[requestID])
		info.qprsSize.Store(int64(qprsDuByID[requestID]))
		info.infoSize.Store(int64(infoDuByID[requestID]))
		info.Request.ID = requestID
		requests[requestID] = info
		return nil
	}
	if err := util.VisitFilesWithExt(des, asyncSearchExtInfo, loadInfos); err != nil {
		return nil, err
	}
	return requests, nil
}

func notProcessedTasks(tasks map[string]asyncSearchInfo) []string {
	var toProcess []string
	for id := range tasks {
		task := tasks[id]
		if !task.Finished {
			toProcess = append(toProcess, id)
		}
	}

	// Sort tasks by start time.
	slices.SortFunc(toProcess, func(a, b string) int {
		left := tasks[a].StartedAt
		right := tasks[b].StartedAt
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
	ID    string
	Limit int
	Order seq.DocsOrder
}

type AsyncSearchStatus byte

const (
	AsyncSearchStatusDone AsyncSearchStatus = iota
	AsyncSearchStatusInProgress
	AsyncSearchStatusError
	AsyncSearchStatusCanceled
)

type FetchSearchResultResponse struct {
	Status     AsyncSearchStatus
	QPR        seq.QPR
	CanceledAt time.Time
	Error      string

	StartedAt time.Time
	ExpiresAt time.Time

	FracsDone    int
	FracsInQueue int
	DiskUsage    int

	// Stuff that needed seq-db proxy to complete async search response.
	AggQueries   []processor.AggQuery
	HistInterval uint64

	Query     string
	From      seq.MID
	To        seq.MID
	Retention time.Duration
	WithDocs  bool
	Size      int64
}

func (as *AsyncSearcher) FetchSearchResult(r FetchSearchResultRequest) (FetchSearchResultResponse, bool) {
	info, ok := as.getSearchInfo(r.ID)
	if !ok {
		return FetchSearchResultResponse{}, false
	}

	var qpr seq.QPR
	var fracsDone, fracsInQueue int
	if info.merged.Load() {
		p := path.Join(as.config.DataDir, r.ID+asyncSearchExtMergedQPR)
		qpr, _ = as.loadSearchResult([]string{p}, r.Limit, r.Order)
		fracsDone = len(info.Fractions)
		fracsInQueue = 0
	} else {
		p, err := as.findQPRs(r.ID)
		if err != nil {
			logger.Fatal("can't load async search result", zap.String("id", r.ID), zap.Error(err))
		}
		qpr, _ = as.loadSearchResult(p, r.Limit, r.Order)
		fracsDone = len(p)
		fracsInQueue = len(info.Fractions) - fracsDone
	}

	if info.Error != "" {
		qpr.Errors = append(qpr.Errors, seq.ErrorSource{
			ErrStr: info.Error,
		})
	}

	return FetchSearchResultResponse{
		Status:       info.Status(),
		QPR:          qpr,
		StartedAt:    info.StartedAt,
		ExpiresAt:    info.Expiration(),
		CanceledAt:   info.CanceledAt,
		FracsDone:    fracsDone,
		FracsInQueue: fracsInQueue,
		DiskUsage:    int(info.infoSize.Load() + info.qprsSize.Load()),
		Error:        info.Error,
		AggQueries:   info.Request.Params.AggQ,
		HistInterval: info.Request.Params.HistInterval,
		Query:        info.Request.Query,
		From:         info.Request.Params.From,
		To:           info.Request.Params.To,
		Retention:    info.Request.Retention,
		WithDocs:     info.Request.WithDocs,
		Size:         int64(info.Request.Params.Limit),
	}, true
}

func (as *AsyncSearcher) loadSearchResult(qprsPaths []string, limit int, order seq.DocsOrder) (seq.QPR, int) {
	qpr := seq.QPR{}
	size := 0
	for _, qprPath := range qprsPaths {
		compressedQPR, err := os.ReadFile(qprPath)
		size += len(compressedQPR)
		if err != nil {
			logger.Error("can't read async search result from file", zap.String("path", qprPath), zap.Error(err))
			return seq.QPR{}, 0
		}
		qprRaw, err := zstd.Decompress(compressedQPR, nil)
		if err != nil {
			logger.Fatal("can't decompress async search result", zap.String("path", qprPath), zap.Error(err))
		}

		var tmp seq.QPR
		tail, err := unmarshalQPR(&tmp, qprRaw, limit)
		if err != nil {
			logger.Fatal("can't unmarshal async search result", zap.String("path", qprPath), zap.Error(err))
		}
		if len(tail) > 0 {
			logger.Fatal("unexpected tail when unmarshaling binary QPR", zap.String("path", qprPath))
		}
		seq.MergeQPRs(&qpr, []*seq.QPR{&tmp}, limit, 1, order)
	}
	return qpr, size
}

var timeNow = time.Now

func (as *AsyncSearcher) startMaintenance() {
	for {
		logger.Info("async search maintenance iteration")
		now := timeNow()
		as.removeExpiredResults(now)
		as.merge()
		as.checkDiskUsage()
		const maintenanceInterval = 5 * time.Second
		time.Sleep(maintenanceInterval)
	}
}

func (as *AsyncSearcher) merge() {
	now := timeNow()
	var mergeJobs []mergeJob
	as.requestsMu.RLock()
	for id := range as.requests {
		info := as.requests[id]
		if !info.Finished || info.merged.Load() {
			continue
		}
		if len(info.Fractions) < 2 {
			// Nothing to merge
			continue
		}
		if info.Expiration().Sub(now) < time.Minute*10 {
			// Do not merge QPRs that will be expired soon
			continue
		}
		mergeJobs = append(mergeJobs, mergeJob{
			ID:    id,
			Fracs: info.Fractions,
			Info:  info,
		})
	}
	as.requestsMu.RUnlock()

	for id := range mergeJobs {
		job := mergeJobs[id]
		as.mergeQPRs(job)
	}
}

type mergeJob struct {
	ID    string
	Fracs []fracSearchState

	Info asyncSearchInfo
}

func (as *AsyncSearcher) mergeQPRs(job mergeJob) {
	start := time.Now()
	var qprs []string
	for _, f := range job.Fracs {
		qprFilename := getQPRFilename(job.ID, f.Name)
		qprPath := path.Join(as.config.DataDir, qprFilename)
		qprs = append(qprs, qprPath)
	}
	qpr, sizeBefore := as.loadSearchResult(qprs, job.Info.Request.Params.Limit, seq.DocsOrderDesc)

	var sizeAfter int
	storeMQPR := func(compressed []byte) error {
		sizeAfter = len(compressed)
		mqprPath := path.Join(as.config.DataDir, job.ID+asyncSearchExtMergedQPR)
		util.MustWriteFileAtomic(mqprPath, compressed, 0o666, asyncSearchTmpFile)
		return nil
	}
	if err := compressQPR(&qpr, storeMQPR); err != nil {
		panic(fmt.Errorf("can't compress async search result: %s", err))
	}

	job.Info.merged.Store(true)
	job.Info.qprsSize.Store(int64(sizeAfter))

	for _, qprPath := range qprs {
		// Remove unnecessary QPRs since we have merged QPR result
		if err := os.Remove(qprPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			logger.Fatal("can't remove async search result", zap.Error(err))
		}
	}

	logger.Info("QPRs have been merged",
		zap.String("id", job.ID),
		zap.Float64("ratio", float64(sizeBefore)/float64(sizeAfter)),
		zap.Int("fracs", len(job.Fracs)),
		zap.Duration("took", time.Since(start)),
	)
}

func (as *AsyncSearcher) removeExpiredResults(now time.Time) {
	var toRemove []string
	as.requestsMu.RLock()
	for id := range as.requests {
		r := as.requests[id]
		expired := r.Expiration().Before(now)
		if !expired {
			continue
		}
		if r.Finished {
			// Request processing has been finished and the result should expire
			toRemove = append(toRemove, id)
			continue
		}
		// Edge case: we can't remove the request while the search is running,
		// so cancel this request and remove it later
		as.requestsMu.RUnlock()
		as.updateSearchInfo(id, func(info *asyncSearchInfo) {
			info.CanceledAt = now
			info.cancel()
		})
		as.requestsMu.RLock()
	}
	as.requestsMu.RUnlock()
	if len(toRemove) == 0 {
		return
	}

	as.requestsMu.Lock()
	// Exclude expired requests from Fetch
	for _, id := range toRemove {
		delete(as.requests, id)
	}
	as.requestsMu.Unlock()

	for _, id := range toRemove {
		qprPaths, err := as.findQPRs(id)
		if err != nil {
			logger.Fatal("can't load async search results", zap.String("id", id), zap.Error(err))
		}
		for _, qprPath := range qprPaths {
			util.RemoveFile(qprPath)
		}
		util.RemoveFile(path.Join(as.config.DataDir, id+asyncSearchExtMergedQPR))
		util.RemoveFile(path.Join(as.config.DataDir, id+asyncSearchExtInfo))
	}
	logger.Info("async search results have been removed", zap.Int("count", len(toRemove)), zap.Duration("took", time.Since(now)))
}

func (as *AsyncSearcher) checkDiskUsage() {
	as.requestsMu.RLock()
	defer as.requestsMu.RUnlock()

	infoDu := 0
	qprsDu := 0
	for id := range as.requests {
		r := as.requests[id]
		infoDu += int(r.infoSize.Load())
		qprsDu += int(r.qprsSize.Load())
	}
	asyncSearchDiskUsage.WithLabelValues("info").Set(float64(infoDu))
	asyncSearchDiskUsage.WithLabelValues("qpr").Set(float64(qprsDu))
	asyncSearchStoredRequests.Set(float64(len(as.requests)))

	du := infoDu + qprsDu
	if as.config.MaxSize != 0 && du >= as.config.MaxSize {
		as.readOnly.Store(true)
		logger.Error("disk usage limit exceeded, read-only mode enabled", zap.Int("current", du), zap.Int("limit", as.config.MaxSize))
		asyncSearchReadOnly.Set(1)
	} else {
		as.readOnly.Store(false)
		asyncSearchReadOnly.Set(0)
	}
}

func (as *AsyncSearcher) CancelSearch(id string) {
	as.updateSearchInfo(id, func(info *asyncSearchInfo) {
		if info.CanceledAt.IsZero() {
			// can't cancel finished request
			if info.Status() == AsyncSearchStatusDone {
				return
			}

			info.CanceledAt = time.Now()
			info.cancel()
		}
	})
}

func (as *AsyncSearcher) DeleteSearch(id string) {
	as.updateSearchInfo(id, func(info *asyncSearchInfo) {
		if info.CanceledAt.IsZero() {
			info.CanceledAt = time.Now()
			info.cancel()
		}
		info.Request.Retention = 0
	})
}

type GetAsyncSearchesListRequest struct {
	Status *AsyncSearchStatus
	IDs    []string
}

type AsyncSearchesListItem struct {
	ID     string
	Status AsyncSearchStatus

	StartedAt  time.Time
	ExpiresAt  time.Time
	CanceledAt time.Time

	FracsDone    int
	FracsInQueue int
	DiskUsage    int

	// Search request info
	AggQueries   []processor.AggQuery
	HistInterval uint64
	Query        string
	From         seq.MID
	To           seq.MID
	Retention    time.Duration
	WithDocs     bool
	Size         int64
	Error        string
}

func (as *AsyncSearcher) GetAsyncSearchesList(r GetAsyncSearchesListRequest) []*AsyncSearchesListItem {
	idsMap := make(map[string]struct{})
	for _, id := range r.IDs {
		idsMap[id] = struct{}{}
	}

	as.requestsMu.RLock()
	defer as.requestsMu.RUnlock()

	var items []*AsyncSearchesListItem

	for id := range as.requests {
		info := as.requests[id]
		status := info.Status()

		// Filter by id
		if _, ok := idsMap[id]; !ok && len(idsMap) > 0 {
			continue
		}

		// Filter by status
		if r.Status != nil && status != *r.Status {
			continue
		}

		var fracsDone, fracsInQueue int
		if info.merged.Load() {
			fracsDone = len(info.Fractions)
			fracsInQueue = 0
		} else {
			p, err := as.findQPRs(id)
			if err != nil {
				logger.Fatal("can't load async search result", zap.String("id", id), zap.Error(err))
			}
			fracsDone = len(p)
			fracsInQueue = len(info.Fractions) - fracsDone
		}

		items = append(items, &AsyncSearchesListItem{
			ID:           id,
			Status:       status,
			StartedAt:    info.StartedAt,
			ExpiresAt:    info.Expiration(),
			CanceledAt:   info.CanceledAt,
			FracsDone:    fracsDone,
			FracsInQueue: fracsInQueue,
			DiskUsage:    int(info.infoSize.Load() + info.qprsSize.Load()),
			AggQueries:   info.Request.Params.AggQ,
			HistInterval: info.Request.Params.HistInterval,
			Query:        info.Request.Query,
			From:         info.Request.Params.From,
			To:           info.Request.Params.To,
			Retention:    info.Request.Retention,
			WithDocs:     info.Request.WithDocs,
			Size:         int64(info.Request.Params.Limit),
			Error:        info.Error,
		})
	}

	// order by StartedAt DESC
	sort.Slice(items, func(i, j int) bool {
		return items[i].StartedAt.After(items[j].StartedAt)
	})

	return items
}
