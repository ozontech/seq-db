package processor

import (
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// AggBin is a container for documents which were written in the same time interval.
// When dealing with aggregation (without need in building time series) [AggBin.MID] is equal to [DummyMID].
type AggBin[T comparable] struct {
	MID    seq.MID
	Source T
}

// ExtractMIDFunc is necessary since in aggregators we do not have [idsIndex] interface,
// we need a way to extract timestamps of documents to build time series.
// Must return nil slice for ordinary aggs.
type ExtractMIDFunc func(lids []node.LID, dst []seq.MID) []seq.MID

// twoSources contains sources for groupBy and field
// Source actually means id in the TIDs slice.
type twoSources struct {
	GroupBySource uint32
	FieldSource   uint32
}

// TwoSourceAggregator implements Aggregator interface
// and can iterate over groupBy and field node.Sourced to collect a histogram.
type TwoSourceAggregator struct {
	field *SourcedNodeIterator
	// groupNotExists is the counter for non-existent groups.
	groupNotExists int64
	groupBy        *SourcedNodeIterator
	// groupByNotExists is the map to count non-existent groups by source.
	// Source (key in the map) actually is an index in groupByTIDs.
	groupByNotExists map[uint32]int64
	// collectSamples is a flag to indicate if collect samples is required, this is useful if you need to calculate the quantile.
	collectSamples bool
	// collectValues is a flag to indicate if collect values is required
	collectValues bool
	// countBySource map to count occurrences by histogram source.
	countBySource map[AggBin[twoSources]]int64
	// extractMID will be used for building time series.
	extractMID ExtractMIDFunc
	midsBuf    []seq.MID
	// limits enforces upper bound constraints on how many unique values we parse and hold in memory
	limits AggLimits
}

func NewGroupAndFieldAggregator(
	fieldIterator, groupByIterator *SourcedNodeIterator,
	fn ExtractMIDFunc, collectSamples bool, collectValues bool,
	limits AggLimits,
) *TwoSourceAggregator {
	return &TwoSourceAggregator{
		collectSamples:   collectSamples,
		collectValues:    collectValues,
		countBySource:    make(map[AggBin[twoSources]]int64),
		field:            fieldIterator,
		groupNotExists:   0,
		groupBy:          groupByIterator,
		groupByNotExists: make(map[uint32]int64),
		extractMID:       fn,
		limits:           limits,
	}
}

// Next iterates over groupBy and field iterators (actually trees) to count occurrence.
func (n *TwoSourceAggregator) Next(lids []node.LID) error {
	groupSources, err := n.groupBy.ConsumeTokenSource(lids)
	if err != nil {
		return err
	}

	fieldSources, err := n.field.ConsumeTokenSource(lids)
	if err != nil {
		return err
	}

	mids := n.extractMID(lids, n.midsBuf)
	tsAgg := mids != nil
	n.midsBuf = mids[:0]

	for i := range lids {
		fieldSource := fieldSources[i]
		groupSource := groupSources[i]

		if fieldSource < 0 && groupSource < 0 {
			// Both group and field do not exist.
			continue
		}

		if fieldSource < 0 {
			// Field does not exist, but group exists.
			n.groupByNotExists[uint32(groupSource)]++
			continue
		}

		if groupSource < 0 {
			// Group does not exist, but field exists.
			n.groupNotExists++
			continue
		}

		mid := seq.MID(consts.DummyMID)
		if tsAgg {
			mid = mids[i]
		}

		// Both group and field exist, increment the count for the combined sources.
		source := AggBin[twoSources]{
			MID: mid,
			Source: twoSources{
				GroupBySource: uint32(groupSource),
				FieldSource:   uint32(fieldSource),
			},
		}

		n.countBySource[source]++
	}
	return nil
}

// Aggregate processes and returns the final aggregation result.
func (n *TwoSourceAggregator) Aggregate() (seq.AggregatableSamples, error) {
	n.groupBy.prefetchTokenValues()
	n.field.prefetchTokenValues()

	aggMap := make(map[seq.AggBin]*seq.SamplesContainer, n.groupBy.UniqueSources())

	var sourceValuePool []string
	sourceValuePoolMap := make(map[string]uint32)

	for groupBySource, cnt := range n.groupByNotExists {
		groupByVal := seq.AggBin{Token: n.groupBy.ValueBySource(groupBySource)}
		if aggMap[groupByVal] == nil {
			aggMap[groupByVal] = seq.NewSamplesContainers()
		}
		aggMap[groupByVal].NotExists = cnt
	}

	for bin, cnt := range n.countBySource {
		// Name of the group, for example, it can be service name.
		groupByVal := n.groupBy.ValueBySource(bin.Source.GroupBySource)

		aggBin := seq.AggBin{MID: bin.MID, Token: groupByVal}
		if aggMap[aggBin] == nil {
			aggMap[aggBin] = seq.NewSamplesContainers()
		}
		hist := aggMap[aggBin]

		// For example, for a value named "request_duration" it can be "42.13"
		value := n.field.ValueBySource(bin.Source.FieldSource)

		if n.collectValues {
			poolIdx, exists := sourceValuePoolMap[value]
			if !exists {
				poolIdx = uint32(len(sourceValuePool))
				sourceValuePool = append(sourceValuePool, value)
				sourceValuePoolMap[value] = poolIdx
				if n.limits.MaxFieldValues > 0 && len(sourceValuePool) > n.limits.MaxFieldValues {
					return seq.AggregatableSamples{}, consts.ErrTooManyFieldValues
				}
			}
			hist.InsertValueIndex(poolIdx, cnt)
		} else {
			num, err := parseNum(value)
			if err != nil {
				return seq.AggregatableSamples{}, err
			}

			// The same token can appear multiple times,
			// so we need to insert the num cnt times.
			hist.InsertNTimes(num, cnt)
			if n.collectSamples {
				hist.InsertSampleNTimes(num, cnt)
			}
		}
	}

	return seq.AggregatableSamples{
		NotExists:    n.groupNotExists,
		SamplesByBin: aggMap,
		ValuesPool:   sourceValuePool,
	}, nil
}

func (n *TwoSourceAggregator) Dispose() {
	n.field.Dispose()
	n.groupBy.Dispose()
}

func parseNum(str string) (float64, error) {
	// TODO: allow time.Duration and data units (kb, mb, gb, etc) parsing.
	num, err := strconv.ParseFloat(str, 64)
	if err != nil || math.IsNaN(num) || math.IsInf(num, 0) {
		return 0, fmt.Errorf("parse errors reached, last_value=%q", str)
	}
	return num, nil
}

// sourceCounter stores per-source totals for a single count aggregation.
type sourceCounter interface {
	update(sources []int, mids []seq.MID)
	get(group *SourcedNodeIterator) map[seq.AggBin]*seq.SamplesContainer
	notExists() int64
}

// plainSourceCounter is a counter used in ordinary aggregations. It currently relies on SourcedNodeIterator's
// countBySource internal per-source counter.
type plainSourceCounter struct {
	notExistsCnt int64
}

func newPlainSourceCounter() *plainSourceCounter {
	return &plainSourceCounter{}
}

func (c *plainSourceCounter) update(sources []int, _ []seq.MID) {
	for i := range sources {
		if sources[i] < 0 {
			c.notExistsCnt++
		}
	}
}

func (c *plainSourceCounter) get(group *SourcedNodeIterator) map[seq.AggBin]*seq.SamplesContainer {
	dst := make(map[seq.AggBin]*seq.SamplesContainer, group.UniqueSources())
	group.countBySource.forEach(func(source uint32, cnt uint64) {
		aggBin := seq.AggBin{
			Token: group.ValueBySource(source),
			MID:   consts.DummyMID,
		}
		if dst[aggBin] == nil {
			dst[aggBin] = seq.NewSamplesContainers()
		}
		dst[aggBin].Total = int64(cnt)
	})

	if c.notExistsCnt > 0 {
		// Handle non-existent sources in legacy format.
		dst[seq.AggBin{
			Token: "_not_exists",
			MID:   consts.DummyMID,
		}] = &seq.SamplesContainer{Total: c.notExistsCnt}
	}
	return dst
}

func (c *plainSourceCounter) notExists() int64 {
	return c.notExistsCnt
}

// tsSourceCounter is a counter used in time series count aggregations.
type tsSourceCounter struct {
	counts       map[AggBin[int]]int64
	notExistsCnt int64
}

func newTsSourceCounter() *tsSourceCounter {
	return &tsSourceCounter{
		counts: make(map[AggBin[int]]int64),
	}
}

func (c *tsSourceCounter) update(sources []int, mids []seq.MID) {
	for i, mid := range mids {
		if sources[i] < 0 {
			c.notExistsCnt++
			continue
		}
		c.counts[AggBin[int]{
			MID:    mid,
			Source: sources[i],
		}]++
	}
}

func (c *tsSourceCounter) get(group *SourcedNodeIterator) map[seq.AggBin]*seq.SamplesContainer {
	dst := make(map[seq.AggBin]*seq.SamplesContainer, group.UniqueSources())
	for bin, cnt := range c.counts {
		aggBin := seq.AggBin{
			Token: group.ValueBySource(uint32(bin.Source)),
			MID:   bin.MID,
		}

		samples := dst[aggBin]
		if samples == nil {
			samples = seq.NewSamplesContainers()
			dst[aggBin] = samples
		}
		samples.Total = cnt
	}

	// FIXME(dkharms): It will not work correctly with time series, since
	// we also have to spread [notExists] across different time bins.
	if c.notExistsCnt > 0 {
		// Handle non-existent sources in legacy format.
		dst[seq.AggBin{
			Token: "_not_exists",
			MID:   consts.DummyMID,
		}] = &seq.SamplesContainer{Total: c.notExistsCnt}
	}
	return dst
}

func (c *tsSourceCounter) notExists() int64 {
	return c.notExistsCnt
}

// SingleSourceCountAggregator aggregates counts for a single source.
type SingleSourceCountAggregator struct {
	counter    sourceCounter
	group      *SourcedNodeIterator
	extractMID ExtractMIDFunc
	midsBuf    []seq.MID
}

func NewSingleSourceCountAggregator(
	iterator *SourcedNodeIterator, fn ExtractMIDFunc, timeseries bool,
) *SingleSourceCountAggregator {
	var counts sourceCounter
	if timeseries {
		counts = newTsSourceCounter()
	} else {
		counts = newPlainSourceCounter()
	}
	return &SingleSourceCountAggregator{
		counter:    counts,
		extractMID: fn,
		group:      iterator,
	}
}

// Next iterates over groupBy tree to count occurrence.
func (n *SingleSourceCountAggregator) Next(lids []node.LID) error {
	sources, err := n.group.ConsumeTokenSource(lids)
	if err != nil {
		return err
	}

	mids := n.extractMID(lids, n.midsBuf)
	n.midsBuf = mids[:0]

	n.counter.update(sources, mids)
	return nil
}

func (n *SingleSourceCountAggregator) Aggregate() (seq.AggregatableSamples, error) {
	n.group.prefetchTokenValues()

	return seq.AggregatableSamples{
		NotExists:    n.counter.notExists(),
		SamplesByBin: n.counter.get(n.group),
	}, nil
}

func (n *SingleSourceCountAggregator) Dispose() {
	n.group.Dispose()
}

// SingleSourceUniqueAggregator aggregates unique values for a single source.
type SingleSourceUniqueAggregator struct {
	values    map[int]struct{}
	group     *SourcedNodeIterator
	notExists int64
}

func NewSingleSourceUniqueAggregator(iterator *SourcedNodeIterator) *SingleSourceUniqueAggregator {
	return &SingleSourceUniqueAggregator{
		values:    make(map[int]struct{}),
		notExists: 0,
		group:     iterator,
	}
}

// Next iterates over groupBy tree to count occurrence.
func (n *SingleSourceUniqueAggregator) Next(lids []node.LID) error {
	sources, err := n.group.ConsumeTokenSource(lids)
	if err != nil {
		return err
	}

	for i := range lids {
		if sources[i] >= 0 {
			n.values[sources[i]] = struct{}{}
			continue
		}

		n.notExists++
	}
	return nil
}

func (n *SingleSourceUniqueAggregator) Aggregate() (seq.AggregatableSamples, error) {
	n.group.prefetchTokenValues()

	aggMap := make(map[seq.AggBin]*seq.SamplesContainer, n.group.UniqueSources())

	for val := range n.values {
		aggBin := seq.AggBin{
			Token: n.group.ValueBySource(uint32(val)),
		}

		if aggMap[aggBin] == nil {
			aggMap[aggBin] = seq.NewSamplesContainers()
		}
	}

	return seq.AggregatableSamples{
		NotExists:    n.notExists,
		SamplesByBin: aggMap,
	}, nil
}

func (n *SingleSourceUniqueAggregator) Dispose() {
	n.group.Dispose()
}

type SingleSourceHistogramAggregator struct {
	field          *SourcedNodeIterator
	histogram      map[seq.MID]*seq.SamplesContainer
	collectSamples bool
	extractMID     ExtractMIDFunc
	midsBuf        []seq.MID
}

func NewSingleSourceHistogramAggregator(
	field *SourcedNodeIterator, collectSamples bool, fn ExtractMIDFunc,
) *SingleSourceHistogramAggregator {
	return &SingleSourceHistogramAggregator{
		field:          field,
		histogram:      make(map[seq.MID]*seq.SamplesContainer),
		collectSamples: collectSamples,
		extractMID:     fn,
	}
}

func (n *SingleSourceHistogramAggregator) Next(lids []node.LID) error {
	sources, err := n.field.ConsumeTokenSource(lids)
	if err != nil {
		return err
	}

	mids := n.extractMID(lids, n.midsBuf)
	n.midsBuf = mids[:0]

	for i := range lids {
		mid := seq.MID(consts.DummyMID)
		if mids != nil {
			mid = mids[i]
		}
		if _, ok := n.histogram[mid]; !ok {
			n.histogram[mid] = seq.NewSamplesContainers()
		}
		histogram := n.histogram[mid]

		if sources[i] < 0 {
			histogram.NotExists++
			continue
		}

		// TODO(dkharms): Sequence of `source` values
		// is in a random order so we again lose benefits of kernel read-ahead.
		// Maybe it's worth it to do something like [prefetchTokenValues].
		value := n.field.ValueBySource(uint32(sources[i]))
		num, err := parseNum(value)
		if err != nil {
			return err
		}

		histogram.InsertNTimes(num, 1)
		if n.collectSamples {
			histogram.InsertSample(num)
		}
	}
	return nil
}

func (n *SingleSourceHistogramAggregator) Aggregate() (seq.AggregatableSamples, error) {
	qprHist := seq.AggregatableSamples{
		SamplesByBin: make(map[seq.AggBin]*seq.SamplesContainer, len(n.histogram)),
	}

	for mid, histogram := range n.histogram {
		qprHist.SamplesByBin[seq.AggBin{
			MID: mid,
		}] = histogram
	}

	return qprHist, nil
}

func (n *SingleSourceHistogramAggregator) Dispose() {
	n.field.Dispose()
}

const (
	sourceChunkSize = 1024
	sourceChunkMask = sourceChunkSize - 1
)

// SourcedNodeIterator can iterate the sourced node that returns source, which means index in a tids slice.
type SourcedNodeIterator struct {
	sourcedNode node.Sourced
	ti          tokenIndex
	tids        []uint32
	field       string

	tokensCache map[uint32]string

	uniqSourcesLimit iteratorLimit
	countBySource    sourceCountMap

	lastID     node.LID
	lastSource uint32

	sourcesBuf []int
}

func NewSourcedNodeIterator(sourced node.Sourced, ti tokenIndex, tids []uint32, field string, limit iteratorLimit) *SourcedNodeIterator {
	lastID, lastSource := sourced.NextSourced()
	return &SourcedNodeIterator{
		sourcedNode:      sourced,
		ti:               ti,
		tids:             tids,
		field:            field,
		tokensCache:      make(map[uint32]string),
		uniqSourcesLimit: limit,
		countBySource:    newSourceCountMap(len(tids)),
		lastID:           lastID,
		lastSource:       lastSource,
	}
}

// ConsumeTokenSource resolves token sources for a batch of lids.
// The returned slice is owned by the iterator and reused on the next call.
func (s *SourcedNodeIterator) ConsumeTokenSource(lids []node.LID) ([]int, error) {
	s.sourcesBuf = slices.Grow(s.sourcesBuf[:0], len(lids))[:len(lids)]

	for i, lid := range lids {
		for s.lastID.Less(lid) {
			lastID, lastSource := s.sourcedNode.NextSourcedGeq(lid)
			s.lastID = lastID
			s.lastSource = lastSource
		}

		if s.lastID.IsNull() || s.lastID != lid {
			s.sourcesBuf[i] = -1
			continue
		}

		isNewSource := s.countBySource.add(s.lastSource)
		if isNewSource && s.uniqSourcesLimit.limit > 0 && s.countBySource.size > s.uniqSourcesLimit.limit {
			s.sourcesBuf[i] = -1
			return nil, fmt.Errorf("%w: iterator limit is exceeded", s.uniqSourcesLimit.err)
		}

		s.sourcesBuf[i] = int(s.lastSource)
	}

	return s.sourcesBuf, nil
}

func (s *SourcedNodeIterator) prefetchTokenValues() {
	if s.ti == nil || s.countBySource.size == 0 {
		return
	}
	s.countBySource.forEach(func(source uint32, _ uint64) {
		s.tokensCache[source] = string(s.ti.GetValByTID(s.tids[source], s.field))
	})
}

func (s *SourcedNodeIterator) ValueBySource(source uint32) string {
	if val, ok := s.tokensCache[source]; ok {
		return val
	}

	const useCacheThreshold = 2
	if s.countBySource.count(source) < useCacheThreshold {
		return string(s.ti.GetValByTID(s.tids[source], s.field))
	}

	val := string(s.ti.GetValByTID(s.tids[source], s.field))
	s.tokensCache[source] = val

	return val
}

func (s *SourcedNodeIterator) UniqueSources() int {
	return s.countBySource.size
}

func (s *SourcedNodeIterator) Dispose() {
	if s.sourcedNode != nil {
		s.sourcedNode.Dispose()
		s.sourcedNode = nil
	}
}

// sourceCountMap keeps a number of occurrences for each source. It uses a chunked array
// for better performance.
type sourceCountMap struct {
	chunks []*[sourceChunkSize]uint64
	size   int
}

func newSourceCountMap(size int) sourceCountMap {
	return sourceCountMap{
		chunks: make([]*[sourceChunkSize]uint64, (size+sourceChunkSize-1)/sourceChunkSize),
	}
}

// add increments a counter by source and returns if it was previously zero
func (c *sourceCountMap) add(source uint32) bool {
	chunkIdx := int(source / sourceChunkSize)
	chunk := c.chunks[chunkIdx]
	if chunk == nil {
		chunk = &[sourceChunkSize]uint64{}
		c.chunks[chunkIdx] = chunk
	}

	offset := source & sourceChunkMask
	isNew := chunk[offset] == 0
	chunk[offset]++
	if isNew {
		c.size++
	}

	return isNew
}

func (c *sourceCountMap) count(source uint32) uint64 {
	chunk := c.chunks[source/sourceChunkSize]
	if chunk == nil {
		return 0
	}
	return chunk[source&sourceChunkMask]
}

func (c *sourceCountMap) forEach(fn func(source uint32, count uint64)) {
	for chunkId, chunk := range c.chunks {
		if chunk == nil {
			continue
		}
		for offset, count := range chunk {
			if count == 0 {
				continue
			}
			fn(uint32(chunkId*sourceChunkSize+offset), count)
		}
	}
}

func provideExtractTimeFunc(sw *stopwatch.Stopwatch, idx idsIndex, interval int64) ExtractMIDFunc {
	if interval <= 0 {
		// Dummy implementation for aggregation without time series.
		return ExtractMIDFunc(func([]node.LID, []seq.MID) []seq.MID {
			return nil
		})
	}

	bin := seq.MillisToMID(uint64(interval))
	timer := sw.Timer("agg_get_mid")
	return ExtractMIDFunc(func(lids []node.LID, dst []seq.MID) []seq.MID {
		timer.Start()
		mids := idx.GetMIDs(lids, dst)
		timer.Stop()
		for i, mid := range mids {
			mids[i] = mid - mid%bin
		}
		return mids
	})
}

// QueryStats carries search-side statistics used to choose an aggregation plan.
type QueryStats struct {
	EstimatedSearchLIDs int
	LIDRange            int
}

func NewQueryStats(sample []node.LID, minLID, maxLID uint32) QueryStats {
	return QueryStats{
		EstimatedSearchLIDs: estimateSearchLIDs(sample, minLID, maxLID),
		LIDRange:            int(maxLID - minLID + 1),
	}
}

// estimateSearchLIDs extrapolates total matching LIDs from the first search batch density.
func estimateSearchLIDs(batch []node.LID, minLID, maxLID uint32) int {
	// TODO check reverse order
	if len(batch) == 0 {
		return 0
	}

	searchRange := int(maxLID - minLID + 1)
	if searchRange == 0 {
		return 0
	}

	firstLID := int(batch[0].Unpack())
	lastLID := int(batch[len(batch)-1].Unpack())
	batchRange := util.Abs(lastLID - firstLID)

	if batchRange == 0 {
		batchRange = 1
	}

	n := len(batch) * searchRange / batchRange
	if n < len(batch) {
		n = len(batch)
	}
	if n > searchRange {
		n = searchRange
	}
	return n
}

// treeAggOps roughly estimates CPU operations (complexity) for OR tree aggregation
func treeAggOps(searchLids, aggTids int) int {
	// We overestimate number of CPU operations to be aggTids for each lid pass to an agg tree
	// instead of log2(aggTids), even though agg tree is a binary tree.
	// When skipping happens in agg tree we 'NextGeq' each tree node, while log2(tids) complexity is acheived only
	// when we do not skip lids at all. If no skipping happens, then column plan is way more effective anyway.
	// Therefore, tids is more realstic estimation than just log2(tids).
	return searchLids * aggTids
}

// columnAggOps roughly estimates CPU operations (complexity) for materialized column aggregation
func columnAggOps(searchLids, aggLids int) int {
	return searchLids + aggLids
}

func useColumnAggPlan(stats QueryStats, aggTidsCount int) bool {
	if aggTidsCount == 0 {
		return false
	}

	return columnAggOps(stats.EstimatedSearchLIDs, stats.LIDRange) < treeAggOps(stats.EstimatedSearchLIDs, aggTidsCount)
}
