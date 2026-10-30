package seq

import (
	"cmp"
	"container/heap"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"unsafe"

	"github.com/valyala/fastrand"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/util"
)

type DocsOrder uint8

const (
	DocsOrderDesc DocsOrder = 0
	DocsOrderAsc  DocsOrder = 1
)

func (o DocsOrder) IsDesc() bool {
	return o == DocsOrderDesc
}

func (o DocsOrder) IsReverse() bool {
	return o == DocsOrderAsc
}

const (
	SizeOfIDSource            = int(unsafe.Sizeof(IDSource{}))
	SizeOfErrorSource         = int(unsafe.Sizeof(ErrorSource{}))
	SizeOfAggBin              = int(unsafe.Sizeof(AggBin{}))
	SizeOfAggBinSamples       = int(unsafe.Sizeof(AggBinSamples{}))
	SizeOfSamplesContainer    = int(unsafe.Sizeof(SamplesContainer{}))
	SizeOfAggregatableSamples = int(unsafe.Sizeof(AggregatableSamples{}))
	SizeOfQPR                 = int(unsafe.Sizeof(QPR{}))
)

func (f DocsOrder) String() string {
	switch f {
	case DocsOrderDesc:
		return "desc"
	case DocsOrderAsc:
		return "asc"
	default:
		return "unknown"
	}
}

type IDSource struct {
	ID     ID
	Source uint64
	Hint   string
}

func (id *IDSource) Equal(check IDSource) bool {
	return id.ID.Equal(check.ID) && id.Source == check.Source
}

func (id *IDSource) MemUsage() int {
	if id == nil {
		return 0
	}
	return SizeOfIDSource + len(id.Hint)
}

type IDSources []IDSource

func (p IDSources) Len() int           { return len(p) }
func (p IDSources) Less(i, j int) bool { return Less(p[i].ID, p[j].ID) }
func (p IDSources) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p IDSources) IDs() []ID {
	ids := make([]ID, len(p))
	for i, id := range p {
		ids[i] = id.ID
	}
	return ids
}

func (p IDSources) ApplyHint(hint string) {
	for i := range p {
		p[i].Hint = hint
	}
}

type ErrorSource struct {
	ErrStr string
	Source uint64
}

func (e *ErrorSource) MemUsage() int {
	if e == nil {
		return 0
	}
	return SizeOfErrorSource + len(e.ErrStr)
}

// QPR query partial result, stores intermediate result of running query e.g. result from only one fraction or particular store
// TODO: remove single Agg when n-agg support in proxy is deployed
type QPR struct {
	IDs       IDSources
	Histogram map[MID]uint64
	Aggs      []AggregatableSamples
	Total     uint64
	Errors    []ErrorSource
}

func (q *QPR) Empty() bool {
	return len(q.IDs) == 0 && len(q.Histogram) == 0 && len(q.Aggs) == 0
}

// MemUsage returns estimated total memory used by the QPR and its nested structs.
func (q *QPR) MemUsage() int {
	if q == nil {
		return 0
	}
	total := SizeOfQPR

	for i := range q.IDs {
		total += q.IDs[i].MemUsage()
	}
	for range q.Histogram {
		total += util.SizeOfUint64 + util.SizeOfUint64
	}
	for i := range q.Aggs {
		total += q.Aggs[i].MemUsage()
	}
	for i := range q.Errors {
		total += q.Errors[i].MemUsage()
	}

	return total
}

func (q *QPR) Aggregate(args []AggregateArgs) []AggregationResult {
	allAggregations := make([]AggregationResult, len(q.Aggs))
	for i, agg := range q.Aggs {
		allAggregations[i] = agg.Aggregate(args[i])
	}
	return allAggregations
}

func (q *QPR) CombineErrors() string {
	x := make([]byte, 0)
	for k, err := range q.Errors {
		if k > 5 {
			break
		}
		x = append(x, err.ErrStr...)
	}

	return string(x)
}

type AggFunc byte

const (
	AggFuncCount = iota
	AggFuncSum
	AggFuncMin
	AggFuncMax
	AggFuncAvg
	AggFuncQuantile
	AggFuncUnique
	AggFuncUniqueCount
)

func (f AggFunc) String() string {
	switch f {
	case AggFuncCount:
		return "count"
	case AggFuncSum:
		return "sum"
	case AggFuncMin:
		return "min"
	case AggFuncMax:
		return "max"
	case AggFuncAvg:
		return "avg"
	case AggFuncQuantile:
		return "quantile"
	case AggFuncUnique:
		return "unique"
	case AggFuncUniqueCount:
		return "unique_count"
	default:
		return "unknown"
	}
}

type AggBin struct {
	MID   MID
	Token string
}

func (b *AggBin) MemUsage() int {
	if b == nil {
		return 0
	}
	return SizeOfAggBin + len(b.Token)
}

type AggBinSamples struct {
	Bin     AggBin
	Samples SamplesContainer
}

func CompareAggBin(a, b AggBin) int {
	return cmp.Or(
		cmp.Compare(a.Token, b.Token),
		cmp.Compare(b.MID, a.MID),
	)
}

func SortAggBinSamples(bins []AggBinSamples) {
	slices.SortFunc(bins, func(a, b AggBinSamples) int {
		return CompareAggBin(a.Bin, b.Bin)
	})
}

// AggregatableSamples bins with samples for a particular aggregation request. SamplesByBin must be sorted by
// a bin comparator.
type AggregatableSamples struct {
	SamplesByBin []AggBinSamples // sorted
	NotExists    int64
	ValuesPool   []string
}

type AggregationBucket struct {
	Name      string
	Value     float64
	Quantiles []float64
	NotExists int64
	MID       MID
}

type AggregationResult struct {
	Buckets   []AggregationBucket
	NotExists int64
}

type AggregateArgs struct {
	Func                 AggFunc
	SkipWithoutTimestamp bool
	Quantiles            []float64
}

func (q *AggregatableSamples) Aggregate(args AggregateArgs) AggregationResult {
	buckets := make([]AggregationBucket, 0, len(q.SamplesByBin))

	for i := range q.SamplesByBin {
		bin := q.SamplesByBin[i].Bin
		if args.SkipWithoutTimestamp && bin.MID == consts.DummyMID {
			continue
		}
		buckets = append(buckets, q.getAggBucket(bin, &q.SamplesByBin[i].Samples, args))
	}

	sortBuckets(args.Func, buckets)

	return AggregationResult{
		Buckets:   buckets,
		NotExists: q.NotExists,
	}
}

func sortBuckets(aggFunc AggFunc, buckets []AggregationBucket) {
	sortByValueDescNameAsc := func(left, right AggregationBucket) int {
		return cmp.Or(
			cmp.Compare(left.MID, right.MID),
			cmp.Compare(right.Value, left.Value),
			cmp.Compare(left.Name, right.Name),
		)
	}

	sortByNameAscValueDesc := func(left, right AggregationBucket) int {
		return cmp.Or(
			cmp.Compare(left.MID, right.MID),
			cmp.Compare(left.Name, right.Name),
			cmp.Compare(right.Value, left.Value),
		)
	}

	sortByValueNameAsc := func(left, right AggregationBucket) int {
		return cmp.Or(
			cmp.Compare(left.MID, right.MID),
			cmp.Compare(left.Value, right.Value),
			cmp.Compare(left.Name, right.Name),
		)
	}

	sortFunc := sortByValueDescNameAsc

	switch aggFunc {
	case AggFuncMin:
		// Sort the MIN aggregation result in ascending order.
		sortFunc = sortByValueNameAsc
	case AggFuncQuantile:
		// Sort the QUANTILE aggregation result by name ASC, then by value DESC.
		sortFunc = sortByNameAscValueDesc
	}

	slices.SortFunc(buckets, sortFunc)
}

func (q *AggregatableSamples) getAggBucket(bin AggBin, hist *SamplesContainer, args AggregateArgs) AggregationBucket {
	var (
		value     float64
		quantiles []float64
	)

	switch args.Func {
	case AggFuncCount, AggFuncUnique:
		value = float64(hist.Total)
	case AggFuncSum:
		value = hist.Sum
	case AggFuncMin:
		value = hist.Min
	case AggFuncMax:
		value = hist.Max
	case AggFuncAvg:
		if hist.Total != 0 {
			value = hist.Sum / float64(hist.Total)
		}
	case AggFuncQuantile:
		if len(args.Quantiles) == 0 {
			panic(fmt.Errorf("BUG: empty quantiles"))
		}
		quantiles = make([]float64, 0, len(args.Quantiles))
		for _, q := range args.Quantiles {
			quantiles = append(quantiles, hist.Quantile(q))
		}
		value = quantiles[0]
	case AggFuncUniqueCount:
		value = float64(len(hist.Values))
	default:
		panic(fmt.Errorf("unimplemented aggregation func"))
	}

	if hist.Total == 0 && args.Func != AggFuncCount && args.Func != AggFuncUnique {
		value = math.NaN()
	}

	return AggregationBucket{
		Name:      bin.Token,
		MID:       bin.MID,
		Value:     value,
		Quantiles: quantiles,
		NotExists: hist.NotExists,
	}
}

// FindBin returns the samples stored for bin, or nil if not found.
// Used only in tests.
func (q *AggregatableSamples) FindBin(bin AggBin) *SamplesContainer {
	i, ok := slices.BinarySearchFunc(q.SamplesByBin, bin, func(entry AggBinSamples, target AggBin) int {
		return CompareAggBin(entry.Bin, target)
	})
	if !ok {
		return nil
	}
	return &q.SamplesByBin[i].Samples
}

// Merge merges another AggregatableSamples into this object.  Please note, for performance reasons
// a provided object may be mutated and left in an unpredictable state.
func (q *AggregatableSamples) Merge(aggs []AggregatableSamples) {
	if len(aggs) == 0 {
		return
	}

	poolIndex := make(map[string]uint32, len(q.ValuesPool))
	for i, v := range q.ValuesPool {
		poolIndex[v] = uint32(i)
	}

	cursors := make([]mergeCursor, 0, len(aggs)+1)
	if len(q.SamplesByBin) > 0 {
		cursors = append(cursors, mergeCursor{bins: q.SamplesByBin})
	}
	for i := range aggs {
		q.NotExists += aggs[i].NotExists

		remap := appendValuesPool(&q.ValuesPool, poolIndex, aggs[i].ValuesPool)
		if len(aggs[i].SamplesByBin) == 0 {
			continue
		}
		cursors = append(cursors, mergeCursor{
			bins:  aggs[i].SamplesByBin,
			remap: remap,
		})
	}
	if len(cursors) == 0 || (len(cursors) == 1 && len(q.SamplesByBin) > 0) {
		return
	}
	if len(cursors) == 2 {
		q.SamplesByBin = twoWayMergeBins(cursors[0], cursors[1])
		return
	}
	q.SamplesByBin = kWayMergeBins(cursors)
}

func appendValuesPool(pool *[]string, index map[string]uint32, src []string) []uint32 {
	if len(src) == 0 {
		return nil
	}
	oldToNew := make([]uint32, len(src))
	for oldIdx, v := range src {
		newIdx, ok := index[v]
		if !ok {
			newIdx = uint32(len(*pool))
			*pool = append(*pool, v)
			index[v] = newIdx
		}
		oldToNew[oldIdx] = newIdx
	}
	return oldToNew
}

type mergeCursor struct {
	bins  []AggBinSamples
	idx   int
	remap []uint32
}

func kWayMergeBins(cursors []mergeCursor) []AggBinSamples {
	h := make(binMergeHeap, 0, len(cursors))
	for i := range cursors {
		h = append(h, &cursors[i])
	}
	heap.Init(&h)

	// allocate a slice with size capacity of the largest QPR
	n := 0
	for i := range cursors {
		n = max(n, len(cursors[i].bins))
	}
	out := make([]AggBinSamples, 0, n)

	currBin, currSamples := h.next()

	for h.Len() > 0 {
		bin, samples := h.next()

		if CompareAggBin(bin, currBin) == 0 {
			currSamples.Merge(&samples)
			continue
		}

		out = append(out, AggBinSamples{Bin: currBin, Samples: currSamples})
		currBin = bin
		currSamples = samples
	}
	out = append(out, AggBinSamples{Bin: currBin, Samples: currSamples})
	return out
}

// twoWayMergeBins merges two QPRs via two pointers. The primary user is async searcher, since ordinary
// searches rely on merging several QPRs at a time.
func twoWayMergeBins(left, right mergeCursor) []AggBinSamples {
	n := max(len(left.bins), len(right.bins))
	out := make([]AggBinSamples, 0, n)

	for left.idx < len(left.bins) && right.idx < len(right.bins) {
		c := CompareAggBin(left.bins[left.idx].Bin, right.bins[right.idx].Bin)
		switch c {
		case 0:
			samples := left.bins[left.idx].Samples
			rightSamples := right.bins[right.idx].Samples
			rightSamples.Values = remapValues(rightSamples.Values, right.remap)
			samples.Merge(&rightSamples)
			out = append(out, AggBinSamples{Bin: left.bins[left.idx].Bin, Samples: samples})
			left.idx++
			right.idx++
		case -1:
			out = append(out, left.bins[left.idx])
			left.idx++
		default:
			samples := right.bins[right.idx].Samples
			samples.Values = remapValues(samples.Values, right.remap)
			out = append(out, AggBinSamples{Bin: right.bins[right.idx].Bin, Samples: samples})
			right.idx++
		}
	}
	for left.idx < len(left.bins) {
		out = append(out, left.bins[left.idx])
		left.idx++
	}
	for right.idx < len(right.bins) {
		samples := right.bins[right.idx].Samples
		samples.Values = remapValues(samples.Values, right.remap)
		out = append(out, AggBinSamples{Bin: right.bins[right.idx].Bin, Samples: samples})
		right.idx++
	}
	return out
}

type binMergeHeap []*mergeCursor

func (h binMergeHeap) Len() int {
	return len(h)
}

func (h binMergeHeap) Less(i, j int) bool {
	return CompareAggBin(h[i].bins[h[i].idx].Bin, h[j].bins[h[j].idx].Bin) < 0
}

func (h binMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *binMergeHeap) Push(cursor any) {
	*h = append(*h, cursor.(*mergeCursor))
}

func (h *binMergeHeap) Pop() any {
	old := *h
	last := len(old) - 1
	cursor := old[last]
	*h = old[:last]
	return cursor
}

func (h *binMergeHeap) next() (AggBin, SamplesContainer) {
	cursor := heap.Pop(h).(*mergeCursor)
	idx := cursor.idx
	bin := cursor.bins[idx].Bin
	samples := cursor.bins[idx].Samples
	samples.Values = remapValues(samples.Values, cursor.remap)
	cursor.idx++
	if cursor.idx < len(cursor.bins) {
		heap.Push(h, cursor)
	}
	return bin, samples
}

func remapValues(values map[uint32]struct{}, oldToNew []uint32) map[uint32]struct{} {
	if len(values) == 0 || oldToNew == nil {
		return values
	}
	remapped := make(map[uint32]struct{}, len(values))
	for oldIdx := range values {
		remapped[oldToNew[oldIdx]] = struct{}{}
	}
	return remapped
}

func (q *AggregatableSamples) MemUsage() int {
	if q == nil {
		return 0
	}

	total := SizeOfAggregatableSamples

	total += util.SizeOfString * cap(q.ValuesPool)
	for _, s := range q.ValuesPool {
		total += len(s)
	}

	total += cap(q.SamplesByBin) * SizeOfAggBinSamples
	for i := range q.SamplesByBin {
		total += len(q.SamplesByBin[i].Bin.Token)
		total += samplesMemUsage(&q.SamplesByBin[i].Samples)
	}
	return total
}

func samplesMemUsage(h *SamplesContainer) int {
	total := cap(h.Samples) * util.SizeOfFloat64
	if h.Values != nil {
		for range h.Values {
			total += util.SizeOfUint32
		}
	}
	return total
}

// SamplesContainer is a container that is used for aggregations.
// Implements reservoir sampling algorithm.
type SamplesContainer struct {
	rng fastrand.RNG

	Min float64
	Max float64
	Sum float64
	// Total is the number of inserted values.
	Total int64
	// NotExists is the number of values without a token.
	NotExists int64
	Samples   []float64
	// Values are indexes into AggregatableSamples.ValuesPool values pool
	Values map[uint32]struct{}
}

func NewSamplesContainers() *SamplesContainer {
	h := &SamplesContainer{
		Min: math.MaxInt64,
		Max: math.MinInt64,
	}
	// Fixed seed to have same result on the same input
	h.rng.Seed(73)
	return h
}

func (h *SamplesContainer) MemUsage() int {
	if h == nil {
		return 0
	}
	return SizeOfSamplesContainer + samplesMemUsage(h)
}

// Quantile calculates the quantile value of the histogram.
// The argument should be in [0, 1] range.
//
// The implementation is taken and adapted from github.com/valyala/histogram.
func (h *SamplesContainer) Quantile(quantile float64) float64 {
	if quantile < 0 || quantile > 1 {
		// Must be checked in seqproxy
		panic(fmt.Errorf("BUG: invalid quantile: %f", quantile))
	}

	if len(h.Samples) == 0 {
		return math.NaN()
	}
	if quantile == 1 {
		return h.Max
	}
	if quantile == 0 {
		return h.Min
	}

	slices.Sort(h.Samples)
	index := int(float64(len(h.Samples)-1)*quantile + 0.5) // +0.5 to round up value
	return h.Samples[index]
}

func (h *SamplesContainer) Merge(hist *SamplesContainer) {
	h.NotExists += hist.NotExists

	if hist.Total == 0 {
		return
	}

	if h.Total == 0 {
		h.Min = hist.Min
		h.Max = hist.Max
	} else {
		h.Min = min(h.Min, hist.Min)
		h.Max = max(h.Max, hist.Max)
	}

	h.Sum += hist.Sum
	h.Total += hist.Total

	for _, v := range hist.Samples {
		h.InsertSample(v)
	}
	if len(hist.Values) > 0 {
		if h.Values == nil {
			h.Values = make(map[uint32]struct{}, len(hist.Values))
		}
		maps.Copy(h.Values, hist.Values)
	}
}

func (h *SamplesContainer) InsertNTimes(num float64, cnt int64) {
	if h.Total == 0 {
		h.Min = num
		h.Max = num
	} else {
		h.Min = min(h.Min, num)
		h.Max = max(h.Max, num)
	}
	h.Sum += num * float64(cnt)
	h.Total += cnt
}

func (h *SamplesContainer) InsertSampleNTimes(sample float64, cnt int64) {
	for i := int64(0); i < cnt; i++ {
		h.InsertSample(sample)
	}
}

const maxHistogramSamples = 8096

func (h *SamplesContainer) InsertSample(num float64) {
	if len(h.Samples) < maxHistogramSamples {
		h.Samples = append(h.Samples, num)
	} else {
		h.Samples[h.rng.Uint32()%maxHistogramSamples] = num
	}
}

func (h *SamplesContainer) InsertValueIndex(poolIdx uint32, cnt int64) {
	if h.Values == nil {
		h.Values = make(map[uint32]struct{})
	}
	h.Values[poolIdx] = struct{}{}
	h.Total += cnt
}

// MergeQPRs merges several qprs into dst. Please note, for performance reasons
// provided qprs may be mutated and left in an unpredictable state.
func MergeQPRs(dst *QPR, qprs []*QPR, limit int, histInterval MID, order DocsOrder) {
	if len(qprs) == 0 {
		return
	}

	idsCount := 0
	for _, qpr := range qprs {
		idsCount += len(qpr.IDs)
	}

	dst.IDs = slices.Grow(dst.IDs, idsCount)

	for _, qpr := range qprs {
		dst.Total += qpr.Total
		if qpr.Histogram != nil && dst.Histogram == nil {
			dst.Histogram = make(map[MID]uint64)
		}
		for time, count := range qpr.Histogram {
			dst.Histogram[time] += count
		}

		dst.IDs = append(dst.IDs, qpr.IDs...)
		dst.Errors = append(dst.Errors, qpr.Errors...)
	}

	if len(dst.Aggs) == 0 {
		for _, qpr := range qprs {
			if len(qpr.Aggs) != 0 {
				dst.Aggs = make([]AggregatableSamples, len(qpr.Aggs))
				break
			}
		}
	}
	for i := range dst.Aggs {
		inputs := make([]AggregatableSamples, 0, len(qprs))
		for _, qpr := range qprs {
			if i >= len(qpr.Aggs) {
				continue
			}
			inputs = append(inputs, qpr.Aggs[i])
		}

		dst.Aggs[i].Merge(inputs)
	}

	if order.IsReverse() {
		sort.Sort(dst.IDs)
	} else {
		// it is not a bug: regular order is descending
		sort.Sort(sort.Reverse(dst.IDs))
	}

	ids, repetitionsCount := removeRepetitionsAdvanced(dst.IDs, dst.Histogram, histInterval)
	if repetitionsCount > 0 {
		metric.RepetitionsDocsTotal.Add(float64(repetitionsCount))
	}

	// count only for queries with total
	if dst.Total > 0 {
		dst.Total -= repetitionsCount
	}

	l := min(len(ids), limit)
	dst.IDs = ids[:l]
}

// remove repetition from histogram
func removeHistogramRepetition(repetition IDSource, histogram map[MID]uint64, histInterval MID) {
	bucket := repetition.ID.MID
	bucket -= bucket % histInterval
	histogram[bucket]--
}

// removes repetitions from both ids and histogram
func removeRepetitionsAdvanced(ids IDSources, histogram map[MID]uint64, histInterval MID) (IDSources, uint64) {
	if len(ids) == 0 {
		return ids, 0
	}

	removeCount := 0

	lastID := ids[0]
	for i := 1; i < len(ids); i++ {
		if lastID.ID != ids[i].ID {
			lastID = ids[i]
			ids[i-removeCount] = ids[i]
		} else {
			removeCount++

			if histInterval > 0 {
				removeHistogramRepetition(lastID, histogram, histInterval)
			}
		}
	}

	return ids[:len(ids)-removeCount], uint64(removeCount)
}
