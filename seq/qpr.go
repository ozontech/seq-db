package seq

import (
	"bytes"
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

// QPR query partial result, stores intermediate result of running query e.g. result from only one fraction or particular store.
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
	for i := range q.Aggs {
		allAggregations[i] = q.Aggs[i].Aggregate(args[i])
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
		cmp.Compare(b.MID, a.MID),
		cmp.Compare(a.Token, b.Token),
	)
}

func SortAggBinSamples(bins []AggBinSamples) {
	slices.SortFunc(bins, func(a, b AggBinSamples) int {
		return CompareAggBin(a.Bin, b.Bin)
	})
}

// AggregatableSamples is an aggregation partial state.
// All columns Values must be sorted via the following comparator: (MID desc, Tokens asc)
type AggregatableSamples struct {
	Tokens     []StringColumn
	MIDs       Int64Column // nil for ordinary aggs
	Values     AggValues
	NotExists  int64
	ValuesPool []string
}

func NewAggregatableSamples(width, capacity int) *AggregatableSamples {
	return NewAggregatableSamplesForFunc(AggFuncCount, width, capacity)
}

func NewAggregatableSamplesForFunc(fn AggFunc, width, capacity int) *AggregatableSamples {
	agg := &AggregatableSamples{
		Tokens: make([]StringColumn, width),
		Values: newAggValues(fn, capacity),
	}
	for i := range agg.Tokens {
		agg.Tokens[i] = NewStringColumn(capacity)
	}
	return agg
}

func NewAggregatableSamplesFromColumns(fn AggFunc, tokens []StringColumn) *AggregatableSamples {
	return &AggregatableSamples{
		Tokens: tokens,
		Values: newAggValues(fn, 0),
	}
}

func (q *AggregatableSamples) AppendRow(tokens []string, mid MID, samples SamplesContainer) {
	if len(tokens) != len(q.Tokens) {
		return
	}
	for i, token := range tokens {
		q.Tokens[i].AppendString(token)
	}
	q.appendMID(mid)
	q.Values.appendRow(&samples)
}

func (q *AggregatableSamples) AppendRowBytes(tokens [][]byte, mid MID, samples SamplesContainer) {
	if len(tokens) != len(q.Tokens) {
		return
	}
	for i, token := range tokens {
		q.Tokens[i].Append(token)
	}
	q.appendMID(mid)
	q.Values.appendRow(&samples)
}

func (q *AggregatableSamples) appendMID(mid MID) {
	if mid == consts.DummyMID && q.MIDs == nil {
		return
	}
	if q.MIDs == nil {
		q.MIDs = NewInt64Column(q.tokenRows() - 1)
	}
	for q.MIDs.Len() < q.tokenRows()-1 {
		q.MIDs.Append(int64(consts.DummyMID))
	}
	q.MIDs.Append(int64(mid))
}

func (q *AggregatableSamples) midRows() []MID {
	if q.MIDs == nil {
		return nil
	}
	mids := make([]MID, q.MIDs.Len())
	it := q.MIDs.NewIter()
	for i := 0; it.Next(); i++ {
		mids[i] = MID(it.Value())
	}
	return mids
}

func int64ColumnRows(column Int64Column, rows int) []int64 {
	values := make([]int64, rows)
	if column == nil {
		return values
	}
	it := column.NewIter()
	for i := 0; it.Next(); i++ {
		values[i] = it.Value()
	}
	return values
}

func (q *AggregatableSamples) tokenRows() int {
	if len(q.Tokens) == 0 {
		return 0
	}
	return q.Tokens[0].Len()
}

func (q *AggregatableSamples) RowsCount() int {
	if q.MIDs != nil {
		return q.MIDs.Len()
	}
	return q.tokenRows()
}

func (q *AggregatableSamples) RowMID(row int) MID {
	if q.MIDs == nil || row >= q.MIDs.Len() {
		return consts.DummyMID
	}
	return MID(q.MIDs.At(row))
}

func (q *AggregatableSamples) RowSamples(row int) *SamplesContainer {
	values := &q.Values
	samples := &SamplesContainer{}
	switch values.Func {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		samples.Total = values.Totals.At(row)
		if values.Func != AggFuncCount {
			samples.NotExists = values.NotExists.At(row)
		}
	}
	switch values.Func {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile:
		samples.Sum = values.Sums.At(row)
		samples.Min = values.Mins.At(row)
		samples.Max = values.Maxs.At(row)
		if values.Func == AggFuncQuantile {
			samples.Samples = values.Quantiles.Row(row)
		}
	case AggFuncUniqueCount:
		samples.Values = make(map[uint32]struct{}, len(values.UniqueIDs.Row(row)))
		for _, id := range values.UniqueIDs.Row(row) {
			samples.Values[id] = struct{}{}
		}
	}
	return samples
}

func (q *AggregatableSamples) SortRows() {
	rows := q.RowsCount()
	if rows < 2 {
		return
	}
	midsRows := q.midRows()
	tokensRows := make([][][]byte, len(q.Tokens))
	for i := range q.Tokens {
		tokensRows[i] = materializeColumnRows(q.Tokens[i])
	}
	order := make([]int, rows)
	for i := range order {
		order[i] = i
	}
	midAt := func(row int) MID {
		if len(midsRows) == 0 {
			return consts.DummyMID
		}
		return midsRows[row]
	}
	slices.SortFunc(order, func(left, right int) int {
		if c := cmp.Compare(midAt(right), midAt(left)); c != 0 {
			return c
		}
		for i := range tokensRows {
			if c := bytes.Compare(tokensRows[i][left], tokensRows[i][right]); c != 0 {
				return cmpSign(c)
			}
		}
		return 0
	})
	tokens := make([]StringColumn, len(q.Tokens))
	for i := range tokens {
		tokens[i] = NewStringColumn(rows)
	}
	var mids Int64Column
	if q.MIDs != nil {
		mids = NewInt64Column(len(order))
	}
	values := newAggValues(q.Values.Func, len(order))
	totalsRows := int64ColumnRows(q.Values.Totals, rows)
	notExistsRows := int64ColumnRows(q.Values.NotExists, rows)
	for _, row := range order {
		if mids != nil {
			mids.Append(int64(midsRows[row]))
		}
		values.appendFrom(&q.Values, row, totalsRows[row], notExistsRows[row], nil)
		for j := range tokens {
			tokens[j].Append(tokensRows[j][row])
		}
	}
	q.Tokens = tokens
	q.MIDs = mids
	q.Values = values
}

func materializeColumnRows(col StringColumn) [][]byte {
	rows := make([][]byte, col.Len())
	var arena []byte
	it := col.NewIter()
	for i := 0; it.Next(); i++ {
		row := it.Row()
		start := len(arena)
		arena = append(arena, row...)
		rows[i] = arena[start:]
	}
	return rows
}

func cmpSign(c int) int {
	if c < 0 {
		return -1
	}
	return 1
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
	buckets := make([]AggregationBucket, 0, q.RowsCount())

	var it StringColumnIter
	if len(q.Tokens) > 0 {
		it = q.Tokens[0].NewIter()
	}
	var totals Int64ColumnIter
	switch q.Values.Func {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		totals = q.Values.Totals.NewIter()
	}
	for i := 0; it.Next(); i++ {
		total := int64(0)
		if totals != nil && totals.Next() {
			total = totals.Value()
		}
		mid := q.RowMID(i)
		if args.SkipWithoutTimestamp && mid == consts.DummyMID {
			continue
		}
		bin := AggBin{MID: mid, Token: string(it.Row())}
		buckets = append(buckets, q.aggBucket(bin, i, total, args))
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

func (q *AggregatableSamples) aggBucket(bin AggBin, row int, total int64, args AggregateArgs) AggregationBucket {
	var (
		value     float64
		quantiles []float64
	)
	hist := &q.Values

	switch args.Func {
	case AggFuncCount, AggFuncUnique:
		if args.Func == AggFuncCount {
			value = float64(total)
		}
	case AggFuncSum:
		value = hist.Sums.At(row)
	case AggFuncMin:
		value = hist.Mins.At(row)
	case AggFuncMax:
		value = hist.Maxs.At(row)
	case AggFuncAvg:
		if total != 0 {
			value = hist.Sums.At(row) / float64(total)
		}
	case AggFuncQuantile:
		if len(args.Quantiles) == 0 {
			panic(fmt.Errorf("BUG: empty quantiles"))
		}
		quantiles = make([]float64, 0, len(args.Quantiles))
		for _, q := range args.Quantiles {
			quantiles = append(quantiles, hist.quantile(row, q))
		}
		value = quantiles[0]
	case AggFuncUniqueCount:
		value = float64(hist.uniqueCount(row))
	default:
		panic(fmt.Errorf("unimplemented aggregation func"))
	}

	if total == 0 && args.Func != AggFuncCount && args.Func != AggFuncUnique {
		value = math.NaN()
	}

	return AggregationBucket{
		Name:      bin.Token,
		MID:       bin.MID,
		Value:     value,
		Quantiles: quantiles,
		NotExists: bucketNotExists(hist, row),
	}
}

func bucketNotExists(values *AggValues, row int) int64 {
	switch values.Func {
	case AggFuncCount, AggFuncUnique:
		return 0
	}
	return values.NotExists.At(row)
}

func (q *AggregatableSamples) Merge(aggs []AggregatableSamples) {
	if len(aggs) == 0 {
		return
	}
	if q.Values.Func == 0 && q.RowsCount() == 0 {
		q.Values.Func = aggs[0].Values.Func
		q.Values = newAggValues(q.Values.Func, 0)
	}
	for i := range aggs {
		if aggs[i].Values.Func != q.Values.Func {
			panic(fmt.Errorf("cannot merge %s aggregation into %s aggregation", aggs[i].Values.Func, q.Values.Func))
		}
	}

	poolIndex := make(map[string]uint32, len(q.ValuesPool))
	for i, v := range q.ValuesPool {
		poolIndex[v] = uint32(i)
	}

	width := len(q.Tokens)
	for i := range aggs {
		width = max(width, len(aggs[i].Tokens))
	}

	out := &AggregatableSamples{
		Tokens:     make([]StringColumn, width),
		Values:     newAggValues(q.Values.Func, 0),
		NotExists:  q.NotExists,
		ValuesPool: q.ValuesPool,
	}
	for i := range out.Tokens {
		out.Tokens[i] = NewStringColumn(0)
	}

	cursors := make([]mergeCursor, 0, len(aggs)+1)
	if q.RowsCount() > 0 {
		cursors = append(cursors, mergeCursor{aggs: q})
	}
	haveRows := q.RowsCount() > 0
	for i := range aggs {
		out.NotExists += aggs[i].NotExists

		remap := appendValuesPool(&out.ValuesPool, poolIndex, aggs[i].ValuesPool)
		if aggs[i].RowsCount() == 0 {
			continue
		}
		haveRows = true
		cursors = append(cursors, mergeCursor{aggs: &aggs[i], remap: remap})
	}
	if !haveRows {
		q.ValuesPool = out.ValuesPool
		q.NotExists = out.NotExists
		return
	}
	for i := range cursors {
		cursors[i].init()
	}
	if len(cursors) == 2 {
		twoWayMergeColumns(out, &cursors[0], &cursors[1])
	} else {
		kWayMergeColumns(out, cursors)
	}
	*q = *out
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

func AggregatableSamplesFromBins(bins []AggBinSamples) AggregatableSamples {
	var fn AggFunc = AggFuncCount
	for i := range bins {
		if len(bins[i].Samples.Values) != 0 {
			fn = AggFuncUniqueCount
			break
		}
	}
	if fn == AggFuncCount {
		for i := range bins {
			if bins[i].Samples.Sum != 0 || bins[i].Samples.Min != 0 || bins[i].Samples.Max != 0 || len(bins[i].Samples.Samples) != 0 {
				fn = AggFuncSum
				break
			}
		}
	}
	agg := NewAggregatableSamplesForFunc(fn, 1, len(bins))
	for i := range bins {
		agg.AppendRow([]string{bins[i].Bin.Token}, bins[i].Bin.MID, bins[i].Samples)
	}
	return *agg
}

func (q *AggregatableSamples) FindBin(bin AggBin) *SamplesContainer {
	if len(q.Tokens) != 1 {
		return nil
	}
	target := []byte(bin.Token)
	it := q.Tokens[0].NewIter()
	for row := 0; it.Next(); row++ {
		c := cmp.Compare(bin.MID, q.RowMID(row))
		if c == 0 {
			c = bytes.Compare(it.Row(), target)
		}
		if c > 0 {
			break
		}
		if c == 0 {
			return q.RowSamples(row)
		}
	}
	return nil
}

type mergeCursor struct {
	aggs          *AggregatableSamples
	iters         []StringColumnIter
	mids          Int64ColumnIter
	totals        Int64ColumnIter
	notExistsIter Int64ColumnIter
	idx           int
	remap         []uint32
}

func (c *mergeCursor) init() {
	c.idx = 0
	c.iters = make([]StringColumnIter, len(c.aggs.Tokens))
	for i := range c.aggs.Tokens {
		c.iters[i] = c.aggs.Tokens[i].NewIter()
		c.iters[i].Next()
	}
	if c.aggs.MIDs != nil {
		c.mids = c.aggs.MIDs.NewIter()
		c.mids.Next()
	}
	switch c.aggs.Values.Func {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		c.totals = c.aggs.Values.Totals.NewIter()
		c.totals.Next()
	}
	switch c.aggs.Values.Func {
	case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		c.notExistsIter = c.aggs.Values.NotExists.NewIter()
		c.notExistsIter.Next()
	}
}

func (c *mergeCursor) next() bool {
	c.idx++
	if c.idx >= c.aggs.RowsCount() {
		return false
	}
	for i := range c.iters {
		c.iters[i].Next()
	}
	if c.mids != nil {
		c.mids.Next()
	}
	if c.totals != nil {
		c.totals.Next()
	}
	if c.notExistsIter != nil {
		c.notExistsIter.Next()
	}
	return true
}

func (c *mergeCursor) total() int64 {
	if c.totals == nil {
		return 0
	}
	return c.totals.Value()
}

func (c *mergeCursor) notExists() int64 {
	if c.notExistsIter == nil {
		return 0
	}
	return c.notExistsIter.Value()
}

func (c *mergeCursor) mid() MID {
	if c.mids == nil {
		return consts.DummyMID
	}
	return MID(c.mids.Value())
}

func (c *mergeCursor) valid() bool {
	return c.idx < c.aggs.RowsCount()
}

func (c *mergeCursor) row(i int) []byte {
	if i >= len(c.iters) {
		return nil
	}
	return c.iters[i].Row()
}

func compareMergeCursors(left, right *mergeCursor) int {
	if c := cmp.Compare(right.mid(), left.mid()); c != 0 {
		return c
	}
	width := min(len(left.iters), len(right.iters))
	for i := range width {
		if c := bytes.Compare(left.iters[i].Row(), right.iters[i].Row()); c != 0 {
			return cmpSign(c)
		}
	}
	return 0
}

func twoWayMergeColumns(dst *AggregatableSamples, left, right *mergeCursor) {
	for left.valid() && right.valid() {
		switch compareMergeCursors(left, right) {
		case 0:
			appendMergedRow(dst, left)
			dst.Values.mergeInto(dst.RowsCount()-1, right, right.remap)
			left.next()
			right.next()
		case -1:
			appendMergedRow(dst, left)
			left.next()
		default:
			appendMergedRow(dst, right)
			right.next()
		}
	}
	for left.valid() {
		appendMergedRow(dst, left)
		left.next()
	}
	for right.valid() {
		appendMergedRow(dst, right)
		right.next()
	}
}

func kWayMergeColumns(dst *AggregatableSamples, cursors []mergeCursor) {
	h := make(columnMergeHeap, 0, len(cursors))
	for i := range cursors {
		h = append(h, &cursors[i])
	}
	heap.Init(&h)

	var last lastMergedRow
	cursor := heap.Pop(&h).(*mergeCursor)
	last.save(cursor)
	appendMergedRow(dst, cursor)
	if cursor.next() {
		heap.Push(&h, cursor)
	}

	for h.Len() > 0 {
		cursor = heap.Pop(&h).(*mergeCursor)
		if last.equals(cursor) {
			dst.Values.mergeInto(dst.RowsCount()-1, cursor, cursor.remap)
		} else {
			appendMergedRow(dst, cursor)
			last.save(cursor)
		}
		if cursor.next() {
			heap.Push(&h, cursor)
		}
	}
}

type lastMergedRow struct {
	tokens []byte   // arena with the token bytes concatenated
	slices [][]byte // per-column views into tokens
	mid    MID
}

func (l *lastMergedRow) save(c *mergeCursor) {
	l.tokens = l.tokens[:0]
	l.slices = l.slices[:0]
	for i := range c.iters {
		row := c.iters[i].Row()
		start := len(l.tokens)
		l.tokens = append(l.tokens, row...)
		l.slices = append(l.slices, l.tokens[start:])
	}
	l.mid = c.mid()
}

func (l *lastMergedRow) equals(c *mergeCursor) bool {
	if l.mid != c.mid() {
		return false
	}
	width := min(len(l.slices), len(c.iters))
	for i := range width {
		if !bytes.Equal(l.slices[i], c.iters[i].Row()) {
			return false
		}
	}
	return true
}

func appendMergedRow(dst *AggregatableSamples, cursor *mergeCursor) {
	for i := range dst.Tokens {
		dst.Tokens[i].Append(cursor.row(i))
	}
	dst.appendMID(cursor.mid())
	switch dst.Values.Func {
	case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
		dst.Values.Totals.Append(cursor.total())
	}
	dst.Values.appendFromRest(&cursor.aggs.Values, cursor.idx, cursor.notExists(), cursor.remap)
}

type columnMergeHeap []*mergeCursor

func (h columnMergeHeap) Len() int {
	return len(h)
}

func (h columnMergeHeap) Less(i, j int) bool {
	return compareMergeCursors(h[i], h[j]) < 0
}

func (h columnMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *columnMergeHeap) Push(cursor any) {
	*h = append(*h, cursor.(*mergeCursor))
}

func (h *columnMergeHeap) Pop() any {
	old := *h
	last := len(old) - 1
	cursor := old[last]
	*h = old[:last]
	return cursor
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

	for i := range q.Tokens {
		total += q.Tokens[i].MemUsage()
	}
	if q.MIDs != nil {
		total += q.MIDs.MemUsage()
	}
	total += q.Values.memUsage()
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
	total := cap(h.Samples) * util.SizeOfFloat64
	if h.Values != nil {
		for range h.Values {
			total += util.SizeOfUint32
		}
	}
	return SizeOfSamplesContainer + total
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

const timeSeriesStringColumnCodec = stringColumnCodecZstd

func (q *QPR) ToCompressed() {
	for i := range q.Aggs {
		for j := range q.Aggs[i].Tokens {
			codec := stringColumnCodecZstd
			if j == 0 {
				if q.Aggs[i].MIDs == nil {
					codec = stringColumnCodecFrontZstd
				} else {
					codec = timeSeriesStringColumnCodec
				}
			}
			q.Aggs[i].Tokens[j] = q.Aggs[i].Tokens[j].compress(codec)
		}
		switch q.Aggs[i].Values.Func {
		case AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
			q.Aggs[i].Values.Totals = q.Aggs[i].Values.Totals.compress()
		}
		switch q.Aggs[i].Values.Func {
		case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncAvg, AggFuncQuantile, AggFuncUniqueCount:
			q.Aggs[i].Values.NotExists = q.Aggs[i].Values.NotExists.compress()
		}
		if q.Aggs[i].MIDs != nil {
			q.Aggs[i].MIDs = q.Aggs[i].MIDs.compress()
		}
	}
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
