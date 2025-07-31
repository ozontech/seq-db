package processor

import (
	"fmt"
	"math"
	"strconv"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

// AggBin is a container for documents which were written in the same time interval.
// When dealing with aggregation (without need in building time series) [AggBin.MID] is equal to [DummyMID].
type AggBin[T comparable] struct {
	MID    seq.MID
	Source T
}

// ExtractMIDFunc is necessary since in aggregators we do not have [idsIndex] interface,
// we need a way to extract timestamp of document to build time series.
type ExtractMIDFunc func(seq.LID) seq.MID

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
	// countBySource map to count occurrences by histogram source.
	countBySource map[AggBin[twoSources]]int64
	// extractMID will be used for building time series.
	extractMID ExtractMIDFunc
}

func NewGroupAndFieldAggregator(
	fieldIterator, groupByIterator *SourcedNodeIterator,
	fn ExtractMIDFunc, collectSamples bool,
) *TwoSourceAggregator {
	return &TwoSourceAggregator{
		collectSamples:   collectSamples,
		countBySource:    make(map[AggBin[twoSources]]int64),
		field:            fieldIterator,
		groupNotExists:   0,
		groupBy:          groupByIterator,
		groupByNotExists: make(map[uint32]int64),
		extractMID:       fn,
	}
}

// Next iterates over groupBy and field iterators (actually trees) to count occurrence.
func (n *TwoSourceAggregator) Next(lid uint32) error {
	groupBySource, hasGroupBy, err := n.groupBy.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}

	fieldSource, hasField, err := n.field.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}

	if !hasField && !hasGroupBy {
		// Both group and field do not exist.
		return nil
	}

	if !hasField {
		// Field does not exist, but group exists.
		n.groupByNotExists[groupBySource]++
		return nil
	}

	if !hasGroupBy {
		// Group does not exist, but field exists.
		n.groupNotExists++
		return nil
	}

	// Both group and field exist, increment the count for the combined sources.
	source := AggBin[twoSources]{
		MID: n.extractMID(seq.LID(lid)),
		Source: twoSources{
			GroupBySource: groupBySource,
			FieldSource:   fieldSource,
		},
	}

	n.countBySource[source]++
	return nil
}

// Aggregate processes and returns the final aggregation result.
func (n *TwoSourceAggregator) Aggregate() (seq.AggregatableSamples, error) {
	aggMap := make(map[seq.AggBin]*seq.SamplesContainer, n.groupBy.UniqueSources())

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

	return seq.AggregatableSamples{
		NotExists:    n.groupNotExists,
		SamplesByBin: aggMap,
	}, nil
}

func parseNum(str string) (float64, error) {
	// TODO: allow time.Duration and data units (kb, mb, gb, etc) parsing.
	num, err := strconv.ParseFloat(str, 64)
	if err != nil || math.IsNaN(num) || math.IsInf(num, 0) {
		return 0, fmt.Errorf("parse errors reached, last_value=%q", str)
	}
	return num, nil
}

// SingleSourceCountAggregator aggregates counts for a single source.
type SingleSourceCountAggregator struct {
	// countBySource needs to count occurrences by source.
	countBySource map[AggBin[uint32]]int64
	// notExists is the counter for non-existent sources.
	notExists int64
	group     *SourcedNodeIterator
	// extractMID will be used for building time series.
	extractMID ExtractMIDFunc
}

func NewSingleSourceCountAggregator(
	iterator *SourcedNodeIterator, fn ExtractMIDFunc,
) *SingleSourceCountAggregator {
	return &SingleSourceCountAggregator{
		countBySource: make(map[AggBin[uint32]]int64),
		notExists:     0,
		group:         iterator,
		extractMID:    fn,
	}
}

// Next iterates over groupBy tree to count occurrence.
func (n *SingleSourceCountAggregator) Next(lid uint32) error {
	source, has, err := n.group.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}

	if has {
		mid := n.extractMID(seq.LID(lid))

		n.countBySource[AggBin[uint32]{
			MID:    mid,
			Source: source,
		}]++

		return nil
	}

	n.notExists++
	return nil
}

func (n *SingleSourceCountAggregator) Aggregate() (seq.AggregatableSamples, error) {
	aggMap := make(map[seq.AggBin]*seq.SamplesContainer, n.group.UniqueSources())

	for bin, cnt := range n.countBySource {
		aggBin := seq.AggBin{
			Token: n.group.ValueBySource(bin.Source),
			MID:   bin.MID,
		}

		if aggMap[aggBin] == nil {
			aggMap[aggBin] = seq.NewSamplesContainers()
		}

		aggMap[aggBin].Total = cnt
	}

	// FIXME(dkharms): It will not work correctly with time series, since
	// we also have to spread [notExists] across different time bins.
	if n.notExists > 0 {
		// Handle non-existent sources in legacy format.
		aggMap[seq.AggBin{
			Token: "_not_exists",
			MID:   consts.DummyMID,
		}] = &seq.SamplesContainer{Total: n.notExists}
	}

	return seq.AggregatableSamples{
		NotExists:    n.notExists,
		SamplesByBin: aggMap,
	}, nil
}

// SingleSourceUniqueAggregator aggregates unique values for a single source.
type SingleSourceUniqueAggregator struct {
	values    map[uint32]struct{}
	group     *SourcedNodeIterator
	notExists int64
}

func NewSingleSourceUniqueAggregator(iterator *SourcedNodeIterator) *SingleSourceUniqueAggregator {
	return &SingleSourceUniqueAggregator{
		values:    make(map[uint32]struct{}),
		notExists: 0,
		group:     iterator,
	}
}

// Next iterates over groupBy tree to count occurrence.
func (n *SingleSourceUniqueAggregator) Next(lid uint32) error {
	source, has, err := n.group.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}

	if has {
		n.values[source] = struct{}{}
		return nil
	}

	n.notExists++
	return nil
}

func (n *SingleSourceUniqueAggregator) Aggregate() (seq.AggregatableSamples, error) {
	aggMap := make(map[seq.AggBin]*seq.SamplesContainer, n.group.UniqueSources())

	for val := range n.values {
		aggBin := seq.AggBin{
			Token: n.group.ValueBySource(val),
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

type SingleSourceHistogramAggregator struct {
	field          *SourcedNodeIterator
	histogram      map[seq.MID]*seq.SamplesContainer
	collectSamples bool
	extractMID     ExtractMIDFunc
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

func (n *SingleSourceHistogramAggregator) Next(lid uint32) error {
	source, has, err := n.field.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}

	mid := n.extractMID(seq.LID(lid))
	if _, ok := n.histogram[mid]; !ok {
		n.histogram[mid] = seq.NewSamplesContainers()
	}
	histogram := n.histogram[mid]

	if !has {
		histogram.NotExists++
		return nil
	}

	value := n.field.ValueBySource(source)
	num, err := parseNum(value)
	if err != nil {
		return err
	}

	histogram.InsertNTimes(num, 1)
	if n.collectSamples {
		histogram.InsertSample(num)
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

// SourcedNodeIterator can iterate the sourced node that returns source, which means index in a tids slice.
type SourcedNodeIterator struct {
	sourcedNode node.Sourced
	ti          tokenIndex
	tids        []uint32

	tokensCache map[uint32]string

	uniqSourcesLimit int
	countBySource    map[uint32]int

	lastID     uint32
	lastSource uint32
	has        bool

	less node.LessFn
}

func NewSourcedNodeIterator(sourced node.Sourced, ti tokenIndex, tids []uint32, limit int, reverse bool) *SourcedNodeIterator {
	lastID, lastSource, has := sourced.NextSourced()
	return &SourcedNodeIterator{
		sourcedNode:      sourced,
		ti:               ti,
		tids:             tids,
		tokensCache:      make(map[uint32]string),
		uniqSourcesLimit: limit,
		countBySource:    make(map[uint32]int),
		lastID:           lastID,
		lastSource:       lastSource,
		has:              has,
		less:             node.GetLessFn(reverse),
	}
}

func (s *SourcedNodeIterator) ConsumeTokenSource(lid uint32) (uint32, bool, error) {
	for s.has && s.less(s.lastID, lid) {
		s.lastID, s.lastSource, s.has = s.sourcedNode.NextSourced()
	}

	exists := s.has && s.lastID == lid
	if !exists {
		return 0, false, nil
	}

	if s.uniqSourcesLimit <= 0 {
		return s.lastSource, true, nil
	}

	s.countBySource[s.lastSource]++

	if len(s.countBySource) > s.uniqSourcesLimit {
		return lid, true, fmt.Errorf("%w: iterator limit is exceeded", consts.ErrTooManyUniqValues)
	}

	return s.lastSource, true, nil
}

func (s *SourcedNodeIterator) ValueBySource(source uint32) string {
	const useCacheThreshold = 2
	if s.countBySource[source] < useCacheThreshold {
		return string(s.ti.GetValByTID(s.tids[source]))
	}

	val, ok := s.tokensCache[source]
	if ok {
		return val
	}
	val = string(s.ti.GetValByTID(s.tids[source]))
	s.tokensCache[source] = val
	return val
}

func (s *SourcedNodeIterator) UniqueSources() int {
	return len(s.countBySource)
}

func provideExtractTimeFunc(sw *stopwatch.Stopwatch, idx idsIndex, interval int64) ExtractMIDFunc {
	if interval <= 0 {
		// Dummy implementation for aggregation without time series.
		return ExtractMIDFunc(func(seq.LID) seq.MID {
			return seq.MID(consts.DummyMID)
		})
	}

	return ExtractMIDFunc(func(lid seq.LID) seq.MID {
		m := sw.Start("get_mid")
		mid := idx.GetMID(seq.LID(lid))
		m.Stop()
		return mid - (mid % seq.MID(interval))
	})
}
