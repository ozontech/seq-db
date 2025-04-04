package processor

import (
	"fmt"
	"math"
	"strconv"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
)

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
	countBySource map[twoSources]int64
}

func NewGroupAndFieldAggregator(fieldIterator, groupByIterator *SourcedNodeIterator, collectSamples bool) *TwoSourceAggregator {
	return &TwoSourceAggregator{
		collectSamples:   collectSamples,
		countBySource:    make(map[twoSources]int64),
		field:            fieldIterator,
		groupNotExists:   0,
		groupBy:          groupByIterator,
		groupByNotExists: make(map[uint32]int64),
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
	source := twoSources{
		GroupBySource: groupBySource,
		FieldSource:   fieldSource,
	}
	n.countBySource[source]++
	return nil
}

// Aggregate processes and returns the final aggregation result.
func (n *TwoSourceAggregator) Aggregate() (seq.QPRHistogram, error) {
	aggMap := make(map[string]*seq.AggregationHistogram, n.groupBy.UniqueSources())

	for groupBySource, cnt := range n.groupByNotExists {
		groupByVal := n.groupBy.ValueBySource(groupBySource)
		if aggMap[groupByVal] == nil {
			aggMap[groupByVal] = seq.NewAggregationHistogram()
		}
		aggMap[groupByVal].NotExists = cnt
	}

	for source, cnt := range n.countBySource {
		// Name of the group, for example, it can be service name.
		groupByVal := n.groupBy.ValueBySource(source.GroupBySource)

		if aggMap[groupByVal] == nil {
			aggMap[groupByVal] = seq.NewAggregationHistogram()
		}

		hist := aggMap[groupByVal]

		// For example, for a field named "request_duration" it can be "42.13"
		field := n.field.ValueBySource(source.FieldSource)
		num, err := parseNum(field)
		if err != nil {
			return seq.QPRHistogram{}, err
		}

		// The same token can appear multiple times,
		// so we need to insert the num cnt times.
		hist.InsertNTimes(num, cnt)
		if n.collectSamples {
			hist.InsertSampleNTimes(num, cnt)
		}
	}

	return seq.QPRHistogram{
		NotExists:        n.groupNotExists,
		HistogramByToken: aggMap,
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
	countBySource map[uint32]int64
	// notExists is the counter for non-existent sources.
	notExists int64
	group     *SourcedNodeIterator
}

func NewSingleSourceCountAggregator(iterator *SourcedNodeIterator) *SingleSourceCountAggregator {
	return &SingleSourceCountAggregator{
		countBySource: make(map[uint32]int64),
		notExists:     0,
		group:         iterator,
	}
}

// Next iterates over groupBy tree to count occurrence.
func (n *SingleSourceCountAggregator) Next(lid uint32) error {
	source, has, err := n.group.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}
	if has {
		n.countBySource[source]++
	} else {
		n.notExists++
	}
	return nil
}

func (n *SingleSourceCountAggregator) Aggregate() (seq.QPRHistogram, error) {
	aggMap := make(map[string]*seq.AggregationHistogram, n.group.UniqueSources())
	for source, cnt := range n.countBySource {
		field := n.group.ValueBySource(source)
		if aggMap[field] == nil {
			aggMap[field] = seq.NewAggregationHistogram()
		}
		aggMap[field].Total = cnt
	}
	if n.notExists > 0 {
		// Handle non-existent sources in legacy format.
		aggMap["_not_exists"] = &seq.AggregationHistogram{Total: n.notExists}
	}

	return seq.QPRHistogram{
		NotExists:        n.notExists,
		HistogramByToken: aggMap,
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
	} else {
		n.notExists++
	}
	return nil
}

func (n *SingleSourceUniqueAggregator) Aggregate() (seq.QPRHistogram, error) {
	aggMap := make(map[string]*seq.AggregationHistogram, n.group.UniqueSources())
	for val := range n.values {
		field := n.group.ValueBySource(val)
		if aggMap[field] == nil {
			aggMap[field] = &seq.AggregationHistogram{}
		}
	}

	return seq.QPRHistogram{
		NotExists:        n.notExists,
		HistogramByToken: aggMap,
	}, nil
}

type SingleSourceHistogramAggregator struct {
	field          *SourcedNodeIterator
	histogram      *seq.AggregationHistogram
	collectSamples bool
}

func NewSingleSourceHistogramAggregator(field *SourcedNodeIterator, collectSamples bool) *SingleSourceHistogramAggregator {
	return &SingleSourceHistogramAggregator{
		field:          field,
		histogram:      seq.NewAggregationHistogram(),
		collectSamples: collectSamples,
	}
}

func (n *SingleSourceHistogramAggregator) Next(lid uint32) error {
	source, has, err := n.field.ConsumeTokenSource(lid)
	if err != nil {
		return err
	}
	if !has {
		n.histogram.NotExists++
		return nil
	}

	field := n.field.ValueBySource(source)
	num, err := parseNum(field)
	if err != nil {
		return err
	}

	n.histogram.InsertNTimes(num, 1)
	if n.collectSamples {
		n.histogram.InsertSample(num)
	}

	return nil
}

func (n *SingleSourceHistogramAggregator) Aggregate() (seq.QPRHistogram, error) {
	return seq.QPRHistogram{
		NotExists:        0,
		HistogramByToken: map[string]*seq.AggregationHistogram{"": n.histogram},
	}, nil
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
