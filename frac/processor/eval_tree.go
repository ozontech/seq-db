package processor

import (
	"fmt"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type createLeafFunc func(parser.Token) (node.Node, error)

// buildEvalTree builds eval tree based on syntax tree (of search query) where each leaf is DataNode
func buildEvalTree(root *parser.ASTNode, minVal, maxVal uint32, stats *searchStats, reverse bool, newLeaf createLeafFunc) (node.Node, error) {
	children := make([]node.Node, 0, len(root.Children))
	for _, child := range root.Children {
		childNode, err := buildEvalTree(child, minVal, maxVal, stats, reverse, newLeaf)
		if err != nil {
			return nil, err
		}
		children = append(children, childNode)
	}

	switch token := root.Value.(type) {
	case *parser.Literal:
		return newLeaf(token)
	case *parser.Range:
		return newLeaf(token)
	case *parser.IPRange:
		return newLeaf(token)
	case *parser.Logical:
		switch token.Operator {
		case parser.LogicalAnd:
			stats.NodesTotal++
			return node.NewAnd(children[0], children[1], reverse), nil
		case parser.LogicalOr:
			stats.NodesTotal++
			return node.NewOr(children[0], children[1], reverse), nil
		case parser.LogicalNAnd:
			stats.NodesTotal++
			return node.NewNAnd(children[0], children[1], reverse), nil
		case parser.LogicalNot:
			stats.NodesTotal++
			return node.NewNot(children[0], minVal, maxVal, reverse), nil
		}
	}
	return nil, fmt.Errorf("unknown token type")
}

// evalLeaf finds suitable matching fraction tokens and returns Node that generate corresponding tokens LIDs
func evalLeaf(ti tokenIndex, token parser.Token, sw *stopwatch.Stopwatch, stats *searchStats, minLID, maxLID uint32, order seq.DocsOrder) (node.Node, error) {
	m := sw.Start("get_tids_by_token_expr")
	tids, err := ti.GetTIDsByTokenExpr(token)
	m.Stop()
	if err != nil {
		return nil, err
	}

	m = sw.Start("get_lids_from_tids")
	lidsTids := ti.GetLIDsFromTIDs(tids, stats, minLID, maxLID, order)
	m.Stop()

	stats.LeavesTotal += len(lidsTids)
	if len(lidsTids) > 0 {
		stats.NodesTotal += len(lidsTids)*2 - 1
	}

	return node.BuildORTree(lidsTids, order.IsReverse()), nil
}

type Aggregator interface {
	// Next iterates to count the next lid.
	Next(lid uint32) error
	// Aggregate processes and returns the final aggregation result.
	Aggregate() (seq.AggregatableSamples, error)
}

type AggLimits struct {
	// MaxFieldTokens max AggQuery.Field uniq values to parse.
	MaxFieldTokens int
	// MaxGroupTokens max AggQuery.GroupBy unique values.
	MaxGroupTokens int
	// MaxTIDsPerFraction max number of tokens per fraction.
	MaxTIDsPerFraction int
}

// evalAgg evaluates aggregation with given limits. Returns a suitable aggregator.
func evalAgg(
	ti tokenIndex, query AggQuery, sw *stopwatch.Stopwatch,
	stats *searchStats, minLID, maxLID uint32, limits AggLimits,
	extractMID ExtractMIDFunc, order seq.DocsOrder,
) (Aggregator, error) {
	switch query.Func {
	case seq.AggFuncCount, seq.AggFuncUnique:
		groupIterator, err := iteratorFromLiteral(
			ti, query.GroupBy, sw, stats, minLID, maxLID,
			limits.MaxTIDsPerFraction, limits.MaxGroupTokens, order,
		)
		if err != nil {
			return nil, err
		}

		if query.Func == seq.AggFuncCount {
			return NewSingleSourceCountAggregator(groupIterator, extractMID), nil
		}

		return NewSingleSourceUniqueAggregator(groupIterator), nil

	case seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncSum, seq.AggFuncAvg, seq.AggFuncQuantile:
		fieldIterator, err := iteratorFromLiteral(
			ti, query.Field, sw, stats, minLID, maxLID,
			limits.MaxTIDsPerFraction, limits.MaxFieldTokens, order,
		)
		if err != nil {
			return nil, err
		}

		collectSamples := query.Func == seq.AggFuncQuantile &&
			haveNotMinMaxQuantiles(query.Quantiles)

		if query.GroupBy == nil {
			return NewSingleSourceHistogramAggregator(
				fieldIterator, collectSamples, extractMID,
			), nil
		}

		groupIterator, err := iteratorFromLiteral(
			ti, query.GroupBy, sw, stats, minLID, maxLID,
			limits.MaxTIDsPerFraction, limits.MaxGroupTokens, order,
		)
		if err != nil {
			return nil, err
		}

		return NewGroupAndFieldAggregator(
			fieldIterator, groupIterator, extractMID, collectSamples,
		), nil

	default:
		panic(fmt.Errorf("BUG: unknown aggregation function: %d", query.Func))
	}
}

// haveNotMinMaxQuantiles returns true if any quantile is in the range (0.0, 1.0)
func haveNotMinMaxQuantiles(quantiles []float64) bool {
	have := false
	const minQuantile = 0.0 // Quantile 0.0 means minimum value of the sequence
	const maxQuantile = 1.0 // Quantile 1.0 means maximum value of the sequence
	for _, quantile := range quantiles {
		if quantile > minQuantile && quantile < maxQuantile {
			have = true
			break
		}
	}
	return have
}

func iteratorFromLiteral(ti tokenIndex, literal *parser.Literal, sw *stopwatch.Stopwatch, stats *searchStats, minLID, maxLID uint32, maxTIDs, iteratorLimit int, order seq.DocsOrder) (*SourcedNodeIterator, error) {
	m := sw.Start("get_tids_by_token_expr")
	tids, err := ti.GetTIDsByTokenExpr(literal)
	m.Stop()
	if err != nil {
		return nil, fmt.Errorf("getting TIDs by token expression: %s", err)
	}

	if len(tids) > maxTIDs && maxTIDs > 0 {
		return nil, fmt.Errorf("%w: tokens length (%d) of field %q more than %d", consts.ErrTooManyUniqValues, len(tids), literal.Field, maxTIDs)
	}

	m = sw.Start("get_lids_from_tids")
	lidsTids := ti.GetLIDsFromTIDs(tids, stats, minLID, maxLID, order)
	m.Stop()

	if len(lidsTids) > 0 {
		stats.AggNodesTotal += len(lidsTids)*2 - 1
	}

	sourcedNode := node.BuildORTreeAgg(lidsTids, order.IsReverse())
	return NewSourcedNodeIterator(sourcedNode, ti, tids, iteratorLimit, order.IsReverse()), nil
}
