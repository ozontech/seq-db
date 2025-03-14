package search

import (
	"fmt"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

type createLeafFunc func(parser.Token, *Stats) (node.Node, error)

// buildEvalTree builds eval tree based on syntax tree (of search query) where each leaf is DataNode
func buildEvalTree(root *parser.ASTNode, minVal, maxVal uint32, stats *Stats, reverse bool, newLeaf createLeafFunc) (node.Node, error) {
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
		return newLeaf(token, stats)
	case *parser.Range:
		return newLeaf(token, stats)
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
func evalLeaf(dp frac.DataProvider, token parser.Token, stats *Stats, minLID, maxLID uint32, order seq.DocsOrder) (node.Node, error) {

	m := dp.Stopwatch().Start("get_tids_by_token_expr")
	tids, err := dp.GetTIDsByTokenExpr(token, nil)
	m.Stop()
	if err != nil {
		return nil, err
	}

	m = dp.Stopwatch().Start("get_lids_from_tids")
	lidsTids := dp.GetLIDsFromTIDs(tids, stats, minLID, maxLID, order)
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
	Aggregate() (seq.QPRHistogram, error)
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
func evalAgg(dp frac.DataProvider, query AggQuery, stats *Stats, minLID, maxLID uint32, limits AggLimits, order seq.DocsOrder) (Aggregator, error) {
	switch query.Func {
	case seq.AggFuncCount, seq.AggFuncUnique:
		groupIterator, err := iteratorFromLiteral(dp, query.GroupBy, stats, minLID, maxLID, limits.MaxTIDsPerFraction, limits.MaxGroupTokens, order)
		if err != nil {
			return nil, err
		}
		if query.Func == seq.AggFuncCount {
			return NewSingleSourceCountAggregator(groupIterator), nil
		}
		return NewSingleSourceUniqueAggregator(groupIterator), nil
	case seq.AggFuncMin, seq.AggFuncMax, seq.AggFuncSum, seq.AggFuncAvg, seq.AggFuncQuantile:
		fieldIterator, err := iteratorFromLiteral(dp, query.Field, stats, minLID, maxLID, limits.MaxTIDsPerFraction, limits.MaxFieldTokens, order)
		if err != nil {
			return nil, err
		}

		collectSamples := query.Func == seq.AggFuncQuantile && haveNotMinMaxQuantiles(query.Quantiles)

		if query.GroupBy == nil {
			return NewSingleSourceHistogramAggregator(fieldIterator, collectSamples), nil
		}

		groupIterator, err := iteratorFromLiteral(dp, query.GroupBy, stats, minLID, maxLID, limits.MaxTIDsPerFraction, limits.MaxGroupTokens, order)
		if err != nil {
			return nil, err
		}
		return NewGroupAndFieldAggregator(fieldIterator, groupIterator, collectSamples), nil
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

func iteratorFromLiteral(dp frac.DataProvider, literal *parser.Literal, stats *Stats, minLID, maxLID uint32, maxTIDs, iteratorLimit int, order seq.DocsOrder) (*SourcedNodeIterator, error) {
	m := dp.Stopwatch().Start("get_tids_by_token_expr")
	tids, err := dp.GetTIDsByTokenExpr(literal, nil)
	m.Stop()
	if err != nil {
		return nil, fmt.Errorf("getting TIDs by token expression: %s", err)
	}

	if len(tids) > maxTIDs && maxTIDs > 0 {
		return nil, fmt.Errorf("%w: tokens length (%d) of field %q more than %d", consts.ErrTooManyUniqValues, len(tids), literal.Field, maxTIDs)
	}

	m = dp.Stopwatch().Start("get_lids_from_tids")
	lidsTids := dp.GetLIDsFromTIDs(tids, stats, minLID, maxLID, order)
	m.Stop()

	if len(lidsTids) > 0 {
		stats.AggNodesTotal += len(lidsTids)*2 - 1
	}

	sourcedNode := node.BuildORTreeAgg(lidsTids)
	return NewSourcedNodeIterator(sourcedNode, dp, tids, iteratorLimit, order.IsReverse()), nil
}
