package processor

import (
	"errors"
	"fmt"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

var errBatchingUnsupported = errors.New("batching unsupported")

// maxBatchedTIDsPerLeaf limits number of TIDs for a single OrMulti node
const maxBatchedTIDsPerLeaf = 5

type leafTIDsCache map[parser.Token][]uint32

// tryBuildBatchEvalTree tries to build a batched eval tree if possible.
//
// Returns errBatchingUnsupported when the non-batched path should be used.
func tryBuildBatchEvalTree(
	root *parser.ASTNode,
	fracVer config.BinaryDataVersion,
	ti tokenIndex,
	queryOpts QueryOptimizationConfig,
	minLID, maxLID uint32,
	stats *searchStats,
	order seq.DocsOrder,
	sw *stopwatch.Stopwatch,
) (node.BatchedNode, error) {
	if !queryOpts.BatchExecution.Enabled {
		return nil, errBatchingUnsupported
	}
	if fracVer < config.BinaryDataV6 {
		// block batching for earlier versions (avoid delta-encoded posting lists converted to bitmaps)
		return nil, errBatchingUnsupported
	}

	if !astSupportsBatching(root) {
		return nil, errBatchingUnsupported
	}

	cache := make(leafTIDsCache)
	cost, err := calculateQueryIterationCost(root, ti, cache)
	if err != nil {
		return nil, err
	}

	threshold := queryOpts.BatchExecution.CostThreshold
	if threshold <= 0 || cost <= uint64(threshold) {
		return nil, errBatchingUnsupported
	}

	return buildBatchEvalTree(root, minLID, maxLID, stats, order.IsDesc(),
		func(token parser.Token) (node.BatchedNode, error) {
			return evalBatchLeaf(ti, token, cache, sw, stats, minLID, maxLID, order)
		},
	)
}

func astSupportsBatching(root *parser.ASTNode) bool {
	if root == nil {
		return false
	}

	switch v := root.Value.(type) {
	case *parser.Literal:
		return len(v.Terms) == 1 && v.Terms[0].Kind == parser.TermText
	case *parser.Range:
		return true
	case *parser.Logical:
		if v.Operator == parser.LogicalNot {
			return false
		}
		for i := range root.Children {
			if !astSupportsBatching(root.Children[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func calculateQueryIterationCost(root *parser.ASTNode, ti tokenIndex, cache leafTIDsCache) (uint64, error) {
	if root == nil {
		return 0, fmt.Errorf("empty AST")
	}

	switch token := root.Value.(type) {
	case *parser.Literal:
		return leafIterationCost(ti, token.Field, token, cache)
	case *parser.Range:
		return leafIterationCost(ti, token.Field, token, cache)
	case *parser.Logical:
		if len(root.Children) == 0 {
			return 0, nil
		}
		childCosts := make([]uint64, len(root.Children))
		for i, child := range root.Children {
			c, err := calculateQueryIterationCost(child, ti, cache)
			if err != nil {
				return 0, err
			}
			childCosts[i] = c
		}
		if len(childCosts) != 2 {
			return 0, fmt.Errorf("logical operator has unsupported count of children: %d", len(childCosts))
		}
		switch token.Operator {
		case parser.LogicalAnd:
			return min(childCosts[0], childCosts[1]), nil
		case parser.LogicalNAnd:
			return childCosts[0] + childCosts[1], nil
		case parser.LogicalOr:
			return childCosts[0] + childCosts[1], nil
		default:
			return 0, fmt.Errorf("unsupported logical operator for cost estimation: %v", token.Operator)
		}
	default:
		return 0, fmt.Errorf("unsupported token type for cost estimation")
	}
}

func leafIterationCost(ti tokenIndex, field string, token parser.Token, cache leafTIDsCache) (uint64, error) {
	tids, err := ti.GetTIDsByTokenExpr(token)
	if err != nil {
		return 0, err
	}

	// Currently we do not support batches for queries with deep trees. For example, queries like 'service:abc* AND level:*'
	if len(tids) > maxBatchedTIDsPerLeaf {
		return 0, errBatchingUnsupported
	}
	cache[token] = tids
	if len(tids) == 0 {
		return 0, nil
	}

	freqs := ti.GetFreqsByTIDs(tids, field)
	var cost uint64
	for _, freq := range freqs {
		cost += uint64(freq)
	}
	return cost, nil
}

// buildBatchEvalTree builds a BatchedNode eval tree using already-validated leaf TIDs.
func buildBatchEvalTree(
	root *parser.ASTNode,
	minLID, maxLID uint32,
	stats *searchStats,
	desc bool,
	newBatchLeaf func(parser.Token) (node.BatchedNode, error),
) (node.BatchedNode, error) {
	if root == nil {
		return nil, fmt.Errorf("empty AST")
	}

	children := make([]node.BatchedNode, 0, len(root.Children))
	for _, child := range root.Children {
		childNode, err := buildBatchEvalTree(child, minLID, maxLID, stats, desc, newBatchLeaf)
		if err != nil {
			return nil, err
		}
		children = append(children, childNode)
	}

	switch token := root.Value.(type) {
	case *parser.Literal:
		return newBatchLeaf(token)
	case *parser.Range:
		return newBatchLeaf(token)
	case *parser.Logical:
		stats.NodesTotal++
		switch token.Operator {
		case parser.LogicalAnd:
			return node.NewAndBatched(children[0], children[1], desc), nil
		case parser.LogicalOr:
			return node.NewOrBatched(children[0], children[1], desc), nil
		case parser.LogicalNAnd:
			return node.NewNAndBatched(children[0], children[1], desc), nil
		default:
			return nil, fmt.Errorf("unsupported logical operator for batched eval: %v", token.Operator)
		}
	default:
		return nil, fmt.Errorf("unknown token type for batched eval")
	}
}

func evalBatchLeaf(
	ti tokenIndex,
	token parser.Token,
	cache leafTIDsCache,
	sw *stopwatch.Stopwatch,
	stats *searchStats,
	minLID, maxLID uint32,
	order seq.DocsOrder,
) (node.BatchedNode, error) {
	stats.LeavesTotal++

	tids, ok := cache[token]
	if !ok {
		var err error
		tids, err = ti.GetTIDsByTokenExpr(token)
		if err != nil {
			return nil, err
		}
	}

	if len(tids) == 0 {
		stats.NodesTotal++
		return node.EmptyBatched(), nil
	}

	m := sw.Start("get_batched_lids_from_tids")
	batchedLIDs := ti.GetBatchedLIDsFromTIDs(tids, stats, minLID, maxLID, order)
	m.Stop()

	stats.NodesTotal++

	return node.NewOrBatchedMulti(batchedLIDs, order.IsDesc()), nil
}
