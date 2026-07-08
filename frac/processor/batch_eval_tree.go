package processor

import (
	"errors"
	"fmt"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
)

var errBatchingUnsupported = errors.New("batching unsupported")

type createBatchLeafFunc func(parser.Token) (node.BatchedNode, error)

// canEnableBatching determines if batching execution (batch-at-a-time) can be used for this query.
func canEnableBatching(root *parser.ASTNode, ti tokenIndex, queryOpts QueryOptimizationConfig) bool {
	if !astSupportsBatching(root) {
		return false
	}
	cost, err := queryIterationCost(root, ti)
	if err != nil {
		return false
	}
	threshold := queryOpts.BatchIterationCostThreshold
	return threshold > 0 && cost > uint64(threshold)
}

func astSupportsBatching(root *parser.ASTNode) bool {
	if root == nil {
		return false
	}

	switch root.Value.(type) {
	case *parser.Literal:
		literal := root.Value.(*parser.Literal)
		// currently batching supports only simple terms like 'field:A'
		return (len(literal.Terms) == 1 && literal.Terms[0].Kind == parser.TermText) ||
			(len(literal.Terms) == 2 && literal.Terms[0].Kind == parser.TermText && literal.Terms[1].Kind == parser.TermSymbol)
	case *parser.Range:
		return true
	case *parser.Logical:
		logical := root.Value.(*parser.Logical)
		// batching is not supported for NOT nodes yet
		if logical.Operator == parser.LogicalNot {
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

// queryIterationCost approximates how many NextGeq/Next cursor jumps non-batched execution may need.
// The underlying rationale for this iteration cost is that we are trying to avoid executing queries where
// we convert absolutely every posting lis to bitmap. It currently ignores minLID/maxLID by design.
func queryIterationCost(root *parser.ASTNode, ti tokenIndex) (uint64, error) {
	if root == nil {
		return 0, fmt.Errorf("empty AST")
	}

	switch token := root.Value.(type) {
	case *parser.Literal:
		return leafIterationCost(ti, token.Field, token)
	case *parser.Range:
		return leafIterationCost(ti, token.Field, token)
	case *parser.Logical:
		if len(root.Children) == 0 {
			return 0, nil
		}
		childCosts := make([]uint64, len(root.Children))
		for i, child := range root.Children {
			c, err := queryIterationCost(child, ti)
			if err != nil {
				return 0, err
			}
			childCosts[i] = c
		}
		switch token.Operator {
		case parser.LogicalAnd:
			return min(childCosts[0], childCosts[1]), nil
		case parser.LogicalNAnd:
			return childCosts[0] + childCosts[1], nil
		case parser.LogicalOr:
			return childCosts[0] + childCosts[1], nil
		case parser.LogicalNot:
			return childCosts[0], nil
		default:
			return 0, fmt.Errorf("unsupported logical operator for cost estimation: %v", token.Operator)
		}
	default:
		return 0, fmt.Errorf("unsupported token type for cost estimation")
	}
}

func leafIterationCost(ti tokenIndex, field string, token parser.Token) (uint64, error) {
	tids, err := ti.GetTIDsByTokenExpr(token)
	if err != nil {
		return 0, err
	}
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

// buildBatchEvalTree builds a BatchedNode eval tree for queries that pass canEnableBatching.
// Returns errBatchingUnsupported when the query shape is not yet supported by the batched path.
func buildBatchEvalTree(
	root *parser.ASTNode,
	minLID, maxLID uint32,
	stats *searchStats,
	desc bool,
	newBatchLeaf createBatchLeafFunc,
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
	sw *stopwatch.Stopwatch,
	stats *searchStats,
	minLID, maxLID uint32,
	order seq.DocsOrder,
) (node.BatchedNode, error) {
	m := sw.Start("get_tids_by_token_expr")
	tids, err := ti.GetTIDsByTokenExpr(token)
	m.Stop()
	if err != nil {
		return nil, err
	}

	stats.LeavesTotal++

	if len(tids) == 0 {
		stats.NodesTotal++
		return node.EmptyBatched(), nil
	}

	// batched execution do not yet works great with large OrMulti nodes
	if len(tids) > 5 {
		return nil, errBatchingUnsupported
	}

	m = sw.Start("get_batched_lids_from_tids")
	batchedLIDs := ti.GetBatchedLIDsFromTIDs(tids, stats, minLID, maxLID, order)
	m.Stop()

	stats.NodesTotal++

	return node.NewOrBatchedMulti(batchedLIDs, order.IsDesc()), nil
}
