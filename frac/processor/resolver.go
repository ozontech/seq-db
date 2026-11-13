package processor

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/parser"
)

const (
	// NOTE(dkharms): These costs were picked empirically.
	resolveCostExact  = 1    // `foo:x` is resolved via binary search over a token range
	resolveCostPrefix = 10   // `foo:x*` is resolved via binary search plus the matching token range
	resolveCostScan   = 1000 // `foo:*x*` is resolved by matching against every token block of the field
)

type tidsCache map[parser.Token][]uint32

func (c tidsCache) get(ti tokenIndex, token parser.Token) ([]uint32, error) {
	if tids, ok := c[token]; ok {
		return tids, nil
	}

	tids, err := ti.GetTIDsByTokenExpr(token)
	if err != nil {
		return nil, err
	}

	c[token] = tids
	return tids, nil
}

// planNode is an intermediate representation of AST.
// Basically, all AND-clauses are flattened for convenience.
type planNode struct {
	ast      *parser.ASTNode
	children []*planNode

	pos  int
	cost int
	size uint64
}

func astResolve(
	root *parser.ASTNode, ti tokenIndex,
	cache tidsCache, sw *stopwatch.Stopwatch,
) (*parser.ASTNode, bool, error) {
	plan := flatten(root)
	sortByCost(plan)

	r := astResolver{ti: ti, cache: cache, sw: sw}
	empty, err := r.estimateNodeSize(plan)
	if err != nil || empty {
		return nil, empty, err
	}

	return rebuild(plan), false, nil
}

func flatten(n *parser.ASTNode) *planNode {
	p := &planNode{ast: n}
	children := n.Children

	if isAnd(n) {
		children = flattenAnd(n, nil)
	}

	for i, c := range children {
		child := flatten(c)
		child.pos = i
		p.children = append(p.children, child)
	}

	return p
}

func flattenAnd(n *parser.ASTNode, out []*parser.ASTNode) []*parser.ASTNode {
	if !isAnd(n) {
		return append(out, n)
	}

	for _, c := range n.Children {
		out = flattenAnd(c, out)
	}

	return out
}

func sortByCost(p *planNode) {
	for _, c := range p.children {
		sortByCost(c)
	}

	p.cost = resolveCost(p)
	if !isAnd(p.ast) {
		return
	}

	// TODO(dkharms): We need to put NOT at the end,
	// because NOT-node almost never produces empty result.
	slices.SortStableFunc(p.children, func(a, b *planNode) int {
		return cmp.Compare(a.cost, b.cost)
	})
}

func resolveCost(p *planNode) int {
	switch t := p.ast.Value.(type) {
	case *parser.Literal:
		switch {
		case len(t.Terms) == 1 && t.Terms[0].Kind == parser.TermText:
			return resolveCostExact
		case t.Terms[0].Kind == parser.TermText:
			return resolveCostPrefix
		}
		return resolveCostScan

	case *parser.Re:
		if len(t.Prefix.Value) > 0 && !t.Prefix.Foldable {
			return resolveCostPrefix
		}
		return resolveCostScan

	case *parser.Logical:
		switch t.Operator {
		case parser.LogicalAnd:
			return slices.MinFunc(p.children, func(a, b *planNode) int {
				return cmp.Compare(a.cost, b.cost)
			}).cost
		case parser.LogicalOr:
			return p.children[0].cost + p.children[1].cost
		case parser.LogicalNAnd:
			// TODO(dkharms): I need to transform NAND into X AND NOT Y
			// to calculate resolve cost correctly.
			//
			// For example, consider query: `AND(NAND(neg=bar:*y*, reg=foo:x), baz:z*)`,
			// resolve cost for NAND is one (since reg=foo:x), but we would have to resolve `bar` as well,
			// while `baz` is cheaper to resolve and might return empty set.
			//
			// Once again, NOT-propagation, NAND-conjuction at parsing time was is not a good idea :)
			return p.children[1].cost
		case parser.LogicalNot:
			return p.children[0].cost
		}
	}

	return resolveCostScan
}

type astResolver struct {
	ti    tokenIndex
	cache tidsCache
	sw    *stopwatch.Stopwatch
}

func (r *astResolver) estimateNodeSize(p *planNode) (bool, error) {
	logical, ok := p.ast.Value.(*parser.Logical)
	if !ok {
		return r.estimateLeafSize(p)
	}

	switch logical.Operator {
	case parser.LogicalAnd:
		return r.estimateAndSize(p)
	case parser.LogicalOr:
		return r.estimateOrSize(p)
	case parser.LogicalNAnd:
		return r.estimateNAndSize(p)
	case parser.LogicalNot:
		// NOTE(dkharms): This is a pessimistic estimate since operator
		// NOT may yield the whole LID range regardless of its child.
		p.size = math.MaxUint32
		_, err := r.estimateNodeSize(p.children[0])
		return false, err
	}

	return false, fmt.Errorf("unknown logical operator: %v", logical.Operator)
}

func (r *astResolver) estimateLeafSize(p *planNode) (bool, error) {
	m := r.sw.Start("get_tids_by_token_expr")
	tids, err := r.cache.get(r.ti, p.ast.Value)
	m.Stop()

	if err != nil || len(tids) == 0 {
		return len(tids) == 0, err
	}

	p.size = r.leafSize(p.ast.Value, tids)
	return false, nil
}

func (r *astResolver) leafSize(token parser.Token, tids []uint32) (size uint64) {
	field := parser.GetField(token)
	for _, freq := range r.ti.GetFreqsByTIDs(tids, field) {
		size += uint64(freq)
	}
	return size
}

func (r *astResolver) estimateAndSize(p *planNode) (bool, error) {
	p.size = math.MaxUint64
	for _, c := range p.children {
		empty, err := r.estimateNodeSize(c)
		if err != nil {
			return false, err
		}

		if empty {
			// NOTE(dkharms): This assignment is required for queries
			// like `NOT (X AND Y)`. If Y is empty then we will build
			// subtree which is equivalent to NOT(Y) (or NOT(EMPTY)).
			p.children = []*planNode{c}
			return true, nil
		}

		p.size = min(p.size, c.size)
	}
	return false, nil
}

func (r *astResolver) estimateOrSize(p *planNode) (bool, error) {
	leftEmpty, err := r.estimateNodeSize(p.children[0])
	if err != nil {
		return false, err
	}

	rightEmpty, err := r.estimateNodeSize(p.children[1])
	if err != nil {
		return false, err
	}

	switch {
	case leftEmpty && rightEmpty:
		p.children = p.children[:1]
		return true, nil
	case leftEmpty:
		p.children = p.children[1:]
	case rightEmpty:
		p.children = p.children[:1]
	}

	for _, c := range p.children {
		p.size += c.size
	}

	return false, nil
}

func (r *astResolver) estimateNAndSize(p *planNode) (bool, error) {
	neg, reg := p.children[0], p.children[1]
	regEmpty, err := r.estimateNodeSize(reg)
	if err != nil {
		return false, err
	}

	if regEmpty {
		p.children = p.children[1:]
		return true, nil
	}

	negEmpty, err := r.estimateNodeSize(neg)
	if err != nil {
		return false, err
	}

	if negEmpty {
		p.children = p.children[1:]
	}

	p.size = reg.size
	return false, nil
}

func rebuild(p *planNode) *parser.ASTNode {
	logical, ok := p.ast.Value.(*parser.Logical)
	if !ok {
		return p.ast
	}

	if len(p.children) == 1 && logical.Operator != parser.LogicalNot {
		return rebuild(p.children[0])
	}

	if logical.Operator == parser.LogicalAnd {
		return rebuildAnd(p)
	}

	children := make([]*parser.ASTNode, 0, len(p.children))
	for _, c := range p.children {
		children = append(children, rebuild(c))
	}

	return &parser.ASTNode{Value: p.ast.Value, Children: children}
}

func rebuildAnd(p *planNode) *parser.ASTNode {
	slices.SortFunc(p.children, func(a, b *planNode) int {
		return cmp.Or(
			cmp.Compare(a.size, b.size),
			cmp.Compare(a.pos, b.pos),
		)
	})

	root := rebuild(p.children[0])
	for _, c := range p.children[1:] {
		root = &parser.ASTNode{
			Value:    &parser.Logical{Operator: parser.LogicalAnd},
			Children: []*parser.ASTNode{root, rebuild(c)},
		}
	}

	return root
}

func isAnd(n *parser.ASTNode) bool {
	l, ok := n.Value.(*parser.Logical)
	return ok && l.Operator == parser.LogicalAnd
}
