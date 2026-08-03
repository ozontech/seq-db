package parser

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
	"strings"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/util"
)

type ReLiteral struct {
	Value    []byte
	Foldable bool
}

type Re struct {
	Field string

	Expression         Term
	CompiledExpression *regexp.Regexp

	Prefix ReLiteral
	Middle []ReLiteral
	Suffix ReLiteral
}

func (r *Re) DumpSeqQL(b *strings.Builder) {
	b.WriteString(quoteTokenIfNeeded(r.Field))
	b.WriteString(`:re(`)
	b.WriteString(`"`)
	b.WriteString(r.CompiledExpression.String())
	b.WriteString(`"`)
	b.WriteString(`)`)
}

func parseReFilter(lex *lexer, fieldName string) (*Re, error) {
	if !lex.IsKeyword("(") {
		return nil, fmt.Errorf("expected '(', got %q", lex.Token)
	}

	if err := lex.rawStringLiteral(); err != nil {
		return nil, err
	}

	// TODO(dkharms): Split logic between lexer and parser.
	// Now lexer is aware of structure of inner structure of lexemes.
	//
	// For example, for string 'foo bar' it will return ['foo', 'bar'].
	// For wildcard it will return \uE000 codepoint.
	//
	// But in the case with regular expressions wildcard (*) is a wildcard (*), not
	// some before chosen placeholder.
	//
	// So we can say that lexer is context aware which is not great.
	// This behaviour has negative impact on language extendability.
	expr := lex.Token

	lex.Next()
	if !lex.IsKeyword(")") {
		return nil, fmt.Errorf("expected ')', got %q", lex.Token)
	}
	lex.Next()

	// Here are two important things to keep in mind:
	//  - We perform case-insensitive search by default;
	//  - We force anchoring for the expression;
	//
	// Case sensitivity can be overridden by the user with `(?-i)` inside the pattern.
	// Anchoring is necessary for prefix search optimization.
	//
	// See Prometheus TSDB `FastRegexMatcher` for a similar approach:
	// https://github.com/prometheus/prometheus/blob/19fd0b0b1dbfe01a5e49f5d04544a7c5853c12bb/model/labels/regexp.go#L70

	re, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		return nil, fmt.Errorf("invalid expression for `re` filter: %s", err)
	}

	// NOTE(dkharms): We do not allow overriding of case-sensitivity
	// in case if search is case-insensitive.
	//
	// This way `re` filter works consistently with how `keyword`
	// and `text` search behave.
	overridable := config.CaseSensitive
	if !overridable && hasCaseSensitivityOverride(re) {
		return nil, fmt.Errorf(
			"store is configured for case-insensitive search: " +
				"you cannot override this option",
		)
	}

	// NOTE(dkharms): Again, if store works in case-insensitive mode
	// we simulate behaviour of `keyword` and `text` indexes.
	// Should we really do this?
	if !overridable {
		expr = strings.ToLower(expr)
	}

	// NOTE(dkharms): We force anchoring for the expression.
	// Anchoring is necessary for prefix and suffix search optimization.
	expr = "^" + expr + "$"
	exp, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf(
			"it's likely you've encountered a bug: " +
				"please contact seq-db team",
		)
	}

	re = optimizeRe(re)

	return &Re{
		Field: fieldName,

		Expression:         newTextTerm(expr),
		CompiledExpression: exp,

		Prefix: prefix(re),
		Middle: middle(re),
		Suffix: suffix(re),
	}, nil
}

func hasCaseSensitivityOverride(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpLiteral, syntax.OpCharClass:
		return (re.Flags & syntax.FoldCase) == syntax.FoldCase
	default:
		return slices.ContainsFunc(re.Sub, hasCaseSensitivityOverride)
	}
}

// eliminateCapture transforms regular expression into
// semantically equivalent one but without capturing groups.
func eliminateCapture(re *syntax.Regexp) {
	if re.Op == syntax.OpCapture {
		*re = *re.Sub[0]
	}

	for _, s := range re.Sub {
		eliminateCapture(s)
	}
}

func optimizeRe(re *syntax.Regexp) *syntax.Regexp {
	// Eliminate captures in-place.
	//
	// They do not change semantics of regular expression
	// but make simplification and extraction of literals more difficult.
	eliminateCapture(re)

	// Well, some simplification are not reflected at already parsed tree.
	// So we do it again :)
	re, err := syntax.Parse(re.Simplify().String(), syntax.Perl)
	if err != nil {
		panic(fmt.Sprintf("BUG: cannot parse re after optimization pass: %s", err))
	}

	return re
}

func prefix(re *syntax.Regexp) ReLiteral {
	// For example, we work with regular expression `seqdb-(stg|prod)-[1-9]+`.
	subs := []*syntax.Regexp{re}

	// Yep, this is concatention. We are interesed in its subexpressions.
	if subs[0].Op == syntax.OpConcat {
		subs = subs[0].Sub
	}

	// Skip symbols like `^`.
	if subs[0].Op == syntax.OpBeginText || subs[0].Op == syntax.OpBeginLine {
		subs = subs[1:]
	}

	// Well, not today.
	if len(subs) == 0 || subs[0].Op != syntax.OpLiteral {
		return ReLiteral{}
	}

	return ReLiteral{
		// TODO(dkharms): Check whether it is safe.
		Value:    util.StringToByteUnsafe(string(subs[0].Rune)),
		Foldable: (subs[0].Flags & syntax.FoldCase) == syntax.FoldCase,
	}
}

func middle(re *syntax.Regexp) []ReLiteral {
	var m []ReLiteral

	subs := []*syntax.Regexp{re}
	if subs[0].Op == syntax.OpConcat {
		subs = subs[0].Sub
	}

	for len(subs) > 0 {
		if subs[0].Op == syntax.OpLiteral {
			m = append(m, ReLiteral{
				// TODO(dkharms): Check whether it is safe.
				Value:    util.StringToByteUnsafe(string(subs[0].Rune)),
				Foldable: (subs[0].Flags & syntax.FoldCase) == syntax.FoldCase,
			})
		}
		subs = subs[1:]
	}

	return m
}

func suffix(re *syntax.Regexp) ReLiteral {
	subs := []*syntax.Regexp{re}

	if subs[0].Op == syntax.OpConcat {
		subs = subs[0].Sub
	}

	if subs[len(subs)-1].Op == syntax.OpBeginText || subs[len(subs)-1].Op == syntax.OpBeginLine {
		subs = subs[:len(subs)-1]
	}

	if len(subs) == 0 || subs[len(subs)-1].Op != syntax.OpLiteral {
		return ReLiteral{}
	}

	return ReLiteral{
		// TODO(dkharms): Check whether it is safe.
		Value:    util.StringToByteUnsafe(string(subs[len(subs)-1].Rune)),
		Foldable: (subs[len(subs)-1].Flags & syntax.FoldCase) == syntax.FoldCase,
	}
}
