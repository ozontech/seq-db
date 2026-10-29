package pattern

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/util"
)

type IntRange struct {
	From int64
	To   int64
}

var fullIntRange = []IntRange{{From: math.MinInt64, To: math.MaxInt64}}

type intTokenProvider interface {
	FirstTID() uint32
	LastTID() uint32
	FindIntRange(from, to int64) []uint32
	FindIntToken(ranges []IntRange, minLen int, searcher Searcher) ([]uint32, error)
}

func MatchesStrings(t parser.Token) bool {
	lit, ok := t.(*parser.Literal)
	if !ok || len(lit.Terms) != 1 || lit.Terms[0].Kind != parser.TermText {
		return true
	}
	_, isInt := util.ParseCanonicalInt64([]byte(lit.Terms[0].Data))
	return !isInt
}

func SearchInt(ctx context.Context, t parser.Token, tp intTokenProvider) ([]uint32, error) {
	if util.IsCancelled(ctx) {
		return nil, ctx.Err()
	}

	base := baseSearch{
		first: int(tp.FirstTID()),
		last:  int(tp.LastTID()),
	}

	switch t := t.(type) {
	case *parser.Literal:
		if len(t.Terms) == 1 && t.Terms[0].Kind == parser.TermText {
			v, ok := util.ParseCanonicalInt64([]byte(t.Terms[0].Data))
			if !ok {
				return nil, nil
			}
			return tp.FindIntRange(v, v), nil
		}

		if !intCompatible(t) {
			return nil, nil
		}

		s := newWildcardSearch(base, t)
		ranges := intPrefixRanges(s.prefix)
		if len(ranges) == 0 {
			return nil, nil
		}
		return tp.FindIntToken(ranges, len(s.prefix)+len(s.suffix)+s.middleLen, s)
	case *parser.Range:
		if s := newRangeNumberSearch(base, t); s != nil {
			from, to, ok := s.intBounds()
			if !ok {
				return nil, nil
			}
			return tp.FindIntRange(from, to), nil
		}
		return tp.FindIntToken(fullIntRange, 0, newRangeTextSearch(base, t))
	case *parser.IPRange:
		return nil, nil
	case *parser.Re:
		return tp.FindIntToken(fullIntRange, 0, newReSearch(base, t))
	}
	panic(fmt.Sprintf("unknown token type: %T", t))
}

func intCompatible(lit *parser.Literal) bool {
	for _, term := range lit.Terms {
		if term.Kind != parser.TermText {
			continue
		}
		for i := 0; i < len(term.Data); i++ {
			if c := term.Data[i]; (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
}

func intPrefixRanges(prefix []byte) []IntRange {
	if len(prefix) == 0 {
		return fullIntRange
	}

	neg := prefix[0] == '-'
	digits := prefix
	if neg {
		digits = prefix[1:]
	}

	if len(digits) == 0 {
		return []IntRange{{From: math.MinInt64, To: -1}}
	}
	if len(digits) > 19 {
		return nil
	}

	var p uint64
	for _, c := range digits {
		if c < '0' || c > '9' {
			return nil
		}
		p = p*10 + uint64(c-'0')
	}

	if digits[0] == '0' {
		if neg || len(digits) > 1 {
			return nil
		}
		return []IntRange{{From: 0, To: 0}}
	}

	limit := uint64(math.MaxInt64)
	if neg {
		limit++
	}

	var ranges []IntRange
	lo, hi := p, p
	for lo <= limit {
		top := min(hi, limit)
		if neg {
			ranges = append(ranges, IntRange{From: negate(top), To: negate(lo)})
		} else {
			ranges = append(ranges, IntRange{From: int64(lo), To: int64(top)})
		}

		if lo > limit/10 {
			break
		}
		lo *= 10
		if hi > (math.MaxUint64-9)/10 {
			hi = math.MaxUint64
		} else {
			hi = hi*10 + 9
		}
	}

	if neg {
		slices.Reverse(ranges)
	}
	return ranges
}

func negate(u uint64) int64 {
	return int64(^u + 1)
}

func (s *rangeNumberSearch) intBounds() (int64, int64, bool) {
	from, ok := intLowerBound(s.from, s.includeFrom)
	if !ok {
		return 0, 0, false
	}
	to, ok := intUpperBound(s.to, s.includeTo)
	if !ok {
		return 0, 0, false
	}
	return from, to, from <= to
}

func intLowerBound(f float64, include bool) (int64, bool) {
	c := math.Ceil(f)
	if c >= 0x1p63 {
		return 0, false
	}
	if c < -0x1p63 {
		return math.MinInt64, true
	}

	v := int64(c)
	if !include && c == f {
		if v == math.MaxInt64 {
			return 0, false
		}
		v++
	}
	return v, true
}

func intUpperBound(f float64, include bool) (int64, bool) {
	c := math.Floor(f)
	if c < -0x1p63 {
		return 0, false
	}
	if c >= 0x1p63 {
		return math.MaxInt64, true
	}

	v := int64(c)
	if !include && c == f {
		if v == math.MinInt64 {
			return 0, false
		}
		v--
	}
	return v, true
}
