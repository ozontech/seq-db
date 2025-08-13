package pattern

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/netip"
	"strconv"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/util"
)

type tokenProvider interface {
	GetToken(uint32) []byte
	FirstTID() uint32
	LastTID() uint32
	Ordered() bool
}

type baseSearch struct {
	first int
	last  int
}

func (s *baseSearch) firstTID() uint32 {
	return uint32(s.first)
}

func (s *baseSearch) lastTID() uint32 {
	return uint32(s.last)
}

type literalSearch struct {
	baseSearch
	value    []byte
	narrowed bool
}

func newLiteralSearch(base baseSearch, token *parser.Literal) *literalSearch {
	if len(token.Terms) != 1 || token.Terms[0].Kind != parser.TermText {
		return nil
	}
	return &literalSearch{
		baseSearch: base,
		value:      []byte(token.Terms[0].Data),
	}
}

func (s *literalSearch) Narrow(tp tokenProvider) {
	s.narrowed = true

	s.first = util.BinSearchInRange(s.first, s.last, func(tid int) bool {
		return bytes.Compare(tp.GetToken(uint32(tid)), s.value) >= 0
	})

	if s.first <= s.last && bytes.Equal(tp.GetToken(uint32(s.first)), s.value) {
		s.last = s.first
		return
	}

	// not found
	s.last = s.first - 1 // begin > end: will be considered empty
}

func (s *literalSearch) check(val []byte) bool {
	if s.narrowed {
		return len(s.value) == len(val)
	}
	return bytes.Equal(s.value, val)
}

type wildcardSearch struct {
	baseSearch
	prefix    []byte
	suffix    []byte
	middle    []*substring
	middleLen int
	narrowed  bool
}

func newWildcardSearch(base baseSearch, token *parser.Literal) *wildcardSearch {
	s := &wildcardSearch{
		baseSearch: base,
	}
	terms := token.Terms
	if terms[0].Kind == parser.TermText {
		s.prefix = []byte(terms[0].Data)
	}
	if terms[len(terms)-1].Kind == parser.TermText {
		s.suffix = []byte(terms[len(terms)-1].Data)
	}
	// first must be a prefix or an asterix
	// last must be a suffix or an asterix
	// all of the rest can be an asterix or a middle
	for i := 1; i < len(terms)-1; i++ {
		if terms[i].Kind == parser.TermText {
			term := newSubstringPattern([]byte(terms[i].Data))
			s.middle = append(s.middle, term)
			s.middleLen += len(terms[i].Data)
		}
	}
	return s
}

func cut(b []byte, l int) []byte {
	return b[:min(len(b), l)]
}

func (s *wildcardSearch) Narrow(tp tokenProvider) {
	s.narrowed = true
	l := len(s.prefix)
	s.first = util.BinSearchInRange(s.first, s.last, func(tid int) bool {
		tokenPrefix := cut(tp.GetToken(uint32(tid)), l)
		return bytes.Compare(tokenPrefix, s.prefix) >= 0
	})
	s.last = util.BinSearchInRange(s.first, s.last, func(tid int) bool {
		tokenPrefix := cut(tp.GetToken(uint32(tid)), l)
		return bytes.Compare(tokenPrefix, s.prefix) > 0
	}) - 1
}

func (s *wildcardSearch) checkPrefix(val []byte) bool {
	if s.narrowed || len(s.prefix) == 0 {
		return true
	}
	if len(s.prefix) > len(val) {
		return false
	}
	return bytes.Equal(s.prefix, val[:len(s.prefix)])
}

func (s *wildcardSearch) checkSuffix(val []byte) bool {
	if len(s.suffix) == 0 {
		return true
	}
	if len(val)-len(s.prefix) < len(s.suffix) {
		return false
	}
	return bytes.Equal(val[len(val)-len(s.suffix):], s.suffix)
}

func (s *wildcardSearch) checkMiddle(val []byte) bool {
	if len(s.middle) == 0 {
		return true
	}
	if len(val)-len(s.prefix)-len(s.suffix) < s.middleLen {
		return false
	}
	return findSequence(val[len(s.prefix):len(val)-len(s.suffix)], s.middle) == len(s.middle)
}

func (s *wildcardSearch) check(val []byte) bool {
	return s.checkPrefix(val) && s.checkSuffix(val) && s.checkMiddle(val)
}

type rangeTextSearch struct {
	baseSearch
	token *parser.Range
}

func newRangeTextSearch(base baseSearch, token *parser.Range) *rangeTextSearch {
	return &rangeTextSearch{
		baseSearch: base,
		token:      token,
	}
}

func (s *rangeTextSearch) check(val []byte) bool {
	valStr := string(val)
	if s.token.From.Kind != parser.TermSymbol {
		if s.token.IncludeFrom {
			if !(s.token.From.Data <= valStr) {
				return false
			}
		} else {
			if !(s.token.From.Data < valStr) {
				return false
			}
		}
	}
	if s.token.To.Kind != parser.TermSymbol {
		if s.token.IncludeTo {
			if !(valStr <= s.token.To.Data) {
				return false
			}
		} else {
			if !(valStr < s.token.To.Data) {
				return false
			}
		}
	}
	return true
}

type rangeNumberSearch struct {
	baseSearch
	from        float64
	includeFrom bool
	to          float64
	includeTo   bool
}

func newRangeNumberSearch(base baseSearch, token *parser.Range) *rangeNumberSearch {
	var err error
	s := &rangeNumberSearch{
		baseSearch: base,
	}
	if token.From.Kind == parser.TermSymbol {
		s.from = -math.MaxFloat64 // MinFloat64 == -MaxFloat64
		s.includeFrom = true
	} else {
		s.from, err = strconv.ParseFloat(token.From.Data, 64)
		s.includeFrom = token.IncludeFrom
		if err != nil || isNaNOrInf(s.from) {
			return nil
		}
	}
	if token.To.Kind == parser.TermSymbol {
		s.to = math.MaxFloat64
		s.includeTo = true
	} else {
		s.to, err = strconv.ParseFloat(token.To.Data, 64)
		s.includeTo = token.IncludeTo
		if err != nil || isNaNOrInf(s.to) {
			return nil
		}
	}
	return s
}

func (s *rangeNumberSearch) check(rawVal []byte) bool {
	val, err := strconv.ParseFloat(string(rawVal), 64)
	if err != nil || isNaNOrInf(val) {
		return false
	}

	if s.includeFrom {
		if !(s.from <= val) {
			return false
		}
	} else {
		if !(s.from < val) {
			return false
		}
	}
	if s.includeTo {
		if !(val <= s.to) {
			return false
		}
	} else {
		if !(val < s.to) {
			return false
		}
	}
	return true
}

type rangeIpSearch struct {
	baseSearch
	from netip.Addr
	to   netip.Addr
}

func newRangeIPSearch(base baseSearch, token *parser.IPRange) *rangeIpSearch {
	// only creating text terms, other types are impossible
	if token.From.Kind != parser.TermText || token.To.Kind != parser.TermText {
		panic("BUG: wrong term kind in ip_range")
	}

	var err error
	s := &rangeIpSearch{
		baseSearch: base,
	}

	s.from, err = netip.ParseAddr(token.From.Data)
	if err != nil {
		return nil
	}

	s.to, err = netip.ParseAddr(token.To.Data)
	if err != nil {
		return nil
	}
	return s
}

func (s *rangeIpSearch) check(rawVal []byte) bool {
	val, err := netip.ParseAddr(string(rawVal))
	if err != nil {
		return false
	}

	// s.from <= val <= s.to
	return s.from.Compare(val) <= 0 && val.Compare(s.to) <= 0
}

type searcher interface {
	firstTID() uint32
	lastTID() uint32
	check(val []byte) bool
}

func newSearcher(token parser.Token, tp tokenProvider) searcher {
	base := baseSearch{
		first: int(tp.FirstTID()),
		last:  int(tp.LastTID()),
	}
	switch t := token.(type) {
	case *parser.Literal:
		if s := newLiteralSearch(base, t); s != nil {
			if tp.Ordered() {
				s.Narrow(tp)
			}
			return s
		}
		s := newWildcardSearch(base, t)
		if tp.Ordered() {
			s.Narrow(tp)
		}
		return s
	case *parser.Range:
		// try number search
		if s := newRangeNumberSearch(base, t); s != nil {
			return s
		}
		return newRangeTextSearch(base, t)
	case *parser.IPRange:
		return newRangeIPSearch(base, t)
	}
	panic(fmt.Sprintf("unknown token type: %T", token))
}

func isNaNOrInf(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}

func Search(ctx context.Context, t parser.Token, tp tokenProvider) ([]uint32, error) {
	tids := []uint32{}
	s := newSearcher(t, tp)
	for tid := s.firstTID(); tid <= s.lastTID(); tid++ {
		if util.IsCancelled(ctx) {
			return nil, ctx.Err()
		}
		if s.check(tp.GetToken(tid)) {
			tids = append(tids, tid)
		}
	}
	return tids, nil
}
