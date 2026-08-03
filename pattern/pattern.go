package pattern

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"regexp"
	"strconv"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/util"
)

type tokenProvider interface {
	GetToken(uint32) []byte
	FindContains(needle []byte) ([]uint32, error)
	FindToken(searcher Searcher) ([]uint32, error)
	FirstTID() uint32
	LastTID() uint32
	Ordered() bool
}

type baseSearch struct {
	first int
	last  int
}

func (s *baseSearch) FirstTID() uint32 {
	return uint32(s.first)
}

func (s *baseSearch) LastTID() uint32 {
	return uint32(s.last)
}

func (s *baseSearch) CheckEntry(letters util.LettersBitset) bool {
	return true
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

func (s *literalSearch) Check(val []byte) (bool, error) {
	if s.narrowed {
		return len(s.value) == len(val), nil
	}
	return bytes.Equal(s.value, val), nil
}

type wildcardSearch struct {
	baseSearch
	prefix        []byte
	suffix        []byte
	middle        [][]byte
	middleLen     int
	narrowed      bool
	lettersBitset util.LettersBitset
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
			val := util.StringToByteUnsafe(terms[i].Data)
			s.middle = append(s.middle, val)
			s.middleLen += len(val)
		}
	}

	// compute required letters for block filtering
	allBytes := make([]byte, 0, len(s.prefix)+len(s.suffix)+s.middleLen)
	allBytes = append(allBytes, s.prefix...)
	allBytes = append(allBytes, s.suffix...)
	for _, m := range s.middle {
		allBytes = append(allBytes, m...)
	}
	s.lettersBitset = util.NewLettersBitset(allBytes)

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

func findSequence(haystack []byte, needles [][]byte) int {
	for cur, val := range needles {
		start := bytes.Index(haystack, val)
		if start == -1 {
			return cur
		}
		haystack = haystack[start+len(val):]
	}
	return len(needles)
}

func (s *wildcardSearch) Check(val []byte) (bool, error) {
	return s.checkPrefix(val) && s.checkSuffix(val) && s.checkMiddle(val), nil
}

func (s *wildcardSearch) CheckEntry(letters util.LettersBitset) bool {
	return letters.IsNil() || letters.ContainsAll(s.lettersBitset)
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

func (s *rangeTextSearch) Check(val []byte) (bool, error) {
	valStr := string(val)
	if s.token.From.Kind != parser.TermSymbol {
		if s.token.IncludeFrom {
			if !(s.token.From.Data <= valStr) {
				return false, nil
			}
		} else {
			if !(s.token.From.Data < valStr) {
				return false, nil
			}
		}
	}
	if s.token.To.Kind != parser.TermSymbol {
		if s.token.IncludeTo {
			if !(valStr <= s.token.To.Data) {
				return false, nil
			}
		} else {
			if !(valStr < s.token.To.Data) {
				return false, nil
			}
		}
	}
	return true, nil
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

func (s *rangeNumberSearch) Check(rawVal []byte) (bool, error) {
	val, err := strconv.ParseFloat(string(rawVal), 64)
	if err != nil || isNaNOrInf(val) {
		return false, nil
	}

	if s.includeFrom {
		if !(s.from <= val) {
			return false, nil
		}
	} else {
		if !(s.from < val) {
			return false, nil
		}
	}
	if s.includeTo {
		if !(val <= s.to) {
			return false, nil
		}
	} else {
		if !(val < s.to) {
			return false, nil
		}
	}

	return true, nil
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

func (s *rangeIpSearch) Check(rawVal []byte) (bool, error) {
	val, err := netip.ParseAddr(string(rawVal))
	if err != nil {
		return false, nil
	}

	// s.from <= val <= s.to
	return s.from.Compare(val) <= 0 && val.Compare(s.to) <= 0, nil
}

type reSearch struct {
	baseSearch
	r *regexp.Regexp

	prefix parser.ReLiteral
	middle []parser.ReLiteral
	suffix parser.ReLiteral

	letters  util.LettersBitset
	narrowed bool
	checked  int
}

func newReSearch(base baseSearch, token *parser.Re) *reSearch {
	if token.Expression.Kind != parser.TermText {
		panic("BUG: wrong term kind in re")
	}

	var b util.LetterBitsetBuilder
	b.Add(token.Prefix.Value)
	b.Add(token.Suffix.Value)

	for i := range token.Middle {
		b.Add(token.Middle[i].Value)
	}

	return &reSearch{
		baseSearch: base,
		r:          token.CompiledExpression,

		prefix: token.Prefix,
		middle: token.Middle,
		suffix: token.Suffix,

		letters: b.Build(),
	}
}

func (s *reSearch) Narrow(tp tokenProvider) {
	// TODO(dkharms): Handle case-insensitive search.
	if s.prefix.Foldable {
		return
	}

	s.narrowed = true
	l := len(s.prefix.Value)
	s.first = util.BinSearchInRange(s.first, s.last, func(tid int) bool {
		tokenPrefix := cut(tp.GetToken(uint32(tid)), l)
		return bytes.Compare(tokenPrefix, s.prefix.Value) >= 0
	})

	s.last = util.BinSearchInRange(s.first, s.last, func(tid int) bool {
		tokenPrefix := cut(tp.GetToken(uint32(tid)), l)
		return bytes.Compare(tokenPrefix, s.prefix.Value) > 0
	}) - 1
}

func (s *reSearch) CheckEntry(letters util.LettersBitset) bool {
	return letters.IsNil() || letters.ContainsAll(s.letters)
}

func (s *reSearch) Check(val []byte) (bool, error) {
	if len(s.prefix.Value)+len(s.suffix.Value) > len(val) {
		return false, nil
	}

	if !s.checkPrefix(val) || !s.checkSuffix(val) || !s.checkMiddle(val) {
		return false, nil
	}

	if config.MaxRegexTokensCheck > 0 && s.checked >= config.MaxRegexTokensCheck {
		return false, errors.New(
			"'re' filter exceeded token limit: " +
				"consider using regular filters",
		)
	}

	s.checked++
	return s.r.Match(val), nil
}

func (s *reSearch) checkPrefix(val []byte) bool {
	prefix := s.prefix.Value

	if s.narrowed || len(prefix) == 0 {
		return true
	}

	if s.prefix.Foldable {
		return bytes.EqualFold(prefix, val[:len(prefix)])
	}

	return bytes.Equal(prefix, val[:len(prefix)])
}

func (s *reSearch) checkMiddle(val []byte) bool {
	if len(s.middle) == 0 {
		return true
	}

	for i := range s.middle {
		lit := s.middle[i]

		// We have to perform case-insensitive substring search,
		// so at this point it's just easier to give up and check token
		// via compiled regular expression.
		if lit.Foldable {
			return true
		}

		start := bytes.Index(val, lit.Value)
		if start == -1 {
			return false
		}

		val = val[start+len(lit.Value):]
	}

	return true
}

func (s *reSearch) checkSuffix(val []byte) bool {
	suffix := s.suffix.Value

	if len(suffix) == 0 {
		return true
	}

	if s.suffix.Foldable {
		return bytes.EqualFold(suffix, val[:len(suffix)])
	}

	return bytes.Equal(suffix, val[len(val)-len(suffix):])
}

type Searcher interface {
	FirstTID() uint32
	LastTID() uint32
	Check(val []byte) (bool, error)
	CheckEntry(letters util.LettersBitset) bool
}

func newSearcher(token parser.Token, tp tokenProvider) Searcher {
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
	case *parser.Re:
		// TODO(dkharms): We can benefit from many optimizations when dealing with regular expressions.
		//
		// For example, with the most obvious one we can narrow search space
		// by extracting prefix and suffix from expression if there is any:
		//
		//   prefix := regexp.Compile(expr).LiteralPrefix()
		//   suffix := Reverse(regexp.Compile(Reverse(expr)).LiteralPrefix())
		//
		// and then performing similar logic as in [literalSearch.Narrow] to find
		// boundaries for token ids.
		//
		// There are other techniques which are more complicated so it's
		// worth studying Apache Lucene, TSDB (Prometheus) etc.
		s := newReSearch(base, t)
		if tp.Ordered() {
			s.Narrow(tp)
		}
		return s
	}
	panic(fmt.Sprintf("unknown token type: %T", token))
}

func isNaNOrInf(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}

// isSimpleWildcardContains checks if this AST token is simple wildcard like 'foo:*bar*'
func isSimpleWildcardContains(token parser.Token) (needle []byte, ok bool) {
	lit, ok := token.(*parser.Literal)
	if !ok || len(lit.Terms) != 3 {
		return nil, false
	}
	if !lit.Terms[0].IsWildcard() || lit.Terms[1].Kind != parser.TermText || !lit.Terms[2].IsWildcard() {
		return nil, false
	}
	return []byte(lit.Terms[1].Data), true
}

func Search(ctx context.Context, t parser.Token, tp tokenProvider) ([]uint32, error) {
	if util.IsCancelled(ctx) {
		return nil, ctx.Err()
	}
	if needle, ok := isSimpleWildcardContains(t); ok {
		return tp.FindContains(needle)
	}
	s := newSearcher(t, tp)
	return tp.FindToken(s)
}
