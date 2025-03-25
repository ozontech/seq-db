package pattern

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/util"
)

type TokenFetcher interface {
	FetchToken(int) []byte
}

type Searcher interface {
	Begin() int
	End() int
	Check(val []byte) bool
}

type baseSearch struct {
	begin int
	end   int
}

func (s *baseSearch) Begin() int {
	return s.begin
}

func (s *baseSearch) End() int {
	return s.end
}

type LiteralSearch struct {
	baseSearch
	value    []byte
	narrowed bool
}

func newLiteralSearch(base baseSearch, token *parser.Literal) *LiteralSearch {
	if len(token.Terms) != 1 || token.Terms[0].Kind != parser.TermText {
		return nil
	}
	return &LiteralSearch{
		baseSearch: base,
		value:      []byte(token.Terms[0].Data),
	}
}

func (s *LiteralSearch) Narrow(fetcher TokenFetcher) {
	s.narrowed = true

	s.begin = sort.Search(s.end+1, func(tid int) bool { return bytes.Compare(fetcher.FetchToken(tid), s.value) >= 0 })

	if s.begin <= s.end && bytes.Equal(fetcher.FetchToken(s.begin), s.value) {
		s.end = s.begin
		return
	}

	// not found
	s.end = s.begin - 1 // begin > end: will be considered empty
}

func (s *LiteralSearch) Check(val []byte) bool {
	if s.narrowed {
		return len(s.value) == len(val)
	}
	return bytes.Equal(s.value, val)
}

type WildcardSearch struct {
	baseSearch
	prefix    []byte
	suffix    []byte
	middle    []*substring
	middleLen int
	narrowed  bool
}

func newWildcardSearch(base baseSearch, token *parser.Literal) *WildcardSearch {
	s := &WildcardSearch{
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

func (s *WildcardSearch) Narrow(fetcher TokenFetcher) {
	s.narrowed = true
	l := len(s.prefix)
	s.begin = util.BinSearchInRange(s.begin, s.end, func(tid int) bool {
		tokenPrefix := cut(fetcher.FetchToken(tid), l)
		return bytes.Compare(tokenPrefix, s.prefix) >= 0
	})
	s.end = util.BinSearchInRange(s.begin, s.end, func(tid int) bool {
		tokenPrefix := cut(fetcher.FetchToken(tid), l)
		return bytes.Compare(tokenPrefix, s.prefix) > 0
	}) - 1
}

func (s *WildcardSearch) checkPrefix(val []byte) bool {
	if s.narrowed || len(s.prefix) == 0 {
		return true
	}
	if len(s.prefix) > len(val) {
		return false
	}
	return bytes.Equal(s.prefix, val[:len(s.prefix)])
}

func (s *WildcardSearch) checkSuffix(val []byte) bool {
	if len(s.suffix) == 0 {
		return true
	}
	if len(val)-len(s.prefix) < len(s.suffix) {
		return false
	}
	return bytes.Equal(val[len(val)-len(s.suffix):], s.suffix)
}

func (s *WildcardSearch) checkMiddle(val []byte) bool {
	if len(s.middle) == 0 {
		return true
	}
	if len(val)-len(s.prefix)-len(s.suffix) < s.middleLen {
		return false
	}
	return findSequence(val[len(s.prefix):len(val)-len(s.suffix)], s.middle) == len(s.middle)
}

func (s *WildcardSearch) Check(val []byte) bool {
	return s.checkPrefix(val) && s.checkSuffix(val) && s.checkMiddle(val)
}

type RangeTextSearch struct {
	baseSearch
	token *parser.Range
}

func NewRangeTextSearch(base baseSearch, token *parser.Range) *RangeTextSearch {
	return &RangeTextSearch{
		baseSearch: base,
		token:      token,
	}
}

func (s *RangeTextSearch) Check(val []byte) bool {
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

type RangeNumberSearch struct {
	baseSearch
	from        float64
	includeFrom bool
	to          float64
	includeTo   bool
}

func NewRangeNumberSearch(base baseSearch, token *parser.Range) *RangeNumberSearch {
	var err error
	s := &RangeNumberSearch{
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

func (s *RangeNumberSearch) Check(rawVal []byte) bool {
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

func NewSearcher(token parser.Token, fetcher TokenFetcher, size int) Searcher {
	base := baseSearch{
		end: size - 1,
	}
	switch t := token.(type) {
	case *parser.Literal:
		if s := newLiteralSearch(base, t); s != nil {
			if fetcher != nil {
				s.Narrow(fetcher)
			}
			return s
		}
		s := newWildcardSearch(base, t)
		if fetcher != nil {
			s.Narrow(fetcher)
		}
		return s
	case *parser.Range:
		// try number search
		if s := NewRangeNumberSearch(base, t); s != nil {
			return s
		}
		return NewRangeTextSearch(base, t)
	}
	panic(fmt.Sprintf("unknown token type: %T", token))
}

func isNaNOrInf(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}
