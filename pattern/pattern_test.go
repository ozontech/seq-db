package pattern

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/parser"
)

type testTokenProvider struct {
	ordered  *simpleTokenProvider
	shuffled *simpleTokenProvider
}

func newTestTokenProvider(data []string) testTokenProvider {
	if len(data) < 2 {
		return testTokenProvider{ // empty provider
			ordered:  &simpleTokenProvider{data: data, ordered: true},
			shuffled: &simpleTokenProvider{data: data, ordered: false},
		}
	}

	// prepare shuffled
	shuffled := simpleTokenProvider{
		data:    make([]string, len(data)),
		ordered: true,
	}
	copy(shuffled.data, data)
	for i := 0; i < 100 && shuffled.ordered; i++ {
		rand.Shuffle(len(shuffled.data), func(i int, j int) {
			shuffled.data[i], shuffled.data[j] = shuffled.data[j], shuffled.data[i]
		})
		shuffled.ordered = isOrdered(shuffled.data)
	}

	ordered := simpleTokenProvider{
		data:    make([]string, len(data)),
		ordered: true,
	}
	copy(ordered.data, data)
	sort.Strings(ordered.data)
	ordered.data = uniq(ordered.data)

	return testTokenProvider{
		ordered:  &ordered,
		shuffled: &shuffled,
	}
}

func uniq(data []string) []string {
	var prev string
	i := 0
	for _, v := range data {
		if v == prev {
			continue
		}
		prev = v
		data[i] = v
		i++
	}
	return data[:i]
}

func isOrdered(data []string) bool {
	for i := 0; i < len(data)-1; i++ {
		if data[i] > data[i+1] {
			return false
		}
	}
	return true
}

type simpleTokenProvider struct {
	data    []string
	ordered bool
}

func (tp *simpleTokenProvider) GetToken(i uint32) []byte {
	return []byte(tp.data[i-1])
}

func (tp *simpleTokenProvider) FirstTID() uint32 {
	return 1
}

func (tp *simpleTokenProvider) LastTID() uint32 {
	return uint32(len(tp.data))
}

func (tp *simpleTokenProvider) Ordered() bool {
	return tp.ordered
}

func searchAll(t *testing.T, tp testTokenProvider, req string, expect []string) {
	sort.Strings(expect)
	assert.False(t, tp.shuffled.Ordered(), "data is sorted")
	search(t, tp.shuffled, req, expect)

	assert.True(t, tp.ordered.Ordered(), "data is not sorted")
	search(t, tp.ordered, req, uniq(expect))
}

func parseSingleTokenForTests(query string) (parser.Token, error) {
	ast, err := parser.ParseSeqQL(query, nil)
	if err != nil {
		return nil, err
	}

	// returning only first token
	if len(ast.Root.Children) > 0 {
		return nil, errors.New("more than one token")
	}

	return ast.Root.Value, nil
}

func search(t *testing.T, tp *simpleTokenProvider, req string, expect []string) {
	searchType := "full"
	if tp.Ordered() {
		searchType = "narrow"
	}

	token, err := parseSingleTokenForTests("m:" + req)
	require.NoError(t, err)
	s := newSearcher(token, tp)

	res := []string{}
	for i := s.firstTID(); i <= s.lastTID(); i++ {
		val := tp.GetToken(i)
		if s.check(val) {
			res = append(res, string(val))
		}
	}
	sort.Strings(res)

	assert.Equal(t, expect, res, "%s search request %q failed", searchType, req)
}

type testCase struct {
	query  string
	expect []string
}

func testAll(t *testing.T, tp testTokenProvider, tests []testCase) {
	for _, test := range tests {
		searchAll(t, tp, test.query, test.expect)
	}
}

func TestPatternSimple(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"ab",
		"abc",
		"bcfg",
		"bd",
		"efg",
		"lka",
		"lkk",
		"x",
		"x",
		"zaaa",
	})

	tests := []testCase{
		{"b*", []string{"bcfg", "bd"}},
		{"f*", []string{}},
		{"efg", []string{"efg"}},
		{"ef", []string{}},
		{"lk*", []string{"lka", "lkk"}},
		{"a*", []string{"ab", "abc"}},
		{"z*", []string{"zaaa"}},
		{"ab", []string{"ab"}},
		{"aa", []string{}},
		{"zz", []string{}},
		{"zaaa", []string{"zaaa"}},
		{"b*g", []string{"bcfg"}},
		{"b*d", []string{"bd"}},
		{"z*a", []string{"zaaa"}},
		{"x", []string{"x", "x"}},
	}

	testAll(t, tp, tests)
}

func TestPatternPrefix(t *testing.T) {
	data := []string{
		"a",
		"aa",
		"aba",
		"abc",
		"abc",
		"aca",
		"acb",
		"acba",
		"acbb",
		"acbccc",
		"acbz",
		"acdd",
		"ace",
		"acff",
		"ad",
		"az",
	}
	tp := newTestTokenProvider(data)

	tests := []testCase{
		{"a*", data},
		{"ab*", []string{"aba", "abc", "abc"}},
		{"ac*", []string{"aca", "acb", "acba", "acbb", "acbccc", "acbz", "acdd", "ace", "acff"}},
		{"acb*", []string{"acb", "acba", "acbb", "acbccc", "acbz"}},
		{"acb", []string{"acb"}},
		{"acba*", []string{"acba"}},
		{"acc*", []string{}},
		{"acc", []string{}},
		{"acz*", []string{}},
	}

	testAll(t, tp, tests)
}

func TestPatternEmpty(t *testing.T) {
	tp := newTestTokenProvider([]string{})

	tests := []testCase{
		{"a", []string{}},
		{"abc", []string{}},
		{"*", []string{}},
	}

	testAll(t, tp, tests)
}

func TestPatternSingle(t *testing.T) {
	tp := newTestTokenProvider([]string{"abacaba"})

	tests := []testCase{
		{"abacaba", []string{"abacaba"}},
		{"*", []string{"abacaba"}},
		{"a*", []string{"abacaba"}},
		{"a", []string{}},
		{"abc", []string{}},
	}

	testAll(t, tp, tests)
}

func TestPatternSuffix(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"abc",
		"acd",
		"acdc:suf",
		"acdd",
		"acdd",
		"acdfg:suf",
		"acg",
		"add:suf",
	})

	tests := []testCase{
		{`"acd*:suf"`, []string{`acdc:suf`, `acdfg:suf`}},
		{`acd*`, []string{`acd`, `acdc:suf`, `acdd`, `acdd`, `acdfg:suf`}},
		{`"ac*:suf"`, []string{`acdc:suf`, `acdfg:suf`}},
		{`ac*f`, []string{`acdc:suf`, `acdfg:suf`}},
		{`ac*d`, []string{`acd`, `acdd`, `acdd`}},
		{`"acdc:suf"`, []string{`acdc:suf`}},
		{`"*:suf"`, []string{`acdc:suf`, `acdfg:suf`, `add:suf`}},
	}

	testAll(t, tp, tests)
}

func TestPatternSuffix2(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"aba",
		"abac",
		"abacaba",
		"caba",
	})

	tests := []testCase{
		{"*", []string{"aba", "abac", "abacaba", "caba"}},
		{"aba*", []string{"aba", "abac", "abacaba"}},
		{"aba*aba", []string{"abacaba"}},
		{"abac*aba", []string{"abacaba"}},
		{"aba*caba", []string{"abacaba"}},
		{"abac*caba", []string{}},
		{"*caba", []string{"abacaba", "caba"}},
	}

	testAll(t, tp, tests)
}

func TestPatternMiddle(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"a:b:a",
		"aba",
		"abacaba",
		"abracadabra",
		"some:Data:hey",
	})

	tests := []testCase{
		{`ab*c*ba`, []string{`abacaba`}},
		{`a*b*a`, []string{`a:b:a`, `aba`, `abacaba`, `abracadabra`}},
		{`a*c*a`, []string{`abacaba`, `abracadabra`}},
		{`"a*:b:*a"`, []string{`a:b:a`}},
		{`*acada*`, []string{`abracadabra`}},
		{`*aba*`, []string{`aba`, `abacaba`}},
		{`*ac*ca*`, []string{}},
	}

	testAll(t, tp, tests)
}

func TestRange(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"1",
		"34",
		"12",
		"-3",
		"15",
		"44",
		"45",
		"46",
		"120481",
		"-12",
		"-15",
	})

	tests := []testCase{
		{"[2 to 16]", []string{"12", "15"}},
		{"[1 to 1]", []string{"1"}},
		{"(1 to 1)", []string{}},
		{"(44 to 46)", []string{"45"}},
		{"[44 to 46)", []string{"44", "45"}},
		{"(44 to 46]", []string{"45", "46"}},
		{"[44 to 46]", []string{"44", "45", "46"}},
		{"[-16 to -10]", []string{"-12", "-15"}},

		// result is sorted as strings in test function. actual result is not sorted
		{"[1 to 34]", []string{"1", "12", "15", "34"}},
		{"[16 to 2]", []string{}},
	}

	testAll(t, tp, tests)
}

func TestRangeNumberWildcard(t *testing.T) {
	maxInt64 := strconv.Itoa(math.MaxInt64)
	minInt64 := strconv.Itoa(math.MinInt64)

	tp := newTestTokenProvider([]string{
		"-4",
		"-8",
		"13",
		"3",
		"402.0",
		"Inf",
		"+Inf",
		"-Inf",
		"NaN",
		maxInt64,
		minInt64,
		"0",
		"сорок два",
		"",
		" ",
		"a",
	})

	tests := []testCase{
		{"[* to -8]", []string{"-8", minInt64}},
		{"(* to -8]", []string{"-8", minInt64}},
		{"[* to -8)", []string{minInt64}},
		{"[* to 3]", []string{"-4", "-8", minInt64, "0", "3"}},
		{"[* to 3)", []string{"-4", "-8", minInt64, "0"}},
		{"[13 to *]", []string{"13", "402.0", maxInt64}},
		{"(13 to *]", []string{"402.0", maxInt64}},
		{"[402 to *]", []string{"402.0", maxInt64}},
		{"[402 to *)", []string{"402.0", maxInt64}},
		{"(402 to *]", []string{maxInt64}},
		{"[* to *]", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64}},
		{"(* to *]", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64}},
		{"[* to *)", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64}},
		{"(* to *)", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64}},
		{"[402.0 to 402.0]", []string{"402.0"}},
	}

	testAll(t, tp, tests)
}

func TestRangeText(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"ab",
		"abc",
		"bcfg",
		"bd",
		"efg",
		"lka",
		"lkk",
		"x",
		"zaaa",
	})

	tests := []testCase{
		{"[bd to efg]", []string{"bd", "efg"}},
		{"[bd to efg)", []string{"bd"}},
		{"(bd to efg)", []string{}},
		{"(bd to efg]", []string{"efg"}},
		{"[bb to efg]", []string{"bcfg", "bd", "efg"}},
		{"(bb to efg]", []string{"bcfg", "bd", "efg"}},
		{"[bb to efh]", []string{"bcfg", "bd", "efg"}},
		{"[bb to efh)", []string{"bcfg", "bd", "efg"}},

		{"[* to ab]", []string{"ab"}},
		{"(* to ab]", []string{"ab"}},
		{"[* to ab)", []string{}},
		{"[* to bd]", []string{"ab", "abc", "bcfg", "bd"}},
		{"[* to bd)", []string{"ab", "abc", "bcfg"}},
		{"[lkk to *]", []string{"lkk", "x", "zaaa"}},
		{"(lkk to *]", []string{"x", "zaaa"}},
		{"[zaaa to *]", []string{"zaaa"}},
		{"[zaaa to *)", []string{"zaaa"}},
		{"(zaaa to *]", []string{}},
	}

	testAll(t, tp, tests)
}

func TestPatternSymbols(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"*",
		"**",
		"****",
		"val=*",
		"val=***",
	})

	tests := []testCase{
		{`"\*"`, []string{"*"}},
		{`"\**"`, []string{"*", "**", "****"}},
		{`"\*\*"`, []string{"**"}},
		{`"\*\**"`, []string{"**", "****"}},
		{`"val=*"`, []string{"val=*", "val=***"}},
		{`"val=\*"`, []string{"val=*"}},
		{`"val=\**"`, []string{"val=*", "val=***"}},
		{`"val=\*\*\*"`, []string{"val=***"}},
	}

	testAll(t, tp, tests)
}

func TestPatternIPRange(t *testing.T) {
	data := []string{
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.3",
		"192.168.1.4",
		"192.168.1.5",
	}

	tp := newTestTokenProvider(data)

	tests := []testCase{
		{`ip_range(0.0.0.0, 255.255.255.255)`, data},
		{`ip_range(192.168.1.2, 192.168.1.3)`, []string{"192.168.1.2", "192.168.1.3"}},
		{`ip_range(192.168.1.5, 192.168.1.255)`, []string{"192.168.1.5"}},
		{`ip_range(192.167.1.5, 192.167.1.255)`, []string{}},

		{`ip_range(0.0.0.0/0)`, data},
		{`ip_range(192.168.1.2/31)`, []string{"192.168.1.2", "192.168.1.3"}},
		{`ip_range(192.168.1.0/31)`, []string{"192.168.1.1"}},
		{`ip_range(192.167.1.0/31)`, []string{}},
	}

	testAll(t, tp, tests)
}
