package pattern

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
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

func (tp *simpleTokenProvider) FindContains(firstTID uint32, lastTID uint32, needle []byte) ([]uint32, error) {
	if len(needle) == 0 {
		return nil, nil
	}
	var tids []uint32
	for t := firstTID; t <= lastTID; t++ {
		if bytes.Contains(tp.GetToken(t), needle) {
			tids = append(tids, t)
		}
	}
	return tids, nil
}

func (tp *simpleTokenProvider) FindToken(searcher Searcher) ([]uint32, error) {
	firstTID := searcher.FirstTID()
	lastTID := searcher.LastTID()
	var tids []uint32
	for t := firstTID; t <= lastTID; t++ {
		match, err := searcher.Check(tp.GetToken(t))
		if err != nil {
			return nil, err
		}

		if match {
			tids = append(tids, t)
		}
	}
	return tids, nil
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
	for i := s.FirstTID(); i <= s.LastTID(); i++ {
		val := tp.GetToken(i)

		match, err := s.Check(val)
		if err != nil {
			t.Fatal(err)
		}

		if match {
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

func TestPatternRe(t *testing.T) {
	t.Run("match-uuid", func(t *testing.T) {
		needles := []string{
			"7313c25b-2eae-4839-b773-91dff1f24f1f",
			"e7b555cc-b98f-4cd2-bf09-9e0330c2593f",
		}

		data := append(
			[]string{"not-uuid", "not uuid as well"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")`,
				needles,
			},
		})
	})

	t.Run("match-ip", func(t *testing.T) {
		needles := []string{
			"192.168.1.1",
			"10.0.0.255",
			"256.999.1.1",
		}

		data := append(
			[]string{"no ip here"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")`,
				needles,
			},
		})
	})

	t.Run("match-russian-phone", func(t *testing.T) {
		needles := []string{
			"+78005553535",
			"+7 800 555-35-35",
			"+7 (800) 555-35-35",
			"8(800)555-35-35",
		}

		data := append(
			[]string{"phone: 12345", "not a phone"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("(\+7|8)\s?\(?\d{3}\)?\s?\d{3}-?\d{2}-?\d{2}")`,
				needles,
			},
		})
	})

	t.Run("match-level", func(t *testing.T) {
		needles := []string{
			"[ERROR] connection refused",
			"[WARN] timeout exceeded",
		}

		data := append(
			[]string{"[INFO] all good", "[DEBUG] trace value"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("\[(ERROR|WARN)\].*")`,
				needles,
			},
		})
	})

	t.Run("match-http-status", func(t *testing.T) {
		needles := []string{
			"status=500",
			"status=503",
			"status=599",
		}

		data := append(
			[]string{"status=200", "status=404", "status=301"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("status=5\d{2}")`,
				needles,
			},
		})
	})

	t.Run("match-email", func(t *testing.T) {
		needles := []string{
			"alice@example.com",
			"bob.smith@test.co.uk",
		}

		data := append(
			[]string{"no email", "bare @ sign", "missing@"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("[a-zA-Z0-9.]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")`,
				needles,
			},
		})
	})

	t.Run("match-kv", func(t *testing.T) {
		needles := []string{
			"timeout=30s",
			"retries=5",
		}

		data := append(
			[]string{"no kv here", "just=", "=nope"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("[a-z]+=\w+")`,
				needles,
			},
		})
	})

	t.Run("match-anchored", func(t *testing.T) {
		needles := []string{
			"ERROR: disk empty",
			"ERROR: disk full",
		}

		data := append(
			[]string{"some ERROR in the middle", "info: no error"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("^ERROR: disk (empty|full)$")`,
				needles,
			},
		})
	})

	t.Run("match-empty", func(t *testing.T) {
		data := []string{"foo", "bar", "baz"}

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("^zzz$")`,
				[]string{},
			},
		})
	})

	t.Run("match-asterisk", func(t *testing.T) {
		needles := []string{
			"token=*",
			"token=**",
			"token=***",
		}

		data := append(
			[]string{"token=\\*", "token=abc"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("token=[*]+")`,
				needles,
			},
		})
	})

	t.Run("match-all", func(t *testing.T) {
		data := []string{"abc", "def", "ghi"}

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re(".*")`,
				data,
			},
		})
	})

	t.Run("match-case-insensitive", func(t *testing.T) {
		needles := []string{
			"Error occurred",
			"ERROR occurred",
			"error occurred",
		}

		data := append(
			[]string{"info: all fine"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("(?i)error.*")`,
				needles,
			},
		})
	})

	t.Run("match-range", func(t *testing.T) {
		needles := []string{
			"code: abc",
			"code: abcd",
			"code: abcde",
		}

		data := append(
			[]string{"code: ab", "code: abcdef"},
			needles...,
		)

		tp := newTestTokenProvider(data)

		testAll(t, tp, []testCase{
			{
				`re("code: [a-z]{3,5}$")`,
				needles,
			},
		})
	})
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

func TestPatternIPRangeV6(t *testing.T) {
	data := []string{
		"2001:db8:130f:0000:0000:09c0:876a:130b",
		"2001:db8:130f:0000:0000:09c0:876a:130c",
		"2001:db8:130f:0000:0000:09c0:876a:130d",
		"2001:db8:130f:0000:0000:09c0:876a:130e",
		"2001:db8:130f:0000:0000:09c0:876a:130f",
	}

	tp := newTestTokenProvider(data)

	tests := []testCase{
		// Full IPv6 range
		{
			`ip_range(0:0:0:0:0:0:0:0, ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff)`,
			data,
		},

		// Partial explicit range (start–end)
		{
			`ip_range(2001:db8:130f:0000:0000:09c0:876a:130c, 2001:db8:130f:0000:0000:09c0:876a:130d)`,
			[]string{
				"2001:db8:130f:0000:0000:09c0:876a:130c",
				"2001:db8:130f:0000:0000:09c0:876a:130d",
			},
		},

		// Upper boundary check
		{
			`ip_range(2001:db8:130f:0000:0000:09c0:876a:130f, ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff)`,
			[]string{
				"2001:db8:130f:0000:0000:09c0:876a:130f",
			},
		},

		// No matches in range
		{
			`ip_range(2001:db8:130e::, 2001:db8:130e:ffff:ffff:ffff:ffff:ffff)`,
			[]string{},
		},

		// CIDR: full IPv6 space
		{
			`ip_range(::/0)`,
			data,
		},

		// CIDR /127 (IPv6 analogue of IPv4 /31)
		{
			`ip_range(2001:db8:130f::9c0:876a:130c/127)`,
			[]string{
				"2001:db8:130f:0000:0000:09c0:876a:130c",
				"2001:db8:130f:0000:0000:09c0:876a:130d",
			},
		},

		// Narrow subnet covering the last nibble range
		{
			`ip_range(2001:db8:130f::9c0:876a:1300/124)`,
			data,
		},

		// CIDR with no matches
		{
			`ip_range(2001:db8:130e::/64)`,
			[]string{},
		},

		// Compressed IPv6 notation (::) in explicit range
		{
			`ip_range(2001:db8:130f::9c0:876a:130c, 2001:db8:130f::9c0:876a:130d)`,
			[]string{
				"2001:db8:130f:0000:0000:09c0:876a:130c",
				"2001:db8:130f:0000:0000:09c0:876a:130d",
			},
		},
	}

	testAll(t, tp, tests)
}

func testFindSequence(a *assert.Assertions, cnt int, needles []string, haystack string) {
	var needlesB [][]byte
	for _, needle := range needles {
		needlesB = append(needlesB, []byte(needle))
	}
	res := findSequence([]byte(haystack), needlesB)
	a.Equal(cnt, res, "wrong total number of matches")
}

func TestFindSequence(t *testing.T) {
	a := assert.New(t)

	testFindSequence(a, 2, []string{"abra", "ada"}, "abracadabra")
	testFindSequence(a, 2, []string{"aba", "aba"}, "abacaba")
	testFindSequence(a, 2, []string{"aba", "caba"}, "abacaba")
	testFindSequence(a, 1, []string{"abacaba"}, "abacaba")
	testFindSequence(a, 0, []string{"abacaba"}, "aba")
	testFindSequence(a, 1, []string{"aba"}, "abacaba")
	testFindSequence(a, 0, []string{"dad"}, "abacaba")
	testFindSequence(a, 1, []string{"aba", "dad"}, "abacaba")
	testFindSequence(a, 0, []string{"dad", "aba"}, "abacaba")

	testFindSequence(a, 2, []string{"needle", "haystack"}, "can you find a needle in a haystack?")
	testFindSequence(a, 2, []string{"k8s_pod", "_prod"}, "\"k8s_pod\":{\"main_prod\"}")

	testFindSequence(a, 2, []string{"!13", "37#"}, "woah!13@37#test")

	testFindSequence(a, 1, []string{"abc"}, strings.Repeat("ab", 1024)+"c")
}

func BenchmarkFindSequence_Random(b *testing.B) {
	sizes := []struct {
		name         string
		haystackSize int
		needleSize   int
		needleCount  int
	}{
		{"tiny", 64, 3, 2},
		{"small", 256, 10, 3},
		{"medium", 1024, 50, 5},
		{"large", 16384, 200, 10},
		{"extra-large", 1048576, 1024, 100},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			haystack, needles := generateTestData(
				size.haystackSize, size.needleSize, size.needleCount, 256,
			)
			for b.Loop() {
				findSequence(haystack, needles)
				b.SetBytes(int64(len(haystack)))
			}
		})
	}
}

func generateTestData(haystackSize, needleSize, needleCount, charset int) ([]byte, [][]byte) {
	r := rand.New(rand.NewSource(42))
	haystack := generateRandomBytes(haystackSize, charset, r)

	needles := make([][]byte, needleCount)
	for i := range needleCount {
		pattern := generateRandomBytes(needleSize, charset, r)
		pos := r.Intn(len(haystack) - needleSize)
		copy(haystack[pos:], pattern)
		needles[i] = pattern
	}

	return haystack, needles
}

func generateRandomBytes(size, charset int, r *rand.Rand) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Intn(charset))
	}
	return b
}
