package pattern

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/parser"
)

func (f *SimpleFetcher) isSorted() bool {
	for i := 0; i < len(f.Data)-1; i++ {
		if f.Data[i] > f.Data[i+1] {
			return false
		}
	}
	return true
}

func search(t *testing.T, fetcher *SimpleFetcher, narrow bool, req string, expect []string) {
	searchType := "full"
	if narrow {
		searchType = "narrow"
	}

	token, err := parser.ParseSingleTokenForTests("m", req)
	require.NoError(t, err)
	var s Searcher
	if narrow {
		s = NewSearcher(token, fetcher, len(fetcher.Data))
	} else {
		s = NewSearcher(token, nil, len(fetcher.Data))
	}
	res := []string{}
	for i := s.Begin(); i <= s.End(); i++ {
		val := fetcher.FetchToken(i)
		if s.Check(val) {
			res = append(res, fetcher.Data[i])
		}
	}
	sort.Strings(res)

	assert.Equal(t, expect, res, "%s search request %q failed", searchType, req)
}

func searchAll(t *testing.T, fetch *SimpleFetcher, req string, expect []string) {
	search(t, fetch, false, req, expect)
	search(t, fetch, true, req, expect)
}

func TestPatternSimple(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
			"ab",
			"abc",
			"bcfg",
			"bd",
			"efg",
			"lka",
			"lkk",
			"x",
			"zaaa",
		},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, "b*", []string{"bcfg", "bd"})
	searchAll(t, fetch, "f*", []string{})
	searchAll(t, fetch, "efg", []string{"efg"})
	searchAll(t, fetch, "ef", []string{})
	searchAll(t, fetch, "lk*", []string{"lka", "lkk"})
	searchAll(t, fetch, "x", []string{"x"})
	searchAll(t, fetch, "a*", []string{"ab", "abc"})
	searchAll(t, fetch, "z*", []string{"zaaa"})
	searchAll(t, fetch, "ab", []string{"ab"})
	searchAll(t, fetch, "aa", []string{})
	searchAll(t, fetch, "zz", []string{})
	searchAll(t, fetch, "zaaa", []string{"zaaa"})
}

func TestPatternShuffled(t *testing.T) {
	for i := 0; i < 10000; i++ {
		fetch := &SimpleFetcher{
			Data: []string{
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
			},
		}

		rand.Shuffle(len(fetch.Data), func(i int, j int) {
			fetch.Data[i], fetch.Data[j] = fetch.Data[j], fetch.Data[i]
		})

		search(t, fetch, false, "b*", []string{"bcfg", "bd"})
		search(t, fetch, false, "b*g", []string{"bcfg"})
		search(t, fetch, false, "b*d", []string{"bd"})
		search(t, fetch, false, "f*", []string{})
		search(t, fetch, false, "efg", []string{"efg"})
		search(t, fetch, false, "ef", []string{})
		search(t, fetch, false, "lk*", []string{"lka", "lkk"})
		search(t, fetch, false, "x", []string{"x", "x"})
		search(t, fetch, false, "a*", []string{"ab", "abc"})
		search(t, fetch, false, "z*", []string{"zaaa"})
		search(t, fetch, false, "z*a", []string{"zaaa"})
		search(t, fetch, false, "ab", []string{"ab"})
		search(t, fetch, false, "aa", []string{})
		search(t, fetch, false, "zz", []string{})
	}
}

func TestPatternPrefix(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
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
		},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, "a*", fetch.Data)
	searchAll(t, fetch, "ab*", []string{"aba", "abc", "abc"})
	searchAll(t, fetch, "ac*", []string{"aca", "acb", "acba", "acbb", "acbccc", "acbz", "acdd", "ace", "acff"})
	searchAll(t, fetch, "acb*", []string{"acb", "acba", "acbb", "acbccc", "acbz"})
	searchAll(t, fetch, "acb", []string{"acb"})
	searchAll(t, fetch, "acba*", []string{"acba"})
	searchAll(t, fetch, "acc*", []string{})
	searchAll(t, fetch, "acc", []string{})
	searchAll(t, fetch, "acz*", []string{})
}

func TestPatternEmpty(t *testing.T) {
	a := assert.New(t)
	fetch := &SimpleFetcher{}

	a.True(fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, "a", []string{})
	searchAll(t, fetch, "abc", []string{})
	searchAll(t, fetch, "*", []string{})
}

func TestPatternSingle(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{"abacaba"},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, "abacaba", []string{"abacaba"})
	searchAll(t, fetch, "*", []string{"abacaba"})
	searchAll(t, fetch, "a*", []string{"abacaba"})
	searchAll(t, fetch, "a", []string{})
	searchAll(t, fetch, "abc", []string{})
}

func TestPatternSuffix(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
			"abc",
			"acd",
			"acdc:suf",
			"acdd",
			"acdd",
			"acdfg:suf",
			"acg",
			"add:suf",
		},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, `acd*\:suf`, []string{`acdc:suf`, `acdfg:suf`})
	searchAll(t, fetch, `acd*`, []string{`acd`, `acdc:suf`, `acdd`, `acdd`, `acdfg:suf`})
	searchAll(t, fetch, `ac*\:suf`, []string{`acdc:suf`, `acdfg:suf`})
	searchAll(t, fetch, `ac*f`, []string{`acdc:suf`, `acdfg:suf`})
	searchAll(t, fetch, `ac*d`, []string{`acd`, `acdd`, `acdd`})
	searchAll(t, fetch, `acdc\:suf`, []string{`acdc:suf`})
	searchAll(t, fetch, `*\:suf`, []string{`acdc:suf`, `acdfg:suf`, `add:suf`})
}

func TestPatternSuffix2(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
			"aba",
			"abac",
			"abacaba",
			"caba",
		},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, "*", []string{"aba", "abac", "abacaba", "caba"})
	searchAll(t, fetch, "aba*", []string{"aba", "abac", "abacaba"})
	searchAll(t, fetch, "aba*aba", []string{"abacaba"})
	searchAll(t, fetch, "abac*aba", []string{"abacaba"})
	searchAll(t, fetch, "aba*caba", []string{"abacaba"})
	searchAll(t, fetch, "abac*caba", []string{})
	searchAll(t, fetch, "*caba", []string{"abacaba", "caba"})
}

func TestPatternMiddle(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
			"a:b:a",
			"aba",
			"abacaba",
			"abracadabra",
			"some:Data:hey",
		},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, `ab*c*ba`, []string{`abacaba`})
	searchAll(t, fetch, `a*b*a`, []string{`a:b:a`, `aba`, `abacaba`, `abracadabra`})
	searchAll(t, fetch, `a*c*a`, []string{`abacaba`, `abracadabra`})
	searchAll(t, fetch, `a*\:b\:*a`, []string{`a:b:a`})
	searchAll(t, fetch, `*acada*`, []string{`abracadabra`})
	searchAll(t, fetch, `*aba*`, []string{`aba`, `abacaba`})
	searchAll(t, fetch, `*ac*ca*`, []string{})
}

func TestRange(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
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
		},
	}

	searchAll(t, fetch, "[2 to 16]", []string{"12", "15"})
	searchAll(t, fetch, "[1 to 1]", []string{"1"})
	searchAll(t, fetch, "{1 to 1}", []string{})
	searchAll(t, fetch, "{44 to 46}", []string{"45"})
	searchAll(t, fetch, "[44 to 46}", []string{"44", "45"})
	searchAll(t, fetch, "{44 to 46]", []string{"45", "46"})
	searchAll(t, fetch, "[44 to 46]", []string{"44", "45", "46"})
	searchAll(t, fetch, "[-16 to -10]", []string{"-12", "-15"})
	// result is sorted as strings in test function. actual result is not sorted
	searchAll(t, fetch, "[1 to 34]", []string{"1", "12", "15", "34"})
	searchAll(t, fetch, "[16 to 2]", []string{})
}

func TestRangeNumberWildcard(t *testing.T) {
	maxInt64 := strconv.Itoa(math.MaxInt64)
	minInt64 := strconv.Itoa(math.MinInt64)
	fetch := &SimpleFetcher{
		Data: []string{
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
		},
	}

	searchAll(t, fetch, "[* to -8]", []string{"-8", minInt64})
	searchAll(t, fetch, "{* to -8]", []string{"-8", minInt64})
	searchAll(t, fetch, "[* to -8}", []string{minInt64})
	searchAll(t, fetch, "[* to 3]", []string{"-4", "-8", minInt64, "0", "3"})
	searchAll(t, fetch, "[* to 3}", []string{"-4", "-8", minInt64, "0"})
	searchAll(t, fetch, "[13 to *]", []string{"13", "402.0", maxInt64})
	searchAll(t, fetch, "{13 to *]", []string{"402.0", maxInt64})
	searchAll(t, fetch, "[402 to *]", []string{"402.0", maxInt64})
	searchAll(t, fetch, "[402 to *}", []string{"402.0", maxInt64})
	searchAll(t, fetch, "{402 to *]", []string{maxInt64})
	searchAll(t, fetch, "[* to *]", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, fetch, "{* to *]", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, fetch, "[* to *}", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, fetch, "{* to *}", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, fetch, "[402.0 to 402.0]", []string{"402.0"})
}

func TestRangeText(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
			"ab",
			"abc",
			"bcfg",
			"bd",
			"efg",
			"lka",
			"lkk",
			"x",
			"zaaa",
		},
	}

	searchAll(t, fetch, "[bd to efg]", []string{"bd", "efg"})
	searchAll(t, fetch, "[bd to efg}", []string{"bd"})
	searchAll(t, fetch, "{bd to efg}", []string{})
	searchAll(t, fetch, "{bd to efg]", []string{"efg"})
	searchAll(t, fetch, "[bb to efg]", []string{"bcfg", "bd", "efg"})
	searchAll(t, fetch, "{bb to efg]", []string{"bcfg", "bd", "efg"})
	searchAll(t, fetch, "[bb to efh]", []string{"bcfg", "bd", "efg"})
	searchAll(t, fetch, "[bb to efh}", []string{"bcfg", "bd", "efg"})

	searchAll(t, fetch, "[* to ab]", []string{"ab"})
	searchAll(t, fetch, "{* to ab]", []string{"ab"})
	searchAll(t, fetch, "[* to ab}", []string{})
	searchAll(t, fetch, "[* to bd]", []string{"ab", "abc", "bcfg", "bd"})
	searchAll(t, fetch, "[* to bd}", []string{"ab", "abc", "bcfg"})
	searchAll(t, fetch, "[lkk to *]", []string{"lkk", "x", "zaaa"})
	searchAll(t, fetch, "{lkk to *]", []string{"x", "zaaa"})
	searchAll(t, fetch, "[zaaa to *]", []string{"zaaa"})
	searchAll(t, fetch, "[zaaa to *}", []string{"zaaa"})
	searchAll(t, fetch, "{zaaa to *]", []string{})

}

func TestPatternSymbols(t *testing.T) {
	fetch := &SimpleFetcher{
		Data: []string{
			"*",
			"**",
			"****",
			"val=*",
			"val=***",
		},
	}

	assert.True(t, fetch.isSorted(), "data is not sorted")

	searchAll(t, fetch, `\*`, []string{"*"})
	searchAll(t, fetch, `\**`, []string{"*", "**", "****"})
	searchAll(t, fetch, `\*\*`, []string{"**"})
	searchAll(t, fetch, `\*\**`, []string{"**", "****"})
	searchAll(t, fetch, `val=*`, []string{"val=*", "val=***"})
	searchAll(t, fetch, `val=\*`, []string{"val=*"})
	searchAll(t, fetch, `val=\**`, []string{"val=*", "val=***"})
	searchAll(t, fetch, `val=\*\*\*`, []string{"val=***"})
}
