package frac_test

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (s *FractionTestSuite) TestSearchTypedInt64() {
	var values []string
	for i := range 5000 {
		values = append(values, strconv.Itoa(i*37-50000))
	}
	values = append(values,
		strconv.FormatInt(math.MaxInt64, 10), strconv.FormatInt(math.MinInt64, 10),
		"0", "1", "-1", "12", "120", "129", "1299", "-12", "-120", "-129",
		"007", "-0", "1.5", "5_1", "+12", "abc12", "12abc", "1e3", "x", "12-3", "-",
	)

	var docs []string
	var bulks [][]string
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, v := range values {
		doc := fmt.Sprintf(`{"timestamp":%q,"v":%q}`, base.Add(time.Duration(i)*time.Second).Format(time.RFC3339), v)
		docs = append(docs, doc)
		if i%1000 == 0 {
			bulks = append(bulks, nil)
		}
		bulks[len(bulks)-1] = append(bulks[len(bulks)-1], doc)
	}
	s.insertDocuments(bulks...)

	expect := func(match func(string) bool) []int {
		res := []int{}
		for i := len(values) - 1; i >= 0; i-- {
			if match(values[i]) {
				res = append(res, i)
			}
		}
		return res
	}

	glob := func(pattern string) func(string) bool {
		parts := strings.Split(pattern, "*")
		for i := range parts {
			parts[i] = regexp.QuoteMeta(parts[i])
		}
		re := regexp.MustCompile("^" + strings.Join(parts, ".*") + "$")
		return re.MatchString
	}

	numRange := func(from, to float64, incFrom, incTo bool) func(string) bool {
		return func(v string) bool {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return false
			}
			return (from < f || incFrom && from == f) && (f < to || incTo && f == to)
		}
	}

	for _, v := range []string{"12", "-12", "0", "007", "-0", "1.5", "x", "9223372036854775807", "-9223372036854775808", "-49963", "99999"} {
		s.AssertSearch("v:"+quoteIfNeeded(v), docs, expect(func(x string) bool { return x == v }))
	}

	for _, p := range []string{
		"12*", "-12*", "-*", "*12", "*12*", "1*5", "*0*0*", "0*", "-0*", "*abc*", "12-*", "*-3",
		"9223372036854775807*", "92233720368547758*", "-92233720368547758*", "*7*", "4996*", "-4996*", "*",
	} {
		s.AssertSearch("v:"+quoteIfNeeded(p), docs, expect(glob(p)))
	}

	inf := math.Inf(1)
	s.AssertSearch("v:[-100, 100]", docs, expect(numRange(-100, 100, true, true)))
	s.AssertSearch("v:(-100, 100)", docs, expect(numRange(-100, 100, false, false)))
	s.AssertSearch("v:[1.5, 12]", docs, expect(numRange(1.5, 12, true, true)))
	s.AssertSearch("v:(1.5, 129)", docs, expect(numRange(1.5, 129, false, false)))
	s.AssertSearch("v:[-1000.5, 0.5]", docs, expect(numRange(-1000.5, 0.5, true, true)))
	s.AssertSearch("v:[*, -40000]", docs, expect(numRange(-inf, -40000, true, true)))
	s.AssertSearch("v:[100000, *]", docs, expect(numRange(100000, inf, true, true)))
	s.AssertSearch("v:[5, 4]", docs, expect(func(string) bool { return false }))
	s.AssertSearch("v:[1, z]", docs, expect(func(v string) bool { return "1" <= v && v <= "z" }))

	reMatch := func(expr string) func(string) bool {
		re := regexp.MustCompile("^(?:" + expr + ")$")
		return re.MatchString
	}
	for _, expr := range []string{"12.*", "-?12[0-9]", ".*99"} {
		s.AssertSearch(fmt.Sprintf("v:re(%q)", expr), docs, expect(reMatch(expr)))
	}
}

func quoteIfNeeded(v string) string {
	if strings.ContainsAny(v, "-+.") {
		return strconv.Quote(v)
	}
	return v
}
