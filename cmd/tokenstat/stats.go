package main

import (
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
)

type classStats struct {
	count [classCount]int
	bytes [classCount]int
}

func (s *classStats) add(t tokenEntry) {
	s.count[t.class]++
	s.bytes[t.class] += len(t.value)
}

func (s *classStats) total() (int, int) {
	var n, b int
	for c := range classCount {
		n += s.count[c]
		b += s.bytes[c]
	}
	return n, b
}

func (s *classStats) dominant() tokenClass {
	best := classString
	for c := range classCount {
		if s.count[c] > s.count[best] {
			best = c
		}
	}
	return best
}

func percent(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}

func runClassify(name string, top int, w io.Writer) error {
	f, err := loadFraction(name)
	if err != nil {
		return err
	}

	var global classStats
	perField := map[string]*classStats{}
	var fields []string
	for _, t := range f.tokens {
		global.add(t)
		s, ok := perField[t.field]
		if !ok {
			s = &classStats{}
			perField[t.field] = s
			fields = append(fields, t.field)
		}
		s.add(t)
	}

	n, b := global.total()
	fmt.Fprintf(w, "fields=%d tokens=%d raw_bytes=%d docs=%d\n\n", len(fields), n, b, f.docsTotal)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "class\ttokens\ttokens%\tbytes\tbytes%\t")
	for c := range classCount {
		fmt.Fprintf(tw, "%s\t%d\t%.1f\t%d\t%.1f\t\n",
			c, global.count[c], percent(global.count[c], n), global.bytes[c], percent(global.bytes[c], b))
	}
	tw.Flush()

	slices.SortFunc(fields, func(a, b string) int {
		na, _ := perField[a].total()
		nb, _ := perField[b].total()
		return nb - na
	})

	fmt.Fprintf(w, "\ntop %d fields by token count\n", top)
	for _, field := range fields[:min(top, len(fields))] {
		s := perField[field]
		fn, _ := s.total()
		var parts []string
		for c := range classCount {
			if p := percent(s.count[c], fn); p >= 1 {
				parts = append(parts, fmt.Sprintf("%s:%.0f%%", c, p))
			}
		}
		fmt.Fprintf(w, "  %-28s %9d  %s\n", trim(field, 28), fn, strings.Join(parts, " "))
	}

	fmt.Fprintln(w, "\nmonotyped fields (>=95% of one non-string class, >=500 tokens)")
	for _, field := range fields {
		s := perField[field]
		fn, _ := s.total()
		c := s.dominant()
		if fn >= 500 && c != classString && percent(s.count[c], fn) >= 95 {
			fmt.Fprintf(w, "  %-28s %9d  %s:%.1f%%\n", trim(field, 28), fn, c, percent(s.count[c], fn))
		}
	}

	return nil
}

func runSample(name, field, className string, floatOnly bool, n int, w io.Writer) error {
	f, err := loadFraction(name)
	if err != nil {
		return err
	}

	class, filterClass := classByName(className)
	var picked []tokenEntry
	seen := 0
	r := rand.New(rand.NewPCG(1, 2))
	for _, t := range f.tokens {
		if field != "" && t.field != field {
			continue
		}
		if floatOnly {
			if v, err := strconv.ParseFloat(t.value, 64); err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
		}
		if filterClass && t.class != class {
			continue
		}
		seen++
		if len(picked) < n {
			picked = append(picked, t)
		} else if j := r.IntN(seen); j < n {
			picked[j] = t
		}
	}

	fmt.Fprintf(w, "matched=%d\n", seen)
	for _, t := range picked {
		fmt.Fprintf(w, "%-24s %-8s freq=%-7d %q\n", trim(t.field, 24), t.class, t.freq, trim(t.value, 120))
	}
	return nil
}

func runLengths(name, field string, w io.Writer) error {
	f, err := loadFraction(name)
	if err != nil {
		return err
	}

	const maxLen = 40
	var counts [classCount][maxLen + 1]int
	var total [classCount]int
	all := 0
	for _, t := range f.tokens {
		if t.field != field {
			continue
		}
		l := min(len(t.value), maxLen)
		counts[t.class][l]++
		total[t.class]++
		all++
	}

	fmt.Fprintf(w, "field=%s tokens=%d (lengths >= %d are grouped)\n", field, all, maxLen)
	for c := range classCount {
		if total[c] == 0 {
			continue
		}
		fmt.Fprintf(w, "\n%s: %d tokens (%.1f%%)\n", c, total[c], percent(total[c], all))
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
		fmt.Fprintln(tw, "len\ttokens\tclass_pct\tcum_pct\t")
		cum := 0
		for l, n := range counts[c] {
			if n == 0 {
				continue
			}
			cum += n
			fmt.Fprintf(tw, "%d\t%d\t%.1f\t%.1f\t\n", l, n, percent(n, total[c]), percent(cum, total[c]))
		}
		tw.Flush()
	}
	return nil
}

func trim(s string, l int) string {
	if len(s) > l {
		return s[:l]
	}
	return s
}
