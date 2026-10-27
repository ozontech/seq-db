package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"text/tabwriter"
)

type slowQuery struct {
	TookMs         int `json:"took_ms"`
	FracSearchMs   int `json:"frac_search_total_ms"`
	FracsSearched  int `json:"fracs_searched"`
	FracSearchMean int `json:"frac_search_mean_ms"`
	Params         struct {
		Request string `json:"request"`
	} `json:"params"`
}

var digitNeedleRe = regexp.MustCompile(`([A-Za-z0-9_.@-]+):\*-?([0-9]+)\*`)

func readSlowLog(path string) ([]slowQuery, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var res []slowQuery
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var q slowQuery
		if err := dec.Decode(&q); err == io.EOF {
			return res, nil
		} else if err != nil {
			return nil, err
		}
		res = append(res, q)
	}
}

func runSlowLog(path, field, fracName string, w io.Writer) error {
	queries, err := readSlowLog(path)
	if err != nil {
		return err
	}

	share := func(string, string) float64 { return 1 }
	if fracName != "" {
		f, err := loadFraction(fracName)
		if err != nil {
			return err
		}
		idx := fieldIndexes(f.tokens)
		cache := map[[2]string]float64{}
		share = func(field, needle string) float64 {
			k := [2]string{field, needle}
			if s, ok := cache[k]; ok {
				return s
			}
			s := 1.0
			if fi, ok := idx[field]; ok {
				s = typedScanShare(f.tokens, fi, needle)
			}
			cache[k] = s
			return s
		}
	}

	type key struct {
		field string
		n     int
	}
	type agg struct {
		queries int
		tookMs  int
		cpuMs   int
		estMs   float64
		needles map[string]int
	}

	stats := map[key]*agg{}
	totalCPU, digitCPU, digitQueries := 0, 0, 0
	digitEst := 0.0
	for _, q := range queries {
		totalCPU += q.FracSearchMs
		seen := map[key]float64{}
		queryShare := 0.0
		for _, m := range digitNeedleRe.FindAllStringSubmatch(q.Params.Request, -1) {
			if field != "" && m[1] != field {
				continue
			}
			k := key{field: m[1], n: len(m[2])}
			a, ok := stats[k]
			if !ok {
				a = &agg{needles: map[string]int{}}
				stats[k] = a
			}
			a.needles[m[2]]++
			s := share(m[1], m[2])
			queryShare = max(queryShare, s)
			seen[k] = max(seen[k], s)
		}
		for k, s := range seen {
			a := stats[k]
			a.queries++
			a.tookMs += q.TookMs
			a.cpuMs += q.FracSearchMs
			a.estMs += float64(q.FracSearchMs) * s
		}
		if len(seen) > 0 {
			digitQueries++
			digitCPU += q.FracSearchMs
			digitEst += float64(q.FracSearchMs) * queryShare
		}
	}

	keys := make([]key, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b key) int { return stats[b].cpuMs - stats[a].cpuMs })

	fmt.Fprintf(w, "queries=%d with_digit_needles=%d cpu_total=%.1fs cpu_digit=%.1fs (%.1f%%)\n",
		len(queries), digitQueries, float64(totalCPU)/1000, float64(digitCPU)/1000, percent(digitCPU, totalCPU))
	if fracName != "" {
		saved := float64(digitCPU) - digitEst
		fmt.Fprintf(w, "typed estimate (query cpu x max scan share of its digit needles): cpu_digit %.1fs -> %.1fs, saved %.1fs = %.1f%% of all cpu\n",
			float64(digitCPU)/1000, digitEst/1000, saved/1000, 100*saved/float64(totalCPU))
	}
	fmt.Fprintln(w)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "field\tn\tqueries\ttook_s\tcpu_s\tcpu_pct_of_digit\ttyped_cpu_s\tdistinct_needles\ttop_needle\t")
	for _, k := range keys {
		a := stats[k]
		top, topN := "", 0
		for nd, c := range a.needles {
			if c > topN || (c == topN && nd < top) {
				top, topN = nd, c
			}
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%s (x%d)\t\n",
			trim(k.field, 24), k.n, a.queries, float64(a.tookMs)/1000, float64(a.cpuMs)/1000,
			percent(a.cpuMs, digitCPU), a.estMs/1000, len(a.needles), top, topN)
	}
	tw.Flush()
	return nil
}
