package main

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/ozontech/seq-db/consts"
)

type substrParams struct {
	field       string
	needles     []string
	blockValues int
}

type substrAction uint8

const (
	actionSkip substrAction = iota
	actionExact
	actionScan
)

func digitCount(c tokenClass, v string) int {
	switch c {
	case classInt64:
		return len(strings.TrimPrefix(v, "-"))
	case classDecimal:
		return len(strings.NewReplacer("-", "", ".", "").Replace(v))
	}
	return len(v)
}

func typedAction(c tokenClass, v string, n int) substrAction {
	switch c {
	case classString:
		return actionScan
	case classInt64, classDigits:
		d := digitCount(c, v)
		switch {
		case d < n:
			return actionSkip
		case d == n && c == classInt64:
			return actionExact
		}
		return actionScan
	case classDecimal:
		if digitCount(c, v) < n {
			return actionSkip
		}
		return actionScan
	case classHex:
		if len(v) <= n {
			return actionSkip
		}
		return actionScan
	case classUUID:
		if n > 12 {
			return actionSkip
		}
		return actionScan
	case classIPv4:
		if n > 3 {
			return actionSkip
		}
		return actionScan
	case classIPv6:
		if n > 4 {
			return actionSkip
		}
		return actionScan
	}
	return actionScan
}

func fieldIndexes(tokens []tokenEntry) map[string][]int {
	res := map[string][]int{}
	for i, t := range tokens {
		res[t.field] = append(res[t.field], i)
	}
	return res
}

func typedScanShare(tokens []tokenEntry, fieldIdx []int, needle string) float64 {
	scanned := 0
	for _, i := range fieldIdx {
		if typedAction(tokens[i].class, tokens[i].value, len(needle)) == actionScan {
			scanned++
		}
	}
	return float64(scanned) / float64(len(fieldIdx))
}

func stringStreamBlocks(tokens []tokenEntry, keep func(tokenEntry) bool) []int {
	ids := make([]int, len(tokens))
	block, blockSize := 0, 0
	for i, t := range tokens {
		ids[i] = -1
		if !keep(t) {
			continue
		}
		tokenSize := 4 + len(t.value)
		if blockSize > 0 && blockSize+tokenSize > consts.RegularBlockSize {
			block++
			blockSize = 0
		}
		ids[i] = block
		blockSize += tokenSize
	}
	return ids
}

func typedFieldBlocks(tokens []tokenEntry, field string, blockValues int) []int {
	ids := make([]int, len(tokens))
	byClass := map[tokenClass][]int{}
	for i, t := range tokens {
		ids[i] = -1
		if t.field == field && t.class != classString {
			byClass[t.class] = append(byClass[t.class], i)
		}
	}

	next := 0
	for c := range classCount {
		idx := byClass[c]
		values := make(map[int]typedValue, len(idx))
		for _, i := range idx {
			_, values[i] = parse(tokens[i].value)
		}
		slices.SortFunc(idx, func(a, b int) int {
			switch {
			case values[a].less(values[b]):
				return -1
			case values[b].less(values[a]):
				return 1
			}
			return 0
		})
		for k, i := range idx {
			ids[i] = next + k/blockValues
		}
		next += (len(idx) + blockValues - 1) / blockValues
	}
	return ids
}

func runSubstr(name string, params substrParams, w io.Writer) error {
	f, err := loadFraction(name)
	if err != nil {
		return err
	}

	all := stringStreamBlocks(f.tokens, func(tokenEntry) bool { return true })
	strOnly := stringStreamBlocks(f.tokens, func(t tokenEntry) bool { return t.class == classString })
	typed := typedFieldBlocks(f.tokens, params.field, params.blockValues)

	var fieldIdx []int
	curBlocks := map[int]struct{}{}
	for i, t := range f.tokens {
		if t.field == params.field {
			fieldIdx = append(fieldIdx, i)
			curBlocks[all[i]] = struct{}{}
		}
	}
	if len(fieldIdx) == 0 {
		return fmt.Errorf("field %q not found", params.field)
	}

	fmt.Fprintf(w, "field=%s tokens=%d blocks=%d; typed: ints shorter than needle skipped, same length = binary search (1 block), hex only if longer\n\n",
		params.field, len(fieldIdx), len(curBlocks))

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "needle\tn\tmatched\tcur_tokens\tcur_blocks\ttyp_scan\ttyp_exact\ttyp_blocks\tscan_pct\tblocks_pct\tscan_by_class\t")
	for _, needle := range params.needles {
		n := len(needle)
		matched, scanned, exact := 0, 0, 0
		blocks := map[int]struct{}{}
		var byClass [classCount]int
		for _, i := range fieldIdx {
			t := f.tokens[i]
			if strings.Contains(t.value, needle) {
				matched++
			}
			switch typedAction(t.class, t.value, n) {
			case actionScan:
				scanned++
				byClass[t.class]++
				if t.class == classString {
					blocks[strOnly[i]] = struct{}{}
				} else {
					blocks[-1-typed[i]] = struct{}{}
				}
			case actionExact:
				exact++
			}
		}
		typedBlocks := len(blocks)
		if exact > 0 {
			typedBlocks++
		}

		var parts []string
		for c := range classCount {
			if byClass[c] > 0 {
				parts = append(parts, fmt.Sprintf("%s:%d", c, byClass[c]))
			}
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t%s\t\n",
			needle, n, matched, len(fieldIdx), len(curBlocks), scanned, exact, typedBlocks,
			percent(scanned, len(fieldIdx)), percent(typedBlocks, len(curBlocks)), strings.Join(parts, " "))
	}
	tw.Flush()
	return nil
}
