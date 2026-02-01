package util

import "sort"

// GallopSearchGeq returns the smallest index i in ascending sorted vals such that vals[i] >= geq
func GallopSearchGeq(vals []uint32, x uint32) (idx int, found bool) {
	n := len(vals)
	if n == 0 {
		return 0, false
	}
	if vals[0] >= x {
		return 0, true
	}
	hi := 1
	for hi < n && vals[hi] < x {
		hi *= 2
	}
	searchLen := min(n, hi+1)
	idx = sort.Search(searchLen, func(i int) bool { return vals[i] >= x })
	if idx >= searchLen {
		return 0, false
	}
	return idx, true
}

// GallopSearchLeq returns the largest index i in ascending sorted vals such that vals[i] <= geq
func GallopSearchLeq(vals []uint32, x uint32) (idx int, found bool) {
	n := len(vals)
	if n == 0 {
		return 0, false
	}
	if vals[n-1] <= x {
		return n - 1, true
	}
	left := n - 1
	step := 1
	for left >= 0 && vals[left] > x {
		left -= step
		step *= 2
	}

	left = max(0, left)
	searchLen := n - left
	j := sort.Search(searchLen, func(j int) bool { return vals[left+j] > x })
	if j == 0 {
		return 0, false
	}
	return left + j - 1, true
}
