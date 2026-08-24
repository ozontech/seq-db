package lids

// Galloping (exponential) searches for the NextGeq hot path: zigzag targets
// are monotone and the chunk slice narrows from the consumption edge, so the
// answer is usually a few elements away from the edge. Probing exponentially
// from the edge costs ~2*log2(d) for an answer d away (bounded by ~2x a full
// binary search over the remainder, which pays log2(len) regardless of d).

// searchGeqGallop returns the index of the first element >= t, probing from
// the front (IteratorDesc consumes the slice front-to-back).
func searchGeqGallop(a []uint32, t uint32) int {
	n := len(a)
	if n == 0 || a[0] >= t {
		return 0
	}
	i := 1
	for i < n && a[i] < t {
		i <<= 1
	}
	// a[i>>1] < t, so the answer is in (i>>1, min(i, n)].
	lo := i>>1 + 1
	hi := i
	if hi > n {
		hi = n
	}
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if a[m] < t {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}

// searchGtGallopTail returns the index of the first element > t, probing from
// the back (IteratorAsc consumes the slice back-to-front).
func searchGtGallopTail(a []uint32, t uint32) int {
	n := len(a)
	if n == 0 {
		return 0
	}
	if a[n-1] <= t {
		return n
	}
	i := 1
	for i < n && a[n-1-i] > t {
		i <<= 1
	}
	// a[n-1-(i>>1)] > t, so the answer is <= n-1-(i>>1) < hi.
	hi := n - i>>1
	lo := n - i
	if lo < 0 {
		lo = 0
	}
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if a[m] <= t {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}
