package util

import "math"

func ParseCanonicalInt64(b []byte) (int64, bool) {
	n := len(b)
	if n == 0 || n > 20 {
		return 0, false
	}

	neg := b[0] == '-'
	digits := b
	if neg {
		digits = b[1:]
	}

	if len(digits) == 0 || len(digits) > 19 {
		return 0, false
	}
	if digits[0] == '0' && (len(digits) > 1 || neg) {
		return 0, false
	}

	var v uint64
	for _, c := range digits {
		d := c - '0'
		if d > 9 {
			return 0, false
		}
		v = v*10 + uint64(d)
	}

	if len(digits) == 19 && v < 1e18 {
		return 0, false
	}

	if neg {
		if v > uint64(math.MaxInt64)+1 {
			return 0, false
		}
		return int64(^v + 1), true
	}

	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}
