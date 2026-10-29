package util

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCanonicalInt64(t *testing.T) {
	valid := []int64{0, 1, -1, 9, 10, -10, 1234567890, math.MaxInt64, math.MinInt64, math.MaxInt64 - 1, math.MinInt64 + 1, 1e18, -1e18}
	for _, v := range valid {
		got, ok := ParseCanonicalInt64([]byte(strconv.FormatInt(v, 10)))
		require.True(t, ok, v)
		require.Equal(t, v, got)
	}

	invalid := []string{
		"", "-", "-0", "00", "01", "-01", "+1", "1.0", "1e3", "5_1", " 1", "1 ", "abc",
		"9223372036854775808", "-9223372036854775809", "18446744073709551616",
		"99999999999999999999", "0000000000000000001",
	}
	for _, s := range invalid {
		_, ok := ParseCanonicalInt64([]byte(s))
		require.False(t, ok, s)
	}
}
