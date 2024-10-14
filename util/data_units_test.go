package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDuration(t *testing.T) {
	test := func(expect time.Duration, esDuration string) {
		t.Helper()
		duration, err := ParseDuration(esDuration)
		require.NoError(t, err)
		require.Equal(t, expect, duration)
	}

	test(time.Second, "1s")
	test(time.Minute, "1m")
	test(time.Hour, "1h")
	test(time.Hour*24, "1d")
	test(time.Hour*24*7, "1w")
	test(time.Hour*24*30, "1M")
	test(time.Hour*24*91, "1q")
	test(time.Hour*24*365, "1y")
}
