package frac

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnpackCache_ValsCapacity(t *testing.T) {
	cache := NewUnpackCache()
	require.Equal(t, defaultValsCapacity, cap(cache.valsBuffer))
	require.Equal(t, 0, cap(cache.readVals))

	cache.Start()
	require.Equal(t, defaultValsCapacity, cap(cache.valsBuffer))
	require.Equal(t, 0, cap(cache.readVals))
	cache.Finish()

	require.Equal(t, defaultValsCapacity, cap(cache.valsBuffer))
	require.Equal(t, 0, cap(cache.readVals))
}
