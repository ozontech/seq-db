package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_avgValueCounter_Inc(t *testing.T) {
	c := NewRollingAverage(100)
	c.Append(10)
	c.Append(20)
	c.Append(30)
	c.Append(40)
	c.Append(50)
	assert.Equal(t, 5, c.cursor)
	assert.Equal(t, 150, c.sum)
	assert.Equal(t, float32(150/5), c.Get())

	const (
		start = 111
		end   = 5555
	)
	c = NewRollingAverage(100)
	for i := start; i < end; i++ {
		c.Append(i)
	}

	minVal := end - len(c.elements)
	maxVal := end - 1
	sum := float32(minVal+maxVal) / 2 * float32(len(c.elements)) // sum of the arithmetic progression
	avg := sum / float32(len(c.elements))

	assert.Equal(t, minVal, c.elements[c.cursor])
	assert.Equal(t, maxVal, c.elements[c.cursor-1])
	assert.Equal(t, sum, float32(c.sum))
	assert.Equal(t, avg, c.Get())
}
