package seq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromString(t *testing.T) {
	_, err := FromString("abaf05877b010000-2402dc02d60615cc")
	assert.NoError(t, err)
}
