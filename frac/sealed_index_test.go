package frac

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenKeyHash(t *testing.T) {
	a := assert.New(t)

	// stable within the process
	a.Equal(tokenKeyHash("service:api-gateway"), tokenKeyHash("service:api-gateway"))

	// different keys hash differently (sampling, not a proof)
	a.NotEqual(tokenKeyHash("service:api"), tokenKeyHash("service:api2"))
	a.NotEqual(tokenKeyHash("msg:*timeout"), tokenKeyHash("msg:timeout*"))
}
