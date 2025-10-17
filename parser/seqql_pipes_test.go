package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePipeFields(t *testing.T) {
	test(t, "* | fields  message,error, level", "* | fields message, error, level")
	test(t, "* | fields level", "* | fields level")
	test(t, "* | fields level", "* | fields level")
	test(t, `* | fields "_id"`, `* | fields _id`)
	test(t, `* | fields "_\\message\\_"`, `* | fields "_\\message\\_"`)
	test(t, `* | fields "_\\message*"`, `* | fields "_\\message\*"`)
	test(t, `* | fields k8s_namespace`, `* | fields k8s_namespace`)
}

func TestParsePipeFieldsExcept(t *testing.T) {
	test(t, "* | fields except message,error, level", "* | fields except message, error, level")
	test(t, "* | fields except level", "* | fields except level")
	test(t, `* | fields except "_id"`, `* | fields except _id`)
	test(t, `* | fields except "_\\message\\_"`, `* | fields except "_\\message\\_"`)
	test(t, `* | fields except "_\\message*"`, `* | fields except "_\\message\*"`)
	test(t, `* | fields except k8s_namespace`, `* | fields except k8s_namespace`)
}

func TestParsePipeHistogram(t *testing.T) {
	test(t, `* | histogram 1s`, `* | histogram 1s`)
	test(t, `* | histogram 60s`, `* | histogram 1m0s`)
	test(t, `* | histogram 1m`, `* | histogram 1m0s`)
	test(t, `* | histogram 10m`, `* | histogram 10m0s`)
	test(t, `* | histogram 2h`, `* | histogram 2h0m0s`)
}

func TestPipesComposition(t *testing.T) {
	test(t, `* | fields level | histogram 1s`, `* | fields level | histogram 1s`)
	test(t, `* | histogram 1s | fields level`, `* | histogram 1s | fields level`)
}

func test(t *testing.T, q, expected string) {
	t.Helper()
	query, err := ParseSeqQL(q, nil)
	require.NoError(t, err)
	assert.Equal(t, expected, query.SeqQLString())
}
