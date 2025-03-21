package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePipeFields(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("* | fields  message,error, level", "* | fields message, error, level")
	test("* | fields level", "* | fields level")
	test("* | fields level", "* | fields level")
	test(`* | fields "_id"`, `* | fields _id`)
	test(`* | fields "_\\message\\_"`, `* | fields "_\\message\\_"`)
	test(`* | fields "_\\message*"`, `* | fields "_\\message\*"`)
	test(`* | fields k8s_namespace`, `* | fields k8s_namespace`)
}

func TestParsePipeFieldsExcept(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("* | fields except message,error, level", "* | fields except message, error, level")
	test("* | fields except level", "* | fields except level")
	test(`* | fields except "_id"`, `* | fields except _id`)
	test(`* | fields except "_\\message\\_"`, `* | fields except "_\\message\\_"`)
	test(`* | fields except "_\\message*"`, `* | fields except "_\\message\*"`)
	test(`* | fields except k8s_namespace`, `* | fields except k8s_namespace`)
}
