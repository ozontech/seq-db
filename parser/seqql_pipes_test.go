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

func TestParsePipeFilter(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test(`service:my_service | filter field:"some value"`, `service:my_service | filter field:"some value"`)
	test(`service:my_service | filter field:value`, `service:my_service | filter field:value`)
}

func TestParsePipeFilterErrors(t *testing.T) {
	test := func(q string) {
		t.Helper()
		_, err := ParseSeqQL(q, nil)
		require.Error(t, err)
	}

	test(`service:my_service | filter`)
	test(`service:my_service | filter field`)
	test(`service:my_service | filter :value`)
	test(`service:my_service | filter a:1 | filter b:2`)
}

func TestParsePipeStats(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("service:my-service | stats count by (service)", "service:my-service | stats count by (service)")
	test("service:my-service | stats sum(level) by (service)", "service:my-service | stats sum(level) by (service)")
	test("service:my-service | stats count by (service) interval(1m)", "service:my-service | stats count by (service) interval(1m)")
	test("service:my-service | stats min(response_time) by (service)", "service:my-service | stats min(response_time) by (service)")
	test("service:my-service | stats max(response_time) by (service)", "service:my-service | stats max(response_time) by (service)")
	test("service:my-service | stats avg(response_time) by (service)", "service:my-service | stats avg(response_time) by (service)")
	test("service:my-service | stats unique by (service)", "service:my-service | stats unique by (service)")
	test("service:my-service | stats unique_count by (service)", "service:my-service | stats unique_count by (service)")
}

func TestParsePipeStatsMultiple(t *testing.T) {
	test := func(q string) {
		t.Helper()
		_, err := ParseSeqQL(q, nil)
		require.Error(t, err)
	}

	test("service:my-service | stats count by (service), sum(level) by (service)")
	test("service:my-service | stats count by (service) interval(1m), sum(level) by (service) interval(1m)")
}

func TestParsePipeStatsQuantile(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("service:my-service | stats quantile(response_time, 0.5, 0.95) by (service)", "service:my-service | stats quantile(response_time, 0.5, 0.95) by (service)")
}

func TestParsePipeSort(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("service:my_service | sort asc", "service:my_service | sort asc")
	test("service:my_service | sort desc", "service:my_service | sort desc")
}

func TestParsePipeSortErrors(t *testing.T) {
	test := func(q string) {
		t.Helper()
		_, err := ParseSeqQL(q, nil)
		require.Error(t, err)
	}

	test(`service:my_service | sort invalid_order`)
	test(`service:my_service | sort asc | sort desc`)
	test("service:my_service | sort")
}

func TestParsePipeLimit(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("service:my_service | limit 10", "service:my_service | limit 10")
	test("service:my_service | limit 100", "service:my_service | limit 100")
	test("service:my_service | limit 100 | offset 100", "service:my_service | limit 100 | offset 100")
}

func TestParsePipeLimitErrors(t *testing.T) {
	test := func(q string) {
		t.Helper()
		_, err := ParseSeqQL(q, nil)
		require.Error(t, err)
	}

	test(`service:my_service | limit`)
	test(`service:my_service | limit abc`)
	test(`service:my_service | limit 0`)
	test(`service:my_service | limit -1`)
	test(`service:my_service | limit 10 | limit 20`)
}

func TestParsePipeOffset(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("service:my_service | offset 10", "service:my_service | offset 10")
	test("service:my_service | offset 100", "service:my_service | offset 100")
	test("service:my_service | limit 100 | offset 100", "service:my_service | limit 100 | offset 100")
}

func TestParsePipeOffsetErrors(t *testing.T) {
	test := func(q string) {
		t.Helper()
		_, err := ParseSeqQL(q, nil)
		require.Error(t, err)
	}

	test(`service:my_service | offset`)
	test(`service:my_service | offset abc`)
	test(`service:my_service | offset 0`)
	test(`service:my_service | offset -1`)
	test(`service:my_service | offset 10 | offset 20`)
}

// Stream-only pipes are parsed by ParseSeqQL but must be rejected by methods that
// do not support them via ValidateStreamPipes().
func TestParseSeqQLRejectsStreamPipes(t *testing.T) {
	test := func(q string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Error(t, query.ValidateStreamPipes())
	}

	test(`service:my_service | stats count by (service)`)
	test(`service:my_service | sort asc`)
	test(`service:my_service | limit 10`)
	test(`service:my_service | offset 10`)
	test(`service:my_service | filter field:value`)
}

func TestValidatePipesAllowsFields(t *testing.T) {
	t.Parallel()
	query, err := ParseSeqQL(`service:my_service | fields message, level`, nil)
	require.NoError(t, err)
	require.NoError(t, query.ValidateStreamPipes())
}

func TestParsePipeOrder(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test(
		"service:my_service | stats count by (service) | filter field:value | fields message | sort asc | limit 10 | offset 5",
		"service:my_service | stats count by (service) | filter field:value | fields message | sort asc | limit 10 | offset 5",
	)
	test("service:my_service | stats count by (service) | limit 10", "service:my_service | stats count by (service) | limit 10")
	test("service:my_service | fields message | offset 5", "service:my_service | fields message | offset 5")
	test("service:my_service | sort asc | limit 10", "service:my_service | sort asc | limit 10")
	test("service:my_service | fields message | sort asc", "service:my_service | fields message | sort asc")
	test("service:my_service | filter field:value | limit 10", "service:my_service | filter field:value | limit 10")
}

func TestParsePipeOrderErrors(t *testing.T) {
	test := func(q string) {
		t.Helper()
		_, err := ParseSeqQL(q, nil)
		require.Error(t, err)
	}

	test(`service:my_service | fields message | stats count by (service)`)
	test(`service:my_service | sort asc | fields message`)
	test(`service:my_service | limit 10 | sort asc`)
	test(`service:my_service | offset 5 | limit 10`)
	test(`service:my_service | offset 5 | sort asc | limit 10`)
	test(`service:my_service | offset 5 | limit 10 | sort asc | fields message | stats count by (service)`)
	test(`service:my_service | fields message | filter field:value`)
}
