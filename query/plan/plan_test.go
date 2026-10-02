package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

const (
	testFrom seq.MID = 1000
	testTo   seq.MID = 2000
)

func TestBuildDocsPipes(t *testing.T) {
	seqql := &parser.SeqQLQuery{
		Pipes: []parser.Pipe{
			&parser.PipeFilter{Condition: parser.FilterCondition{Field: "service", Value: "api"}},
			&parser.PipeFields{Fields: []string{"service", "level"}},
			&parser.PipeSort{Order: "asc"},
			&parser.PipeLimit{Limit: 10},
			&parser.PipeOffset{Offset: 5},
		},
	}

	p, err := build(seqql, "", true)
	require.NoError(t, err)

	assert.False(t, p.IsAgg())
	assert.Equal(t, query.DocsSchema, p.Schema)
	assert.Equal(t, seq.DocsOrderAsc, p.Scan.Order)
	assert.True(t, p.Scan.WithTotal)

	// the filter needs the field to be extracted ahead of it
	require.Len(t, p.Ops, 4)
	extractOp, ok := p.Ops[0].(*ExtractOp)
	require.True(t, ok)
	assert.Equal(t, []string{"service"}, extractOp.Fields)
	filterOp, ok := p.Ops[1].(*FilterOp)
	require.True(t, ok)
	assert.Equal(t, "service", filterOp.Cond.Field)
	assert.Equal(t, "api", filterOp.Cond.Value)
	assert.True(t, filterOp.WithTotal)
	projectOp, ok := p.Ops[2].(*ProjectOp)
	require.True(t, ok)
	assert.Equal(t, []string{"service", "level"}, projectOp.Fields)
	assert.True(t, projectOp.AllowList)
	limitOp, ok := p.Ops[3].(*LimitOp)
	require.True(t, ok)
	assert.Equal(t, 10, limitOp.Limit)
	assert.Equal(t, 5, limitOp.Offset)
}

func TestBuildFieldsExcept(t *testing.T) {
	seqql := &parser.SeqQLQuery{
		Pipes: []parser.Pipe{&parser.PipeFields{Fields: []string{"k8s_pod"}, Except: true}},
	}

	p, err := build(seqql, "", false)
	require.NoError(t, err)

	require.Len(t, p.Ops, 1)
	projectOp, ok := p.Ops[0].(*ProjectOp)
	require.True(t, ok)
	assert.Equal(t, []string{"k8s_pod"}, projectOp.Fields)
	assert.False(t, projectOp.AllowList)
}

type testProducer struct{}

func (testProducer) Next() *query.Record      { return nil }
func (testProducer) Finalize() *query.Summary { return nil }

func TestOpsSchemaChange(t *testing.T) {
	// the schema flows through the ops: the extract extends it,
	// the filter resolves its column from the extension
	var input query.RecordProducer = testProducer{}
	schema := query.DocsSchema

	extractOp := &ExtractOp{Fields: []string{"service"}, OpField: query.DocsDataCol}
	producer, schema := extractOp.Apply(input, schema)
	assert.Equal(t, 3, schema.Len())
	assert.Equal(t, 2, schema.MustColumn[string]("service").Idx())

	filterOp := &FilterOp{Cond: parser.FilterCondition{Field: "service", Value: "api"}}
	producer, schema = filterOp.Apply(producer, schema)
	assert.Equal(t, 3, schema.Len()) // filter does't change the schema
	require.NotNil(t, producer)
}

func TestBuildDefaultOrder(t *testing.T) {
	// no sort pipe: Order stays zero (desc), the scan range is untouched
	p, err := build(&parser.SeqQLQuery{}, "", false)
	require.NoError(t, err)
	assert.Equal(t, seq.DocsOrder(0), p.Scan.Order)
	assert.Equal(t, testFrom, p.Scan.From)
	assert.Equal(t, testTo, p.Scan.To)
}

func TestBuildAgg(t *testing.T) {
	seqql := &parser.SeqQLQuery{
		Pipes: []parser.Pipe{
			&parser.PipeStats{Agg: parser.StatsAgg{Func: "sum", Field: "level", GroupBy: "service", Interval: "1m"}},
		},
	}

	p, err := build(seqql, "", false)
	require.NoError(t, err)

	assert.True(t, p.IsAgg())
	assert.Equal(t, query.AggsSchema, p.Schema)
	assert.Empty(t, p.Ops)
	require.Len(t, p.Scan.AggQ, 1)
	assert.EqualValues(t, seq.AggFuncSum, p.Scan.AggQ[0].Func)
}

func TestBuildAggValidation(t *testing.T) {
	testCases := []struct {
		name string
		agg  parser.StatsAgg
		want string
	}{
		{"unknown func", parser.StatsAgg{Func: "median"}, "unknown aggregation function"},
		{"count without groupBy", parser.StatsAgg{Func: "count"}, "groupBy is required"},
		{"sum without field", parser.StatsAgg{Func: "sum", GroupBy: "service"}, "field is required"},
		{"quantile without args", parser.StatsAgg{Func: "quantile", Field: "level", GroupBy: "service"}, "expect an argument"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			seqql := &parser.SeqQLQuery{
				Pipes: []parser.Pipe{&parser.PipeStats{Agg: tc.agg}},
			}
			_, err := build(seqql, "", false)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestBuildOffsetID(t *testing.T) {
	const offsetID = "0000000000000001-0000000000000001"
	id, err := seq.FromString(offsetID)
	require.NoError(t, err)

	t.Run("explicit asc narrows from", func(t *testing.T) {
		seqql := &parser.SeqQLQuery{Pipes: []parser.Pipe{&parser.PipeSort{Order: "asc"}}}
		p, err := build(seqql, offsetID, false)
		require.NoError(t, err)
		assert.Equal(t, id, p.Scan.OffsetID)
		assert.Equal(t, id.MID, p.Scan.From)
		assert.Equal(t, testTo, p.Scan.To)
	})

	t.Run("default desc narrows to", func(t *testing.T) {
		p, err := build(&parser.SeqQLQuery{}, offsetID, false)
		require.NoError(t, err)
		assert.Equal(t, testFrom, p.Scan.From)
		assert.Equal(t, id.MID, p.Scan.To)
	})

	t.Run("with offset", func(t *testing.T) {
		seqql := &parser.SeqQLQuery{
			Pipes: []parser.Pipe{&parser.PipeOffset{Offset: 5}},
		}
		_, err := build(seqql, "0000000000000001-0000000000000001", false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `only one of "offset" and "offset_id"`)
	})

	t.Run("with agg", func(t *testing.T) {
		seqql := &parser.SeqQLQuery{
			Pipes: []parser.Pipe{&parser.PipeStats{Agg: parser.StatsAgg{Func: "count", GroupBy: "service"}}},
		}
		_, err := build(seqql, "0000000000000001-0000000000000001", false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "offset_id is not supported")
	})

	t.Run("invalid id", func(t *testing.T) {
		_, err := build(&parser.SeqQLQuery{}, "not-an-id", false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "could not parse offset_id")
	})
}

func TestBuildInputValidation(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		_, err := Build(BuildParams{SeqQL: &parser.SeqQLQuery{}, DocField: query.DocsDataCol})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "input schema is not set")
	})
	t.Run("doc column not in schema", func(t *testing.T) {
		s := query.MustNewSchema(query.ColumnDesc{Name: "other", Type: query.DataTypeString})
		_, err := Build(BuildParams{SeqQL: &parser.SeqQLQuery{}, Input: s, DocField: query.DocsDataCol})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "doc column:")
	})
}

func build(seqql *parser.SeqQLQuery, offsetID string, withTotal bool) (*Plan, error) {
	return Build(BuildParams{
		SeqQL:     seqql,
		Input:     query.DocsSchema,
		DocField:  query.DocsDataCol,
		From:      testFrom,
		To:        testTo,
		OffsetID:  offsetID,
		WithTotal: withTotal,
	})
}
