package plan

import (
	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/exec"
)

// Op is one operation wrapping the record stream.
// Each op has its own parameters, resolves its input columns from the
// schema of the pipeline built so far, and knows its output schema.
type Op interface {
	// Apply wraps the input producer and returns it with the op's output schema.
	// Ops that do not change the record shape return the input schema unchanged.
	Apply(input query.RecordProducer, in *query.Schema) (query.RecordProducer, *query.Schema)
}

type ExtractOp struct {
	Fields []string
	// OpField is the name of the column to perform operation on.
	OpField string
}

func (op *ExtractOp) Apply(input query.RecordProducer, in *query.Schema) (query.RecordProducer, *query.Schema) {
	descs := make([]query.ColumnDesc, len(op.Fields))
	for i, f := range op.Fields {
		descs[i] = query.ColumnDesc{Name: f, Type: query.DataTypeString}
	}

	out := in.Extend(descs...)
	docCol := in.MustColumn[*insaneJSON.Root](op.OpField)
	return exec.NewDocFieldsExtractor(input, docCol, op.Fields...), out
}

type FilterOp struct {
	Cond      parser.FilterCondition
	WithTotal bool
}

func (op *FilterOp) Apply(input query.RecordProducer, in *query.Schema) (query.RecordProducer, *query.Schema) {
	col := in.MustColumn[string](op.Cond.Field)
	return exec.NewFilter(input, col, exec.NewEq(op.Cond.Value), op.WithTotal), in
}

type ProjectOp struct {
	Fields    []string
	AllowList bool
	// OpField is the name of the column to perform operation on.
	OpField string
}

func (op *ProjectOp) Apply(input query.RecordProducer, in *query.Schema) (query.RecordProducer, *query.Schema) {
	docCol := in.MustColumn[*insaneJSON.Root](op.OpField)
	return exec.NewDocProjector(input, docCol, &exec.FieldsFilter{
		Fields:    op.Fields,
		AllowList: op.AllowList,
	}), in
}

type LimitOp struct {
	Limit  int
	Offset int
}

func (op *LimitOp) Apply(input query.RecordProducer, in *query.Schema) (query.RecordProducer, *query.Schema) {
	// set limit=limit+offset and offset=0 to merge stores' results correctly on proxy
	return exec.NewLimiter(input, uint32(op.Limit+op.Offset), 0), in
}
