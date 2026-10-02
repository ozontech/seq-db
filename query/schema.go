package query

import (
	"cmp"
	"fmt"
	"slices"
)

type ColumnDesc struct {
	Name string
	Type DataType
}

// Schema is an immutable ordered list of column descriptors.
type Schema struct {
	cols   []ColumnDesc
	byName map[string]int
}

func NewSchema(cols ...ColumnDesc) (*Schema, error) {
	s := &Schema{
		cols:   cols,
		byName: make(map[string]int, len(cols)),
	}
	for i, c := range cols {
		if _, ok := s.byName[c.Name]; ok {
			return nil, fmt.Errorf("duplicate column name: %s", c.Name)
		}
		s.byName[c.Name] = i
	}
	return s, nil
}

func MustNewSchema(cols ...ColumnDesc) *Schema {
	s, err := NewSchema(cols...)
	if err != nil {
		panic(err)
	}
	return s
}

// Index returns the index of the named column, or error if the column is absent.
func (s *Schema) Index(name string) (int, error) {
	i, ok := s.byName[name]
	if !ok {
		return 0, fmt.Errorf("schema has no column %q", name)
	}
	return i, nil
}

// Column resolves the column by name. It fails when the
// column is absent or its declared type is incompatible with T.
func (s *Schema) Column[T any](name string) (Column[T], error) {
	idx, err := s.Index(name)
	if err != nil {
		return Column[T]{}, err
	}

	t := s.cols[idx].Type
	get, ok := columnGetters[t].(func(*RecordVals) T)
	if !ok {
		return Column[T]{}, fmt.Errorf("column %q has type %s, incompatible with %T", name, t, *new(T))
	}

	// cmp stays nil for types without a total order (document, bytes, arrays)
	cmpFunc, _ := columnCmps[t].(func(T, T) int)

	return Column[T]{idx: idx, dataType: t, get: get, cmp: cmpFunc}, nil
}

// MustColumn resolves the column by name. Panics instead of returning an error.
func (s *Schema) MustColumn[T any](name string) Column[T] {
	c, err := s.Column[T](name)
	if err != nil {
		panic(err)
	}
	return c
}

// Equal reports whether both schemas list identical (name, type) pairs in the same order.
func (s *Schema) Equal(other *Schema) bool {
	if s == nil || other == nil || len(s.cols) != len(other.cols) {
		return false
	}
	for i, c := range s.cols {
		if c != other.cols[i] {
			return false
		}
	}
	return true
}

// Extend returns a new schema with the given columns appended.
// The original schema is untouched. Panics on duplicate names.
func (s *Schema) Extend(cols ...ColumnDesc) *Schema {
	return MustNewSchema(append(slices.Clone(s.cols), cols...)...)
}

// Len returns the number of columns.
func (s *Schema) Len() int {
	return len(s.cols)
}

// Cols returns the column descriptors in schema order.
func (s *Schema) Cols() []ColumnDesc {
	return s.cols
}

var (
	columnGetters = map[DataType]any{
		DataTypeSeqID:        (*RecordVals).AsSeqID,
		DataTypeBytes:        (*RecordVals).AsBytes,
		DataTypeString:       (*RecordVals).AsString,
		DataTypeDocument:     (*RecordVals).AsDoc,
		DataTypeUint32:       (*RecordVals).AsUint32,
		DataTypeUint64:       (*RecordVals).AsUint64,
		DataTypeInt32:        (*RecordVals).AsInt32,
		DataTypeInt64:        (*RecordVals).AsInt64,
		DataTypeFloat64:      (*RecordVals).AsFloat64,
		DataTypeFloat64Array: (*RecordVals).AsFloat64Array,
		DataTypeStringArray:  (*RecordVals).AsStringArray,
	}

	columnCmps = map[DataType]any{
		DataTypeSeqID:   cmpSeqID,
		DataTypeString:  cmp.Compare[string],
		DataTypeUint32:  cmp.Compare[uint32],
		DataTypeUint64:  cmp.Compare[uint64],
		DataTypeInt32:   cmp.Compare[int32],
		DataTypeInt64:   cmp.Compare[int64],
		DataTypeFloat64: cmp.Compare[float64],
	}
)

// We still get hardcoded schemas at the edges of the pipeline.
var (
	DocsIDCol   = "id"
	DocsDataCol = "data"

	DocsSchema = MustNewSchema(
		ColumnDesc{Name: DocsIDCol, Type: DataTypeSeqID},
		ColumnDesc{Name: DocsDataCol, Type: DataTypeDocument},
	)
	AggsSchema = MustNewSchema(
		ColumnDesc{Name: "token", Type: DataTypeString},
		ColumnDesc{Name: "min", Type: DataTypeFloat64},
		ColumnDesc{Name: "max", Type: DataTypeFloat64},
		ColumnDesc{Name: "sum", Type: DataTypeFloat64},
		ColumnDesc{Name: "total", Type: DataTypeUint64},
		ColumnDesc{Name: "not_exists", Type: DataTypeUint64},
		ColumnDesc{Name: "ts", Type: DataTypeUint64},
		ColumnDesc{Name: "samples", Type: DataTypeFloat64Array},
		ColumnDesc{Name: "values", Type: DataTypeStringArray},
	)
	AggResultSchema = MustNewSchema(
		ColumnDesc{Name: "token", Type: DataTypeString},
		ColumnDesc{Name: "value", Type: DataTypeFloat64},
		ColumnDesc{Name: "ts", Type: DataTypeUint64},
		ColumnDesc{Name: "quantiles", Type: DataTypeFloat64Array},
	)
)
