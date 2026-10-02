package query

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestNewSchemaDuplicateName(t *testing.T) {
	_, err := NewSchema(
		ColumnDesc{Name: "id", Type: DataTypeSeqID},
		ColumnDesc{Name: "id", Type: DataTypeDocument},
	)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), `duplicate column name: id`)
}

func TestColumnAllTypes(t *testing.T) {
	t.Run("seq id", func(t *testing.T) {
		s := MustNewSchema(ColumnDesc{Name: "id", Type: DataTypeSeqID})
		c := s.MustColumn[seq.ID]("id")
		assert.Equal(t, 0, c.Idx())
		assert.Equal(t, DataTypeSeqID, c.DataType())
	})
	t.Run("bytes", func(t *testing.T) {
		s := MustNewSchema(ColumnDesc{Name: "b", Type: DataTypeBytes})
		c := s.MustColumn[[]byte]("b")
		assert.Equal(t, DataTypeBytes, c.DataType())
	})
	t.Run("string", func(t *testing.T) {
		s := MustNewSchema(ColumnDesc{Name: "s", Type: DataTypeString})
		c := s.MustColumn[string]("s")
		assert.Equal(t, DataTypeString, c.DataType())
	})
	t.Run("document", func(t *testing.T) {
		s := MustNewSchema(ColumnDesc{Name: "d", Type: DataTypeDocument})
		c := s.MustColumn[*insaneJSON.Root]("d")
		assert.Equal(t, DataTypeDocument, c.DataType())
	})
	t.Run("numeric", func(t *testing.T) {
		s := MustNewSchema(
			ColumnDesc{Name: "u32", Type: DataTypeUint32},
			ColumnDesc{Name: "u64", Type: DataTypeUint64},
			ColumnDesc{Name: "i32", Type: DataTypeInt32},
			ColumnDesc{Name: "i64", Type: DataTypeInt64},
			ColumnDesc{Name: "f64", Type: DataTypeFloat64},
			ColumnDesc{Name: "f64s", Type: DataTypeFloat64Array},
			ColumnDesc{Name: "strs", Type: DataTypeStringArray},
		)
		assert.Equal(t, DataTypeUint32, s.MustColumn[uint32]("u32").DataType())
		assert.Equal(t, DataTypeUint64, s.MustColumn[uint64]("u64").DataType())
		assert.Equal(t, DataTypeInt32, s.MustColumn[int32]("i32").DataType())
		assert.Equal(t, DataTypeInt64, s.MustColumn[int64]("i64").DataType())
		assert.Equal(t, DataTypeFloat64, s.MustColumn[float64]("f64").DataType())
		assert.Equal(t, DataTypeFloat64Array, s.MustColumn[[]float64]("f64s").DataType())
		assert.Equal(t, DataTypeStringArray, s.MustColumn[[]string]("strs").DataType())
	})
	t.Run("cmp", func(t *testing.T) {
		s := MustNewSchema(
			ColumnDesc{Name: "id", Type: DataTypeSeqID},
			ColumnDesc{Name: "s", Type: DataTypeString},
			ColumnDesc{Name: "f64", Type: DataTypeFloat64},
			ColumnDesc{Name: "f64s", Type: DataTypeFloat64Array},
		)
		require.NotNil(t, s.MustColumn[seq.ID]("id").Cmp())
		require.NotNil(t, s.MustColumn[string]("s").Cmp())
		require.NotNil(t, s.MustColumn[float64]("f64").Cmp())
		// Array columns have no total order: Cmp must be nil, not a panic.
		assert.Nil(t, s.MustColumn[[]float64]("f64s").Cmp())
	})
}

func TestColumnMissing(t *testing.T) {
	s := DocsSchema
	_, err := s.Column[seq.ID]("missing")
	require.Error(t, err)
	assert.Equal(t, err.Error(), `schema has no column "missing"`)
}

func TestColumnTypeMismatch(t *testing.T) {
	_, err := DocsSchema.Column[float64]("id")
	require.Error(t, err)
	assert.Equal(t, err.Error(), `column "id" has type seq_id, incompatible with float64`)

	_, err = DocsSchema.Column[float64]("data")
	require.Error(t, err)
	assert.Equal(t, err.Error(), `column "data" has type document, incompatible with float64`)
}

func TestMustColumnPanics(t *testing.T) {
	assert.Panics(t, func() {
		DocsSchema.MustColumn[seq.ID]("missing")
	})
	assert.Panics(t, func() {
		DocsSchema.MustColumn[float64]("id")
	})
}

func TestSchemaEqual(t *testing.T) {
	base := MustNewSchema(
		ColumnDesc{Name: "id", Type: DataTypeSeqID},
		ColumnDesc{Name: "data", Type: DataTypeDocument},
	)
	same := MustNewSchema(
		ColumnDesc{Name: "id", Type: DataTypeSeqID},
		ColumnDesc{Name: "data", Type: DataTypeDocument},
	)
	reordered := MustNewSchema(
		ColumnDesc{Name: "data", Type: DataTypeDocument},
		ColumnDesc{Name: "id", Type: DataTypeSeqID},
	)
	retyped := MustNewSchema(
		ColumnDesc{Name: "id", Type: DataTypeSeqID},
		ColumnDesc{Name: "data", Type: DataTypeString},
	)

	assert.True(t, base.Equal(same))
	assert.False(t, base.Equal(reordered))
	assert.False(t, base.Equal(retyped))
	assert.False(t, base.Equal(DocsSchema.Extend(ColumnDesc{Name: "x", Type: DataTypeString})))
}

func TestSchemaExtend(t *testing.T) {
	extended := DocsSchema.Extend(
		ColumnDesc{Name: "service", Type: DataTypeString},
		ColumnDesc{Name: "level", Type: DataTypeString},
	)
	assert.Equal(t, 4, extended.Len())
	assert.Equal(t, "service", extended.Cols()[2].Name)
	assert.Equal(t, 2, extended.MustColumn[string]("service").Idx())

	assert.Panics(t, func() {
		DocsSchema.Extend(ColumnDesc{Name: "data", Type: DataTypeString})
	})

	// Extend must not mutate the original.
	assert.Equal(t, 2, DocsSchema.Len())
	_, err := DocsSchema.Column[string]("service")
	assert.Error(t, err)
}
