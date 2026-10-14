package query

import (
	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/seq"
)

type Column[T any] struct {
	idx      int
	dataType DataType
	get      func(*RecordVals) T
}

func (c Column[T]) Idx() int {
	return c.idx
}

func (c Column[T]) DataType() DataType {
	return c.dataType
}

// Val returns value of r's column at c.idx index
func (c Column[T]) Val(r *Record) T {
	return c.get(r.Vals[c.idx])
}

// RawData returns raw data of r's column at c.idx index
func (c Column[T]) RawData(r *Record) []byte {
	return r.Vals[c.idx].RawData()
}

func SeqIDColumn(idx int) Column[seq.ID] {
	return Column[seq.ID]{
		idx:      idx,
		dataType: DataTypeSeqID,
		get:      (*RecordVals).AsSeqID,
	}
}

func BytesColumn(idx int) Column[[]byte] {
	return Column[[]byte]{
		idx:      idx,
		dataType: DataTypeBytes,
		get:      (*RecordVals).AsBytes,
	}
}

func StringColumn(idx int) Column[string] {
	return Column[string]{
		idx:      idx,
		dataType: DataTypeString,
		get:      (*RecordVals).AsString,
	}
}

func DocColumn(idx int) Column[*insaneJSON.Root] {
	return Column[*insaneJSON.Root]{
		idx:      idx,
		dataType: DataTypeDocument,
		get:      (*RecordVals).AsDoc,
	}
}

func Float64Column(idx int) Column[float64] {
	return Column[float64]{
		idx:      idx,
		dataType: DataTypeFloat64,
		get:      (*RecordVals).AsFloat64,
	}
}

func Uint64Column(idx int) Column[uint64] {
	return Column[uint64]{
		idx:      idx,
		dataType: DataTypeUint64,
		get:      (*RecordVals).AsUint64,
	}
}

func Int64Column(idx int) Column[int64] {
	return Column[int64]{
		idx:      idx,
		dataType: DataTypeInt64,
		get:      (*RecordVals).AsInt64,
	}
}

func Uint32Column(idx int) Column[uint32] {
	return Column[uint32]{
		idx:      idx,
		dataType: DataTypeUint32,
		get:      (*RecordVals).AsUint32,
	}
}

func Int32Column(idx int) Column[int32] {
	return Column[int32]{
		idx:      idx,
		dataType: DataTypeInt32,
		get:      (*RecordVals).AsInt32,
	}
}

func Float64ArrayColumn(idx int) Column[[]float64] {
	return Column[[]float64]{
		idx:      idx,
		dataType: DataTypeFloat64Array,
		get:      (*RecordVals).AsFloat64Array,
	}
}

func StringArrayColumn(idx int) Column[[]string] {
	return Column[[]string]{
		idx:      idx,
		dataType: DataTypeStringArray,
		get:      (*RecordVals).AsStringArray,
	}
}
