package query

import (
	"encoding/binary"
	"fmt"
)

type RecordType byte

// TODO: do we need it? only AggState?
const (
	RecordTypeDocument RecordType = iota
	RecordTypeAggregation
)

// executors make use of val's index, executor's parameters has colIdx field
type Record struct {
	Vals []*RecordVals
}

func NewRecord(vals []*RecordVals) *Record {
	return &Record{
		Vals: vals,
	}
}

type DataType byte

const (
	DataTypeBytes DataType = iota
	DataTypeDocument
	DataTypeString
	DataTypeUint32
	DataTypeUint64
	DataTypeInt32
	DataTypeInt64
	DataTypeFloat64
	// TODO: the rest of types
)

// TODO: executors make use of val's index. the plan knows which executors use which col indexes
type RecordVals struct {
	Type DataType

	// for lazy decoding
	rawData []byte // raw data

	// TODO: which type:
	// 1 - any + type switch/assert
	// 2 - interface with Eval() method, (like Datum in cockroach)
	decoded any
}

func NewRecordVals(dataType DataType, rawData []byte) *RecordVals {
	return &RecordVals{
		Type:    dataType,
		rawData: rawData,
	}
}

func (rv *RecordVals) Decoded() any {
	if rv.decoded == nil {
		rv.ensureDecoded()
	}
	return rv.decoded
}

func (rv *RecordVals) ensureDecoded() {
	// TODO: all types
	switch rv.Type {
	case DataTypeBytes:
		rv.decoded = rv.rawData
	case DataTypeString:
		rv.decoded = string(rv.rawData)
	case DataTypeUint32:
		rv.decoded = binary.LittleEndian.Uint32(rv.rawData)
	case DataTypeUint64:
		rv.decoded = binary.LittleEndian.Uint64(rv.rawData)
	default:
		panic(fmt.Errorf("unknown data type")) // TODO: ???
	}
}
