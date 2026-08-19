package query

import (
	"fmt"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query/encoding"
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
	DataTypeSeqID
	DataTypeDocument
	DataTypeString
	DataTypeUint32
	DataTypeUint64
	DataTypeInt32
	DataTypeInt64
	DataTypeFloat64
	DataTypeFloat64Array
	DataTypeStringArray
)

// Executors make use of val's index. the plan knows which executors use which col indexes
type RecordVals struct {
	Type DataType

	// for lazy decoding
	rawData []byte // raw data

	decoded any
}

func NewRecordVals(dataType DataType, rawData []byte) *RecordVals {
	return &RecordVals{
		Type:    dataType,
		rawData: rawData,
	}
}

func (rv *RecordVals) RawData() []byte {
	return rv.rawData
}

func (rv *RecordVals) Decoded() any {
	if rv.decoded == nil {
		rv.ensureDecoded()
	}
	return rv.decoded
}

// Release returns the insaneJSON decoder (allocated for DataTypeDocument in
// ensureDecoded) back to the library's internal pool. It is idempotent: after
// the first call rv.decoded is cleared, so repeated calls are a no-op. Calling
// it on a non-document val or a not-yet-decoded val is also a no-op. Safe to
// invoke from every executor that has touched the val — the first caller wins.
func (rv *RecordVals) Release() {
	if rv.Type != DataTypeDocument {
		return
	}
	if r, ok := rv.decoded.(*insaneJSON.Root); ok && r != nil {
		insaneJSON.Release(r)
		rv.decoded = nil
	}
}

func (r *Record) Release() {
	for _, v := range r.Vals {
		v.Release()
	}
}

func (rv *RecordVals) ensureDecoded() {
	switch rv.Type {
	case DataTypeBytes:
		rv.decoded = rv.rawData
	case DataTypeSeqID:
		rv.decoded = encoding.SeqIDFromBytes(rv.rawData)
	case DataTypeDocument:
		root := insaneJSON.Spawn()
		err := root.DecodeBytes(rv.rawData)
		if err != nil {
			panic(fmt.Errorf("error decoding document: %w", err))
		}
		if !root.IsObject() {
			panic(fmt.Errorf("document is not an object: %s", rv.rawData))
		}
		rv.decoded = root
	case DataTypeString:
		rv.decoded = encoding.StringFromBytes(rv.rawData)
	case DataTypeUint32:
		rv.decoded = encoding.Uint32FromBytes(rv.rawData)
	case DataTypeUint64:
		rv.decoded = encoding.Uint64FromBytes(rv.rawData)
	case DataTypeInt32:
		rv.decoded = encoding.Int32FromBytes(rv.rawData)
	case DataTypeInt64:
		rv.decoded = encoding.Int64FromBytes(rv.rawData)
	case DataTypeFloat64:
		rv.decoded = encoding.Float64FromBytes(rv.rawData)
	case DataTypeFloat64Array:
		rv.decoded = encoding.Float64ArrayFromBytes(rv.rawData)
	case DataTypeStringArray:
		rv.decoded = encoding.StringArrayFromBytes(rv.rawData)
	default:
		panic("BUG: unknown data type")
	}
}
