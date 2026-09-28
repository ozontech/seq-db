package query

import (
	"fmt"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query/encoding"
	"github.com/ozontech/seq-db/seq"
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

func (r *Record) Release() {
	for _, v := range r.Vals {
		v.Release()
	}
}

type DataType byte

func (t DataType) String() string {
	switch t {
	case DataTypeBytes:
		return "bytes"
	case DataTypeSeqID:
		return "seq_id"
	case DataTypeDocument:
		return "document"
	case DataTypeString:
		return "string"
	case DataTypeUint32:
		return "uint32"
	case DataTypeUint64:
		return "uint64"
	case DataTypeInt32:
		return "int32"
	case DataTypeInt64:
		return "int64"
	case DataTypeFloat64:
		return "float64"
	case DataTypeFloat64Array:
		return "float64_array"
	case DataTypeStringArray:
		return "string_array"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

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

type RecordVals struct {
	Type DataType

	// decoded marks whether the cache below has been filled.
	// Exactly one cache field is used per val so a single flag covers all types.
	decoded bool

	// for lazy decoding
	rawData []byte // raw data

	doc   *insaneJSON.Root
	str   string
	f64s  []float64
	strs  []string
	seqID seq.ID
	u64   uint64
	i64   int64
	f64   float64
	u32   uint32
	i32   int32
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

func (rv *RecordVals) mustBe(t DataType) {
	if rv.Type != t {
		panic(fmt.Sprintf("BUG: type mismatch - RecordVals has %s, requested %s", rv.Type, t))
	}
}

func (rv *RecordVals) AsBytes() []byte {
	rv.mustBe(DataTypeBytes)
	return rv.rawData
}

func (rv *RecordVals) AsSeqID() seq.ID {
	rv.mustBe(DataTypeSeqID)
	if !rv.decoded {
		rv.seqID = encoding.SeqIDFromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.seqID
}

func (rv *RecordVals) AsDoc() *insaneJSON.Root {
	rv.mustBe(DataTypeDocument)
	if !rv.decoded {
		root := insaneJSON.Spawn()
		err := root.DecodeBytes(rv.rawData)
		if err != nil {
			panic(fmt.Errorf("error decoding document: %w", err))
		}
		if !root.IsObject() {
			panic(fmt.Errorf("document is not an object: %s", rv.rawData))
		}
		rv.doc = root
		rv.decoded = true
	}
	return rv.doc
}

func (rv *RecordVals) AsString() string {
	rv.mustBe(DataTypeString)
	if !rv.decoded {
		rv.str = encoding.StringFromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.str
}

func (rv *RecordVals) AsUint32() uint32 {
	rv.mustBe(DataTypeUint32)
	if !rv.decoded {
		rv.u32 = encoding.Uint32FromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.u32
}

func (rv *RecordVals) AsUint64() uint64 {
	rv.mustBe(DataTypeUint64)
	if !rv.decoded {
		rv.u64 = encoding.Uint64FromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.u64
}

func (rv *RecordVals) AsInt32() int32 {
	rv.mustBe(DataTypeInt32)
	if !rv.decoded {
		rv.i32 = encoding.Int32FromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.i32
}

func (rv *RecordVals) AsInt64() int64 {
	rv.mustBe(DataTypeInt64)
	if !rv.decoded {
		rv.i64 = encoding.Int64FromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.i64
}

func (rv *RecordVals) AsFloat64() float64 {
	rv.mustBe(DataTypeFloat64)
	if !rv.decoded {
		rv.f64 = encoding.Float64FromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.f64
}

func (rv *RecordVals) AsFloat64Array() []float64 {
	rv.mustBe(DataTypeFloat64Array)
	if !rv.decoded {
		rv.f64s = encoding.Float64ArrayFromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.f64s
}

func (rv *RecordVals) AsStringArray() []string {
	rv.mustBe(DataTypeStringArray)
	if !rv.decoded {
		rv.strs = encoding.StringArrayFromBytes(rv.rawData)
		rv.decoded = true
	}
	return rv.strs
}

// Release returns the insaneJSON root allocated in AsDoc back to the library's
// internal pool. It is idempotent: after the first call rv.doc is cleared, so
// repeated calls are a no-op. Calling it on a non-document val or a
// not-yet-decoded val is also a no-op. Safe to invoke from every executor that
// has touched the val — the first caller wins.
func (rv *RecordVals) Release() {
	if rv.doc != nil {
		insaneJSON.Release(rv.doc)
		rv.doc = nil
	}
}
