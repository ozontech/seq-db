package query

import (
	"fmt"

	"github.com/ozontech/seq-db/query/encoding"
	"github.com/ozontech/seq-db/seq"
)

type Record struct {
	Vals []*RecordVals
}

func NewRecord(vals []*RecordVals) *Record {
	return &Record{
		Vals: vals,
	}
}

type DataType byte

func (t DataType) String() string {
	switch t {
	case DataTypeBytes:
		return "bytes"
	case DataTypeSeqID:
		return "seq_id"
	case DataTypeJSON:
		return "json"
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
	DataTypeJSON
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

	// decoded marks whether the f64s/strs cache has been filled.
	// Exactly one cache field is used per val so a single flag covers all types.
	decoded bool

	// for lazy decoding
	rawData []byte // raw data

	// Scalars are decoded from rawData on every access — the decode is a
	// single load, caching it in a struct field costs more (bigger struct,
	// extra branch) than it saves.
	f64s []float64
	strs []string
}

func NewRecordVals(dataType DataType, rawData []byte) *RecordVals {
	return &RecordVals{
		Type:    dataType,
		rawData: rawData,
	}
}

// Typed constructors below encode the passed value into rawData themselves;
// prefer them over NewRecordVals when a typed value is at hand.

func NewBytesVals(data []byte) *RecordVals {
	return &RecordVals{Type: DataTypeBytes, rawData: data}
}

// NewStringVals aliases s — no copy, so s must outlive the val.
func NewStringVals(s string) *RecordVals {
	return &RecordVals{Type: DataTypeString, rawData: encoding.StringToBytes(s)}
}

func NewSeqIDVals(id seq.ID) *RecordVals {
	return &RecordVals{Type: DataTypeSeqID, rawData: encoding.SeqIDToBytes(id)}
}

func NewUint32Vals(v uint32) *RecordVals {
	return &RecordVals{Type: DataTypeUint32, rawData: encoding.Uint32ToBytes(v)}
}

func NewUint64Vals(v uint64) *RecordVals {
	return &RecordVals{Type: DataTypeUint64, rawData: encoding.Uint64ToBytes(v)}
}

func NewInt32Vals(v int32) *RecordVals {
	return &RecordVals{Type: DataTypeInt32, rawData: encoding.Int32ToBytes(v)}
}

func NewInt64Vals(v int64) *RecordVals {
	return &RecordVals{Type: DataTypeInt64, rawData: encoding.Int64ToBytes(v)}
}

func NewFloat64Vals(v float64) *RecordVals {
	return &RecordVals{Type: DataTypeFloat64, rawData: encoding.Float64ToBytes(v)}
}

func NewFloat64ArrayVals(v []float64) *RecordVals {
	return &RecordVals{Type: DataTypeFloat64Array, rawData: encoding.Float64ArrayToBytes(v)}
}

func NewStringArrayVals(v []string) *RecordVals {
	return &RecordVals{Type: DataTypeStringArray, rawData: encoding.StringArrayToBytes(v)}
}

// NewJSONVals aliases payload — no copy. The payload must stay unmodified for
// as long as the val is alive — pooled or reused buffers are unsafe here.
func NewJSONVals(payload []byte) *RecordVals {
	return &RecordVals{Type: DataTypeJSON, rawData: payload}
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
	return encoding.SeqIDFromBytes(rv.rawData)
}

// AsJSON returns the raw json bytes without decoding.
func (rv *RecordVals) AsJSON() []byte {
	rv.mustBe(DataTypeJSON)
	return rv.rawData
}

func (rv *RecordVals) AsString() string {
	rv.mustBe(DataTypeString)
	return encoding.StringFromBytes(rv.rawData)
}

func (rv *RecordVals) AsUint32() uint32 {
	rv.mustBe(DataTypeUint32)
	return encoding.Uint32FromBytes(rv.rawData)
}

func (rv *RecordVals) AsUint64() uint64 {
	rv.mustBe(DataTypeUint64)
	return encoding.Uint64FromBytes(rv.rawData)
}

func (rv *RecordVals) AsInt32() int32 {
	rv.mustBe(DataTypeInt32)
	return encoding.Int32FromBytes(rv.rawData)
}

func (rv *RecordVals) AsInt64() int64 {
	rv.mustBe(DataTypeInt64)
	return encoding.Int64FromBytes(rv.rawData)
}

func (rv *RecordVals) AsFloat64() float64 {
	rv.mustBe(DataTypeFloat64)
	return encoding.Float64FromBytes(rv.rawData)
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
