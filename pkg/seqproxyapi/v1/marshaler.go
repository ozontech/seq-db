package seqproxyapi

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
)

// TestDoc is Document wrapper that is used to omit methods like MarshalJSON.
type TestDoc Document

type stringDocument struct {
	*TestDoc
	Data json.RawMessage `json:"data"`
}

// MarshalJSON replaces struct { "data" []byte } with struct { "data" json.RawMessage }.
func (d *Document) MarshalJSON() ([]byte, error) {
	return json.Marshal(&stringDocument{TestDoc: (*TestDoc)(d), Data: d.Data})
}

// UnmarshalJSON replaces struct { "data" json.RawMessage } with struct { "data" []byte }.
func (d *Document) UnmarshalJSON(data []byte) error {
	strDoc := &stringDocument{TestDoc: (*TestDoc)(d)}
	err := json.Unmarshal(data, strDoc)
	d.Data = strDoc.Data
	return err
}

// TestAggBucket is a type alias to avoid recursion in MarshalJSON.
type TestAggBucket Aggregation_Bucket

type rawValueBucket struct {
	*TestAggBucket
	Value json.RawMessage `json:"value"`
}

// MarshalJSON overrides "value" field to encode math.NaN and math.Inf as string.
func (b *Aggregation_Bucket) MarshalJSON() ([]byte, error) {
	val := json.RawMessage(strconv.FormatFloat(b.Value, 'f', -1, 64))
	// Convert math.NaN and math.Inf to quoted string.
	if math.IsNaN(b.Value) || math.IsInf(b.Value, 0) {
		val = json.RawMessage(strconv.Quote(string(val)))
	}

	bucket := rawValueBucket{
		TestAggBucket: (*TestAggBucket)(b),
		Value:         val,
	}
	return json.Marshal(bucket)
}

func (b *Aggregation_Bucket) UnmarshalJSON(data []byte) error {
	var bucket rawValueBucket
	err := json.Unmarshal(data, &bucket)
	if err != nil {
		return err
	}
	if bucket.TestAggBucket != nil {
		*b = *(*Aggregation_Bucket)(bucket.TestAggBucket)
	}

	bucket.Value = bytes.Trim(bucket.Value, `"`)

	b.Value, err = strconv.ParseFloat(string(bucket.Value), 64)
	if err != nil {
		return err
	}

	return err
}
