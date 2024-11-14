package seqproxyapi

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
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

// TestStoreStatusValues is StoreStatusValues wrapper that is used to omit methods like MarshalJSON.
type TestStoreStatusValues StoreStatusValues

type formattedTimeStoreStatusValues struct {
	OldestTime json.RawMessage `json:"oldest_time"`
	*TestStoreStatusValues
}

// MarshalJSON overrides oldest_time field with formatted string instead of google.protobuf.Timestamp.
func (s *StoreStatusValues) MarshalJSON() ([]byte, error) {
	storeStatus := &formattedTimeStoreStatusValues{
		TestStoreStatusValues: (*TestStoreStatusValues)(s),
		OldestTime:            marshalTime(s.OldestTime),
	}
	return json.Marshal(storeStatus)
}

func (s *StoreStatusValues) UnmarshalJSON(data []byte) error {
	var storeStatus formattedTimeStoreStatusValues
	err := json.Unmarshal(data, &storeStatus)
	if err != nil {
		return err
	}

	s.OldestTime, err = unmarshalTime(storeStatus.OldestTime)
	if err != nil {
		return err
	}

	return nil
}

// TestStatusResponse is StatusResponse wrapper that is used to omit methods like MarshalJSON.
type TestStatusResponse StatusResponse

type formattedTimeStatusResponse struct {
	OldestStorageTime *json.RawMessage `json:"oldest_storage_time"`
	*TestStatusResponse
}

// MarshalJSON overrides oldest_storage_time field with formatted string instead of google.protobuf.Timestamp.
func (s *StatusResponse) MarshalJSON() ([]byte, error) {
	status := &formattedTimeStatusResponse{
		TestStatusResponse: (*TestStatusResponse)(s),
	}

	if s.OldestStorageTime != nil {
		marshaledTime := marshalTime(s.OldestStorageTime)
		status.OldestStorageTime = &marshaledTime
	}

	return json.Marshal(status)
}

func (s *StatusResponse) UnmarshalJSON(data []byte) error {
	var status formattedTimeStatusResponse
	err := json.Unmarshal(data, &status)
	if err != nil {
		return err
	}

	if status.OldestStorageTime == nil {
		return nil
	}

	s.OldestStorageTime, err = unmarshalTime(*status.OldestStorageTime)
	if err != nil {
		return err
	}

	return nil
}

func marshalTime(ts *timestamppb.Timestamp) json.RawMessage {
	return json.RawMessage(strconv.Quote(ts.AsTime().Format(time.RFC3339Nano)))
}

func unmarshalTime(val json.RawMessage) (*timestamppb.Timestamp, error) {
	parsed, err := time.Parse(time.RFC3339Nano, string(bytes.Trim(val, `"`)))
	if err != nil {
		return nil, err
	}
	return timestamppb.New(parsed), nil
}
