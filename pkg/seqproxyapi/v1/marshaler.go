package seqproxyapi

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	pbMarshaller  = protojson.MarshalOptions{EmitDefaultValues: true}
	pbUnmarshaler = protojson.UnmarshalOptions{}
)

// TestDoc is Document wrapper that is used to omit methods like MarshalJSON.
type TestDoc Document

type stringDocument struct {
	*TestDoc
	Data json.RawMessage `json:"data"`
	Time json.RawMessage `json:"time"`
}

// MarshalJSON replaces struct { "data" []byte } with struct { "data" json.RawMessage }.
func (d *Document) MarshalJSON() ([]byte, error) {
	return json.Marshal(&stringDocument{TestDoc: (*TestDoc)(d), Data: d.Data, Time: marshalTime(d.Time)})
}

// UnmarshalJSON replaces struct { "data" json.RawMessage } with struct { "data" []byte }.
func (d *Document) UnmarshalJSON(data []byte) error {
	strDoc := &stringDocument{TestDoc: (*TestDoc)(d)}

	if err := json.Unmarshal(data, strDoc); err != nil {
		return err
	}
	d.Data = strDoc.Data

	t, err := unmarshalTime(strDoc.Time)
	if err != nil {
		return err
	}
	d.Time = t

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

// TestExplainEntry is ExplainEntry wrapper that is used to omit methods like MarshalJSON.
type TestExplainEntry ExplainEntry

type formattedExplainEntry struct {
	Duration json.RawMessage `json:"duration"`
	*TestExplainEntry
}

// MarshalJSON overrides duration field with formatted string instead of google.protobuf.Duration.
func (e *ExplainEntry) MarshalJSON() ([]byte, error) {
	ee := &formattedExplainEntry{
		TestExplainEntry: (*TestExplainEntry)(e),
		Duration:         json.RawMessage(strconv.Quote(e.Duration.AsDuration().String())),
	}
	return json.Marshal(ee)
}

func (e *ExplainEntry) UnmarshalJSON(data []byte) error {
	var ee formattedExplainEntry
	err := json.Unmarshal(data, &ee)
	if err != nil {
		return err
	}

	duration, err := time.ParseDuration(string(bytes.Trim(ee.Duration, `"`)))
	if err != nil {
		return err
	}

	e.Duration = durationpb.New(duration)

	return nil
}

func (e *Error) MarshalJSON() ([]byte, error) {
	b, err := pbMarshaller.Marshal(e)
	return b, err
}

func (e *Error) UnmarshalJSON(data []byte) error {
	return pbUnmarshaler.Unmarshal(data, e)
}

func (r *StartAsyncSearchRequest) MarshalJSON() ([]byte, error) {
	return pbMarshaller.Marshal(r)
}

// TestFetchAsyncSearchResultResponse is FetchAsyncSearchResultResponse wrapper that is used to omit methods like MarshalJSON.
// Need this marshaler to not conflict with Document's custom marshaler
type TestFetchAsyncSearchResultResponse FetchAsyncSearchResultResponse

type formattedFetchAsyncSearchResultResponse struct {
	Status json.RawMessage `json:"status"`
	*TestFetchAsyncSearchResultResponse
	DiskUsage  json.RawMessage  `json:"disk_usage"`
	StartedAt  json.RawMessage  `json:"started_at"`
	ExpiresAt  json.RawMessage  `json:"expires_at"`
	CanceledAt *json.RawMessage `json:"canceled_at,omitempty"`
}

// MarshalJSON overrides timestamp fields and other fields with custom formatting for FetchAsyncSearchResultResponse.
func (r *FetchAsyncSearchResultResponse) MarshalJSON() ([]byte, error) {
	fetchResponse := &formattedFetchAsyncSearchResultResponse{
		Status:                             json.RawMessage(strconv.Quote(r.Status.String())),
		TestFetchAsyncSearchResultResponse: (*TestFetchAsyncSearchResultResponse)(r),
		DiskUsage:                          json.RawMessage(strconv.Quote(strconv.FormatUint(r.DiskUsage, 10))),
		StartedAt:                          marshalTime(r.StartedAt),
		ExpiresAt:                          marshalTime(r.ExpiresAt),
	}

	if r.CanceledAt != nil {
		marshaledTime := marshalTime(r.CanceledAt)
		fetchResponse.CanceledAt = &marshaledTime
	}

	return json.Marshal(fetchResponse)
}

func (i *AsyncSearchesListItem) MarshalJSON() ([]byte, error) {
	return pbMarshaller.Marshal(i)
}
