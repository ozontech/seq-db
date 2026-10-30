package seqproxyapi

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ozontech/seq-db/query/encoding"
)

func NewStreamSearchRewriter() func(ctx context.Context, resp proto.Message) (any, error) {
	var (
		mu      sync.RWMutex
		schemas = make(map[context.Context]*rowSchema) // key per stream
	)

	return func(ctx context.Context, resp proto.Message) (any, error) {
		res, ok := resp.(*StreamSearchResponse)
		if !ok {
			return resp, nil
		}

		switch {
		case res.GetHeader() != nil:
			mu.Lock()
			schemas[ctx] = newRowSchema(res.GetHeader().GetTyping())
			for c := range schemas {
				// clean up streams aborted mid way
				if c.Err() != nil {
					delete(schemas, c)
				}
			}
			mu.Unlock()
		case res.GetData() != nil:
			mu.RLock()
			schema := schemas[ctx]
			mu.RUnlock()
			if schema != nil {
				return &renderedData{schema: schema, batch: res.GetData().GetBatch()}, nil
			}
		case res.GetSummary() != nil:
			// delete the entry for completed stream
			mu.Lock()
			delete(schemas, ctx)
			mu.Unlock()
		}

		return res, nil
	}
}

type renderedData struct {
	schema *rowSchema
	batch  *RecordsBatch
}

func (d *renderedData) MarshalJSON() ([]byte, error) {
	records := make([]json.RawMessage, len(d.batch.GetRecords()))
	for i, rec := range d.batch.GetRecords() {
		raw, err := d.schema.renderRecord(rec.GetRawData())
		if err != nil {
			// schema mismatch, return raw base64 instead of failing the whole stream.
			if raw, err = json.Marshal(rec.GetRawData()); err != nil {
				return nil, err
			}
		}
		records[i] = raw
	}
	return json.Marshal(renderedDataJSON{
		Data: renderedDataBodyJSON{
			Batch: renderedBatchJSON{Records: records},
		},
	})
}

type renderedDataJSON struct {
	Data renderedDataBodyJSON `json:"data,omitempty"`
}

type renderedDataBodyJSON struct {
	Batch renderedBatchJSON `json:"batch,omitempty"`
}

type renderedBatchJSON struct {
	Records []json.RawMessage `json:"records,omitempty"`
}

// rowSchema renders raw data of StreamSearch records as human readable values according to the Typing from header.
type rowSchema []*Typing

func newRowSchema(typing []*Typing) *rowSchema {
	s := rowSchema(typing)
	return &s
}

func (s *rowSchema) renderRecord(cells [][]byte) (json.RawMessage, error) {
	row, err := s.render(cells)
	if err != nil {
		return nil, err
	}
	return json.Marshal(row)
}

func (s *rowSchema) render(cells [][]byte) ([]any, error) {
	if len(cells) != len(*s) {
		return nil, fmt.Errorf("record has %d cells, schema declares %d columns", len(cells), len(*s))
	}
	row := make([]any, len(cells))
	for i, cell := range cells {
		row[i] = (*s)[i].renderCell(cell)
	}
	return row, nil
}

// timestampTitles holds uint64 column titles that carry time as nanoseconds since epoch;
// so timestamp rendering is keyed on the column title instead of the type alone.
var timestampTitles = map[string]struct{}{
	"time": {},
	"ts":   {},
}

func (t *Typing) renderCell(cell []byte) any {
	switch t.Type {
	case DataType_DATA_TYPE_SEQ_ID, DataType_DATA_TYPE_STRING:
		return string(cell)
	case DataType_DATA_TYPE_BYTES:
		return cell
	case DataType_DATA_TYPE_RAW_DOCUMENT:
		return json.RawMessage(cell)
	case DataType_DATA_TYPE_UINT32:
		return encoding.Uint32FromBytes(cell)
	case DataType_DATA_TYPE_UINT64:
		if _, ok := timestampTitles[t.Title]; ok {
			return renderTimestamp(cell)
		}
		return encoding.Uint64FromBytes(cell)
	case DataType_DATA_TYPE_INT32:
		return encoding.Int32FromBytes(cell)
	case DataType_DATA_TYPE_INT64:
		return encoding.Int64FromBytes(cell)
	case DataType_DATA_TYPE_FLOAT64:
		return marshalFloat64(encoding.Float64FromBytes(cell))
	case DataType_DATA_TYPE_FLOAT64_ARRAY:
		return encoding.Float64ArrayFromBytes(cell)
	case DataType_DATA_TYPE_STRING_ARRAY:
		return encoding.StringArrayFromBytes(cell)
	default:
		return cell
	}
}

func renderTimestamp(cell []byte) string {
	if len(cell) != 8 {
		return ""
	}
	if ns := encoding.Uint64FromBytes(cell); ns != 0 {
		return time.Unix(0, int64(ns)).UTC().Format(time.RFC3339Nano)
	}
	return ""
}
