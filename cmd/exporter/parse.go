package main

import (
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

type JSONResp struct {
	Responses []struct {
		Hits struct {
			Hits []struct {
				ID     string         `json:"_id"`
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	} `json:"responses"`
}

// Parse receives response in elastic json format and returns slice of bytes of documents
//   - each document in json format
//   - one document per line ("\n" separated)
//   - adds "SEQ-ID" field
//
// Returns an error if partial response
func parse(data []byte) ([]byte, int, error) {
	resp := JSONResp{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, 0, err
	}

	cnt := 0
	lastID := ""
	res := make([]byte, 0)

	for _, doc := range resp.Responses[0].Hits.Hits {

		if val, ok := doc.Source["seq_db_partial_response"]; ok {
			if partial, ok := val.(bool); ok && partial {
				return nil, 0, errors.New("partial response")
			}
		}

		lastID = doc.ID
		doc.Source["SEQ-ID"] = lastID
		d, err := json.Marshal(doc.Source)
		if err != nil {
			return nil, 0, err
		}
		res = append(res, d...)
		res = append(res, '\n')
		cnt++
	}

	if len(res) > 0 {
		res = res[:len(res)-1] // cut trailing "\n"
	}

	lastIDStr := "none"
	if lastID != "" {
		id, err := seq.FromString(lastID)
		if err != nil {
			return nil, 0, err
		}
		lastIDStr = id.MID.Time().Format(rangeFormat)
	}

	logger.Info("parse response", zap.String("lastID", lastIDStr), zap.Int("docs", cnt))

	return res, cnt, nil
}
