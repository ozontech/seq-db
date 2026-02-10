package setup

import (
	"encoding/json"
	"time"
)

// InlineJSON is a string representing valid json
// similar as json.RawMessage, but it's a string
type InlineJSON string

func (j *InlineJSON) MarshalJSON() ([]byte, error) {
	return []byte(*j), nil
}

func (j *InlineJSON) UnmarshalJSON(v []byte) error {
	*j = InlineJSON(v)
	return nil
}

// ExampleDoc is useful for testing and benchmarking
// instead of hardcoding json docs in code,
// you can hardcode struct which will be turned in json
type ExampleDoc struct {
	Service        string     `json:"service,omitempty"`
	Message        string     `json:"message,omitempty"`
	TraceID        string     `json:"traceID,omitempty"`
	Source         string     `json:"source,omitempty"`
	Zone           string     `json:"zone,omitempty"`
	RequiestObject InlineJSON `json:"requestObject,omitempty"`
	Level          int        `json:"level,omitempty"`
	Timestamp      time.Time  `json:"timestamp,omitempty"`
}

func DocsToStrings(docs []ExampleDoc) []string {
	docStr := make([]string, 0, len(docs))
	for i := 0; i < len(docs); i++ {
		b, err := json.Marshal(docs[i])
		if err != nil {
			panic(err)
		}
		docStr = append(docStr, string(b))
	}
	return docStr
}
