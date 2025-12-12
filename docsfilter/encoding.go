package docsfilter

import (
	"encoding/json"

	"github.com/ozontech/seq-db/seq"
)

func marshalIDs(in seq.IDSources) ([]byte, error) {
	// TODO: use LIDs instead of IDs
	// TODO: binary ids format with blocks, delta encoding etc.
	// TODO: use pools
	// TODO: compress file
	type ids struct {
		IDs []string `json:"ids"`
	}
	found := make([]string, 0, len(in))
	for _, id := range in.IDs() {
		found = append(found, id.String())
	}
	toJson := ids{
		IDs: found,
	}
	marshaled, err := json.Marshal(toJson)
	if err != nil {
		return nil, err
	}

	return marshaled, nil
}
