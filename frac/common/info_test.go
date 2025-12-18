package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestInfo_MarshalJSON(t *testing.T) {
	info := &Info{
		Path:         "test-frac",
		Ver:          "2",
		DocsTotal:    100,
		DocsOnDisk:   1000,
		DocsRaw:      2000,
		MetaOnDisk:   500,
		IndexOnDisk:  1500,
		From:         seq.MID(1761812502000000000),
		To:           seq.MID(1761812503000000000),
		CreationTime: 1666193044479,
		SealingTime:  1666193045000,
	}

	jsonBytes, err := json.Marshal(info)
	require.NoError(t, err)

	var jsonMap map[string]interface{}
	err = json.Unmarshal(jsonBytes, &jsonMap)
	require.NoError(t, err)

	fromRaw, ok := jsonMap["from"].(float64)
	require.True(t, ok, "from should be a number")
	assert.Equal(t, float64(1761812502000), fromRaw, "should scale from from millis on marshal")
	toRaw, ok := jsonMap["to"].(float64)
	require.True(t, ok, "to should be a number")
	assert.Equal(t, float64(1761812503000), toRaw, "should scale from to millis on marshal")

	// validate that original fields are not changed while marshaling (safety check)
	assert.Equal(t, seq.MID(1761812502000000000), info.From, "must not change while marshaling")
	assert.Equal(t, seq.MID(1761812503000000000), info.To, "must not change while marshaling")
}

func TestInfo_UnmarshalJSON(t *testing.T) {
	jsonData := `{
		"name": "test-frac",
		"ver": "2",
		"docs_total": 100,
		"docs_on_disk": 1000,
		"docs_raw": 2000,
		"meta_on_disk": 500,
		"index_on_disk": 1500,
		"from": 1761812502000,
		"to": 1761812503000,
		"creation_time": 1666193044479,
		"sealing_time": 1666193045000
	}`

	var info Info
	err := json.Unmarshal([]byte(jsonData), &info)
	require.NoError(t, err)

	assert.Equal(t, seq.MID(1761812502000000000), info.From, "should scale to nanoseconds")
	assert.Equal(t, seq.MID(1761812503000000000), info.To, "should scale to nanoseconds")
	assert.Equal(t, "test-frac", info.Path)
	assert.Equal(t, uint32(100), info.DocsTotal)
}

func TestInfo_MarshalUnmarshal(t *testing.T) {
	original := &Info{
		Path:         "test-frac",
		Ver:          "2",
		DocsTotal:    100,
		DocsOnDisk:   1000,
		DocsRaw:      2000,
		MetaOnDisk:   500,
		IndexOnDisk:  1500,
		From:         seq.MID(1761812502000000000),
		To:           seq.MID(1761812503000000000),
		CreationTime: 1666193044479,
		SealingTime:  1666193045000,
	}

	jsonBytes, err := json.Marshal(original)
	require.NoError(t, err)

	var unmarshaled Info
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)

	assert.EqualExportedValues(t, original, &unmarshaled, "should match after marshal/unmarshal")
}

func TestInfo_MarshalUnmarshalWithNanos(t *testing.T) {
	original := &Info{
		Path:         "test-frac",
		Ver:          "2",
		From:         seq.MID(1761812502000000777),
		To:           seq.MID(1761812503000000777),
		CreationTime: 1666193044479,
		SealingTime:  1666193045000,
	}

	jsonBytes, err := json.Marshal(original)
	require.NoError(t, err)

	var unmarshaled Info
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)

	// we can't represent nanos in millis while saving, so "from" is floored (rounded down) to near millisecond,
	// while "to" is ceiled (rounded up) to near millisecond
	assert.Equal(t, seq.MID(1761812502000000000), unmarshaled.From)
	assert.Equal(t, seq.MID(1761812503001000000), unmarshaled.To)
}
