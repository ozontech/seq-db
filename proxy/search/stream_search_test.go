package search

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

func TestSchemaFromTyping(t *testing.T) {
	typing := []*storeapi.Typing{
		{Title: "id", Type: storeapi.DataType_SEQ_ID},
		{Title: "data", Type: storeapi.DataType_RAW_DOCUMENT},
	}

	schema, types, err := schemaFromTyping(typing)
	require.NoError(t, err)
	assert.Equal(t, 2, schema.Len())
	assert.Equal(t, []query.DataType{query.DataTypeSeqID, query.DataTypeDocument}, types)
	assert.Equal(t, 0, schema.MustColumn[seq.ID]("id").Idx())
	assert.Equal(t, query.DataTypeDocument, schema.MustColumn[*insaneJSON.Root]("data").DataType())
}

func TestSchemaFromTypingDuplicateName(t *testing.T) {
	typing := []*storeapi.Typing{
		{Title: "id", Type: storeapi.DataType_SEQ_ID},
		{Title: "id", Type: storeapi.DataType_SEQ_ID},
	}
	_, _, err := schemaFromTyping(typing)
	assert.Error(t, err)
}

func TestValidateShardSchemas(t *testing.T) {
	newIterator := func(typing []*storeapi.Typing) *StreamSearchIterator {
		schema, types, err := schemaFromTyping(typing)
		require.NoError(t, err)
		return &StreamSearchIterator{schema: schema, types: types}
	}

	docsTyping := []*storeapi.Typing{
		{Title: "id", Type: storeapi.DataType_SEQ_ID},
		{Title: "data", Type: storeapi.DataType_RAW_DOCUMENT},
	}
	otherTyping := []*storeapi.Typing{
		{Title: "id", Type: storeapi.DataType_SEQ_ID},
		{Title: "payload", Type: storeapi.DataType_RAW_DOCUMENT},
	}
	aggsTyping := []*storeapi.Typing{
		{Title: "token", Type: storeapi.DataType_STRING},
	}

	t.Run("no shards", func(t *testing.T) {
		assert.NoError(t, validateShardSchemas(nil, nil))
	})

	t.Run("matching", func(t *testing.T) {
		streams := []*StreamSearchIterator{newIterator(docsTyping), newIterator(docsTyping)}
		assert.NoError(t, validateShardSchemas(streams, nil))
	})

	t.Run("mismatch", func(t *testing.T) {
		streams := []*StreamSearchIterator{newIterator(docsTyping), newIterator(otherTyping)}
		err := validateShardSchemas(streams, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "shard schemas mismatch")
	})

	t.Run("expected schema mismatch", func(t *testing.T) {
		streams := []*StreamSearchIterator{newIterator(docsTyping), newIterator(docsTyping)}
		err := validateShardSchemas(streams, query.AggsSchema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "shard schema mismatch")
	})

	t.Run("expected schema match", func(t *testing.T) {
		streams := []*StreamSearchIterator{newIterator(aggsTyping), newIterator(aggsTyping)}
		expected, _, err := schemaFromTyping(aggsTyping)
		require.NoError(t, err)
		assert.NoError(t, validateShardSchemas(streams, expected))
	})
}
