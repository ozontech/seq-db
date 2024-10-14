package search

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
)

func TestAggMin(t *testing.T) {
	searchIngestor := NewIngestor(Config{}, map[string]storeapi.StoreApiClient{})
	now, _ := time.Parse(time.RFC3339, "2050-01-01T10:00:00.000Z")

	ids := []seq.ID{
		seq.NewID(now.Add(time.Minute*15), 0),
		seq.NewID(now.Add(time.Minute*14), 0),
		seq.NewID(now.Add(time.Minute*10), 1),
		seq.NewID(now.Add(time.Minute*10), 0),
		seq.NewID(now.Add(time.Minute*5), 0),
		seq.NewID(now.Add(time.Minute*0), 0),
	}

	times, docs, _ := searchIngestor.aggregate(nil, nil, seq.TimeToMID(now.Add(time.Minute*15)), seq.DurationToMID(time.Minute*5), ids)
	assert.Equal(t, 4, len(docs), "wrong bucket count")
	assert.Equal(t, 3, docs[1], "wrong doc count")
	assert.Equal(t, 4, len(times), "wrong bucket count")
}

func TestAggHour(t *testing.T) {
	searchIngestor := NewIngestor(Config{}, map[string]storeapi.StoreApiClient{})
	now, _ := time.Parse(time.RFC3339, "2050-01-01T10:00:00.000Z")

	ids := []seq.ID{
		seq.NewID(now.Add(time.Minute+2), 0),
		seq.NewID(now.Add(time.Minute+1), 0),
		seq.NewID(now.Add(time.Minute-1), 0),
		seq.NewID(now.Add(time.Minute-2), 0),
	}

	times, docs, _ := searchIngestor.aggregate(nil, nil, seq.TimeToMID(now.Add(time.Hour)), seq.DurationToMID(time.Hour), ids)
	assert.Equal(t, 2, len(docs), "wrong bucket count")
	assert.Equal(t, 2, len(times), "wrong bucket count")
}
