package stores

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStoresFromString(t *testing.T) {
	// test empty
	stores := NewStoresFromString("", 1)
	assert.Equal(t, [][]string{}, stores.Shards, "Shards is not empty slice")
	assert.Equal(t, []string{}, stores.Vers, "Vers is not empty slice")
	assert.Equal(t, "", stores.String(), "String repr is wrong")

	// test simple
	stores = NewStoresFromString("myhost1,myhost2", 1)
	assert.Equal(t, [][]string{{"myhost1"}, {"myhost2"}}, stores.Shards, "Hosts in shard differ")
	assert.Equal(t,
		"replica set index=0, ver=: myhost1\nreplica set index=1, ver=: myhost2\n",
		stores.String(),
		"String repr is wrong")

	// test replicas 2
	stores = NewStoresFromString("myhost1,myhost2", 2)
	assert.Equal(t, [][]string{{"myhost1", "myhost2"}}, stores.Shards, "Hosts in shard differ")
	assert.Equal(t,
		"replica set index=0, ver=: myhost1,myhost2\n",
		stores.String(),
		"String repr is wrong")
}
