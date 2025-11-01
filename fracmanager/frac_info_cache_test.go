package fracmanager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
)

const dummyFracFixture = `{"a":{"name":"a","ver":"1.1","docs_total":1,"docs_on_disk":363,"docs_raw":450,"meta_on_disk":0,"index_on_disk":1284,"const_regular_block_size":16384,"const_ids_per_block":4096,"const_lid_block_cap":65536,"from":1666193255114,"to":1666193255114,"creation_time":1666193044479},"b":{"name":"b","ver":"1.2","docs_total":1,"docs_on_disk":363,"docs_raw":450,"meta_on_disk":0,"index_on_disk":1276,"const_regular_block_size":16384,"const_ids_per_block":4096,"const_lid_block_cap":65536,"from":1666193602304,"to":1666193602304,"creation_time":1666193598979}}`

func loadInfoCache(fileName string) (map[string]*common.Info, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	fracCache := make(map[string]*common.Info)
	err = json.Unmarshal(content, &fracCache)
	if err != nil {
		return nil, err
	}

	return fracCache, err
}

func TestEmpty(t *testing.T) {
	filename := filepath.Join(t.TempDir(), consts.FracCacheFileSuffix)

	f := NewFracInfoCache(filename)

	err := f.SyncWithDisk()
	assert.NoError(t, err)
	content, err := os.ReadFile(filename)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{}"), content)

	currentFracInfo, ok := f.Get("a")
	assert.Nil(t, currentFracInfo)
	assert.Equal(t, false, ok)
}

func TestLoadFromDisk(t *testing.T) {
	filename := filepath.Join(t.TempDir(), consts.FracCacheFileSuffix)
	assert.NoError(t, os.WriteFile(filename, []byte(dummyFracFixture), 0o660))

	f := NewFracInfoCache(filename)
	f.LoadFromDisk(filename)

	el, has := f.Get("a")
	assert.True(t, has)
	assert.NotNil(t, el)
	assert.Equal(t, "a", el.Name())
	assert.Equal(t, "1.1", el.Ver)
	assert.Equal(t, uint64(1284), el.IndexOnDisk)

	el, has = f.Get("b")
	assert.True(t, has)
	assert.Equal(t, "b", el.Path)
	assert.Equal(t, "1.2", el.Ver)
	assert.Equal(t, uint64(1276), el.IndexOnDisk)

	el, has = f.Get("c")
	assert.False(t, has)
	assert.Nil(t, el)
}

func TestRemoveFraction(t *testing.T) {
	filename := filepath.Join(t.TempDir(), consts.FracCacheFileSuffix)
	assert.NoError(t, os.WriteFile(filename, []byte(dummyFracFixture), 0o660))

	f := NewFracInfoCache(filename)
	f.LoadFromDisk(filename)

	f.Remove("a")
	f.Remove("b")

	assert.NoError(t, f.SyncWithDisk())

	contents, err := os.ReadFile(filename)
	assert.NoError(t, err)
	assert.Equal(t, contents, []byte("{}"))

	newInfo := common.NewInfo("/data/c", 11, 12)

	f.Add(newInfo)
	assert.NoError(t, f.SyncWithDisk())

	m, err := loadInfoCache(filename)
	assert.NoError(t, err)
	expected := map[string]*common.Info{"c": newInfo}

	assert.Equal(t, expected, m)
	f.Remove("c")
	assert.NoError(t, f.SyncWithDisk())

	contents, err = os.ReadFile(filename)
	assert.NoError(t, err)
	assert.Equal(t, contents, []byte("{}"))
}

func TestWriteToDisk(t *testing.T) {
	filename := filepath.Join(t.TempDir(), consts.FracCacheFileSuffix)
	assert.NoError(t, os.WriteFile(filename, []byte(dummyFracFixture), 0o660))

	f := NewFracInfoCache(filename)
	f.LoadFromDisk(filename)

	infoExpected := common.NewInfo("/data/c", 11, 12)
	f.Add(infoExpected)

	infoActual, has := f.Get(filepath.Base(infoExpected.Path))
	assert.True(t, has)
	assert.NotNil(t, infoActual)
	assert.Equal(t, infoExpected.ConstLIDBlockCap, infoActual.ConstLIDBlockCap)

	assert.NoError(t, f.SyncWithDisk())

	mapFracCache, err := loadInfoCache(filename)
	assert.NoError(t, err)
	assert.Equal(t, mapFracCache["c"], infoExpected)

	fracA, has := f.Get("a")
	assert.True(t, has)
	assert.Equal(t, mapFracCache["a"], fracA)

	fracB, has := f.Get("b")
	assert.True(t, has)
	assert.Equal(t, mapFracCache["b"], fracB)

	f.Remove("a")
	f.Remove("b")
	f.Remove("c")

	assert.NoError(t, f.SyncWithDisk())

	_, has = f.Get("a")
	assert.False(t, has)

	_, has = f.Get("b")
	assert.False(t, has)

	_, has = f.Get("c")
	assert.False(t, has)

	_, has = mapFracCache["blabla"]
	assert.False(t, has)
}

func TestUnusedFractionsCleanup(t *testing.T) {
	filename := filepath.Join(t.TempDir(), consts.FracCacheFileSuffix)
	assert.NoError(t, os.WriteFile(filename, []byte(dummyFracFixture), 0o660))

	diskFracCache := NewFracInfoCacheFromDisk(filename)
	currentFracInfo, has := diskFracCache.Get("a")
	assert.True(t, has)
	expected := map[string]*common.Info{"a": currentFracInfo}

	f := NewFracInfoCache(filename)
	f.Add(currentFracInfo)
	assert.NoError(t, f.SyncWithDisk())
	cache, err := loadInfoCache(filename)
	assert.NoError(t, err)
	assert.Equal(t, expected, cache)

	f.Remove("a")
	assert.NoError(t, f.SyncWithDisk())

	contents, err := os.ReadFile(filename)
	assert.NoError(t, err)
	assert.Equal(t, contents, []byte("{}"))
}
