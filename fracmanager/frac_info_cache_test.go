package fracmanager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/tests/common"
)

const dummyFracFixture = `{"a":{"name":"a","ver":"1.1","docs_total":1,"docs_on_disk":363,"docs_raw":450,"meta_on_disk":0,"index_on_disk":1284,"const_regular_block_size":16384,"const_ids_per_block":4096,"const_lid_block_cap":65536,"from":1666193255114,"to":1666193255114,"creation_time":1666193044479},"b":{"name":"b","ver":"1.2","docs_total":1,"docs_on_disk":363,"docs_raw":450,"meta_on_disk":0,"index_on_disk":1276,"const_regular_block_size":16384,"const_ids_per_block":4096,"const_lid_block_cap":65536,"from":1666193602304,"to":1666193602304,"creation_time":1666193598979}}`

func loadFracCacheContent(dataDir string) ([]byte, error) {
	fileName := filepath.Join(dataDir, consts.FracCacheFileSuffix)
	content, err := os.ReadFile(fileName)
	return content, err
}

func loadFracCache(dataDir string) (map[string]*frac.Info, error) {
	content, err := loadFracCacheContent(dataDir)
	if err != nil {
		return nil, err
	}

	fracCache := make(map[string]*frac.Info)
	err = json.Unmarshal(content, &fracCache)
	if err != nil {
		return nil, err
	}

	return fracCache, err
}

func writeToFracCache(dataDir, fname, data string) error {
	fullPath := filepath.Join(dataDir, fname)
	err := os.WriteFile(fullPath, []byte(data), 0o660)
	return err
}

func TestEmpty(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	f := NewFracInfoCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	err := f.SyncWithDisk()
	assert.NoError(t, err)
	content, err := loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{}"), content)

	currentFracInfo, ok := f.Get("a")
	assert.Nil(t, currentFracInfo)
	assert.Equal(t, false, ok)

	err = f.SyncWithDisk()
	assert.NoError(t, err)
	content, err = loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{}"), content)
}

func TestLoadFromDisk(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	err := writeToFracCache(dataDir, consts.FracCacheFileSuffix, dummyFracFixture)
	assert.NoError(t, err)

	f := NewFracInfoCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	f.LoadFromDisk(filepath.Join(dataDir, consts.FracCacheFileSuffix))

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
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	err := writeToFracCache(dataDir, consts.FracCacheFileSuffix, dummyFracFixture)
	assert.NoError(t, err)

	f := NewFracInfoCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	f.LoadFromDisk(filepath.Join(dataDir, consts.FracCacheFileSuffix))

	f.Remove("a")
	f.Remove("b")

	err = f.SyncWithDisk()
	assert.NoError(t, err)

	contents, err := loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, contents, []byte("{}"))

	newInfo := &frac.Info{
		Path:                  "/data/c",
		Ver:                   "1.3",
		DocsTotal:             0,
		DocsOnDisk:            0,
		DocsRaw:               0,
		MetaOnDisk:            0,
		IndexOnDisk:           0,
		ConstRegularBlockSize: 0,
		ConstIDsPerBlock:      0,
		ConstLIDBlockCap:      100500,
		From:                  0,
		To:                    0,
		CreationTime:          0,
	}
	f.Add(newInfo)
	err = f.SyncWithDisk()
	assert.NoError(t, err)

	m, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	expected := map[string]*frac.Info{"c": newInfo}

	assert.Equal(t, expected, m)
	f.Remove("c")
	err = f.SyncWithDisk()
	assert.NoError(t, err)

	contents, err = loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, contents, []byte("{}"))
}

func TestWriteToDisk(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	err := writeToFracCache(dataDir, consts.FracCacheFileSuffix, dummyFracFixture)
	assert.NoError(t, err)

	f := NewFracInfoCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	f.LoadFromDisk(filepath.Join(dataDir, consts.FracCacheFileSuffix))

	newInfo := &frac.Info{
		Path:                  "/data/c",
		Ver:                   "1.3",
		DocsTotal:             0,
		DocsOnDisk:            0,
		DocsRaw:               0,
		MetaOnDisk:            0,
		IndexOnDisk:           0,
		ConstRegularBlockSize: 0,
		ConstIDsPerBlock:      0,
		ConstLIDBlockCap:      100500,
		From:                  0,
		To:                    0,
		CreationTime:          0,
	}

	f.Add(newInfo)

	fracFromDisk, has := f.Get(filepath.Base(newInfo.Path))
	assert.True(t, has)
	assert.NotNil(t, fracFromDisk)
	assert.Equal(t, newInfo.ConstLIDBlockCap, fracFromDisk.ConstLIDBlockCap)

	err = f.SyncWithDisk()
	assert.NoError(t, err)

	mapFracCache, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, mapFracCache["c"], newInfo)

	fracA, has := f.Get("a")
	assert.True(t, has)
	assert.Equal(t, mapFracCache["a"], fracA)

	fracB, has := f.Get("b")
	assert.True(t, has)
	assert.Equal(t, mapFracCache["b"], fracB)

	f.Remove("a")
	f.Remove("b")
	f.Remove("c")

	err = f.SyncWithDisk()
	assert.NoError(t, err)

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
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	err := writeToFracCache(dataDir, consts.FracCacheFileSuffix, dummyFracFixture)
	assert.NoError(t, err)

	expected := map[string]*frac.Info{}

	cacheFile := filepath.Join(dataDir, consts.FracCacheFileSuffix)
	diskFracCache := NewFracInfoCacheFromDisk(cacheFile)
	f := NewFracInfoCache(cacheFile)

	currentFracInfo, has := diskFracCache.Get("a")
	assert.True(t, has)
	expected["a"] = currentFracInfo
	f.Add(currentFracInfo)
	err = f.SyncWithDisk()
	assert.NoError(t, err)
	cache, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, expected, cache)

	f.Remove("a")
	err = f.SyncWithDisk()
	assert.NoError(t, err)
	cacheStr, err := loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{}"), cacheStr)
}
