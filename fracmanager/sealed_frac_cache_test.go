package fracmanager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
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

	f := NewSealedFracCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	err := f.SyncWithDisk()
	assert.NoError(t, err)
	content, err := loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{}"), content)

	currentFracInfo, ok := f.GetFracInfo("a")
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

	f := NewSealedFracCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	f.LoadFromDisk(filepath.Join(dataDir, consts.FracCacheFileSuffix))

	el, has := f.GetFracInfo("a")
	assert.True(t, has)
	assert.NotNil(t, el)
	assert.Equal(t, "a", el.Name())
	assert.Equal(t, "1.1", el.Ver)
	assert.Equal(t, uint64(1284), el.IndexOnDisk)

	el, has = f.GetFracInfo("b")
	assert.True(t, has)
	assert.Equal(t, "b", el.Path)
	assert.Equal(t, "1.2", el.Ver)
	assert.Equal(t, uint64(1276), el.IndexOnDisk)

	el, has = f.GetFracInfo("c")
	assert.False(t, has)
	assert.Nil(t, el)
}

func TestRemoveFraction(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)
	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	err := writeToFracCache(dataDir, consts.FracCacheFileSuffix, dummyFracFixture)
	assert.NoError(t, err)

	f := NewSealedFracCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
	f.LoadFromDisk(filepath.Join(dataDir, consts.FracCacheFileSuffix))

	f.RemoveFraction("a")
	f.RemoveFraction("b")

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
	f.AddFraction(newInfo.Name(), newInfo)
	err = f.SyncWithDisk()
	assert.NoError(t, err)

	m, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	expected := map[string]*frac.Info{"c": newInfo}

	assert.Equal(t, expected, m)
	f.RemoveFraction("c")
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

	f := NewSealedFracCache(filepath.Join(dataDir, consts.FracCacheFileSuffix))
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

	f.AddFraction(newInfo.Name(), newInfo)

	fracFromDisk, has := f.GetFracInfo(filepath.Base(newInfo.Path))
	assert.True(t, has)
	assert.NotNil(t, fracFromDisk)
	assert.Equal(t, newInfo.ConstLIDBlockCap, fracFromDisk.ConstLIDBlockCap)

	err = f.SyncWithDisk()
	assert.NoError(t, err)

	mapFracCache, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, mapFracCache["c"], newInfo)

	fracA, has := f.GetFracInfo("a")
	assert.True(t, has)
	assert.Equal(t, mapFracCache["a"], fracA)

	fracB, has := f.GetFracInfo("b")
	assert.True(t, has)
	assert.Equal(t, mapFracCache["b"], fracB)

	f.RemoveFraction("a")
	f.RemoveFraction("b")
	f.RemoveFraction("c")

	err = f.SyncWithDisk()
	assert.NoError(t, err)

	_, has = f.GetFracInfo("a")
	assert.False(t, has)

	_, has = f.GetFracInfo("b")
	assert.False(t, has)

	_, has = f.GetFracInfo("c")
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
	diskFracCache := NewFracCacheFromDisk(cacheFile)
	f := NewSealedFracCache(cacheFile)

	currentFracInfo, has := diskFracCache.GetFracInfo("a")
	assert.True(t, has)
	expected["a"] = currentFracInfo
	f.AddFraction(currentFracInfo.Name(), currentFracInfo)
	err = f.SyncWithDisk()
	assert.NoError(t, err)
	cache, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, expected, cache)

	f.RemoveFraction("a")
	err = f.SyncWithDisk()
	assert.NoError(t, err)
	cacheStr, err := loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{}"), cacheStr)
}

func rotateAndSeal(fm *FracManager) frac.Fraction {
	active3 := fm.rotate()
	fm.seal(active3)
	return active3.ref.instance
}

func TestFracInfoSavedToCache(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	const maxSize = 10000

	fm, err := NewFracManagerWithBackgroundStart(&Config{
		FracSize:         100,
		TotalSize:        maxSize * 2,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})
	assert.NoError(t, err)

	dp := frac.NewDocProvider()
	metaRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(metaRoot)

	infos := map[string]*frac.Info{}
	totalSize := uint64(0)
	cnt := 1
	for totalSize < maxSize {
		addDummyDoc(t, fm, dp, seq.SimpleID(cnt))
		cnt++
		fm.GetActiveFrac().WaitWriteIdle()
		fracInstance := rotateAndSeal(fm)
		totalSize += fracInstance.FullSize()
		info := fracInstance.Info()
		infos[info.Name()] = info
		dp.TryReset()
	}

	err = fm.fracCache.SyncWithDisk()
	assert.NoError(t, err)

	fracCacheFromDisk, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, fracCacheFromDisk, fm.fracCache.fracCache)
	assert.Equal(t, fracCacheFromDisk, infos)
}

type item struct {
	value string
	size  int
}

type evictingQueue struct {
	values  []item
	size    int
	maxSize int
}

func newEvictingQueue(maxSize int) evictingQueue {
	return evictingQueue{
		values:  []item{},
		maxSize: maxSize,
		size:    0,
	}
}

func (q *evictingQueue) Add(v item) {
	q.values = append(q.values, v)
	q.size += v.size

	for q.size > q.maxSize {
		q.size -= q.values[0].size
		q.values = q.values[1:]
	}
}

func (q *evictingQueue) GetItems() []item {
	return q.values
}

func appendGlob(files []string, dataDir, glob string) []string {
	docs, _ := filepath.Glob(filepath.Join(dataDir, glob))
	files = append(files, docs...)
	return files
}

func TestExtraFractionsRemoved(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	const maxSize = 5500
	const times = 10

	q := newEvictingQueue(maxSize)

	fm, err := NewFracManagerWithBackgroundStart(&Config{
		FracSize:         100,
		TotalSize:        maxSize,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})

	assert.NoError(t, err)

	dp := frac.NewDocProvider()
	metaRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(metaRoot)

	infos := map[string]*frac.Info{}

	for i := 1; i < times+1; i++ {
		addDummyDoc(t, fm, dp, seq.SimpleID(i))
		fm.GetActiveFrac().WaitWriteIdle()
		fracInstance := rotateAndSeal(fm)
		info := fracInstance.Info()
		q.Add(item{
			value: info.Name(),
			size:  int(fracInstance.FullSize()),
		})
		infos[info.Name()] = info
		dp.TryReset()
	}

	expectedFracs := []string{}
	for _, itemValue := range q.GetItems() {
		expectedFracs = append(expectedFracs, itemValue.value)
	}

	sealWG := sync.WaitGroup{}
	suicideWG := sync.WaitGroup{}
	fm.maintenance(&sealWG, &suicideWG) // shrinkSizes should be called
	sealWG.Wait()
	suicideWG.Wait()

	fracsOnDisk := []string{}
	fracCacheFromDisk, err := loadFracCache(dataDir)
	assert.NoError(t, err)
	for k := range fracCacheFromDisk {
		fracsOnDisk = append(fracsOnDisk, k)
	}

	sort.Strings(expectedFracs)
	sort.Strings(fracsOnDisk)

	assert.Equal(t, expectedFracs, fracsOnDisk)
}

func TestMissingCacheFilesDeleted(t *testing.T) {
	dataDir := common.GetTestTmpDir(t)

	common.RecreateDir(dataDir)
	defer common.RemoveDir(dataDir)

	const maxSize = 5500
	const times = 10
	// make some fractions
	fm, err := NewFracManagerWithBackgroundStart(&Config{
		FracSize:         100,
		TotalSize:        maxSize,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})
	assert.NoError(t, err)

	dp := frac.NewDocProvider()
	metaRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(metaRoot)

	for i := 1; i < times+1; i++ {
		addDummyDoc(t, fm, dp, seq.SimpleID(i))
		fm.GetActiveFrac().WaitWriteIdle()
		rotateAndSeal(fm)
		dp.TryReset()
	}

	// make sure the disk is in sync with the in-memory fraction cache
	sealWG := sync.WaitGroup{}
	suicideWG := sync.WaitGroup{}
	fm.maintenance(&sealWG, &suicideWG) // shrinkSizes should be called
	sealWG.Wait()
	suicideWG.Wait()

	// remove the fraction files
	files := []string{}
	files = appendGlob(files, dataDir, "*.docs")
	files = appendGlob(files, dataDir, "*.index")
	files = appendGlob(files, dataDir, "*.meta")
	for _, file := range files {
		err := os.RemoveAll(file)
		assert.NoError(t, err)
	}

	// create a new fracmanager that will read the fraction cache file
	fm2, err := NewFracManagerWithBackgroundStart(&Config{
		FracSize:         100,
		TotalSize:        maxSize,
		ShouldReplay:     false,
		ShouldRemoveMeta: true,
		DataDir:          dataDir,
	})
	assert.NoError(t, err)

	sealWG2 := sync.WaitGroup{}
	suicideWG2 := sync.WaitGroup{}
	fm2.maintenance(&sealWG2, &suicideWG2) // shrinkSizes should be called
	sealWG2.Wait()
	suicideWG2.Wait()

	// make sure the missing files are removed from the fraction cache
	fracCacheFromDisk, err := loadFracCacheContent(dataDir)
	assert.NoError(t, err)
	assert.Equal(t, fracCacheFromDisk, []byte("{}"))
}
