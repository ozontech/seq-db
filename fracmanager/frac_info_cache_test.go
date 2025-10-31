package fracmanager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/seq"
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

func rotateAndSeal(fm *FracManager) frac.Fraction {
	active := fm.rotate()
	fm.seal(active)
	return active.ref.instance
}

func TestFracInfoSavedToCache(t *testing.T) {
	const maxSize = 10000

	_, fm, tearDown := setupFracManager(t, &Config{
		FracSize:  100,
		TotalSize: maxSize * 2,
	})
	defer tearDown()

	dp := frac.NewDocProvider()
	metaRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(metaRoot)

	infos := map[string]*common.Info{}
	totalSize := uint64(0)
	cnt := 1
	for totalSize < maxSize {
		addDummyDoc(t, fm, dp, seq.SimpleID(cnt))
		cnt++
		fracInstance := rotateAndSeal(fm)
		totalSize += fracInstance.Info().FullSize()
		info := fracInstance.Info()
		infos[info.Name()] = info
		dp.TryReset()
	}

	assert.NoError(t, fm.fracCache.SyncWithDisk())

	fracCacheFromDisk, err := loadInfoCache(fm.fracCache.fullPath)
	assert.NoError(t, err)
	assert.Equal(t, fracCacheFromDisk, fm.fracCache.cache)
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
	const times = 10
	const maxSize = 5500

	q := newEvictingQueue(maxSize)

	_, fm, tearDown := setupFracManager(t, &Config{
		FracSize:  100,
		TotalSize: maxSize,
	})

	dp := frac.NewDocProvider()
	infos := map[string]*common.Info{}

	for i := 1; i < times+1; i++ {
		addDummyDoc(t, fm, dp, seq.SimpleID(i))
		fracInstance := rotateAndSeal(fm)
		info := fracInstance.Info()
		q.Add(item{
			value: info.Name(),
			size:  int(fracInstance.Info().FullSize()),
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
	tearDown()

	fracsOnDisk := []string{}
	fracCacheFromDisk, err := loadInfoCache(fm.fracCache.fullPath)

	assert.NoError(t, err)
	for k := range fracCacheFromDisk {
		fracsOnDisk = append(fracsOnDisk, k)
	}

	sort.Strings(expectedFracs)
	sort.Strings(fracsOnDisk)

	assert.Equal(t, expectedFracs, fracsOnDisk)
}

func TestMissingCacheFilesDeleted(t *testing.T) {
	const times = 10
	const maxSize = 5500

	cfg, fm, tearDown := setupFracManager(t, &Config{
		FracSize:  100,
		TotalSize: maxSize,
	})

	dp := frac.NewDocProvider()
	metaRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(metaRoot)

	for i := 1; i < times+1; i++ {
		addDummyDoc(t, fm, dp, seq.SimpleID(i))
		rotateAndSeal(fm)
		dp.TryReset()
	}

	// make sure the disk is in sync with the in-memory fraction cache
	sealWG := sync.WaitGroup{}
	suicideWG := sync.WaitGroup{}
	fm.maintenance(&sealWG, &suicideWG) // shrinkSizes should be called
	sealWG.Wait()
	suicideWG.Wait()

	tearDown()

	// remove the fraction files
	files := []string{}
	files = appendGlob(files, cfg.DataDir, "*.docs")
	files = appendGlob(files, cfg.DataDir, "*.sdocs")
	files = appendGlob(files, cfg.DataDir, "*.index")
	files = appendGlob(files, cfg.DataDir, "*.meta")
	for _, file := range files {
		err := os.RemoveAll(file)
		assert.NoError(t, err)
	}

	// create a new fracmanager that will read the fraction cache file
	// fm2, err := New(t.Context(), cfg, nil)
	// assert.NoError(t, err)
	cfg, fm2, tearDown := setupFracManager(t, cfg)
	defer tearDown()

	sealWG2 := sync.WaitGroup{}
	suicideWG2 := sync.WaitGroup{}
	fm2.maintenance(&sealWG2, &suicideWG2) // shrinkSizes should be called
	sealWG2.Wait()
	suicideWG2.Wait()

	// make sure the missing files are removed from the fraction cache
	fracCacheFromDisk, err := os.ReadFile(fm.fracCache.fullPath)
	assert.NoError(t, err)
	assert.Equal(t, fracCacheFromDisk, []byte("{}"))
}
