package fracmanager

import (
	"bufio"
	"flag"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

var (
	cpuProfileFlag = flag.Bool("custom.cpuprofile", false, "Enable CPU profiling")
	memProfileFlag = flag.Bool("custom.memprofile", false, "Enable Mem profiling")
)

func TestMain(m *testing.M) {
	flag.Parse()
	m.Run()
}

func fillActiveFraction(active *frac.Active) error {
	const muliplier = 10

	docRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(docRoot)

	file, err := os.Open(filepath.Join(common.TestDataDir, "k8s.logs"))
	if err != nil {
		return err
	}
	defer file.Close()

	k := 0
	wg := sync.WaitGroup{}
	dp := frac.NewDocProvider()
	for i := 0; i < muliplier; i++ {
		dp.TryReset()

		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			k++
			doc := scanner.Bytes()
			if err := docRoot.DecodeBytes(doc); err != nil {
				return err
			}

			id := seq.NewID(time.Now(), uint64(rand.Int63()))
			dp.Append(doc, docRoot, id, seq.Tokens(
				"_all_:",
				"service:service"+strconv.Itoa(rand.Intn(200)),
				"k8s_pod1:"+strconv.Itoa(k%100000),
				"k8s_pod2:"+strconv.Itoa(k%1000000),
			))
		}
		docs, metas := dp.Provide()
		wg.Add(1)
		if err := active.Append(docs, metas, &wg); err != nil {
			return err
		}
	}

	wg.Wait()
	return nil
}

func defaultSealingParams() frac.SealParams {
	const minZstdLevel = 1
	return frac.SealParams{
		IDsZstdLevel:           minZstdLevel,
		LIDsZstdLevel:          minZstdLevel,
		TokenListZstdLevel:     minZstdLevel,
		DocsPositionsZstdLevel: minZstdLevel,
		TokenTableZstdLevel:    minZstdLevel,
		DocBlocksZstdLevel:     minZstdLevel,
		DocBlockSize:           128 * int(units.KiB),
	}
}

func Benchmark_SealingNoSort(b *testing.B) {
	runSealingBench(b, &frac.Config{SkipSortDocs: true})
}

func Benchmark_SealingWithSort(b *testing.B) {
	runSealingBench(b, &frac.Config{})
}

func runSealingBench(b *testing.B, cfg *frac.Config) {
	cm := NewCacheMaintainer(uint64(units.MiB)*64, uint64(units.MiB)*64, nil)
	fp := newFractionProvider(cfg, nil, cm, 1, 1)
	defer fp.Stop()

	dataDir := filepath.Join(b.TempDir(), "BenchmarkSealing")
	common.RecreateDir(dataDir)

	active := fp.NewActive(filepath.Join(dataDir, "test"))
	err := fillActiveFraction(active)
	assert.NoError(b, err)

	params := defaultSealingParams()
	// The first sealing will sort all the LIDs, so we take this load out of the measurement range
	_, err = frac.Seal(active, params)
	assert.NoError(b, err)

	b.ReportAllocs()

	if cpuProfileFlag != nil && *cpuProfileFlag {
		defer profile.Start(
			profile.CPUProfile,
			profile.ProfilePath("../."),
			profile.NoShutdownHook,
		).Stop()
	} else if memProfileFlag != nil && *memProfileFlag {
		defer profile.Start(
			profile.MemProfileHeap,
			profile.ProfilePath("../."),
			profile.NoShutdownHook,
		).Stop()
	}

	for b.Loop() {
		_, err = frac.Seal(active, params)
		assert.NoError(b, err)
	}
}
