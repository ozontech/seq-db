package fracmanager

import (
	"bufio"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	tests_common "github.com/ozontech/seq-db/tests/common"
)

func fillActiveFraction(active *frac.Active) error {
	const muliplier = 10

	docRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(docRoot)

	file, err := os.Open(filepath.Join(tests_common.TestDataDir, "k8s.logs"))
	if err != nil {
		return err
	}
	defer file.Close()

	k := 0
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
		if err := active.Append(dp.Provide()); err != nil {
			return err
		}
	}

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
		DocBlockSize:           128 * consts.KB,
	}

}

func Benchmark_SealingNoSort(b *testing.B) {
	runSealingBench(b, &frac.Config{SkipSortDocs: true})
}

func Benchmark_SealingWithSort(b *testing.B) {
	runSealingBench(b, &frac.Config{})
}

func runSealingBench(b *testing.B, cfg *frac.Config) {
	cm := NewCacheMaintainer(consts.MB*64, consts.MB*64, nil)
	fp := newFractionProvider(cfg, cm, 1, 1)
	defer fp.Stop()

	dataDir := filepath.Join(b.TempDir(), "BenchmarkSealing")
	tests_common.RecreateDir(dataDir)

	active := fp.NewActive(filepath.Join(dataDir, "test"))
	err := fillActiveFraction(active)
	assert.NoError(b, err)

	active.WaitWriteIdle()

	params := defaultSealingParams()
	_, err = frac.Seal(active, params, true)
	assert.NoError(b, err)

	b.ReportAllocs()

	defer profile.Start(
		profile.CPUProfile,
		profile.ProfilePath("."),
		profile.NoShutdownHook,
	).Stop()

	for b.Loop() {
		_, err = frac.Seal(active, params, true)
		assert.NoError(b, err)
	}
}
