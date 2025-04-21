package fracmanager

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

func fillActiveFraction(active *frac.Active) error {
	const muliplier = 10

	docRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(docRoot)

	dp := frac.NewDocProvider()

	file, err := os.Open(filepath.Join(common.TestDataDir, "k8s.logs"))
	if err != nil {
		return err
	}
	defer file.Close()

	for i := 0; i < muliplier; i++ {
		dp.TryReset()

		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		tokens := seq.Tokens("_all_:", "service:100500", "k8s_pod:"+strconv.Itoa(i))

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			doc := scanner.Bytes()
			err := docRoot.DecodeBytes(doc)
			if err != nil {
				return err
			}
			dp.Append(doc, docRoot, seq.SimpleID(0), tokens)
		}
		docs, metas := dp.Provide()
		if err := active.Append(docs, metas, atomic.NewUint64(0)); err != nil {
			return err
		}
	}

	return nil
}

func getCacheMaintainer() (*CacheMaintainer, func()) {
	done := make(chan struct{})
	cm := NewCacheMaintainer(1024*consts.MB, nil)
	wg := cm.RunCleanLoop(done, time.Second, time.Second)
	return cm, func() {
		close(done)
		wg.Wait()
	}
}

func BenchmarkSealing(b *testing.B) {
	b.ResetTimer()
	b.StopTimer()
	b.ReportAllocs()

	cm, stopFn := getCacheMaintainer()
	defer stopFn()

	dataDir := filepath.Join(b.TempDir(), "BenchmarkSealing")
	common.RecreateDir(dataDir)

	readLimiter := disk.NewReadLimiter(1, metric.StoreBytesRead)

	indexWorkers := frac.NewIndexWorkers(10, 10)

	indexWorkers.Start()
	defer indexWorkers.Stop()

	const minZstdLevel = -5
	defaultSealParams := frac.SealParams{
		IDsZstdLevel:           minZstdLevel,
		LIDsZstdLevel:          minZstdLevel,
		TokenListZstdLevel:     minZstdLevel,
		DocsPositionsZstdLevel: minZstdLevel,
		TokenTableZstdLevel:    minZstdLevel,
		DocBlocksZstdLevel:     minZstdLevel,
		DocBlockSize:           256 * consts.KB,
	}
	dc := cm.CreateADocBlockCache()

	for i := 0; i < b.N; i++ {
		active := frac.NewActive(filepath.Join(dataDir, "test_"+strconv.Itoa(i)), indexWorkers, readLimiter, dc)

		err := fillActiveFraction(active)
		assert.NoError(b, err)

		active.WaitWriteIdle()
		active.GetAllDocuments() // emulate search-pre-sorted LIDs

		b.StartTimer()
		_, err = active.Seal(defaultSealParams)
		assert.NoError(b, err)

		b.StopTimer()
	}
}
