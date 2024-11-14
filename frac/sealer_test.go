package frac

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tests/common"
)

func fillActiveFraction(active *Active) error {
	const muliplier = 10

	docRoot := insaneJSON.Spawn()
	defer insaneJSON.Release(docRoot)

	dp := NewDocProvider()

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

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			doc := scanner.Bytes()
			err := docRoot.DecodeBytes(doc)
			if err != nil {
				return err
			}
			dp.Append(doc, docRoot, seq.SimpleID(0), nil)
		}
		docs, metas := dp.Provide()
		if err := active.Append(docs, metas, atomic.NewUint64(0)); err != nil {
			return err
		}
	}

	return nil
}

func BenchmarkSealing(b *testing.B) {
	b.ResetTimer()
	b.StopTimer()
	b.ReportAllocs()

	dataDir := filepath.Join(b.TempDir(), "BenchmarkSealing")
	common.RecreateDir(dataDir)

	dr := disk.NewReader(metric.StoreBytesRead)

	indexWorkers := NewIndexWorkers(10, 10)

	indexWorkers.Start()
	defer indexWorkers.Stop()

	const minZstdLevel = -5
	defaultSealParams := SealParams{
		IDsZstdLevel:           minZstdLevel,
		LIDsZstdLevel:          minZstdLevel,
		TokenListZstdLevel:     minZstdLevel,
		DocsPositionsZstdLevel: minZstdLevel,
		TokenTableZstdLevel:    minZstdLevel,
	}
	for i := 0; i < b.N; i++ {
		active := NewActive(filepath.Join(dataDir, "test_"+strconv.Itoa(i)), true, indexWorkers, dr, nil)
		err := fillActiveFraction(active)
		assert.NoError(b, err)

		active.WaitWriteIdle()
		active.GetAllDocuments() // emulate search-pre-sorted LIDs

		b.StartTimer()
		err = active.Seal(defaultSealParams)
		assert.NoError(b, err)

		b.StopTimer()
	}
}
