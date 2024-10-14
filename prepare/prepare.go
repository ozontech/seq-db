package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/tests/common"
)

func getBatches() []*bytes.Buffer {
	f, err := os.Open(common.TestDataDir + "/complicated_1/raw-docs.json")
	if err != nil {
		logger.Fatal(err.Error())
		return nil
	}
	buf := make([]byte, 16*1024*1024)
	batches := make([]*bytes.Buffer, 0)
	batch := make([]byte, 0)
	read := make([]byte, 0)
	stat, err := f.Stat()
	if err != nil {
		logger.Fatal(err.Error())
		return nil
	}
	total := stat.Size()
	offset := 0
	root := insaneJSON.Spawn()
	batchNum := 0
	for {
		n, err := f.Read(buf)

		if err == io.EOF {
			if len(read) != 0 {
				logger.Fatal("batch compose error")
			}
			break
		}

		if err != nil {
			logger.Fatal(err.Error())
		}

		read = append(read, buf[:n]...)
		batch = batch[:0]
		for {
			pos := bytes.IndexByte(read, '\n')
			if pos == -1 {
				break
			}
			batch = append(batch, `{"index":""}`...)
			batch = append(batch, '\n')
			batch = append(batch, read[:pos+1]...)
			err := root.DecodeBytes(read[:pos+1])
			if err != nil {
				logger.Fatal("wrong json in batch",
					zap.Int("batch_num", batchNum),
					zap.String("data", string(read[:pos+1])),
				)
			}

			read = read[pos+1:]
		}
		if len(batch) != 0 {
			buf := bytes.NewBuffer(nil)
			buf.Write(batch)
			batches = append(batches, buf)
		}
		batchNum++
		offset += n

		logger.Info("batch prepared",
			zap.Int("offset", offset),
			zap.Int64("total", total),
		)
	}

	return batches
}

func main() {
	batches := getBatches()
	batchesDir := filepath.Join(common.TestDataDir, "complicated_1", "batches")
	err := os.MkdirAll(batchesDir, 0o777)
	if err != nil {
		logger.Fatal("can't create dir for batches", zap.Error(err))
	}
	for i, batch := range batches {
		err := ioutil.WriteFile(fmt.Sprintf("%s/%d.json", batchesDir, i), batch.Bytes(), 0o777)
		if err != nil {
			logger.Fatal("can't write batch",
				zap.Int("i", i),
				zap.Error(err),
			)
		}
	}
}
