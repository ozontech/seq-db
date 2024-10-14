package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
)

// Calc hists for all fractions:
//
//	(for i in `ls /data/*.index`; do echo "Reading fraction: $i"; ./analyzer $i; done) | tee stats.txt
func main() {
	if len(os.Args) < 2 {
		fmt.Println("No args")
		return
	}

	fracPath := os.Args[1]

	maintainer := fracmanager.NewCacheMaintainer(consts.GB, 128*consts.MB, nil)
	maintainer.RunCleanLoop(nil, time.Second)
	cache := maintainer.CreateSealedIndexCache()

	reader := disk.NewReader(metric.StoreBytesRead)
	defer reader.Stop()
	blocksReader := disk.NewBlocksReader(cache.Registry, fracPath, nil)
	var outBuf []byte

	// skip tokens
	blockIndex := uint32(1)
	for {
		header := blocksReader.GetBlockHeader(blockIndex)
		blockIndex++
		if header.Len() == 0 {
			break
		}
	}

	// skip tokenTable
	for {
		header := blocksReader.GetBlockHeader(blockIndex)
		blockIndex++
		if header.Len() == 0 {
			break
		}
	}

	blockIndex++ // skip position

	first := true
	cnt := map[time.Time]int{}
	minutes := []time.Time{}

	for {
		readTask := reader.ReadIndexBlock(blocksReader, blockIndex, outBuf)
		outBuf = readTask.Buf
		if readTask.Err != nil {
			logger.Fatal("error reading MIDs block", zap.String("file", blocksReader.GetFileName()), zap.Error(readTask.Err))
		}
		bytes := readTask.Buf
		if len(bytes) == 0 { // empty
			break
		}

		mid := uint64(0)

		for len(bytes) != 0 {
			delta, n := binary.Varint(bytes)
			bytes = bytes[n:]
			mid += uint64(delta)

			if first { // skip first stub ID
				first = false
				continue
			}

			minute := seq.MID(mid).Time().Truncate(time.Minute)
			if _, ok := cnt[minute]; !ok {
				minutes = append(minutes, minute)
			}
			cnt[minute]++
		}
		blockIndex += 3
	}

	// getting sealing time
	stat, err := blocksReader.GetFileStat()
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", blocksReader.GetFileName()), zap.Error(err))
	}
	indexTime := stat.ModTime()

	sum := 0
	fmt.Printf("%4s\t%16s\t%10s\t%10s\n", "Num", "MINUTE BUCKET", "COUNT", "DELAY, min")
	fmt.Println("----------------------------------------------------------")
	for i, minute := range minutes {
		delay := indexTime.Sub(minute) / time.Minute
		sum += cnt[minute]
		fmt.Printf("%4d\t%16s\t%10d\t%10d\n", i+1, minute.Format("2006-01-02 15:04"), cnt[minute], delay)
	}
	fmt.Println("----------------------------------------------------------")
	fmt.Printf("%4s\t%-16s\t%10d\t%10s\n", "", "TOTAL", sum, "N/A")
}
