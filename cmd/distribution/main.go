package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

const savePeriod = 30 * time.Second

var reader *disk.Reader

func init() {
	reader = disk.NewReader(metric.StoreBytesRead)
}

func printDistribution(dist *seq.MIDsDistribution) {
	for i, bucket := range dist.GetDist() {
		logger.Info("    * disribution data", zap.Int("num", i), zap.Time("bucket", bucket))
	}
}

func getAllFracs(dataDir string) []string {
	files, err := filepath.Glob(filepath.Join(dataDir, "seq-db-*.index"))
	if err != nil {
		logger.Fatal("error getting fracs list", zap.Error(err))
	}
	return files
}

func getReader(path string, f *os.File) *disk.BlocksReader {
	c := cache.NewCache[[]byte](nil, nil)
	return disk.NewBlocksReader(c, f, nil)
}

func readBlock(blocksReader *disk.BlocksReader, blockIndex uint32) []byte {
	data, _, err := reader.ReadIndexBlock(blocksReader, blockIndex, nil)
	if err != nil {
		logger.Fatal("error reading block", zap.Error(err))
	}
	return data
}

func loadInfo(path string) frac.Info {
	f, _ := os.Open(path)
	blocksReader := getReader(path, f)
	result := readBlock(blocksReader, 0)
	if len(result) < 4 {
		logger.Fatal("seq-db index file header corrupted", zap.String("file", f.Name()))
	}

	info := frac.Info{}
	info.Load(result[4:])
	info.MetaOnDisk = 0
	stat, err := f.Stat()
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", f.Name()), zap.Error(err))
	}
	info.IndexOnDisk = uint64(stat.Size())

	return info
}

func buildDist(dist *seq.MIDsDistribution, path string, _ frac.Info) {
	f, _ := os.Open(path)
	blocksReader := getReader(path, f)

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

	cnt := 0
	for {
		bytes := readBlock(blocksReader, blockIndex)
		if len(bytes) == 0 { // empty
			break
		}

		mid := uint64(0)

		for len(bytes) != 0 {
			delta, n := binary.Varint(bytes)
			bytes = bytes[n:]
			mid += uint64(delta)
			if cnt > 0 { // skip only first stub ID
				dist.Add(seq.MID(mid))
			}
			cnt++
		}
		blockIndex += 3
	}

	logger.Info("read IDs", zap.Int("count", cnt))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("No args")
		return
	}

	dataDir := os.Args[1]
	filePathOrig := filepath.Join(dataDir, consts.FracCacheFileSuffix)
	filePathDist := filePathOrig + ".dist"

	if exist, err := fileExist(filePathDist); err != nil {
		logger.Fatal("error getting file stat", zap.Error(err))
	} else if !exist {
		if err := copyFile(filePathOrig, filePathDist); err != nil {
			logger.Fatal("error copying file", zap.Error(err))
		}
	}

	fc := fracmanager.NewSealedFracCache(filePathDist)

	lastSavedTime := time.Now()
	for _, path := range getAllFracs(dataDir) {
		base := filepath.Base(path)
		key := base[:len(base)-len(filepath.Ext(base))]

		logger.Info("start process", zap.String("name", key))

		info, ok := fc.GetFracInfo(key)
		if ok {
			logger.Info("found in frac-cache", zap.String("key", key))
		} else {
			info = loadInfo(path)
			logger.Info("loaded info from index",
				zap.String("key", key),
				zap.String("name", info.Name()),
				zap.String("ver", info.Ver),
				zap.Uint32("docs_total", info.DocsTotal),
				zap.String("from", util.MsTsToESFormat(uint64(info.From))),
				zap.String("to", util.MsTsToESFormat(uint64(info.To))),
				zap.String("creation_time", util.MsTsToESFormat(info.CreationTime)),
			)
		}

		if info.Distribution != nil {
			logger.Info("distribution present")
			printDistribution(info.Distribution)
			continue
		}

		if !info.InitEmptyDistribution() {
			logger.Info("distribution is not needed")
			continue
		}

		buildDist(info.Distribution, path, info)
		fc.AddFraction(key, info)
		logger.Info("built distribution", zap.Int("affected_minutes", len(info.Distribution.GetDist())))
		printDistribution(info.Distribution)

		if time.Since(lastSavedTime) > savePeriod {
			if err := fc.SyncWithDisk(); err != nil {
				logger.Fatal("file rename error", zap.Error(err))
			}
			lastSavedTime = time.Now()
		}
	}

	if err := fc.SyncWithDisk(); err != nil {
		logger.Fatal("file rename error", zap.Error(err))
	}
}

func copyFile(src, dst string) error {
	if data, err := os.ReadFile(src); err != nil {
		return err
	} else if err := os.WriteFile(dst, data, 0o644); err != nil {
		return err
	}
	return nil
}

func fileExist(file string) (bool, error) {
	_, err := os.Stat(file)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
