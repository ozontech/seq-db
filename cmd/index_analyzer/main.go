package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
)

// Launch as:
//
// > go run ./cmd/index_analyzer/... ./data/*.index | tee ~/report.txt
func main() {
	if len(os.Args) < 2 {
		fmt.Println("No args")
		return
	}

	cm, stopFn := getCacheMaintainer()
	defer stopFn()

	readLimiter := disk.NewReadLimiter(1, nil)

	mergedTokensUniq := map[string]map[string]int{}
	mergedTokensValuesUniq := map[string]int{}

	stats := []Stats{}
	for _, path := range os.Args[1:] {
		fmt.Println(path)
		stats = append(stats, analyzeIndex(path, cm, readLimiter, mergedTokensUniq, mergedTokensValuesUniq))
	}

	fmt.Println("\nUniq Tokens Stats")
	printTokensStat(stats)

	fmt.Println("\nLIDs Histogram")
	printLIDsHistStat(stats)

	fmt.Println("\nTokens Histogram")
	printTokensHistStat(stats)

	fmt.Println("\nUniq LIDs Stats")
	printUniqLIDsStats(stats)
}

func getCacheMaintainer() (*fracmanager.CacheMaintainer, func()) {
	done := make(chan struct{})
	cm := fracmanager.NewCacheMaintainer(uint64(units.GiB), uint64(units.MiB*64), nil)
	wg := cm.RunCleanLoop(done, time.Second, time.Second)
	return cm, func() {
		close(done)
		wg.Wait()
	}
}

func analyzeIndex(
	path string,
	cm *fracmanager.CacheMaintainer,
	reader *disk.ReadLimiter,
	mergedTokensUniq map[string]map[string]int,
	allTokensValuesUniq map[string]int,
) Stats {
	var blockIndex uint32
	cache := cm.CreateIndexCache()

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	indexReader := disk.NewIndexReader(reader, f, cache.Registry)

	readBlock := func() []byte {
		data, _, err := indexReader.ReadIndexBlock(blockIndex, nil)
		blockIndex++
		if err != nil {
			logger.Fatal("error reading block", zap.String("file", f.Name()), zap.Error(err))
		}
		return data
	}

	// load info
	b := frac.BlockInfo{}
	_ = b.Unpack(readBlock())
	docsCount := int(b.Info.DocsTotal)

	// load tokens
	tokens := [][]byte{}
	for {
		data := readBlock()
		if len(data) == 0 { // empty block - is section separator
			break
		}
		block := token.Block{}
		if err := block.Unpack(data); err != nil {
			logger.Fatal("error unpacking tokens", zap.Error(err))
		}
		for i := range block.Len() {
			tokens = append(tokens, block.GetToken(i))
		}
	}

	// load tokens table
	tokenTableBlocks := []token.TableBlock{}
	for {
		data := readBlock()
		if len(data) == 0 { // empty block - is section separator
			break
		}
		block := token.TableBlock{}
		block.Unpack(data)
		tokenTableBlocks = append(tokenTableBlocks, block)
	}
	tokenTable := token.TableFromBlocks(tokenTableBlocks)

	// skip position
	blockIndex++

	// skip IDS
	for {
		data := readBlock()
		if len(data) == 0 { // empty block - is section separator
			break
		}
		blockIndex++ // skip RID
		blockIndex++ // skip Param
	}

	// load LIDs
	tid := 0
	lidsTotal := 0
	lidsUniq := map[[16]byte]int{}
	lidsLens := make([]int, len(tokens))
	tokenLIDs := []uint32{}
	for {
		data := readBlock()
		if len(data) == 0 { // empty block - is section separator
			break
		}

		block := &lids.Block{}
		if err := block.Unpack(data, &lids.UnpackBuffer{}); err != nil {
			logger.Fatal("error unpacking lids block", zap.Error(err))
		}

		last := len(block.Offsets) - 2
		for i := 0; i <= last; i++ {
			tokenLIDs = append(tokenLIDs, block.LIDs[block.Offsets[i]:block.Offsets[i+1]]...)
			if i < last || block.IsLastLID { // the end of token lids
				lidsTotal += len(tokenLIDs)
				lidsLens[tid] = len(tokenLIDs)
				lidsUniq[getLIDsHash(tokenLIDs)] = len(tokenLIDs)
				tokenLIDs = tokenLIDs[:0]
				tid++
			}
		}
	}

	lidsUniqCnt := 0
	for _, l := range lidsUniq {
		lidsUniqCnt += l
	}

	mergeAllTokens(mergedTokensUniq, allTokensValuesUniq, tokenTable, tokens, lidsLens)
	return newStats(mergedTokensUniq, allTokensValuesUniq, tokens, docsCount, lidsUniqCnt, lidsTotal)
}

func getLIDsHash(tokenLIDs []uint32) [16]byte {
	hasher := fnv.New128a()
	buf := make([]byte, 4)
	for _, l := range tokenLIDs {
		binary.LittleEndian.PutUint32(buf, l)
		hasher.Write(buf)
	}
	var res [16]byte
	hasher.Sum(res[:0])
	return res
}

func mergeAllTokens(allTokensUniq map[string]map[string]int, allTokensValuesUniq map[string]int, tokensTable token.Table, tokens [][]byte, lidsLens []int) {
	for k, v := range tokensTable {
		fieldsTokens, ok := allTokensUniq[k]
		if !ok {
			fieldsTokens = map[string]int{}
			allTokensUniq[k] = fieldsTokens
		}
		for _, e := range v.Entries {
			for tid := e.StartTID; tid < e.StartTID+e.ValCount; tid++ {
				fieldsTokens[string(tokens[tid-1])] += lidsLens[tid-1]
				allTokensValuesUniq[string(tokens[tid-1])]++
			}
		}
	}
}
