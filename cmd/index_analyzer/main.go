package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/storage"
)

// Launch as:
//
// > go run ./cmd/index_analyzer/... ./data/*.info | tee ~/report.txt
func main() {
	if len(os.Args) < 2 {
		fmt.Println("No args")
		return
	}

	cm, stopFn := getCacheMaintainer()
	defer stopFn()

	readLimiter := storage.NewReadLimiter(1, nil)

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

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		cm.RunCleanLoop(done, time.Second, time.Second)
	}()

	return cm, func() {
		close(done)
		wg.Wait()
	}
}

// basePath strips any known index suffix to return the fraction base path.
func basePath(path string) string {
	for _, suffix := range []string{
		consts.InfoFileSuffix,
		consts.TokenFileSuffix,
		consts.OffsetsFileSuffix,
		consts.IDFileSuffix,
		consts.LIDFileSuffix,
	} {
		if strings.HasSuffix(path, suffix) {
			return path[:len(path)-len(suffix)]
		}
	}
	return path
}

func openFile(path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	return f
}

func analyzeIndex(
	path string,
	cm *fracmanager.CacheMaintainer,
	rl *storage.ReadLimiter,
	mergedTokensUniq map[string]map[string]int,
	allTokensValuesUniq map[string]int,
) Stats {
	base := basePath(path)
	indexCache := cm.CreateIndexCache()

	// Open per-section files.
	infoFile := openFile(base + consts.InfoFileSuffix)
	tokenFile := openFile(base + consts.TokenFileSuffix)
	lidFile := openFile(base + consts.LIDFileSuffix)
	defer infoFile.Close()
	defer tokenFile.Close()
	defer lidFile.Close()

	tokenReader := storage.NewIndexReader(rl, tokenFile.Name(), tokenFile, indexCache.TokenRegistry)
	lidReader := storage.NewIndexReader(rl, lidFile.Name(), lidFile, indexCache.LIDRegistry)

	// --- Info ---
	var blockIndex uint32
	infoData, err := io.ReadAll(infoFile)
	if err != nil {
		logger.Fatal("error reading info block", zap.String("file", infoFile.Name()), zap.Error(err))
	}
	var b sealed.BlockInfo
	if err := b.Unpack(infoData); err != nil {
		logger.Fatal("error unpacking block info", zap.Error(err))
	}
	ver := b.Info.BinaryDataVer
	docsCount := int(b.Info.DocsTotal)

	// --- Tokens (.token file) ---
	// Token blocks start at index 0, followed by an empty separator, then token table blocks.
	blockIndex = 0
	readTokenBlock := func() []byte {
		data, _, err := tokenReader.ReadIndexBlock(blockIndex, nil)
		blockIndex++
		if err != nil {
			logger.Fatal("error reading token block", zap.String("file", tokenFile.Name()), zap.Error(err))
		}
		return data
	}

	tokens := [][]byte{}
	tokenUnpackBuf := &token.UnpackBuffer{}
	for {
		data := readTokenBlock()
		if len(data) == 0 { // empty block - section separator
			break
		}
		block := token.Block{}
		if err := block.Unpack(data, b.Info.BinaryDataVer, tokenUnpackBuf); err != nil {
			logger.Fatal("error unpacking tokens", zap.Error(err))
		}
		for i := range block.Len() {
			tokens = append(tokens, block.GetToken(i))
		}
	}

	tokenTableBlocks := []token.TableBlock{}
	for {
		data := readTokenBlock()
		if len(data) == 0 { // empty block - section separator
			break
		}
		block := token.TableBlock{}
		block.Unpack(data)
		tokenTableBlocks = append(tokenTableBlocks, block)
	}
	tokenTable := token.TableFromBlocks(tokenTableBlocks)

	// --- LIDs (.lid file) ---
	blockIndex = 0
	readLIDBlock := func() []byte {
		data, _, err := lidReader.ReadIndexBlock(blockIndex, nil)
		blockIndex++
		if err != nil {
			logger.Fatal("error reading lid block", zap.String("file", lidFile.Name()), zap.Error(err))
		}
		return data
	}

	tid := 0
	lidsTotal := 0
	lidsUniq := map[[16]byte]int{}
	lidsLens := make([]int, len(tokens))
	tokenLIDs := []uint32{}
	lidUnpackBuf := &lids.UnpackBuffer{}
	for {
		data := readLIDBlock()
		if len(data) == 0 { // empty block - section separator
			break
		}

		block := &lids.Block{}
		if err := block.Unpack(data, ver, lidUnpackBuf); err != nil {
			logger.Fatal("error unpacking lids block", zap.Error(err))
		}

		listsCount := block.GetCount()
		for i := 0; i < listsCount; i++ {
			lidsBatch := block.GetLIDs(i)
			iter := lidsBatch.Iter()
			for {
				lid, ok := iter.Next()
				if !ok {
					break
				}
				tokenLIDs = append(tokenLIDs, lid)
			}

			if i < listsCount || block.IsLastLID() { // the end of token lids
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
