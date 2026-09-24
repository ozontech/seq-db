package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/alecthomas/units"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/node"
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

	var stats []Stats
	for _, path := range os.Args[1:] {
		fmt.Println(path)
		stats = append(stats, analyzeIndex(path, cm, readLimiter))
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
		consts.IndexFileSuffix,
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

func analyzeIndex(
	path string,
	cm *fracmanager.CacheMaintainer,
	rl *storage.ReadLimiter,
) Stats {
	fracSrc, release := openFrac(path, cm, rl)
	defer release()

	tokensUniq := map[string]map[string]int{}
	tokensValuesUniq := map[string]int{}

	docsCount := int(fracSrc.Info().DocsTotal)

	var tokens [][]byte
	lidsTotal := 0
	lidsUniq := map[[16]byte]int{}

	for field, fieldPostings := range fracSrc.TokenTriplets() {
		for tokenLIDs, err := range fieldPostings {
			if err != nil {
				logger.Fatal("error reading token lids", zap.String("field", field), zap.Error(err))
			}

			token := append([]byte(nil), tokenLIDs.First...)
			tokens = append(tokens, token)

			lidsTotal += len(tokenLIDs.Second)
			lidsUniq[getLIDsHash(tokenLIDs.Second)] = len(tokenLIDs.Second)

			fieldsTokens, ok := tokensUniq[field]
			if !ok {
				fieldsTokens = map[string]int{}
				tokensUniq[field] = fieldsTokens
			}
			fieldsTokens[string(token)] += len(tokenLIDs.Second)
			tokensValuesUniq[string(token)]++
		}
	}

	lidsUniqCnt := 0
	for _, l := range lidsUniq {
		lidsUniqCnt += l
	}

	return newStats(tokensUniq, tokensValuesUniq, tokens, docsCount, lidsUniqCnt, lidsTotal)
}

func openFrac(
	path string,
	cm *fracmanager.CacheMaintainer,
	rl *storage.ReadLimiter,
) (indexwriter.Source, func()) {
	base := basePath(path)
	legacy := strings.HasSuffix(path, consts.IndexFileSuffix)

	sealed := frac.NewSealed(
		base,
		rl,
		cm.CreateIndexCache(),
		cm.CreateSortDocsCache(),
		nil,
		&frac.Config{},
		noopSkipMaskProvider{},
		legacy,
	)
	return frac.NewSealedSource(sealed), sealed.Release
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

type noopSkipMaskProvider struct{}

func (noopSkipMaskProvider) GetIDsIteratorByFrac(_ string, _, _ uint32, reverse bool) (node.Node, bool, func() error, error) {
	return node.NewStatic(nil, !reverse), false, func() error { return nil }, nil
}

func (noopSkipMaskProvider) GetIDsBitmapByFrac(_ string, _, _ uint32) (*roaring.Bitmap, error) {
	return nil, nil
}

func (noopSkipMaskProvider) RemoveFrac(_ string) {}
