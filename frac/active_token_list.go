package frac

import (
	"context"
	"hash/crc32"
	"sync"

	"github.com/ozontech/seq-db/seq"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/util"
)

type tokenData struct {
	tokenLIDs *TokenLIDs
	field     string
	value     []byte
	tid       uint32
}

type tokenTask struct {
	res           chan []tokenData
	remap         []int
	tokens        [][]byte
	fieldsLengths []int
	tlids         []*TokenLIDs
}

type TokenList struct {
	chList []chan tokenTask

	fieldsMu  sync.RWMutex
	FieldTIDs map[string][]uint32

	sizesMu    sync.Mutex
	fieldSizes map[string]uint32

	allTokenLIDs *TokenLIDs

	tidMu     sync.RWMutex
	tidToVal  [][]byte
	tidToLIDs []*TokenLIDs
}

func NewActiveTokenList(workers int) *TokenList {
	tl := &TokenList{
		chList:     make([]chan tokenTask, workers),
		FieldTIDs:  make(map[string][]uint32),
		fieldSizes: make(map[string]uint32),
	}
	tl.tidToVal = append(tl.tidToVal, nil)
	tl.tidToLIDs = append(tl.tidToLIDs, nil)

	for i := range tl.chList {
		tl.chList[i] = make(chan tokenTask)
		go tl.tokenLIDsWorker(tl.chList[i], make(map[string]*TokenLIDs))
	}

	tl.initSystemTokens()

	return tl
}
func (tl *TokenList) Stop() {
	for _, c := range tl.chList {
		close(c)
	}
}

func (tl *TokenList) tokenLIDsWorker(ch chan tokenTask, tokenToLIDs map[string]*TokenLIDs) {
	nonExistent := make([]int, 0)
	for task := range ch {
		bufSize := 0
		nonExistent = nonExistent[:0]
		for _, i := range task.remap {
			if t, ok := tokenToLIDs[util.ByteToStringUnsafe(task.tokens[i])]; ok {
				task.tlids[i] = t
				continue
			}
			bufSize += len(task.tokens[i])
			nonExistent = append(nonExistent, i)
		}

		buf := make([]byte, 0, bufSize)
		newTokenLIDs := make([]TokenLIDs, len(nonExistent))
		newTokensData := make([]tokenData, len(nonExistent))

		for i, j := range nonExistent {
			var token string
			td := &newTokensData[i]
			td.tokenLIDs = &newTokenLIDs[i]
			token, td.field, td.value, buf = copyAndSplit(task.tokens[j], task.fieldsLengths[j], buf)
			tokenToLIDs[token] = td.tokenLIDs
			task.tlids[j] = td.tokenLIDs
		}
		task.res <- newTokensData
	}
}

func copyAndSplit(token []byte, fLen int, dest []byte) (string, string, []byte, []byte) {
	p := len(dest)
	dest = append(dest, token...)
	tokenCopy := dest[p:]

	return util.ByteToStringUnsafe(tokenCopy),
		util.ByteToStringUnsafe(tokenCopy[:fLen]),
		tokenCopy[fLen+1:],
		dest
}

func (tl *TokenList) initSystemTokens() {
	token := []byte(seq.TokenAll + ":")
	tlids := tl.Append([][]byte{token}, []int{len(seq.TokenAll)}, []*TokenLIDs{nil})
	tl.allTokenLIDs = tlids[0]
}

func (tl *TokenList) GetValByTID(tid uint32) []byte {
	tl.tidMu.RLock()
	defer tl.tidMu.RUnlock()

	// we only append to this slice, so it is safe to return it
	return tl.tidToVal[tid]
}

func (tl *TokenList) Provide(tid uint32) *TokenLIDs {
	tl.tidMu.RLock()
	defer tl.tidMu.RUnlock()

	// we only append to this slice, so it is safe to return it
	return tl.tidToLIDs[tid]
}

func (tl *TokenList) GetAllTokenLIDs() *TokenLIDs {
	return tl.allTokenLIDs
}

func (tl *TokenList) GetTIDsByField(f string) []uint32 {
	tl.fieldsMu.RLock()
	defer tl.fieldsMu.RUnlock()

	return tl.FieldTIDs[f]
}

func (tl *TokenList) FindPattern(ctx context.Context, token parser.Token, tids []uint32, sw *stopwatch.Stopwatch) ([]uint32, error) {
	tk := parser.GetField(token)

	inverseIndex := tl.GetTIDsByField(tk)

	tl.tidMu.RLock()
	tidToVal := tl.tidToVal
	tl.tidMu.RUnlock()

	searcher := pattern.NewSearcher(token, nil, len(inverseIndex))

	doneCh := ctx.Done()

	m := sw.Start("search_cycle")
	defer m.Stop()

	for i := searcher.Begin(); i <= searcher.End(); i++ {

		select {
		case <-doneCh:
			return nil, ctx.Err()
		default:
		}

		id := inverseIndex[i]
		if searcher.Check(tidToVal[id]) {
			tids = append(tids, id)
		}
	}

	return tids, nil
}

func getTokenHash(token []byte) uint32 {
	return crc32.ChecksumIEEE(token)
}

func (tl *TokenList) getTokenLIDs(tokens [][]byte, fieldsLengths []int, tlids []*TokenLIDs) []tokenData {
	n := len(tl.chList)
	remap := make([][]int, n)
	for i, token := range tokens {
		k := getTokenHash(token) % uint32(n)
		remap[k] = append(remap[k], i)
	}

	res := make(chan []tokenData, n)
	defer close(res)

	for i, c := range tl.chList {
		c <- tokenTask{
			res:           res,
			remap:         remap[i],
			tokens:        tokens,
			fieldsLengths: fieldsLengths,
			tlids:         tlids,
		}
	}

	resultSize := 0
	subResults := make([][]tokenData, n)
	for i := 0; i < n; i++ {
		subResults[i] = <-res
		resultSize += len(subResults[i])
	}

	newTokensData := make([]tokenData, 0, resultSize)
	for _, d := range subResults {
		newTokensData = append(newTokensData, d...)
	}

	return newTokensData
}

func (tl *TokenList) Append(tokens [][]byte, fieldsLengths []int, tokenLIDsPlaces []*TokenLIDs) []*TokenLIDs {
	newTokensData := tl.getTokenLIDs(tokens, fieldsLengths, tokenLIDsPlaces)

	tl.createTIDs(newTokensData)
	tl.fillFieldTIDs(newTokensData)
	tl.fillSizes(newTokensData)

	return tokenLIDsPlaces
}

func (tl *TokenList) createTIDs(newTokensData []tokenData) {
	tl.tidMu.Lock()
	for i, token := range newTokensData {
		newTokensData[i].tid = uint32(len(tl.tidToVal))
		tl.tidToVal = append(tl.tidToVal, token.value)
		tl.tidToLIDs = append(tl.tidToLIDs, token.tokenLIDs)
	}
	tl.tidMu.Unlock()
}

func (tl *TokenList) fillFieldTIDs(newTokensData []tokenData) {
	tl.fieldsMu.Lock()
	for _, token := range newTokensData {
		field := token.field
		tl.FieldTIDs[field] = append(tl.FieldTIDs[field], token.tid)
	}
	tl.fieldsMu.Unlock()
}

func (tl *TokenList) fillSizes(newTokensData []tokenData) {
	tl.sizesMu.Lock()
	for _, token := range newTokensData {
		tl.fieldSizes[token.field] += uint32(len(token.value))
	}
	tl.sizesMu.Unlock()
}

func (tl *TokenList) GetFieldSizes() map[string]uint32 {
	tl.sizesMu.Lock()
	defer tl.sizesMu.Unlock()

	return tl.fieldSizes
}
