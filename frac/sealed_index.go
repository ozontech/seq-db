package frac

import (
	"fmt"
	"math"
	"time"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SealedIndexProvider struct {
	f           *Sealed
	sc          *SearchCell
	fracVersion BinaryDataVersion
	midCache    *UnpackCache
	ridCache    *UnpackCache
}

func (ip *SealedIndexProvider) Indexes() []Index {
	return []Index{&SealedIndex{ip: ip}}
}

type SealedIndex struct {
	ip *SealedIndexProvider
}

func (index *SealedIndex) IsIntersecting(from, to seq.MID) bool {
	return index.ip.f.IsIntersecting(from, to)
}
func (index *SealedIndex) Contains(mid seq.MID) bool {
	return index.ip.f.Contains(mid)
}

func (index *SealedIndex) IDsIndex() IDsIndex {
	return &SealedIDsIndex{
		sc:          index.ip.sc,
		midCache:    index.ip.midCache,
		ridCache:    index.ip.ridCache,
		fracVersion: index.ip.fracVersion,
		loader:      NewIDsLoader(index.ip.f.indexReader, index.ip.f.indexCache, index.ip.f.idsTable),
	}
}

func (index *SealedIndex) TokenIndex() TokenIndex {
	f := index.ip.f
	return &SealedTokenIndex{
		ip:               index.ip,
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.indexReader, f.indexCache.Tokens, index.ip.sc),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.indexReader, f.indexCache.TokenTable),
	}
}

func (index *SealedIndex) DocsIndex() DocsIndex {
	return &SealedDocsIndex{
		idsIndex: index.IDsIndex(),
		idsLoader: NewIDsLoader(
			index.ip.f.indexReader,
			index.ip.f.indexCache,
			index.ip.f.idsTable,
		),
		docsReader:    index.ip.f.docsReader,
		blocksOffsets: index.ip.f.BlocksOffsets,
	}
}

type SealedDocsIndex struct {
	idsIndex      IDsIndex
	idsLoader     *IDsLoader
	docsReader    *disk.DocsReader
	blocksOffsets []uint64
}

func (di *SealedDocsIndex) GetBlocksOffsets(num uint32) uint64 {
	return di.blocksOffsets[num]
}

func (di *SealedDocsIndex) GetDocPos(ids []seq.ID) []DocPos {
	return di.getDocPosByLIDs(di.findLIDs(ids))
}

func (di *SealedDocsIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return di.docsReader.ReadDocs(blockOffset, docOffsets)
}

// findLIDs returns a slice of LIDs. If seq.ID is not found, LID has the value 0 at the corresponding position
func (di *SealedDocsIndex) findLIDs(ids []seq.ID) []seq.LID {
	res := make([]seq.LID, len(ids))

	// left and right it is search range
	left := 1                      // first
	right := di.idsIndex.Len() - 1 // last

	for i, id := range ids {

		if i == 0 || !seq.Less(id, ids[i-1]) {
			// reset search range (it is not DESC sorted IDs)
			left = 1
		}

		lid := seq.LID(util.BinSearchInRange(left, right, func(lid int) bool {
			return di.idsIndex.LessOrEqual(seq.LID(lid), id)
		}))

		if id.MID == di.idsIndex.GetMID(lid) && id.RID == di.idsIndex.GetRID(lid) {
			res[i] = lid
		}

		// try to refine the search range, but this optimization works for DESC sorted IDs only
		left = int(lid)
	}

	return res
}

// GetDocPosByLIDs returns a slice of DocPos for the corresponding LIDs.
// Passing sorted LIDs (asc or desc) will improve the performance of this method.
// For LID with zero value will return DocPos with `DocPosNotFound` value
func (di *SealedDocsIndex) getDocPosByLIDs(localIDs []seq.LID) []DocPos {
	var (
		prevIndex int64
		positions []uint64
		startLID  seq.LID
	)

	res := make([]DocPos, len(localIDs))
	for i, lid := range localIDs {
		if lid == 0 {
			res[i] = DocPosNotFound
			continue
		}

		index := di.idsLoader.getIDBlockIndexByLID(lid)
		if positions == nil || prevIndex != index {
			positions = di.idsLoader.GetParamsBlock(uint32(index))
			startLID = seq.LID(index * consts.IDsPerBlock)
		}

		res[i] = DocPos(positions[lid-startLID])
	}

	return res
}

type SealedTokenIndex struct {
	ip               *SealedIndexProvider
	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader
}

func (ti *SealedTokenIndex) GetValByTID(tid uint32) []byte {
	tokenTable := ti.tokenTableLoader.Load()
	if entry := tokenTable.GetEntryByTID(tid); entry != nil {
		return ti.tokenBlockLoader.Load(entry).GetValByTID(tid)
	}
	return nil
}

func (ti *SealedTokenIndex) GetTIDsByTokenExpr(t parser.Token) ([]uint32, error) {
	field := parser.GetField(t)
	searchStr := parser.GetHint(t)

	tokenTable := ti.tokenTableLoader.Load()
	entries := tokenTable.SelectEntries(field, searchStr)
	if len(entries) == 0 {
		return nil, nil
	}

	fetcher := token.NewFetcher(ti.tokenBlockLoader, entries)
	searcher := pattern.NewSearcher(t, fetcher, fetcher.GetTokensCount())

	begin := searcher.Begin()
	end := searcher.End()
	if begin > end {
		return nil, nil
	}

	blockIndex := fetcher.GetBlockIndex(begin)
	lastTID := fetcher.GetTIDFromIndex(end)

	entry := entries[blockIndex]
	tokensBlock := ti.tokenBlockLoader.Load(entry)
	entryLastTID := entry.GetLastTID()

	tids := []uint32{}
	for tid := fetcher.GetTIDFromIndex(begin); tid <= lastTID; tid++ {
		if tid > entryLastTID {
			if ti.ip.sc.Exit.Load() {
				return nil, consts.ErrUnexpectedInterruption
			}
			if ti.ip.sc.IsCancelled() {
				err := fmt.Errorf("search cancelled when matching tokens: reason=%s field=%s, query=%s", ti.ip.sc.Context.Err(), field, searchStr)
				ti.ip.sc.Cancel(err)
				return nil, err
			}
			blockIndex++
			entry = entries[blockIndex]
			tokensBlock = ti.tokenBlockLoader.Load(entry)
			entryLastTID = entry.GetLastTID()
		}

		val := tokensBlock.GetValByTID(tid)
		if searcher.Check(val) {
			tids = append(tids, tid)
		}
	}

	return tids, nil
}

func (ti *SealedTokenIndex) GetLIDsFromTIDs(tids []uint32, counter lids.Counter, minLID, maxLID uint32, order seq.DocsOrder) []node.Node {
	var (
		getBlockIndex   func(tid uint32) uint32
		getLIDsIterator func(uint32, uint32) node.Node
	)

	loader := lids.NewLoader(ti.ip.f.indexReader, ti.ip.f.indexCache.LIDs, ti.ip.sc)

	if order.IsReverse() {
		getBlockIndex = func(tid uint32) uint32 { return ti.ip.f.lidsTable.GetLastBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorAsc)(lids.NewLIDsCursor(ti.ip.f.lidsTable, loader, startIndex, tid, counter, minLID, maxLID))
		}
	} else {
		getBlockIndex = func(tid uint32) uint32 { return ti.ip.f.lidsTable.GetFirstBlockIndexForTID(tid) }
		getLIDsIterator = func(startIndex uint32, tid uint32) node.Node {
			return (*lids.IteratorDesc)(lids.NewLIDsCursor(ti.ip.f.lidsTable, loader, startIndex, tid, counter, minLID, maxLID))
		}
	}

	t := time.Now()
	startIndexes := make([]uint32, len(tids))
	for i, tid := range tids {
		startIndexes[i] = getBlockIndex(tid)
	}
	ti.ip.sc.AddLIDBlocksSearchTimeNS(time.Since(t))

	nodes := make([]node.Node, len(tids))
	for i, tid := range tids {
		nodes[i] = getLIDsIterator(startIndexes[i], tid)
	}

	return nodes
}

type SealedIDsIndex struct {
	sc          *SearchCell
	midCache    *UnpackCache
	ridCache    *UnpackCache
	fracVersion BinaryDataVersion
	loader      *IDsLoader
}

func (ii *SealedIDsIndex) GetMID(lid seq.LID) seq.MID {
	ii.loader.GetMIDsBlock(ii.sc, seq.LID(lid), ii.midCache)
	return seq.MID(ii.midCache.GetValByLID(uint64(lid)))
}

func (ii *SealedIDsIndex) GetRID(lid seq.LID) seq.RID {
	ii.loader.GetRIDsBlock(ii.sc, seq.LID(lid), ii.ridCache, ii.fracVersion)
	return seq.RID(ii.ridCache.GetValByLID(uint64(lid)))
}

func (ii *SealedIDsIndex) Len() int {
	return int(ii.loader.table.IDsTotal)
}

func (ii *SealedIDsIndex) LessOrEqual(lid seq.LID, id seq.ID) bool {
	if lid >= seq.LID(ii.loader.table.IDsTotal) {
		// out of right border
		return true
	}

	blockIndex := ii.loader.getIDBlockIndexByLID(lid)
	if !seq.LessOrEqual(ii.loader.table.MinBlockIDs[blockIndex], id) {
		// the LID's block min ID is greater than the given ID, so any ID of that block is also greater
		return false
	}

	if blockIndex > 0 && seq.LessOrEqual(ii.loader.table.MinBlockIDs[blockIndex-1], id) {
		// the min ID of the previous block is also less than or equal to the given ID,
		// so any ID of this block is definitely less than or equal to the given ID.
		return true
	}

	checkedMID := ii.GetMID(lid)
	if checkedMID == id.MID {
		if id.RID == math.MaxUint64 {
			// this is a real use case for LessOrEqual
			// in this case the <= condition always becomes true,
			// so we don't need to load the RID from the disk
			return true
		}
		return ii.GetRID(lid) <= id.RID
	}
	return checkedMID < id.MID
}
