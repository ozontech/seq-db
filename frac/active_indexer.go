package frac

import (
	"encoding/binary"
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

type ActiveIndexer struct {
	ch          chan *indexTask
	chMerge     chan *mergeTask
	workerCount int

	stopFn func()
}

type indexTask struct {
	Frac  *Active
	Metas disk.DocBlock
	Pos   uint64
	Wg    *sync.WaitGroup
}

type mergeTask struct {
	frac      *Active
	tokenLIDs *TokenLIDs
}

func NewActiveIndexer(workerCount, chLen int) *ActiveIndexer {
	return &ActiveIndexer{
		ch:          make(chan *indexTask, chLen),
		chMerge:     make(chan *mergeTask, chLen),
		workerCount: workerCount,
	}
}

func (ai *ActiveIndexer) Index(frac *Active, metas []byte, wg *sync.WaitGroup, sw *stopwatch.Stopwatch) {
	m := sw.Start("send_index_chan")
	ai.ch <- &indexTask{
		Pos:   disk.DocBlock(metas).GetExt2(),
		Metas: metas,
		Frac:  frac,
		Wg:    wg,
	}
	m.Stop()
}

func (ai *ActiveIndexer) Start() {
	wg := sync.WaitGroup{}
	wg.Add(ai.workerCount)

	for i := 0; i < ai.workerCount; i++ {
		go func(index int) {
			defer wg.Done()
			ai.appendWorker(index)
		}(i)
	}

	wg.Add(ai.workerCount)
	for i := 0; i < ai.workerCount; i++ {
		go func() {
			defer wg.Done()
			ai.mergeWorker()
		}()
	}

	ai.stopFn = func() {
		close(ai.ch)
		close(ai.chMerge)

		wg.Wait()

		ai.stopFn = nil
	}
}

func (ai *ActiveIndexer) mergeWorker() {
	for task := range ai.chMerge {
		task.tokenLIDs.GetLIDs(task.frac.MIDs, task.frac.RIDs) // GetLIDs cause sort and merge LIDs from queue
	}
}

func (ai *ActiveIndexer) Stop() {
	if ai.stopFn != nil {
		ai.stopFn()
	}
}

var metaDataPool = sync.Pool{
	New: func() any {
		return new(MetaData)
	},
}

func (ai *ActiveIndexer) appendWorker(index int) {
	// collector of bulk meta data
	collector := newMetaDataCollector()

	for task := range ai.ch {
		var err error

		sw := stopwatch.New()
		total := sw.Start("total_indexing")

		metaBuf := bytespool.Acquire(int(task.Metas.RawLen()))

		if metaBuf.B, err = task.Metas.DecompressTo(metaBuf.B); err != nil {
			logger.Panic("error decompressing meta", zap.Error(err)) // TODO: error handling
		}
		metasPayload := metaBuf.B

		active := task.Frac
		blockIndex := active.DocBlocks.Append(task.Pos)
		collector.Init(blockIndex)

		parsingMetric := sw.Start("metas_parsing")
		meta := metaDataPool.Get().(*MetaData)
		for len(metasPayload) > 0 {
			n := binary.LittleEndian.Uint32(metasPayload)
			metasPayload = metasPayload[4:]
			documentMetadata := metasPayload[:n]
			metasPayload = metasPayload[n:]

			if err := meta.UnmarshalBinary(documentMetadata); err != nil {
				logger.Panic("BUG: can't unmarshal meta", zap.Error(err))
			}
			collector.AppendMeta(*meta)
		}
		metaDataPool.Put(meta)
		bytespool.Release(metaBuf)
		parsingMetric.Stop()

		m := sw.Start("doc_params_set")
		appendedIDs := active.DocsPositions.SetMultiple(collector.IDs, collector.Positions)
		if len(appendedIDs) != len(collector.IDs) {
			// There are duplicates in the active fraction.
			// It is possible in case we retry same bulk requests.
			// So we need to remove duplicates from collector.
			doublesCnt := len(collector.IDs) - len(appendedIDs)
			metric.BulkDuplicateDocsTotal.Observe(float64(doublesCnt))
			logger.Warn("found duplicates", zap.Int("batch", doublesCnt), zap.Int("worker", index))
			collector.Filter(appendedIDs)
		}
		m.Stop()

		m = sw.Start("append_ids")
		lids := active.AppendIDs(collector.IDs)
		m.Stop()

		m = sw.Start("token_list_append")
		tokenLIDsPlaces := collector.PrepareTokenLIDsPlaces()
		active.TokenList.Append(collector.TokensValues, collector.FieldsLengths, tokenLIDsPlaces)
		m.Stop()

		m = sw.Start("group_lids")
		groups := collector.GroupLIDsByToken(lids)
		m.Stop()

		m = sw.Start("put_lids_queue")
		tokensToMerge := addLIDsToTokens(tokenLIDsPlaces, groups)
		ai.sendTokensToMergeWorkers(active, tokensToMerge)
		m.Stop()

		active.UpdateStats(collector.MinMID, collector.MaxMID, collector.DocsCounter, collector.SizeCounter)

		task.Wg.Done()

		total.Stop()
		sw.Export(bulkStagesSeconds)
	}
}

func (ai *ActiveIndexer) sendTokensToMergeWorkers(frac *Active, tokens []*TokenLIDs) {
	for _, tl := range tokens {
		task := mergeTask{
			frac:      frac,
			tokenLIDs: tl,
		}
		select {
		case ai.chMerge <- &task:
		default: // skip background merge if workers are busy
		}
	}
}

func addLIDsToTokens(tlids []*TokenLIDs, lids [][]uint32) []*TokenLIDs {
	const minMergeQueue = 10000

	needMerge := make([]*TokenLIDs, 0, len(tlids))
	for i, tl := range tlids {
		if l := tl.PutLIDsInQueue(lids[i]); l > minMergeQueue {
			needMerge = append(needMerge, tl)
		}
	}
	return needMerge
}
