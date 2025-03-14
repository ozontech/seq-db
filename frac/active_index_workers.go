package frac

import (
	"encoding/binary"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

type IndexWorkers struct {
	ch          chan *IndexTask
	chMerge     chan *MergeTask
	workerCount int

	stopFn func()
}

type IndexTask struct {
	DocsLen uint64
	Frac    *Active
	Metas   disk.DocBlock
	Pos     uint64

	AppendQueue *atomic.Uint32
}

func (t *IndexTask) GetDocsLen() uint64 {
	return t.DocsLen
}

func (t *IndexTask) GetMetaLen() uint64 {
	return uint64(len(t.Metas))
}

type MergeTask struct {
	frac      *Active
	tokenLIDs *TokenLIDs
}

func NewIndexWorkers(workerCount, chLen int) *IndexWorkers {
	return &IndexWorkers{
		ch:          make(chan *IndexTask, chLen),
		chMerge:     make(chan *MergeTask, chLen),
		workerCount: workerCount,
	}
}

func (w *IndexWorkers) Start() {
	wg := sync.WaitGroup{}
	wg.Add(w.workerCount)

	for i := 0; i < w.workerCount; i++ {
		go func(index int) {
			defer wg.Done()
			w.appendWorker(index)
		}(i)
	}

	wg.Add(w.workerCount)
	for i := 0; i < w.workerCount; i++ {
		go func() {
			defer wg.Done()
			w.mergeWorker()
		}()
	}

	w.stopFn = func() {
		close(w.ch)
		close(w.chMerge)

		wg.Wait()

		w.stopFn = nil
	}
}

func (w *IndexWorkers) mergeWorker() {
	for task := range w.chMerge {
		task.tokenLIDs.GetLIDs(task.frac.MIDs, task.frac.RIDs) // GetLIDs cause sort and merge LIDs from queue
	}
}

func (w *IndexWorkers) Stop() {
	if w.stopFn != nil {
		w.stopFn()
	}
}

func (w *IndexWorkers) In(t *IndexTask) {
	w.ch <- t
}

func (w *IndexWorkers) appendWorker(index int) {
	// just a reusable buffer for unpacking
	var metasPayload []byte

	// collector of bulk meta data
	collector := newMetaDataCollector()

	for task := range w.ch {
		var err error

		sw := stopwatch.New()
		total := sw.Start("total_indexing")

		if metasPayload, err = disk.DocBlock(task.Metas).DecompressTo(metasPayload); err != nil {
			logger.Panic("error decompressing meta", zap.Error(err)) // TODO: error handling
		}

		active := task.Frac
		blockIndex := active.DocBlocks.Append(task.Pos)
		collector.Init(blockIndex)

		parsingMetric := sw.Start("metas_parsing")
		for len(metasPayload) > 0 {
			n := binary.LittleEndian.Uint32(metasPayload)
			metasPayload = metasPayload[4:]
			documentMetadata := metasPayload[:n]
			metasPayload = metasPayload[n:]

			var meta MetaData
			if err := meta.UnmarshalBinary(documentMetadata); err != nil {
				logger.Panic("BUG: can't unmarshal meta", zap.Error(err))
			}
			collector.AppendMeta(meta)
		}
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
		w.sendTokensToMergeWorkers(active, tokensToMerge)
		m.Stop()

		active.UpdateStats(collector.MinMID, collector.MaxMID, collector.DocsCounter, collector.SizeCounter)

		task.AppendQueue.Dec()

		total.Stop()
		sw.Export(metric.BulkStagesSeconds)
	}
}

func (w *IndexWorkers) sendTokensToMergeWorkers(frac *Active, tokens []*TokenLIDs) {
	for _, tl := range tokens {
		task := MergeTask{
			frac:      frac,
			tokenLIDs: tl,
		}
		select {
		case w.chMerge <- &task:
		default: // skip background merge if workers are busy
		}
	}
}

func addLIDsToTokens(tlids []*TokenLIDs, lids [][]uint32) []*TokenLIDs {
	needMerge := make([]*TokenLIDs, 0, len(tlids))
	for i, tl := range tlids {
		if l := tl.PutLIDsInQueue(lids[i]); l > consts.MinMergeQueue {
			needMerge = append(needMerge, tl)
		}
	}
	return needMerge
}
