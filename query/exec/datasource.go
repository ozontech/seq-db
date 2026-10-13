package exec

import (
	"context"
	"errors"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/query/encoding"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

const searcherBatchLimit = 100

// SearcherDataSource is limitless: for the documents
// path it walks the matched set in fixed-size batches via cursor pagination:
//   - repeatedly call Searcher.SearchDocs with a constant per-batch limit and a
//     cursor (OffsetId) advanced from the previous batch's last ID;
//   - the Fetcher.FetchDocs step is performed on demand — only the current
//     document is fetched right before it is returned from Next().
//
// The aggregation path is a scan-all request (one SearchDocs, no cursor):
// aggregations require a full scan and the proxy-side DistributedAggregator
// merges complete results.
type SearcherDataSource struct {
	ctx context.Context
	tr  *querytracer.Tracer

	searchParams processor.SearchParams
	isAgg        bool

	fracManager *fracmanager.FracManager
	searcher    *fracmanager.Searcher
	fetcher     *fracmanager.Fetcher

	qpr         *seq.QPR
	agg         *storeapi.SearchResponse_Agg
	aggsScanned bool

	curIdx int

	// total accumulates the final total.
	total uint64
	// batchNo counts completed batches; 0 means no batch has been scanned yet.
	batchNo int
	// done marks the documents path as exhausted (a batch returned no IDs).
	done bool

	// fracs are acquired once and held for the producer's lifetime so that
	// cursor-pagination batches and per-document fetches share the same set.
	fracs   fracmanager.List
	release func()

	// err holds the first error encountered during scanning/fetching. Reported via Finalize.
	err error
}

func NewSearcherDataSource(
	ctx context.Context,
	tr *querytracer.Tracer,
	searchParams processor.SearchParams,
	fracManager *fracmanager.FracManager,
	searcher *fracmanager.Searcher,
	fetcher *fracmanager.Fetcher,
) *SearcherDataSource {
	return &SearcherDataSource{
		ctx:          ctx,
		tr:           tr,
		searchParams: searchParams,
		fracManager:  fracManager,
		searcher:     searcher,
		fetcher:      fetcher,
		isAgg:        len(searchParams.AggQ) > 0,
	}
}

func (s *SearcherDataSource) Next() *query.Record {
	if s.isAgg {
		return s.nextAgg()
	}
	return s.nextDoc()
}

func (s *SearcherDataSource) Finalize() *query.Summary {
	if s.release != nil {
		s.release()
	}
	return &query.Summary{Total: s.total, Err: s.err}
}

func (s *SearcherDataSource) nextDoc() *query.Record {
	if s.done {
		return nil
	}

	if s.qpr == nil || s.curIdx >= len(s.qpr.IDs) {
		if err := s.scanBatch(); err != nil {
			s.err = err
			return nil
		}
		if s.done {
			return nil
		}
	}

	idSrc := s.qpr.IDs[s.curIdx]
	s.curIdx++

	docs, err := s.fetcher.FetchDocs(s.Ctx(), s.fracs, []seq.IDSource{idSrc}, true)
	if err != nil {
		s.err = err
		return nil
	}

	return makeDocumentRecord(idSrc.ID, docs[0])
}

func (s *SearcherDataSource) nextAgg() *query.Record {
	if !s.aggsScanned {
		if err := s.scanAgg(); err != nil {
			s.err = err
			return nil
		}
		s.aggsScanned = true
	}

	if s.agg == nil || s.curIdx >= len(s.agg.Timeseries) {
		return nil
	}

	record := makeAggRecord(s.agg.Timeseries[s.curIdx], s.agg.ValuesPool)

	s.curIdx++

	return record
}

func (s *SearcherDataSource) Ctx() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// scanBatch runs one SearchDocs iteration of the documents path with cursor
// pagination. The first batch honors searchParams.WithTotal (to capture the
// full total once); subsequent batches drop WithTotal and advance the cursor
// (OffsetId) from the previous batch's last ID, narrowing the time range.
func (s *SearcherDataSource) scanBatch() error {
	params := s.searchParams

	if s.fracs == nil {
		s.fracs, s.release = s.fracManager.AcquireFractionsInRange(params.From, params.To)
	}

	if params.Limit == 0 || params.Limit > searcherBatchLimit {
		params.Limit = searcherBatchLimit
	}

	if s.batchNo > 0 {
		// Advance the cursor
		params.WithTotal = false
		lastID := s.qpr.IDs[len(s.qpr.IDs)-1].ID
		params.OffsetId = lastID
	}

	qpr, err := s.searcher.SearchDocs(s.Ctx(), s.fracs, params, s.tr)
	if err != nil {
		return err
	}
	if err := qprErrors(qpr); err != nil {
		return err
	}

	if len(qpr.IDs) == 0 {
		s.done = true
		return nil
	}

	s.qpr = qpr
	s.curIdx = 0
	s.batchNo++
	if params.WithTotal {
		s.total = qpr.Total
	}
	return nil
}

// scanAgg runs the single scan-all SearchDocs for the aggregation path.
func (s *SearcherDataSource) scanAgg() error {
	s.fracs, s.release = s.fracManager.AcquireFractionsInRange(s.searchParams.From, s.searchParams.To)

	qpr, err := s.searcher.SearchDocs(s.Ctx(), s.fracs, s.searchParams, s.tr)
	if err != nil {
		return err
	}
	if err := qprErrors(qpr); err != nil {
		return err
	}

	s.qpr = qpr
	s.total = qpr.Total
	s.agg = buildAgg(qpr)
	return nil
}

// qprErrors joins all store-level errors reported in the QPR, if any.
func qprErrors(qpr *seq.QPR) error {
	if len(qpr.Errors) == 0 {
		return nil
	}
	var resErr error
	for _, e := range qpr.Errors {
		resErr = errors.Join(errors.New(e.ErrStr))
	}
	return resErr
}

func buildAgg(qpr *seq.QPR) *storeapi.SearchResponse_Agg {
	if len(qpr.Aggs) == 0 {
		return nil
	}
	// we expect only one agg
	fromAgg := qpr.Aggs[0]

	agg := &storeapi.SearchResponse_Agg{}
	from := fromAgg.SamplesByBin
	to := make(map[string]*storeapi.SearchResponse_Histogram, len(from))

	for bin, hist := range from {
		pbhist := &storeapi.SearchResponse_Histogram{
			Min:       hist.Min,
			Max:       hist.Max,
			Sum:       hist.Sum,
			Total:     hist.Total,
			Samples:   hist.Samples,
			NotExists: hist.NotExists,
		}

		if len(hist.Values) > 0 {
			pbhist.Values = make([]uint32, 0, len(hist.Values))
			for idx := range hist.Values {
				pbhist.Values = append(pbhist.Values, idx)
			}
		}

		agg.Timeseries = append(agg.Timeseries,
			&storeapi.SearchResponse_Bin{
				Label: bin.Token,
				Ts:    timestamppb.New(bin.MID.Time()),
				Hist:  pbhist,
			},
		)

		to[bin.Token] = pbhist
	}

	agg.NotExists = fromAgg.NotExists
	agg.AggHistogram = to
	agg.ValuesPool = fromAgg.ValuesPool

	return agg
}

func makeDocumentRecord(id seq.ID, payload []byte) *query.Record {
	return &query.Record{
		Vals: []*query.RecordVals{
			query.NewRecordVals(query.DataTypeSeqID, encoding.SeqIDToBytes(id)),
			query.NewRecordVals(query.DataTypeDocument, payload),
		},
	}
}

func makeAggRecord(bin *storeapi.SearchResponse_Bin, valuesPool []string) *query.Record {
	// For unique_count, the store holds unique field values as indices into the per-agg ValuesPool.
	// Resolve them to strings here so each record is self-contained, no shared pool.
	var values []string
	if len(bin.Hist.Values) > 0 && len(valuesPool) > 0 {
		values = make([]string, 0, len(bin.Hist.Values))
		for _, idx := range bin.Hist.Values {
			values = append(values, valuesPool[idx])
		}
	}
	return &query.Record{
		Vals: []*query.RecordVals{
			query.NewRecordVals(query.DataTypeBytes, []byte(bin.Label)),
			query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(bin.Hist.Min)),
			query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(bin.Hist.Max)),
			query.NewRecordVals(query.DataTypeFloat64, encoding.Float64ToBytes(bin.Hist.Sum)),
			query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(uint64(bin.Hist.Total))),
			query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(uint64(bin.Hist.NotExists))),
			query.NewRecordVals(query.DataTypeUint64, encoding.Uint64ToBytes(uint64(bin.Ts.AsTime().UnixNano()))),
			query.NewRecordVals(query.DataTypeFloat64Array, encoding.Float64ArrayToBytes(bin.Hist.Samples)),
			query.NewRecordVals(query.DataTypeStringArray, encoding.StringArrayToBytes(values)),
		},
	}
}
