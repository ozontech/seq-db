package exec

import (
	"context"
	"fmt"
	"math"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

// AggDataSource is a scan-all data source: a single SearchDocs request (no
// cursor, no limit) — aggregations require a full scan and the proxy-side
// DistributedAggregator merges complete results.
//
// searchParams.AggQ must be set: it drives both the aggregation computation
// itself and IsScanAllRequest(), which makes the searcher scan all fractions.
type AggDataSource struct {
	ctx context.Context
	tr  *querytracer.Tracer

	searchParams processor.SearchParams

	fracManager *fracmanager.FracManager
	searcher    *fracmanager.Searcher

	schema *query.Schema

	agg         *storeapi.SearchResponse_Agg
	aggsScanned bool

	curIdx int

	// total accumulates the final total.
	total        uint64
	aggNotExists uint64

	// fracs are acquired once and held for the producer's lifetime.
	fracs   fracmanager.List
	release func()

	// err holds the first error encountered during scanning. Reported via Finalize.
	err error
}

func NewAggDataSource(
	ctx context.Context,
	tr *querytracer.Tracer,
	searchParams processor.SearchParams,
	fracManager *fracmanager.FracManager,
	searcher *fracmanager.Searcher,
	schema *query.Schema,
) *AggDataSource {
	if ctx == nil {
		ctx = context.Background()
	}

	return &AggDataSource{
		ctx:          ctx,
		tr:           tr,
		searchParams: searchParams,
		fracManager:  fracManager,
		searcher:     searcher,
		schema:       schema,
	}
}

func (a *AggDataSource) Next() *query.Record {
	if !a.aggsScanned {
		if err := a.scanAgg(); err != nil {
			a.err = err
			return nil
		}
		a.aggsScanned = true
	}

	if a.agg == nil || a.curIdx >= len(a.agg.Timeseries) {
		return nil
	}

	record := a.makeRecord(a.agg.Timeseries[a.curIdx], a.agg.ValuesPool)

	a.curIdx++

	return record
}

func (a *AggDataSource) Finalize() *query.Summary {
	if a.release != nil {
		a.release()
	}
	return &query.Summary{Total: a.total, NotExists: a.aggNotExists, Err: a.err}
}

// scanAgg runs the single scan-all SearchDocs request.
func (a *AggDataSource) scanAgg() error {
	a.fracs, a.release = a.fracManager.AcquireFractionsInRange(a.searchParams.From, a.searchParams.To)

	qpr, err := a.searcher.SearchDocs(a.ctx, a.fracs, a.searchParams, a.tr)
	if err != nil {
		return err
	}
	if err := qprErrors(qpr); err != nil {
		return err
	}

	a.total = qpr.Total
	if len(qpr.Aggs) > 0 {
		a.aggNotExists = uint64(qpr.Aggs[0].NotExists)
	}
	a.agg = buildAgg(qpr, a.searchParams.AggQ[0].Interval > 0)
	return nil
}

func buildAgg(qpr *seq.QPR, timeseries bool) *storeapi.SearchResponse_Agg {
	if len(qpr.Aggs) == 0 {
		return nil
	}
	// we expect only one agg
	fromAgg := qpr.Aggs[0]

	agg := &storeapi.SearchResponse_Agg{}
	from := fromAgg.SamplesByBin

	for bin, hist := range from {
		if timeseries && bin.MID == consts.DummyMID {
			continue
		}

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
	}

	agg.ValuesPool = fromAgg.ValuesPool

	return agg
}

func (a *AggDataSource) makeRecord(bin *storeapi.SearchResponse_Bin, valuesPool []string) *query.Record {
	vals := make([]*query.RecordVals, 0, a.schema.Len())
	for _, col := range a.schema.Cols() {
		vals = append(vals, aggColumnVals(col.Name, bin, valuesPool))
	}
	return &query.Record{Vals: vals}
}

func aggColumnVals(name string, bin *storeapi.SearchResponse_Bin, valuesPool []string) *query.RecordVals {
	switch name {
	case "token":
		return query.NewStringVals(bin.Label)
	case "ts":
		return query.NewUint64Vals(uint64(bin.Ts.AsTime().UnixNano()))
	case "min":
		if bin.Hist.Total == 0 {
			return query.NewFloat64Vals(math.NaN())
		}
		return query.NewFloat64Vals(bin.Hist.Min)
	case "max":
		if bin.Hist.Total == 0 {
			return query.NewFloat64Vals(math.NaN())
		}
		return query.NewFloat64Vals(bin.Hist.Max)
	case "sum":
		return query.NewFloat64Vals(bin.Hist.Sum)
	case "total":
		return query.NewUint64Vals(uint64(bin.Hist.Total))
	case "not_exists":
		return query.NewUint64Vals(uint64(bin.Hist.NotExists))
	case "samples":
		return query.NewFloat64ArrayVals(bin.Hist.Samples)
	case "values":
		// For unique_count, the store holds unique field values as indices into
		// the per-agg ValuesPool. Resolve them to strings here so each record is
		// self-contained, no shared pool.
		var values []string
		if len(bin.Hist.Values) > 0 && len(valuesPool) > 0 {
			values = make([]string, 0, len(bin.Hist.Values))
			for _, idx := range bin.Hist.Values {
				values = append(values, valuesPool[idx])
			}
		}
		return query.NewStringArrayVals(values)
	default:
		panic(fmt.Errorf("BUG: unknown aggregation column %q", name))
	}
}
