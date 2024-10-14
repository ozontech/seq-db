package frac

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"go.uber.org/atomic"
)

type SearchCell struct {
	Explain   bool
	ExplainSB SearchExplainSB
	Exit      atomic.Bool

	Context context.Context

	mu     sync.Mutex
	Errors []error
}

func NewSearchCell(ctx context.Context) *SearchCell {
	return &SearchCell{Context: ctx}
}

func (s *SearchCell) IsCancelled() bool {
	return s.Context.Err() != nil
}

func (s *SearchCell) Cancel(err error) {
	s.AddError(err)
	s.Exit.Store(true)
}

func (s *SearchCell) AddError(err error) {
	s.mu.Lock()
	s.Errors = append(s.Errors, err)
	s.mu.Unlock()
}

func (s *SearchCell) AddComment(comment string) {
	s.mu.Lock()
	s.ExplainSB.Comments = append(s.ExplainSB.Comments, comment)
	s.mu.Unlock()
}

func (s *SearchCell) AddQueryParseTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.ParseTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddFractionsUsed(count int) {
	if s == nil {
		return
	}

	s.ExplainSB.FractionsUsed.Add(uint64(count))
}

func (s *SearchCell) AddReadIDTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.ReadIDTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddReadLIDTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.ReadLIDTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddReadFieldTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.ReadFieldTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddDecodeIDTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.DecodeIDTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddDecodeFieldTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.DecodeFieldTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddDecodeLIDTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.DecodeLIDTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddFieldCacheSetTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.FieldCacheSetTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddLIDBlocksSearchTimeNS(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.LIDBlocksSearchTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddMonoSeqs(count int) {
	if s == nil {
		return
	}

	s.ExplainSB.MonoSeqsUsed.Add(uint64(count))
}

func (s *SearchCell) AddMultiSeqs(count int) {
	if s == nil {
		return
	}

	s.ExplainSB.MultiSeqsUsed.Add(uint64(count))
}

func (s *SearchCell) AddOpsUsed(count int) {
	if s == nil {
		return
	}

	s.ExplainSB.OpsUsed.Add(uint64(count))
}

func (s *SearchCell) AddMemUsedBytes(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.MemUsedBytes.Add(count)
}

func (s *SearchCell) AddMemAllocatedBytes(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.MemAllocatedBytes.Add(count)
}

func (s *SearchCell) AddLIDBlocksRead(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.LIDBlocksRead.Add(count)
}

func (s *SearchCell) AddLIDBytesRead(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.LIDBytesRead.Add(count)
}

func (s *SearchCell) AddFieldBytesRead(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.FieldBytesRead.Add(count)
}

func (s *SearchCell) AddFieldBlocksRead(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.FieldBlocksRead.Add(count)
}

func (s *SearchCell) AddValsLoaded(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.ValsLoaded.Add(count)
}

func (s *SearchCell) AddOverallTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.OverallTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddEvaluationTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.EvaluationTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddMergeTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.MergeTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddParseTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.ParseTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddProvideTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.ProvideTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddSendTime(duration time.Duration) {
	if s == nil {
		return
	}

	s.ExplainSB.SendTimeNS.Add(uint64(duration.Nanoseconds()))
}

func (s *SearchCell) AddFound(count uint64) {
	if s == nil {
		return
	}

	s.ExplainSB.Found.Add(count)
}

type SearchExplainSB struct {
	Found atomic.Uint64

	LIDBytesRead    atomic.Uint64
	TIDBytesRead    atomic.Uint64
	FieldBytesRead  atomic.Uint64
	LIDBlocksRead   atomic.Uint64
	FieldBlocksRead atomic.Uint64
	ValsLoaded      atomic.Uint64

	OverallTimeNS    atomic.Uint64
	ParseTimeNS      atomic.Uint64
	ProvideTimeNS    atomic.Uint64
	EvaluationTimeNS atomic.Uint64
	MergeTimeNS      atomic.Uint64
	SendTimeNS       atomic.Uint64

	// by frac
	ReadIDTimeNS          atomic.Uint64
	ReadLIDTimeNS         atomic.Uint64
	ReadFieldTimeNS       atomic.Uint64
	DecodeIDTimeNS        atomic.Uint64
	DecodeFieldTimeNS     atomic.Uint64
	DecodeTIDTimeNS       atomic.Uint64
	DecodeLIDTimeNS       atomic.Uint64
	FieldCacheSetTimeNS   atomic.Uint64
	LIDBlocksSearchTimeNS atomic.Uint64

	FractionsUsed atomic.Uint64
	MonoSeqsUsed  atomic.Uint64
	MultiSeqsUsed atomic.Uint64
	OpsUsed       atomic.Uint64

	MemUsedBytes      atomic.Uint64
	MemAllocatedBytes atomic.Uint64

	Comments []string
}

func (sb SearchExplainSB) String() string {
	if sb.FractionsUsed.Load() == 0 {
		return ">>>>> EXPLAIN FRAC COUNT IS ZERO"
	}

	return fmt.Sprintf(
		">>>>> EXPLAIN\n"+
			"|BASE|\n	time=%.2fms\n	found=%d\n	speed=%d/sec\n	lid bytes read=%s\n	tid bytes read=%s\n	field bytes read=%s\n	lid blocks read=%d\n	field blocks read=%d\n	vals loaded=%d\n"+
			"|GLOBAL TIME|\n	parse=%.2fms\n	provide=%.2fms\n	eval=%.2fms\n	merge=%.2fms\n	send=%.2fms\n"+
			"|FRAC CUM|\n	read lid=%.2fms\n	read fields=%.2fms\n	decode tid=%.2fms\n	decode lid=%.2fms\n	decode field=%.2fms\n	field cache set=%.2fms\n	lid blocks search=%.2fms\n"+
			"|STATS|\n	fractions=%d\n	mono seqs=%d\n	multi seqs=%d\n	op tids=%d\n	op tids per frac=%d\n"+
			"|MEM|\n	used per frac=%s\n	alloc=%s\n",
		float64(sb.OverallTimeNS.Load())/float64(time.Millisecond),
		sb.Found.Load(),
		uint64(float64(sb.Found.Load())/(float64(sb.OverallTimeNS.Load())/float64(time.Second))),
		datasize.ByteSize(sb.LIDBytesRead.Load()).HR(),
		datasize.ByteSize(sb.TIDBytesRead.Load()).HR(),
		datasize.ByteSize(sb.FieldBytesRead.Load()).HR(),
		sb.LIDBlocksRead.Load(),
		sb.FieldBlocksRead.Load(),
		sb.ValsLoaded.Load(),
		float64(sb.ParseTimeNS.Load())/float64(time.Millisecond),
		float64(sb.ProvideTimeNS.Load())/float64(time.Millisecond),
		float64(sb.EvaluationTimeNS.Load())/float64(time.Millisecond),
		float64(sb.MergeTimeNS.Load())/float64(time.Millisecond),
		float64(sb.SendTimeNS.Load())/float64(time.Millisecond),

		float64(sb.ReadLIDTimeNS.Load())/float64(time.Millisecond),
		float64(sb.ReadFieldTimeNS.Load())/float64(time.Millisecond),
		float64(sb.DecodeTIDTimeNS.Load())/float64(time.Millisecond),
		float64(sb.DecodeLIDTimeNS.Load())/float64(time.Millisecond),
		float64(sb.DecodeFieldTimeNS.Load())/float64(time.Millisecond),
		float64(sb.FieldCacheSetTimeNS.Load())/float64(time.Millisecond),
		float64(sb.LIDBlocksSearchTimeNS.Load())/float64(time.Millisecond),

		sb.FractionsUsed.Load(),
		sb.MonoSeqsUsed.Load(),
		sb.MultiSeqsUsed.Load(),
		sb.OpsUsed.Load(),
		sb.OpsUsed.Load()/sb.FractionsUsed.Load(),
		datasize.ByteSize(sb.MemUsedBytes.Load()/sb.FractionsUsed.Load()).HR(),
		datasize.ByteSize(sb.MemAllocatedBytes.Load()).HR(),
	)
}
