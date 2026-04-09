package frac

import (
	"iter"
	"slices"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

// SealedSource implements [indexwriter.Source] for a sealed fraction.
// Used as input to [compaction.MergeSource] when compacting multiple fractions.
type SealedSource struct {
	f *Sealed

	idsProvider *seqids.Provider
	lidsLoader  *lids.Loader

	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader

	lastErr error
}

func NewSealedSource(f *Sealed) *SealedSource {
	f.load()
	return &SealedSource{
		f: f,
		idsProvider: seqids.NewProvider(
			&f.idReader,
			f.indexCache.MIDs,
			f.indexCache.RIDs,
			f.indexCache.Params,
			&f.blocksData.IDsTable,
			f.info.BinaryDataVer,
		),
		lidsLoader:       lids.NewLoader(&f.lidReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, &f.tokenReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, &f.tokenReader, f.indexCache.TokenTable),
	}
}

func (s *SealedSource) Info() *common.Info {
	return s.f.info
}

func (s *SealedSource) BlockOffsets() []uint64 {
	return s.f.blocksData.BlocksOffsets
}

func (s *SealedSource) ID() iter.Seq2[seq.ID, seq.DocPos] {
	return func(yield func(seq.ID, seq.DocPos) bool) {
		for lid := uint32(0); lid < s.f.blocksData.IDsTable.IDsTotal; lid++ {
			mid, err := s.idsProvider.MID(seq.LID(lid))
			if err != nil {
				s.lastErr = err
				return
			}

			rid, err := s.idsProvider.RID(seq.LID(lid))
			if err != nil {
				s.lastErr = err
				return
			}

			pos, err := s.idsProvider.DocPos(seq.LID(lid))
			if err != nil {
				s.lastErr = err
				return
			}

			if !yield(seq.ID{MID: mid, RID: rid}, pos) {
				return
			}
		}
	}
}

func (s *SealedSource) TokenTriplet() iter.Seq2[string, iter.Seq2[[]byte, []uint32]] {
	tokenTable := s.tokenTableLoader.Load()

	fields := make([]string, 0, len(tokenTable))
	for field := range tokenTable {
		fields = append(fields, field)
	}

	slices.Sort(fields)
	return func(yield func(string, iter.Seq2[[]byte, []uint32]) bool) {
		for _, field := range fields {
			if !yield(field, s.tokensForField(field)) {
				return
			}
		}
	}
}

func (s *SealedSource) tokensForField(field string) iter.Seq2[[]byte, []uint32] {
	lidsTable := s.f.blocksData.LIDsTable
	tokenTable := s.tokenTableLoader.Load()

	var lidsbuf []uint32
	return func(yield func([]byte, []uint32) bool) {
		for _, entry := range tokenTable[field].Entries {
			block := s.tokenBlockLoader.Load(entry.BlockIndex)

			for tid := entry.StartTID; tid < entry.StartTID+entry.ValCount; tid++ {
				lidsbuf = lidsbuf[:0]

				tokenVal := block.GetToken(entry.GetIndexInTokensBlock(tid))
				firstBlock := lidsTable.GetFirstBlockIndexForTID(tid)
				lastBlock := lidsTable.GetLastBlockIndexForTID(tid)

				for bi := firstBlock; bi <= lastBlock; bi++ {
					lidBlock, err := s.lidsLoader.GetLIDsBlock(bi)
					if err != nil {
						s.lastErr = err
						return
					}

					chunkIdx := lidsTable.GetChunkIndex(bi, tid)
					lidsbuf = append(lidsbuf, lidBlock.LIDs[lidBlock.Offsets[chunkIdx]:lidBlock.Offsets[chunkIdx+1]]...)
				}

				if !yield(tokenVal, lidsbuf) {
					return
				}
			}
		}
	}
}

func (s *SealedSource) DocBlock() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		// We do not want to cache payload of DocBlock because
		// it will just pollute cache and cause unnecessary evictions.
		r := storage.NewDocBlocksReader(s.f.readLimiter, s.f.docsFile)

		for _, offset := range s.f.blocksData.BlocksOffsets {
			// Read DocBlock payload (including its header) but do not decompress it.
			// Caller of [SealedSource.DocBlock] will decide whether it requires decompressed data.
			payload, _, err := r.ReadDocBlock(int64(offset))
			if err != nil {
				s.lastErr = err
				return
			}

			if !yield(payload) {
				return
			}
		}
	}
}

func (s *SealedSource) LastError() error {
	return s.lastErr
}
