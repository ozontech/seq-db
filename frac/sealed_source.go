package frac

import (
	"iter"
	"slices"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type DocBlockLocation = util.Pair[[]byte, uint64]

// SealedSource implements [indexwriter.Source] for a sealed fraction.
// Used as input to [compaction.MergeSource] when compacting multiple fractions.
type SealedSource struct {
	f *Sealed

	idsProvider *seqids.Provider
	lidsLoader  *lids.Loader

	tokenBlockLoader *token.BlockLoader
	tokenTableLoader *token.TableLoader
}

func NewSealedSource(f *Sealed) *SealedSource {
	f.init(true)

	idReader := f.mustGetReader(f.idReaderProvider)
	lidReader := f.mustGetReader(f.lidReaderProvider)
	tokenReader := f.mustGetReader(f.tokenReaderProvider)

	return &SealedSource{
		f: f,
		idsProvider: seqids.NewProvider(
			&idReader,
			f.indexCache.MIDs,
			f.indexCache.RIDs,
			f.indexCache.Params,
			&f.blocksData.IDsTable,
			f.info.BinaryDataVer,
		),
		lidsLoader:       lids.NewLoader(f.Info().BinaryDataVer, &lidReader, f.indexCache.LIDs),
		tokenBlockLoader: token.NewBlockLoader(f.BaseFileName, f.Info().BinaryDataVer, &tokenReader, f.indexCache.Tokens),
		tokenTableLoader: token.NewTableLoader(f.BaseFileName, f.Info().BinaryDataVer, f.IsLegacy, &tokenReader, f.indexCache.TokenTable),
	}
}

func (s *SealedSource) Info() *common.Info {
	return s.f.info
}

func (s *SealedSource) BlockOffsets() []uint64 {
	return s.f.blocksData.BlocksOffsets
}

func (s *SealedSource) IDs() iter.Seq2[indexwriter.DocLocation, error] {
	return func(yield func(indexwriter.DocLocation, error) bool) {
		for lid := uint32(0); lid < s.f.blocksData.IDsTable.IDsTotal; lid++ {
			mid, err := s.idsProvider.MID(seq.LID(lid))
			if err != nil {
				yield(indexwriter.DocLocation{}, err)
				return
			}

			rid, err := s.idsProvider.RID(seq.LID(lid))
			if err != nil {
				yield(indexwriter.DocLocation{}, err)
				return
			}

			pos, err := s.idsProvider.DocPos(seq.LID(lid))
			if err != nil {
				yield(indexwriter.DocLocation{}, err)
				return
			}

			if !yield(indexwriter.DocLocation{First: seq.ID{MID: mid, RID: rid}, Second: pos}, nil) {
				return
			}
		}
	}
}

func (s *SealedSource) TokenTriplets() iter.Seq2[string, iter.Seq2[indexwriter.TokenLIDs, error]] {
	tokenTable := s.tokenTableLoader.Load()

	fields := make([]string, 0, len(tokenTable))
	for field := range tokenTable {
		fields = append(fields, field)
	}

	slices.Sort(fields)
	return func(yield func(string, iter.Seq2[indexwriter.TokenLIDs, error]) bool) {
		for _, field := range fields {
			if !yield(field, s.postingsForField(field)) {
				return
			}
		}
	}
}

func (s *SealedSource) postingsForField(field string) iter.Seq2[indexwriter.TokenLIDs, error] {
	lidsTable := s.f.blocksData.LIDsTable
	tokenTable := s.tokenTableLoader.Load()

	var lidsBuf []uint32
	return func(yield func(indexwriter.TokenLIDs, error) bool) {
		for _, entry := range tokenTable[field].Entries {
			block := s.tokenBlockLoader.Load(entry.BlockIndex)

			for tid := entry.StartTID; tid <= entry.GetLastTID(); tid++ {
				lidsBuf = lidsBuf[:0]

				tokenVal := block.GetToken(entry.GetIndexInTokensBlock(tid))
				firstBlock := lidsTable.GetFirstBlockIndexForTID(tid)
				lastBlock := lidsTable.GetLastBlockIndexForTID(tid)

				for bi := firstBlock; bi <= lastBlock; bi++ {
					lidBlock, err := s.lidsLoader.GetLIDsBlock(bi)
					if err != nil {
						yield(indexwriter.TokenLIDs{}, err)
						return
					}

					chunkIdx := lidsTable.GetChunkIndex(bi, tid)
					lidsBuf = lidBlock.AppendLIDsTo(chunkIdx, lidsBuf)
				}

				if !yield(indexwriter.TokenLIDs{First: tokenVal, Second: lidsBuf}, nil) {
					return
				}
			}
		}
	}
}

func (s *SealedSource) DocBlocks() iter.Seq2[DocBlockLocation, error] {
	return func(yield func(DocBlockLocation, error) bool) {
		// We do not want to cache payload of DocBlock because
		// it will just pollute cache and cause unnecessary evictions.
		r := storage.NewDocBlocksReader(s.f.readLimiter, s.f.docsFile)

		for _, offset := range s.f.blocksData.BlocksOffsets {
			// Read DocBlock payload (including its header) but do not decompress it.
			// Caller of [SealedSource.DocBlock] will decide whether it requires decompressed data.
			payload, _, err := r.ReadDocBlock(int64(offset))
			if err != nil {
				yield(DocBlockLocation{}, err)
				return
			}

			loc := DocBlockLocation{First: payload, Second: offset}
			if !yield(loc, nil) {
				return
			}
		}
	}
}
