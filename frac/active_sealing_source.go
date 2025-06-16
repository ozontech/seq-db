package frac

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"time"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

type SealParams struct {
	IDsZstdLevel           int
	LIDsZstdLevel          int
	TokenListZstdLevel     int
	DocsPositionsZstdLevel int
	TokenTableZstdLevel    int

	DocBlocksZstdLevel int // DocBlocksZstdLevel is the zstd compress level of each document block.
	DocBlockSize       int // DocBlockSize is decompressed payload size of document block.
}

type ActiveSealingSource struct {
	info          *Info
	created       time.Time
	sortedLIDs    []uint32
	mids          *UInt64s
	rids          *UInt64s
	fields        []string
	fieldsMaxTIDs []uint32
	tids          []uint32
	tokens        [][]byte
	lids          []*TokenLIDs
	docsFile      *os.File
	positions     map[seq.ID]seq.DocPos
	blocksOffsets []uint64
}

func NewActiveSealingSource(active *Active, params SealParams) (*ActiveSealingSource, error) {
	info := *active.info // copy
	sortedLIDs := active.GetAllDocuments()
	sortedFields, fieldsMaxTIDs := sortFields(active.TokenList)
	sortedTIDs := sortTokens(sortedFields, active.TokenList)

	src := ActiveSealingSource{
		info:          &info,
		created:       time.Now(),
		sortedLIDs:    sortedLIDs,
		mids:          active.MIDs,
		rids:          active.RIDs,
		fields:        sortedFields,
		tids:          sortedTIDs,
		fieldsMaxTIDs: fieldsMaxTIDs,
		tokens:        active.TokenList.TidToVal,
		lids:          active.TokenList.tidToLIDs,
		docsFile:      active.docsFile,
		positions:     active.DocsPositions.positions,
		blocksOffsets: active.DocBlocks.vals,
	}

	src.prepareInfo()

	if !active.Config.SkipSortDocs {
		logger.Info("sorting docs...")
		if err := src.SortDocs(active.sortReader, params.DocBlockSize, params.DocBlocksZstdLevel); err != nil {
			return nil, err
		}
	}

	return &src, nil
}

func sortFields(tl *TokenList) ([]string, []uint32) {
	fields := make([]string, 0, len(tl.FieldTIDs))
	for field := range tl.FieldTIDs {
		fields = append(fields, field)
	}
	slices.Sort(fields)

	pos := 0
	maxTIDs := make([]uint32, 0, len(fields))
	for _, field := range fields {
		pos += len(tl.FieldTIDs[field])
		maxTIDs = append(maxTIDs, uint32(pos))
	}

	return fields, maxTIDs
}

func sortTokens(sortedFields []string, tl *TokenList) []uint32 {
	pos := 0
	tids := make([]uint32, 0, len(tl.TidToVal))
	for _, field := range sortedFields {
		tids = append(tids, tl.FieldTIDs[field]...)
		chunk := tids[pos:]
		slices.SortFunc(chunk, func(i, j uint32) int {
			a := tl.TidToVal[chunk[i]]
			b := tl.TidToVal[chunk[j]]
			return bytes.Compare(a, b)
		})
		pos = len(tids)
	}
	return tids
}

func (src ActiveSealingSource) prepareInfo() {
	src.info.MetaOnDisk = 0
	src.info.SealingTime = uint64(src.created.UnixMilli())
	src.info.BuildDistributionIter(src.IDs())
}

func (src ActiveSealingSource) Info() *Info {
	return src.info
}

func (src ActiveSealingSource) DocFile() *os.File {
	return src.docsFile
}

func (src ActiveSealingSource) Tokens() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, tid := range src.tids {
			if !yield(src.tokens[tid]) {
				return
			}
		}
	}
}

func (src ActiveSealingSource) Fields() iter.Seq2[string, uint32] {
	return func(yield func(string, uint32) bool) {
		for i, field := range src.fields {
			if !yield(field, src.fieldsMaxTIDs[i]) {
				return
			}
		}
	}
}

func (src ActiveSealingSource) IDs() iter.Seq[seq.ID] {
	return func(yield func(seq.ID) bool) {
		mids := src.mids.vals
		rids := src.rids.vals
		if !yield(seq.ID{MID: seq.MID(mids[0]), RID: seq.RID(rids[0])}) {
			return
		}
		for _, lid := range src.sortedLIDs {
			if !yield(seq.ID{MID: seq.MID(mids[lid]), RID: seq.RID(rids[lid])}) {
				return
			}
		}
	}
}

func (src ActiveSealingSource) Pos() iter.Seq[seq.DocPos] {
	return func(yield func(seq.DocPos) bool) {
		for id := range src.IDs() {
			if !yield(src.positions[id]) {
				return
			}
		}
	}
}

func (src ActiveSealingSource) BlocksOffsets() []uint64 {
	return src.blocksOffsets
}

func (src ActiveSealingSource) TokenLIDs() iter.Seq[iter.Seq[uint32]] {
	return func(yield func(iter.Seq[uint32]) bool) {
		oldToNewLIDsIndex := src.inverser()
		for _, tid := range src.tids {
			lids := src.lids[tid].GetLIDs(src.mids, src.rids)
			f := func(yield func(uint32) bool) {
				for _, lid := range lids {
					if !yield(oldToNewLIDsIndex[lid]) {
						return
					}
				}
			}
			if !yield(f) {
				return
			}
		}
	}
}

func (src ActiveSealingSource) inverser() []uint32 {
	inverser := make([]uint32, len(src.sortedLIDs))
	for i, lid := range src.sortedLIDs {
		inverser[lid] = uint32(i + 1)
	}
	return inverser
}

func (src *ActiveSealingSource) SortDocs(reader disk.DocsReader, blockSize, compressLevel int) error {
	sdocsFile, err := os.Create(src.info.Path + consts.SdocsTmpFileSuffix)
	if err != nil {
		return err
	}

	bw := bytespool.AcquireWriterSize(sdocsFile, consts.MB)
	defer bytespool.FlushReleaseWriter(bw)

	blocksOffsets, positions, err := src.writeDocs(bw, reader, blockSize, compressLevel)
	if err != nil {
		return err
	}
	bw.Flush()

	stat, err := sdocsFile.Stat()
	if err != nil {
		return err
	}

	// update for index
	src.positions = positions
	src.blocksOffsets = blocksOffsets
	src.info.DocsOnDisk = uint64(stat.Size())

	if sdocsFile, err = SyncRename(sdocsFile, src.info.Path+consts.SdocsFileSuffix); err != nil {
		return err
	}
	src.docsFile = sdocsFile

	return nil
}

func (src *ActiveSealingSource) writeDocs(w io.Writer, reader disk.DocsReader, blockSize, compressLevel int) ([]uint64, map[seq.ID]seq.DocPos, error) {
	blockIndex := uint32(0)

	docsBuf := make([]byte, 0, blockSize)
	blockBuf := make([]byte, 0, blockSize)

	newPositions := make(map[seq.ID]seq.DocPos, len(src.positions))
	newBlocksOffsets := make([]uint64, 0, len(src.blocksOffsets))

	offset := 0
	sortedDocs, checkErr := src.sortedDocs(reader)
	for id, doc := range sortedDocs {
		if len(docsBuf) >= blockSize {
			newBlocksOffsets[blockIndex] = uint64(offset)
			blockBuf = disk.CompressDocBlock(docsBuf, blockBuf[:0], compressLevel)
			if _, err := w.Write(blockBuf); err != nil {
				return nil, nil, err
			}
			blockIndex++
			offset += len(blockBuf)
			docsBuf = docsBuf[:0]
		}
		newPositions[id] = seq.PackDocPos(blockIndex, uint64(len(docsBuf)))
		docsBuf = append(docsBuf, doc...)
	}
	if err := checkErr(); err != nil {
		return nil, nil, err
	}
	// last block
	newBlocksOffsets[blockIndex] = uint64(offset)
	blockBuf = disk.CompressDocBlock(docsBuf, blockBuf[:0], compressLevel)
	if _, err := w.Write(blockBuf); err != nil {
		return nil, nil, err
	}

	return newBlocksOffsets, newPositions, nil
}

type iteratorError func() error

func (src *ActiveSealingSource) sortedDocs(reader disk.DocsReader) (iter.Seq2[seq.ID, []byte], iteratorError) {
	var err error
	return func(yield func(seq.ID, []byte) bool) {
		var lastID seq.ID
		for id := range src.IDs() {
			if id == lastID { // IDs have duplicates in case of nested index.
				continue // In this case we need to store the original document once.
			}
			lastID = id

			pos, ok := src.positions[id]
			if !ok || pos == seq.DocPosNotFound {
				err = fmt.Errorf("BUG: can't find doc position")
				return
			}

			blockIndex, docOffset := pos.Unpack()
			blockOffset := src.blocksOffsets[blockIndex]

			stop := false
			err := reader.ReadDocsFunc(blockOffset, []uint64{docOffset}, func(doc []byte) error {
				if stop {
					return nil
				}
				stop = !yield(id, doc)
				return nil
			})
			if stop || err != nil {
				return
			}
		}
	}, func() error { return err }
}

func SyncRename(f *os.File, newName string) (*os.File, error) {
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(f.Name(), newName); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return os.OpenFile(newName, os.O_RDONLY, 0o666) // reopen with new name
}
