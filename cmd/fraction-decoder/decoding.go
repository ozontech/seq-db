package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/storage"
)

const (
	kindInfo    = "info"
	kindDoc     = "doc"
	kindID      = "id"
	kindToken   = "token"
	kindOffsets = "offsets"
)

var decodableKinds = []string{kindInfo, kindDoc, kindID, kindToken, kindOffsets}

type infoRecord struct {
	Kind string `json:"kind"`
	*common.Info
}

func (r infoRecord) MarshalJSON() ([]byte, error) {
	info, err := json.Marshal(r.Info)
	if err != nil {
		return nil, err
	}

	// splice the kind field in front of the info fields, keeping
	// the record flat and the kind first
	out := make([]byte, 0, len(info)+len(`{"kind":"info",`))
	out = append(out, `{"kind":"`+kindInfo+`",`...)
	out = append(out, info[1:]...) // drop the opening '{'

	return out, nil
}

type docRecord struct {
	Kind string `json:"kind"`
	LID  uint32 `json:"lid"`
	Doc  string `json:"doc"`
}

type idRecord struct {
	Kind string `json:"kind"`
	LID  uint32 `json:"lid"`
	MID  uint64 `json:"mid"`
	RID  uint64 `json:"rid"`
	Pos  uint64 `json:"pos"`
}

type tokenRecord struct {
	Kind  string   `json:"kind"`
	TID   uint32   `json:"tid"`
	Field string   `json:"field"`
	Token string   `json:"token"`
	Freq  uint32   `json:"freq"`
	LIDs  []uint32 `json:"lids,omitempty"`
}

type offsetsRecord struct {
	Kind   string   `json:"kind"`
	Values []uint64 `json:"values"`
}

func parseOnly(only string) ([]string, error) {
	if only == "" {
		return decodableKinds, nil
	}

	var kinds []string
	for _, name := range strings.Split(only, ",") {
		if !slices.Contains(decodableKinds, name) {
			return nil, fmt.Errorf(
				"unknown section to decode: %s; known sections: %s",
				name,
				strings.Join(decodableKinds, ","),
			)
		}
		if !slices.Contains(kinds, name) {
			kinds = append(kinds, name)
		}
	}

	return kinds, nil
}

func decodeFraction(
	fracName string,
	only string,
	w io.Writer,
) error {
	kinds, err := parseOnly(only)
	if err != nil {
		return err
	}

	info, err := loadInfo(fracName)
	if err != nil {
		return err
	}

	f := frac.NewSealed(
		fracName,
		storage.NewReadLimiter(1, nil),
		frac.NewIndexCache(),
		cache.NewConcurrentCache[[]byte](nil, nil),
		info,
		&frac.Config{},
		stubSkipMaskProvider{},
	)
	defer f.Release()

	src := frac.NewSealedSource(f)

	buf := bufio.NewWriter(w)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)

	for _, kind := range decodableKinds {
		if !slices.Contains(kinds, kind) {
			continue
		}

		switch kind {
		case kindInfo:
			err = enc.Encode(infoRecord{Kind: kind, Info: info})
		case kindDoc:
			err = writeDocs(src, enc)
		case kindID:
			err = writeIDs(src, enc)
		case kindToken:
			err = writeTokens(src, enc, slices.Equal(kinds, []string{kindToken}))
		case kindOffsets:
			err = enc.Encode(offsetsRecord{Kind: kind, Values: src.BlockOffsets()})
		}
		if err != nil {
			return err
		}
	}

	return buf.Flush()
}

func writeDocs(src *frac.SealedSource, enc *json.Encoder) error {
	lid := uint32(0)
	for loc, err := range src.DocBlocks() {
		if err != nil {
			return err
		}

		payload, err := storage.DocBlock(loc.First).DecompressTo(nil)
		if err != nil {
			return fmt.Errorf("cannot decompress doc block: %w", err)
		}

		for len(payload) > 0 {
			l := binary.LittleEndian.Uint32(payload)

			// a leading empty doc is a placeholder for seq.SystemID (lid 0)
			if l > 0 {
				if err := enc.Encode(docRecord{
					Kind: kindDoc,
					LID:  lid,
					Doc:  string(payload[4 : 4+l]),
				}); err != nil {
					return err
				}
			}

			payload = payload[4+l:]
			lid++
		}
	}
	return nil
}

func writeIDs(src *frac.SealedSource, enc *json.Encoder) error {
	lid := uint32(0)
	for loc, err := range src.IDs() {
		if err != nil {
			return err
		}

		if err := enc.Encode(idRecord{
			Kind: kindID,
			LID:  lid,
			MID:  uint64(loc.First.MID),
			RID:  uint64(loc.First.RID),
			Pos:  uint64(loc.Second),
		}); err != nil {
			return err
		}

		lid++
	}

	return nil
}

func writeTokens(src *frac.SealedSource, enc *json.Encoder, withoutPostings bool) error {
	tid := uint32(1) // TID 0 is reserved for the system token
	for field, postings := range src.TokenTriplets() {
		for triplet, err := range postings {
			if err != nil {
				return err
			}

			rec := tokenRecord{
				Kind:  kindToken,
				TID:   tid,
				Field: field,
				Token: string(triplet.First),
				Freq:  uint32(len(triplet.Second)),
			}
			if !withoutPostings {
				rec.LIDs = triplet.Second
			}

			if err := enc.Encode(rec); err != nil {
				return err
			}

			tid++
		}
	}

	return nil
}

// loadInfo reads the fraction info and rewrites its recorded path with the
// actual one, so a fraction moved on disk still opens: frac.Sealed resolves
// file locations through info.Path, not through the name it was given.
// Legacy fractions keep info in the first block of their .index file, so
// the .info file is not expected to exist there.
func loadInfo(fracName string) (*common.Info, error) {
	if exists(fracName + consts.IndexFileSuffix) {
		legacyFile, err := os.Open(fracName + consts.IndexFileSuffix)
		if err != nil {
			return nil, fmt.Errorf("cannot open index file: %w", err)
		}
		defer legacyFile.Close()

		legacyReader := storage.NewIndexReader(
			storage.NewReadLimiter(1, nil),
			legacyFile.Name(),
			legacyFile,
			cache.NewConcurrentCache[[]byte](nil, nil),
		)

		block, _, err := legacyReader.ReadIndexBlock(0, nil)
		if err != nil {
			return nil, fmt.Errorf("cannot read info block: %w", err)
		}

		var bi sealed.BlockInfo
		if err := bi.Unpack(block); err != nil {
			return nil, fmt.Errorf("cannot unpack info block: %w", err)
		}

		bi.Info.Path = fracName

		st, err := legacyFile.Stat()
		if err != nil {
			return nil, fmt.Errorf("cannot stat index file: %w", err)
		}
		bi.Info.IndexOnDisk = uint64(st.Size())

		return bi.Info, nil
	}

	data, err := os.ReadFile(fracName + consts.InfoFileSuffix)
	if err != nil {
		return nil, fmt.Errorf("cannot load info: %w", err)
	}

	var bi sealed.BlockInfo
	if err := bi.Unpack(data); err != nil {
		return nil, fmt.Errorf("cannot unpack info block: %w", err)
	}

	bi.Info.Path = fracName

	for _, suffix := range []string{
		consts.TokenFileSuffix, consts.OffsetsFileSuffix,
		consts.IDFileSuffix, consts.LIDFileSuffix,
	} {
		st, err := os.Stat(fracName + suffix)
		if err != nil {
			continue
		}
		bi.Info.IndexOnDisk += uint64(st.Size())
	}

	return bi.Info, nil
}

func exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}
