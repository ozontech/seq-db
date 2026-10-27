package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"text/tabwriter"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/zstd"
)

type repackParams struct {
	level       int
	blockValues int
	typedZstd   bool
	top         int
}

type packed struct {
	blocks int
	raw    int
	bitpck int
	zstd   int
}

func (p *packed) add(o packed) {
	p.blocks += o.blocks
	p.raw += o.raw
	p.bitpck += o.bitpck
	p.zstd += o.zstd
}

func (p packed) disk() int {
	return p.zstd + p.blocks*storage.IndexBlockHeaderSize
}

type encoder struct {
	level         int
	typedZstd     bool
	freqThreshold uint32
	buf           []byte
	zbuf          []byte
	buf32         []uint32
	buf64         []uint64
}

func (e *encoder) compress(raw []byte) int {
	e.zbuf = zstd.CompressLevel(raw, e.zbuf[:0], e.level)
	return min(len(e.zbuf), len(raw))
}

func (e *encoder) appendFreqs(dst []byte, idx []uint16, freqs []uint32) []byte {
	if len(idx) == 0 {
		return dst
	}
	dst = packer.CompressDeltaBitpackUint16(dst, idx, e.buf32[:0])
	return packer.CompressDeltaBitpackUint32(dst, freqs, e.buf32[:0])
}

type stringPacker struct {
	enc   *encoder
	block token.Block
	size  int
	res   packed
}

func newStringPacker(enc *encoder) *stringPacker {
	return &stringPacker{enc: enc}
}

func (p *stringPacker) add(value string, freq uint32) {
	tokenSize := 4 + len(value)
	if p.size > 0 && p.size+tokenSize > consts.RegularBlockSize {
		p.flush()
	}
	if len(p.block.Offsets) == 0 {
		p.block.Offsets = append(p.block.Offsets, 0)
	}
	idx := len(p.block.Offsets) - 1
	p.block.Payload = append(p.block.Payload, value...)
	p.block.Offsets = append(p.block.Offsets, uint32(len(p.block.Payload)))
	if freq >= p.enc.freqThreshold {
		p.block.FreqIndexes = append(p.block.FreqIndexes, uint16(idx))
		p.block.Freqs = append(p.block.Freqs, freq)
	}
	p.size += tokenSize
	p.res.raw += len(value)
}

func (p *stringPacker) flush() {
	if p.size == 0 {
		return
	}
	p.enc.buf = p.block.Pack(p.enc.buf[:0], p.enc.buf32[:0])
	p.res.blocks++
	p.res.bitpck += len(p.enc.buf)
	p.res.zstd += p.enc.compress(p.enc.buf)

	p.block.Payload = p.block.Payload[:0]
	p.block.Offsets = p.block.Offsets[:0]
	p.block.FreqIndexes = p.block.FreqIndexes[:0]
	p.block.Freqs = p.block.Freqs[:0]
	p.size = 0
}

func (p *stringPacker) finish() packed {
	p.flush()
	return p.res
}

type typedItem struct {
	v    typedValue
	freq uint32
	raw  int
}

func packTyped(enc *encoder, class tokenClass, items []typedItem, blockValues int) packed {
	slices.SortFunc(items, func(a, b typedItem) int {
		if a.v.less(b.v) {
			return -1
		}
		if b.v.less(a.v) {
			return 1
		}
		return 0
	})

	var res packed
	for start := 0; start < len(items); start += blockValues {
		chunk := items[start:min(start+blockValues, len(items))]
		enc.buf = encodeTypedBlock(enc, class, chunk, enc.buf[:0])
		res.blocks++
		res.bitpck += len(enc.buf)
		if enc.typedZstd {
			res.zstd += enc.compress(enc.buf)
		} else {
			res.zstd += len(enc.buf)
		}
		for _, it := range chunk {
			res.raw += it.raw
		}
	}
	return res
}

func encodeTypedBlock(enc *encoder, class tokenClass, chunk []typedItem, dst []byte) []byte {
	dst = append(dst, byte(class))

	type run struct {
		param uint8
		from  int
		to    int
	}
	var runs []run
	for i, it := range chunk {
		if len(runs) == 0 || runs[len(runs)-1].param != it.v.param {
			runs = append(runs, run{param: it.v.param, from: i})
		}
		runs[len(runs)-1].to = i + 1
	}

	dst = binary.AppendUvarint(dst, uint64(len(runs)))
	for _, r := range runs {
		dst = append(dst, r.param)
		dst = binary.AppendUvarint(dst, uint64(r.to-r.from))
	}

	for _, r := range runs {
		values := enc.buf64[:0]
		for _, it := range chunk[r.from:r.to] {
			values = append(values, it.v.hi)
		}
		dst = packer.CompressDeltaBitpackUint64(dst, values, make([]uint64, 0, len(values)+1))
		enc.buf64 = values
	}

	for _, it := range chunk {
		if isWide(class, it.v) {
			dst = binary.LittleEndian.AppendUint64(dst, it.v.lo)
		}
	}

	var idx []uint16
	var freqs []uint32
	for i, it := range chunk {
		if it.freq >= enc.freqThreshold {
			idx = append(idx, uint16(i))
			freqs = append(freqs, it.freq)
		}
	}
	return enc.appendFreqs(dst, idx, freqs)
}

type fieldResult struct {
	field    string
	tokens   int
	baseline packed
	typed    packed
}

func repackGroup(enc *encoder, tokens []tokenEntry, blockValues int) (packed, [classCount]packed) {
	var perClass [classCount]packed
	str := newStringPacker(enc)
	typed := map[tokenClass][]typedItem{}
	for _, t := range tokens {
		if t.class == classString {
			str.add(t.value, t.freq)
			continue
		}
		_, v := parse(t.value)
		typed[t.class] = append(typed[t.class], typedItem{v: v, freq: t.freq, raw: len(t.value)})
	}
	perClass[classString] = str.finish()

	var total packed
	total.add(perClass[classString])
	for c, items := range typed {
		perClass[c] = packTyped(enc, c, items, blockValues)
		total.add(perClass[c])
	}
	return total, perClass
}

func baselineGroup(enc *encoder, tokens []tokenEntry, filter func(tokenEntry) bool) packed {
	p := newStringPacker(enc)
	for _, t := range tokens {
		if filter(t) {
			p.add(t.value, t.freq)
		}
	}
	return p.finish()
}

type sectionStats struct {
	blocks int
	bytes  int
	raw    int
}

func readIndexFile(path string) ([]sectionStats, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 16 {
		return nil, fmt.Errorf("index file too short: %s", path)
	}
	regPos := binary.LittleEndian.Uint64(data[0:8])
	regLen := binary.LittleEndian.Uint64(data[8:16])
	registry := data[regPos : regPos+regLen]

	sections := []sectionStats{{}}
	for i := 0; i+storage.IndexBlockHeaderSize <= len(registry); i += storage.IndexBlockHeaderSize {
		h := storage.IndexBlockHeader(registry[i : i+storage.IndexBlockHeaderSize])
		if h.Len() == 0 {
			sections = append(sections, sectionStats{})
			continue
		}
		s := &sections[len(sections)-1]
		s.blocks++
		s.bytes += int(h.Len())
		s.raw += int(h.RawLen())
	}
	return sections, nil
}

func runRepack(name string, params repackParams, w io.Writer) error {
	sections, err := readIndexFile(name + consts.TokenFileSuffix)
	if err != nil {
		return err
	}
	actual := sections[0]

	f, err := loadFraction(name)
	if err != nil {
		return err
	}

	enc := &encoder{
		level:         params.level,
		typedZstd:     params.typedZstd,
		freqThreshold: uint32(math.Ceil(float64(f.docsTotal) * consts.DefaultTokenFreqThresholdPercentage / 100)),
	}

	all := func(tokenEntry) bool { return true }
	baseline := baselineGroup(enc, f.tokens, all)

	var classBaseline [classCount]packed
	var classCounts [classCount]int
	for c := range classCount {
		classBaseline[c] = baselineGroup(enc, f.tokens, func(t tokenEntry) bool { return t.class == c })
	}
	for _, t := range f.tokens {
		classCounts[t.class]++
	}

	var typedTotal packed
	var classTyped [classCount]packed
	var fields []fieldResult
	for start := 0; start < len(f.tokens); {
		end := start
		for end < len(f.tokens) && f.tokens[end].field == f.tokens[start].field {
			end++
		}
		group := f.tokens[start:end]
		total, perClass := repackGroup(enc, group, params.blockValues)
		for c := range classCount {
			if c != classString {
				classTyped[c].add(perClass[c])
				typedTotal.add(perClass[c])
			}
		}
		fields = append(fields, fieldResult{
			field:    group[0].field,
			tokens:   len(group),
			baseline: baselineGroup(enc, group, all),
			typed:    total,
		})
		start = end
	}
	classTyped[classString] = classBaseline[classString]
	typedTotal.add(classBaseline[classString])

	fmt.Fprintf(w, "actual .tokens: token_blocks=%d (%s on disk, %s raw) table=%s\n",
		actual.blocks, size(actual.bytes), size(actual.raw), size(sections[len(sections)-1].bytes))
	fmt.Fprintf(w, "simulated baseline: blocks=%d raw_tokens=%s packed=%s zstd=%s disk(+headers)=%s\n",
		baseline.blocks, size(baseline.raw), size(baseline.bitpck), size(baseline.zstd), size(baseline.disk()))
	fmt.Fprintf(w, "simulated typed:    blocks=%d raw_tokens=%s packed=%s zstd=%s disk(+headers)=%s\n",
		typedTotal.blocks, size(typedTotal.raw), size(typedTotal.bitpck), size(typedTotal.zstd), size(typedTotal.disk()))
	fmt.Fprintf(w, "typed / baseline = %.3f (saved %s)\n\n",
		float64(typedTotal.disk())/float64(baseline.disk()), size(baseline.disk()-typedTotal.disk()))

	fmt.Fprintln(w, "per class: string-format cost vs typed cost (class tokens packed in isolation)")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "class\ttokens\traw\tstr_disk\tB/tok\ttyped_packed\ttyped_disk\tB/tok\tratio\t")
	for c := range classCount {
		b, t, n := classBaseline[c], classTyped[c], classCounts[c]
		if n == 0 {
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%.2f\t%s\t%s\t%.2f\t%.3f\t\n",
			c, n, size(b.raw), size(b.disk()), float64(b.disk())/float64(n),
			size(t.bitpck), size(t.disk()), float64(t.disk())/float64(n), float64(t.disk())/float64(b.disk()))
	}
	tw.Flush()

	slices.SortFunc(fields, func(a, b fieldResult) int {
		return (b.baseline.disk() - b.typed.disk()) - (a.baseline.disk() - a.typed.disk())
	})
	fmt.Fprintf(w, "\ntop %d fields by saved bytes (field packed in isolation)\n", params.top)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "field\ttokens\tstr_disk\ttyped_disk\tsaved\tratio\t")
	for _, fr := range fields[:min(params.top, len(fields))] {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%.3f\t\n",
			trim(fr.field, 28), fr.tokens, size(fr.baseline.disk()), size(fr.typed.disk()),
			size(fr.baseline.disk()-fr.typed.disk()), float64(fr.typed.disk())/float64(fr.baseline.disk()))
	}
	tw.Flush()

	return nil
}

func size(b int) string {
	const unit = 1024
	sign := ""
	if b < 0 {
		sign, b = "-", -b
	}
	switch {
	case b >= unit*unit:
		return fmt.Sprintf("%s%.2fMiB", sign, float64(b)/(unit*unit))
	case b >= unit:
		return fmt.Sprintf("%s%.1fKiB", sign, float64(b)/unit)
	}
	return fmt.Sprintf("%s%dB", sign, b)
}
