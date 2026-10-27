package main

import (
	"bytes"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/zstd"
)

const (
	benchZstdLevel   = 3
	benchBlockValues = 2048
)

type stringBlock struct {
	compressed []byte
	payload    []byte
	offsets    []uint32
}

type typedBlock struct {
	bitpack    []byte
	compressed []byte
	values     []uint64
	minDigits  int
	maxDigits  int
}

type scanData struct {
	tokens  int
	strings []stringBlock
	typed   []typedBlock
}

var loadScanData = sync.OnceValues(func() (*scanData, error) {
	name := os.Getenv("TOKENSTAT_FRAC")
	field := os.Getenv("TOKENSTAT_FIELD")
	if field == "" {
		field = "request"
	}

	f, err := loadFraction(name)
	if err != nil {
		return nil, err
	}

	var ints []string
	for _, t := range f.tokens {
		if t.field == field && t.class == classInt64 {
			ints = append(ints, t.value)
		}
	}

	d := &scanData{tokens: len(ints)}
	d.strings = buildStringBlocks(ints)
	d.typed = buildTypedBlocks(ints)
	return d, nil
})

func buildStringBlocks(values []string) []stringBlock {
	var (
		res   []stringBlock
		block token.Block
		size  int
		buf32 []uint32
	)

	flush := func() {
		if size == 0 {
			return
		}
		raw := block.Pack(nil, buf32)
		res = append(res, stringBlock{
			compressed: zstd.CompressLevel(raw, nil, benchZstdLevel),
			payload:    slices.Clone(block.Payload),
			offsets:    slices.Clone(block.Offsets),
		})
		block.Payload = block.Payload[:0]
		block.Offsets = block.Offsets[:0]
		size = 0
	}

	for _, v := range values {
		if size > 0 && size+4+len(v) > consts.RegularBlockSize {
			flush()
		}
		if len(block.Offsets) == 0 {
			block.Offsets = append(block.Offsets, 0)
		}
		block.Payload = append(block.Payload, v...)
		block.Offsets = append(block.Offsets, uint32(len(block.Payload)))
		size += 4 + len(v)
	}
	flush()
	return res
}

func buildTypedBlocks(values []string) []typedBlock {
	keys := make([]uint64, 0, len(values))
	for _, v := range values {
		_, tv := parse(v)
		keys = append(keys, tv.hi)
	}
	slices.Sort(keys)

	var res []typedBlock
	for start := 0; start < len(keys); start += benchBlockValues {
		chunk := slices.Clone(keys[start:min(start+benchBlockValues, len(keys))])
		bitpack := packer.CompressDeltaBitpackUint64(nil, chunk, make([]uint64, 0, len(chunk)+1))
		lo, hi := int64(chunk[0]^signBit), int64(chunk[len(chunk)-1]^signBit)
		minDigits, maxDigits := intDigits(lo), intDigits(hi)
		if lo < 0 {
			minDigits, maxDigits = 1, max(intDigits(lo), intDigits(hi))
		}
		res = append(res, typedBlock{
			bitpack:    bitpack,
			compressed: zstd.CompressLevel(bitpack, nil, benchZstdLevel),
			values:     chunk,
			minDigits:  minDigits,
			maxDigits:  maxDigits,
		})
	}
	return res
}

func intDigits(v int64) int {
	if v < 0 {
		v = -v
	}
	n := 1
	for v >= 10 {
		v /= 10
		n++
	}
	return n
}

var benchSink int

func scanStrings(payload []byte, offsets []uint32, needle []byte) int {
	matched := 0
	for i := 1; i < len(offsets); i++ {
		if bytes.Contains(payload[offsets[i-1]:offsets[i]], needle) {
			matched++
		}
	}
	return matched
}

func scanInts(values []uint64, needle, scratch []byte) int {
	matched := 0
	for _, k := range values {
		scratch = strconv.AppendInt(scratch[:0], int64(k^signBit), 10)
		if bytes.Contains(scratch, needle) {
			matched++
		}
	}
	return matched
}

func BenchmarkScan(b *testing.B) {
	if os.Getenv("TOKENSTAT_FRAC") == "" {
		b.Skip("TOKENSTAT_FRAC is not set")
	}
	d, err := loadScanData()
	if err != nil {
		b.Fatal(err)
	}

	for _, needle := range []string{"9", "69", "163", "1638", "537421", "1022114211629000"} {
		nd := []byte(needle)

		b.Run("needle="+needle+"/layout=string/cold", func(b *testing.B) {
			var (
				raw   []byte
				block token.Block
				ub    token.UnpackBuffer
			)
			for b.Loop() {
				for _, sb := range d.strings {
					raw, _ = zstd.Decompress(sb.compressed, raw[:0])
					block.Offsets = block.Offsets[:0]
					if err := block.Unpack(raw, config.BinaryDataV6, &ub); err != nil {
						b.Fatal(err)
					}
					benchSink += scanStrings(block.Payload, block.Offsets, nd)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=string/hot", func(b *testing.B) {
			for b.Loop() {
				for _, sb := range d.strings {
					benchSink += scanStrings(sb.payload, sb.offsets, nd)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/cold-zstd", func(b *testing.B) {
			var (
				raw     []byte
				values  []uint64
				buf     []uint64
				scratch = make([]byte, 0, 24)
			)
			for b.Loop() {
				for _, tb := range d.typed {
					raw, _ = zstd.Decompress(tb.compressed, raw[:0])
					_, values, _ = packer.DecompressDeltaBitpackUint64(raw, values, buf)
					benchSink += scanInts(values, nd, scratch)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/cold", func(b *testing.B) {
			var (
				values  []uint64
				buf     []uint64
				scratch = make([]byte, 0, 24)
			)
			for b.Loop() {
				for _, tb := range d.typed {
					_, values, _ = packer.DecompressDeltaBitpackUint64(tb.bitpack, values, buf)
					benchSink += scanInts(values, nd, scratch)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/cold-pruned", func(b *testing.B) {
			var (
				values  []uint64
				buf     []uint64
				scratch = make([]byte, 0, 24)
			)
			nv, _ := strconv.ParseInt(needle, 10, 64)
			key := uint64(nv) ^ signBit
			for b.Loop() {
				for _, tb := range d.typed {
					if tb.maxDigits < len(nd) {
						continue
					}
					_, values, _ = packer.DecompressDeltaBitpackUint64(tb.bitpack, values, buf)
					if tb.minDigits == len(nd) && tb.maxDigits == len(nd) {
						if _, ok := slices.BinarySearch(values, key); ok {
							benchSink++
						}
						continue
					}
					benchSink += scanInts(values, nd, scratch)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/hot", func(b *testing.B) {
			scratch := make([]byte, 0, 24)
			for b.Loop() {
				for _, tb := range d.typed {
					benchSink += scanInts(tb.values, nd, scratch)
				}
			}
			reportPerToken(b, d.tokens)
		})
	}
}

const bcdWidth = 10

var bcd100 = func() (t [100]byte) {
	for i := range t {
		t[i] = byte(i/10)<<4 | byte(i%10)
	}
	return t
}()

func appendBCD(dst []byte, keys []uint64) []byte {
	start := len(dst)
	dst = slices.Grow(dst, len(keys)*bcdWidth)[:start+len(keys)*bcdWidth]
	for i, k := range keys {
		cell := dst[start+i*bcdWidth : start+(i+1)*bcdWidth]
		for j := range cell {
			cell[j] = 0xFF
		}
		x := int64(k ^ signBit)
		neg := x < 0
		v := uint64(x)
		if neg {
			v = uint64(-x)
		}
		pos := bcdWidth
		for v >= 100 {
			pos--
			cell[pos] = bcd100[v%100]
			v /= 100
		}
		pos--
		if v >= 10 {
			cell[pos] = bcd100[v]
		} else {
			cell[pos] = 0xF0 | byte(v)
		}
		if neg {
			if cell[pos]>>4 == 0xF {
				cell[pos] = 0xE0 | cell[pos]&0x0F
			} else {
				cell[pos-1] = 0xFE
			}
		}
	}
	return dst
}

type bcdPattern struct {
	core     []byte
	hasHead  bool
	head     byte
	hasTail  bool
	tail     byte
	headSkip int
}

func newBCDPatterns(needle string) [2]bcdPattern {
	d := make([]byte, len(needle))
	for i := range needle {
		d[i] = needle[i] - '0'
	}

	build := func(digits []byte) (core []byte, hasTail bool, tail byte) {
		for i := 0; i+1 < len(digits); i += 2 {
			core = append(core, digits[i]<<4|digits[i+1])
		}
		if len(digits)%2 == 1 {
			return core, true, digits[len(digits)-1]
		}
		return core, false, 0
	}

	var p [2]bcdPattern
	p[0].core, p[0].hasTail, p[0].tail = build(d)
	p[1].core, p[1].hasTail, p[1].tail = build(d[1:])
	p[1].hasHead, p[1].head, p[1].headSkip = true, d[0], 1
	return p
}

type bcdNeedle struct {
	digits   []byte
	patterns [2]bcdPattern
}

func newBCDNeedle(needle string) bcdNeedle {
	d := make([]byte, len(needle))
	for i := range needle {
		d[i] = needle[i] - '0'
	}
	return bcdNeedle{digits: d, patterns: newBCDPatterns(needle)}
}

func searchBCDShort(buf []byte, d []byte) int {
	matched := 0
	last := -1
	for k, b := range buf {
		var ok bool
		if len(d) == 1 {
			ok = b>>4 == d[0] || b&0x0F == d[0]
		} else {
			ok = b == d[0]<<4|d[1] || (b&0x0F == d[0] && k+1 < len(buf) && buf[k+1]>>4 == d[1])
		}
		if ok {
			if idx := k / bcdWidth; idx != last {
				last = idx
				matched++
			}
		}
	}
	return matched
}

func searchBCD(buf []byte, nd bcdNeedle) int {
	if len(nd.digits) <= 2 {
		return searchBCDShort(buf, nd.digits)
	}

	var seen [benchBlockValues / 64]uint64
	matched := 0
	for _, p := range nd.patterns {
		for off := 0; off < len(buf); {
			i := bytes.Index(buf[off:], p.core)
			if i < 0 {
				break
			}
			pos := off + i
			off = pos + 1
			if p.hasHead && (pos == 0 || buf[pos-1]&0x0F != p.head) {
				continue
			}
			end := pos + len(p.core)
			if p.hasTail && (end >= len(buf) || buf[end]>>4 != p.tail) {
				continue
			}
			idx := (pos - p.headSkip) / bcdWidth
			if bit := uint64(1) << (idx % 64); seen[idx/64]&bit == 0 {
				seen[idx/64] |= bit
				matched++
			}
		}
	}
	return matched
}

func appendBatch(dst []byte, keys []uint64) []byte {
	for _, k := range keys {
		dst = strconv.AppendInt(dst, int64(k^signBit), 10)
		dst = append(dst, ' ')
	}
	return dst
}

func searchBatch(buf, needle []byte) int {
	matched := 0
	for off := 0; off < len(buf); {
		i := bytes.Index(buf[off:], needle)
		if i < 0 {
			break
		}
		matched++
		next := bytes.IndexByte(buf[off+i:], ' ')
		if next < 0 {
			break
		}
		off += i + next + 1
	}
	return matched
}

func BenchmarkScanBCD(b *testing.B) {
	if os.Getenv("TOKENSTAT_FRAC") == "" {
		b.Skip("TOKENSTAT_FRAC is not set")
	}
	d, err := loadScanData()
	if err != nil {
		b.Fatal(err)
	}

	hotBCD := make([][]byte, len(d.typed))
	hotBatch := make([][]byte, len(d.typed))
	for i, tb := range d.typed {
		hotBCD[i] = appendBCD(nil, tb.values)
		hotBatch[i] = appendBatch(nil, tb.values)
	}

	for _, needle := range []string{"9", "69", "163", "1638", "537421", "1022114211629000"} {
		nd := []byte(needle)
		bn := newBCDNeedle(needle)

		want := 0
		for _, tb := range d.typed {
			want += scanInts(tb.values, nd, nil)
		}
		gotBCD, gotBatch := 0, 0
		for i := range d.typed {
			gotBCD += searchBCD(hotBCD[i], bn)
			gotBatch += searchBatch(hotBatch[i], nd)
		}
		if gotBCD != want || gotBatch != want {
			b.Fatalf("needle %s: want %d matches, bcd %d, batch %d", needle, want, gotBCD, gotBatch)
		}

		b.Run("needle="+needle+"/layout=typed/cold-bcd", func(b *testing.B) {
			var (
				values []uint64
				buf    []uint64
				bcd    []byte
			)
			for b.Loop() {
				for _, tb := range d.typed {
					_, values, _ = packer.DecompressDeltaBitpackUint64(tb.bitpack, values, buf)
					bcd = appendBCD(bcd[:0], values)
					benchSink += searchBCD(bcd, bn)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/hot-bcd", func(b *testing.B) {
			for b.Loop() {
				for _, buf := range hotBCD {
					benchSink += searchBCD(buf, bn)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/cold-batch", func(b *testing.B) {
			var (
				values []uint64
				buf    []uint64
				batch  []byte
			)
			for b.Loop() {
				for _, tb := range d.typed {
					_, values, _ = packer.DecompressDeltaBitpackUint64(tb.bitpack, values, buf)
					batch = appendBatch(batch[:0], values)
					benchSink += searchBatch(batch, nd)
				}
			}
			reportPerToken(b, d.tokens)
		})

		b.Run("needle="+needle+"/layout=typed/hot-batch", func(b *testing.B) {
			for b.Loop() {
				for _, buf := range hotBatch {
					benchSink += searchBatch(buf, nd)
				}
			}
			reportPerToken(b, d.tokens)
		})
	}
}

func reportPerToken(b *testing.B, tokens int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(tokens), "ns/token")
}
