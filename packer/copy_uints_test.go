package packer

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"testing"
	"unsafe"
)

// Reference implementations — the element-wise loops used before the memcpy
// change (introduced with the packer in #328). Kept in tests as the
// differential oracle: on little-endian targets the bulk copy must be
// byte-for-byte equivalent to the LE-decode loop.

func refCopyAsUints32(src []byte, dst []uint32) []uint32 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint32(src))
		src = src[sizeOfUint32:]
	}
	return dst
}

func refCopyAsUints64(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint64(src))
		src = src[8:]
	}
	return dst
}

func TestCopyAsUintsMatchesReference(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	sizes := []int{0, 1, 2, 7, 8, 9, 16, 127, 128, 129, 512, 4095, 4096}

	// Larger backing buffer so src can start at any byte offset: block payload
	// begins at +4 after the uint32 header in the real stream, and memmove
	// must handle every alignment.
	backing := make([]byte, 4096*8+16)
	for i := range backing {
		backing[i] = byte(r.Uint32())
	}

	for _, words := range sizes {
		for offset := 0; offset < 8; offset++ {
			src := backing[offset : offset+words*8]

			// dst modes: nil, undersized, exact, oversized with garbage.
			dsts64 := [][]uint64{
				nil,
				make([]uint64, 0, words/2),
				make([]uint64, 0, words),
				append(make([]uint64, 0, words+7), 0xDEAD, 0xBEEF)[:2],
			}
			want64 := refCopyAsUints64(src, nil)
			for i, dst := range dsts64 {
				got := copyAsUints64(src, dst)
				if len(got) != len(want64) {
					t.Fatalf("u64 words=%d offset=%d dst#%d: len %d != %d", words, offset, i, len(got), len(want64))
				}
				for j := range got {
					if got[j] != want64[j] {
						t.Fatalf("u64 words=%d offset=%d dst#%d: mismatch at %d", words, offset, i, j)
					}
				}
			}

			src32 := backing[offset : offset+words*4]
			dsts32 := [][]uint32{
				nil,
				make([]uint32, 0, words/2),
				make([]uint32, 0, words),
				append(make([]uint32, 0, words+7), 0xDEAD, 0xBEEF)[:2],
			}
			want32 := refCopyAsUints32(src32, nil)
			for i, dst := range dsts32 {
				got32 := copyAsUints32(src32, dst)
				if len(got32) != len(want32) {
					t.Fatalf("u32 words=%d offset=%d dst#%d: len %d != %d", words, offset, i, len(got32), len(want32))
				}
				for j := range got32 {
					if got32[j] != want32[j] {
						t.Fatalf("u32 words=%d offset=%d dst#%d: mismatch at %d", words, offset, i, j)
					}
				}
			}
		}
	}
}

// TestCopyAsUintsRaggedPanics pins the fail-fast contract: input whose length
// is not a multiple of the word size means a corrupted stream and panics, the
// same way the element-wise decode did (out-of-range on the short tail). All
// production callers validate the length before slicing.
func TestCopyAsUintsRaggedPanics(t *testing.T) {
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic on ragged input", name)
			}
		}()
		f()
	}
	src := make([]byte, 20)
	mustPanic("u64", func() { copyAsUints64(src, nil) })
	mustPanic("u32", func() { copyAsUints32(src[:19], nil) })
}

func FuzzCopyAsUints64(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add(bytes.Repeat([]byte{0xFF}, 8*129))
	f.Fuzz(func(t *testing.T, b []byte) {
		n := len(b) / 8 * 8
		want := refCopyAsUints64(b[:n], nil)
		got := copyAsUints64(b[:n], nil)
		if len(got) != len(want) {
			t.Fatalf("len %d != %d", len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("mismatch at %d", i)
			}
		}
	})
}

func FuzzCopyAsUints32(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, b []byte) {
		n := len(b) / 4 * 4
		want := refCopyAsUints32(b[:n], nil)
		got := copyAsUints32(b[:n], nil)
		if len(got) != len(want) {
			t.Fatalf("len %d != %d", len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("mismatch at %d", i)
			}
		}
	})
}

// TestDecompressBoundarySizes exercises the Decompress round-trip at the
// intcomp sub-block boundaries, where the raw-residual path (the copy IS the
// whole decode) switches over to the bitpacked path.
func TestDecompressBoundarySizes(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	for _, n := range []int{0, 1, 63, 127, 128, 129, 255, 256, 257, 4096} {
		vals32 := make([]uint32, n)
		vals64 := make([]uint64, n)
		v32, v64 := uint32(0), uint64(0)
		for i := 0; i < n; i++ {
			v32 += r.Uint32N(64)
			v64 += uint64(r.Uint32N(1 << 24))
			vals32[i] = v32
			vals64[i] = v64
		}

		data32 := CompressDeltaBitpackUint32(nil, vals32, nil)
		_, got32, err := DecompressDeltaBitpackUint32(data32, nil, nil)
		if err != nil {
			t.Fatalf("u32 n=%d: %v", n, err)
		}
		if len(got32) != n {
			t.Fatalf("u32 n=%d: got %d values", n, len(got32))
		}
		for i := range got32 {
			if got32[i] != vals32[i] {
				t.Fatalf("u32 n=%d: mismatch at %d", n, i)
			}
		}

		data64 := CompressDeltaBitpackUint64(nil, vals64, nil)
		_, got64, err := DecompressDeltaBitpackUint64(data64, nil, nil)
		if err != nil {
			t.Fatalf("u64 n=%d: %v", n, err)
		}
		if len(got64) != n {
			t.Fatalf("u64 n=%d: got %d values", n, len(got64))
		}
		for i := range got64 {
			if got64[i] != vals64[i] {
				t.Fatalf("u64 n=%d: mismatch at %d", n, i)
			}
		}
	}
}

// Benchmarks: variant=loop is the pre-change element-wise decode, variant=memcpy
// is the production implementation. Sizes reflect the real streams:
//   512w  = 4 KiB   — compressed LID-block scale;
//   1456w = 11.4 KiB — compressed MID-block stream (nanosecond MIDs, bitlen≈23);
//   4096w = 32 KiB  — a fully raw 4096-value block (upper bound).
func benchCopySizes() []struct {
	name  string
	words int
} {
	return []struct {
		name  string
		words int
	}{
		{"512w", 512},
		{"1456w", 1456},
		{"4096w", 4096},
	}
}

// indexedCopyAsUints64 is the safe-Go middle ground: the element-wise decode
// with indexed writes instead of append. Benchmarked to attribute the win
// honestly: how much comes from removing append bookkeeping vs from memmove.
func indexedCopyAsUints64(src []byte, dst []uint64) []uint64 {
	n := len(src) / 8
	if cap(dst) < n {
		dst = make([]uint64, n)
	}
	dst = dst[:n]
	for i := 0; i < n; i++ {
		dst[i] = binary.LittleEndian.Uint64(src[i*8 : i*8+8])
	}
	return dst
}

func BenchmarkCopyAsUints64(b *testing.B) {
	r := rand.New(rand.NewPCG(3, 4))
	for _, sz := range benchCopySizes() {
		src := make([]byte, sz.words*8)
		for i := range src {
			src[i] = byte(r.Uint32())
		}
		b.Run("variant=loop/size="+sz.name, func(b *testing.B) {
			dst := make([]uint64, 0, sz.words)
			b.SetBytes(int64(len(src)))
			for i := 0; i < b.N; i++ {
				dst = refCopyAsUints64(src, dst)
			}
		})
		b.Run("variant=indexed/size="+sz.name, func(b *testing.B) {
			dst := make([]uint64, 0, sz.words)
			b.SetBytes(int64(len(src)))
			for i := 0; i < b.N; i++ {
				dst = indexedCopyAsUints64(src, dst)
			}
		})
		b.Run("variant=memcpy/size="+sz.name, func(b *testing.B) {
			dst := make([]uint64, 0, sz.words)
			b.SetBytes(int64(len(src)))
			for i := 0; i < b.N; i++ {
				dst = copyAsUints64(src, dst)
			}
		})
	}
}

func BenchmarkCopyAsUints32(b *testing.B) {
	r := rand.New(rand.NewPCG(5, 6))
	for _, sz := range benchCopySizes() {
		src := make([]byte, sz.words*4)
		for i := range src {
			src[i] = byte(r.Uint32())
		}
		b.Run("variant=loop/size="+sz.name, func(b *testing.B) {
			dst := make([]uint32, 0, sz.words)
			b.SetBytes(int64(len(src)))
			for i := 0; i < b.N; i++ {
				dst = refCopyAsUints32(src, dst)
			}
		})
		b.Run("variant=memcpy/size="+sz.name, func(b *testing.B) {
			dst := make([]uint32, 0, sz.words)
			b.SetBytes(int64(len(src)))
			for i := 0; i < b.N; i++ {
				dst = copyAsUints32(src, dst)
			}
		})
	}
}

// BenchmarkDecompressLIDBlock is the end-to-end regression guard for the LID
// path: a full 65536-entry postings block of ascending uint32 LIDs with
// realistic gaps (a ~3% token) through the full Decompress.
func BenchmarkDecompressLIDBlock(b *testing.B) {
	r := rand.New(rand.NewPCG(11, 12))
	lids := make([]uint32, 65536)
	v := uint32(0)
	for i := range lids {
		v += 1 + r.Uint32N(60) // avg gap ~30
		lids[i] = v
	}
	data := CompressDeltaBitpackUint32(nil, lids, nil)

	// Pre-sized buffers model the production callers, which pass pooled
	// buffers with adequate capacity (lids/seqids unpack buffers).
	buf := make([]uint32, 0, 65536)
	compressed := make([]uint32, 0, 65536)
	b.SetBytes(65536 * 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		_, buf, err = DecompressDeltaBitpackUint32(data, buf, compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
	if buf[0] != lids[0] || buf[65535] != lids[65535] {
		b.Fatal("bad roundtrip")
	}
}

// BenchmarkDecompressSmallBlock covers small streams, where the raw-residual
// path dominates (values below one intcomp sub-block are stored verbatim, so
// the byte->word copy IS most of the decode). In production small streams come
// from the offsets arrays of blocks dominated by a single heavy token, the
// tail block of every fraction, and freshly sealed small fractions — NOT from
// short per-token postings: LID blocks are shared containers holding many
// tokens' lists (lids.Block{LIDs, Offsets}).
func BenchmarkDecompressSmallBlock(b *testing.B) {
	r := rand.New(rand.NewPCG(13, 14))
	for _, n := range []int{1, 16, 127} {
		vals := make([]uint32, n)
		v := uint32(0)
		for i := range vals {
			v += 1 + r.Uint32N(1000)
			vals[i] = v
		}
		data := CompressDeltaBitpackUint32(nil, vals, nil)
		b.Run("n="+itoa(n), func(b *testing.B) {
			buf := make([]uint32, 0, 256)
			compressed := make([]uint32, 0, 256)
			b.SetBytes(int64(n) * 4)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				_, buf, err = DecompressDeltaBitpackUint32(data, buf, compressed)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func itoa(v int) string {
	s := ""
	for v > 0 {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	if s == "" {
		s = "0"
	}
	return s
}

// rawMemmoveUints64 is the unconditional memmove path (no small-input loop) —
// test-only, used by BenchmarkSmallCopyCrossover to locate the loop/memmove
// crossover that sets smallCopyWords.
func rawMemmoveUints64(src []byte, dst []uint64) []uint64 {
	n := len(src) / 8
	if cap(dst) < n {
		dst = make([]uint64, n)
	}
	dst = dst[:n]
	if n == 0 {
		return dst
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), n*8), src)
	return dst
}

// BenchmarkSmallCopyCrossover justifies the smallCopyWords threshold: the
// element-wise loop vs the unconditional memmove on tiny inputs.
func BenchmarkSmallCopyCrossover(b *testing.B) {
	r := rand.New(rand.NewPCG(21, 22))
	src := make([]byte, 32*8)
	for i := range src {
		src[i] = byte(r.Uint32())
	}
	for _, words := range []int{1, 2, 4, 6, 8, 12, 16, 24, 32} {
		s := src[:words*8]
		b.Run("variant=loop/w="+itoa(words), func(b *testing.B) {
			dst := make([]uint64, 0, words)
			for i := 0; i < b.N; i++ {
				dst = refCopyAsUints64(s, dst)
			}
		})
		b.Run("variant=memmove/w="+itoa(words), func(b *testing.B) {
			dst := make([]uint64, 0, words)
			for i := 0; i < b.N; i++ {
				dst = rawMemmoveUints64(s, dst)
			}
		})
	}
}

// BenchmarkDecompressMIDBlock is the end-to-end regression guard for the MID
// path: a realistic 4096-value block of nanosecond MIDs (descending, ~3.6ms
// steps -> zigzag deltas, bitlen≈23) through the full Decompress.
func BenchmarkDecompressMIDBlock(b *testing.B) {
	r := rand.New(rand.NewPCG(7, 8))
	mids := make([]uint64, 4096)
	cur := uint64(1_754_600_000_000_000_000) // unix nanos scale
	for i := range mids {
		mids[i] = cur
		cur -= 3_400_000 + r.Uint64N(400_000) // 3.4-3.8ms in nanos
	}
	data := CompressDeltaBitpackUint64(nil, mids, nil)

	// Pre-sized buffers model the production callers (seqids unpackCache
	// allocates values/tmp with IDsPerBlock capacity).
	buf := make([]uint64, 0, 4096)
	compressed := make([]uint64, 0, 4096)
	b.SetBytes(4096 * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		_, buf, err = DecompressDeltaBitpackUint64(data, buf, compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
	if buf[0] != mids[0] || buf[4095] != mids[4095] {
		b.Fatal("bad roundtrip")
	}
}
