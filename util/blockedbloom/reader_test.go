package blockedbloom

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLayout(groups int) bloomLayout {
	return bloomLayout{blocks: groups * blocksPerGroup, probes: 7}
}

func hashesByGroup(layout bloomLayout) []uint64 {
	hashes := make([]uint64, layout.groupCount())
	found := make([]bool, len(hashes))

	for i, missing := uint64(0), len(hashes); missing > 0; i++ {
		hash := hashUint64(i)
		group := layout.blockIndex(hash) / blocksPerGroup
		if !found[group] {
			hashes[group], found[group] = hash, true
			missing--
		}
	}

	return hashes
}

func encodeFilter(t *testing.T, filter *Filter) []byte {
	t.Helper()

	var encoded bytes.Buffer
	_, err := filter.WriteTo(&encoded)
	require.NoError(t, err)

	return encoded.Bytes()
}

type bloomRead struct {
	offset int64
	size   int
}

type recordingBloomReader struct {
	io.ReaderAt
	reads []bloomRead
}

func (r *recordingBloomReader) ReadAt(p []byte, offset int64) (int, error) {
	r.reads = append(r.reads, bloomRead{offset: offset, size: len(p)})
	return r.ReaderAt.ReadAt(p, offset)
}

func TestBlockReader(t *testing.T) {
	for _, groups := range []int{1, 9, 1008, 1009} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			layout := testLayout(groups)
			filter := newFilter(layout)

			keys := layout.blocks * 10
			for i := range keys {
				filter.Add(hashUint64(uint64(i)))
			}

			file, err := os.Create(filepath.Join(t.TempDir(), "bloom"))
			require.NoError(t, err)
			defer file.Close()

			written, err := filter.WriteTo(file)
			require.NoError(t, err)

			info, err := file.Stat()
			require.NoError(t, err)
			require.Equal(t, info.Size(), written)

			source := &recordingBloomReader{ReaderAt: file}
			diskFilter, err := Open(source)
			require.NoError(t, err)

			lookup := func(hash uint64) bool {
				source.reads = source.reads[:0]
				found, err := diskFilter.MayContain(hash)
				require.NoError(t, err)

				require.Len(t, source.reads, 1)
				require.Equal(t, groupSize, source.reads[0].size)
				require.Zero(t, source.reads[0].offset%groupSize)

				return found
			}

			for i := 0; i < keys; i += max(1, keys/10_000) {
				require.True(t, lookup(hashUint64(uint64(i))), "false negative for key %d", i)
			}

			rejected := 0
			for i := range 10_000 {
				hash := hashUint64(uint64(keys + i))
				found := lookup(hash)
				require.Equal(t, filter.MayContain(hash), found, "absent key %d", i)
				if !found {
					rejected++
				}
			}
			assert.NotZero(t, rejected)
		})
	}
}

func TestBlockReaderChecksumMismatch(t *testing.T) {
	const target = 2
	layout := testLayout(4)
	hashes := hashesByGroup(layout)

	filter := newFilter(layout)
	for _, hash := range hashes {
		filter.Add(hash)
	}
	encoded := encodeFilter(t, filter)

	groupStart := layout.dataOffset() + target*groupSize
	checksumStart := headerSize + target*checksumSize

	for _, tc := range []struct {
		name    string
		corrupt func(data []byte)
	}{
		{
			name:    "bit flip in group",
			corrupt: func(data []byte) { data[groupStart+groupSize/2] ^= 1 },
		},
		{
			name:    "zeroed group",
			corrupt: func(data []byte) { clear(data[groupStart : groupStart+groupSize]) },
		},
		{
			name: "zeroed group and checksum",
			corrupt: func(data []byte) {
				clear(data[groupStart : groupStart+groupSize])
				clear(data[checksumStart : checksumStart+checksumSize])
			},
		},
		{
			name:    "bit flip in checksum",
			corrupt: func(data []byte) { data[checksumStart] ^= 1 },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Clone(encoded)
			tc.corrupt(data)

			diskFilter, err := Open(bytes.NewReader(data))
			require.NoError(t, err)

			for group, hash := range hashes {
				found, err := diskFilter.MayContain(hash)
				if group == target {
					assert.ErrorIs(t, err, ErrChecksumMismatch)
					assert.False(t, found)
					continue
				}

				assert.NoError(t, err, "intact group %d", group)
				assert.True(t, found, "intact group %d", group)
			}
		})
	}
}

func TestOpenCorruptedHeader(t *testing.T) {
	encoded := encodeFilter(t, newFilter(testLayout(1)))

	for bit := range headerSize * 8 {
		data := bytes.Clone(encoded)
		data[bit/8] ^= 1 << (bit % 8)

		source := &recordingBloomReader{ReaderAt: bytes.NewReader(data)}
		_, err := Open(source)
		require.Error(t, err, "header bit %d", bit)
		require.Len(t, source.reads, 1, "header bit %d", bit)
	}
}
