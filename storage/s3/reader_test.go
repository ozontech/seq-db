package s3

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeBytes(t *testing.T) {
	assert.Equal(t, `bytes=1-1`, rangeBytes(1, 1))
	assert.Equal(t, `bytes=0-9`, rangeBytes(0, 10))
	assert.Equal(t, `bytes=100-149`, rangeBytes(100, 50))
	assert.Panics(t, func() { rangeBytes(100, 0) })
}

func TestReader(t *testing.T) {
	bucket := uuid.New().String()

	s3cli, err := NewClient(
		"http://localhost:9000/",
		"minioadmin", "minioadmin",
		"us-east-1", bucket, 0,
	)
	require.NoError(t, err)

	_, err = s3cli.cli.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	f, data, remove := generateFileWithRandomContent(t)
	defer remove()

	require.NoError(t, NewUploader(s3cli).Upload(t.Context(), f))

	t.Run("read-whole-file", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		readData := make([]byte, len(data))
		_, err = reader.Read(readData)
		require.NoError(t, err)

		assert.Equal(t, data, readData)
	})

	t.Run("read-first-half", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		readData := make([]byte, len(data)/2)
		_, err = reader.ReadAt(readData, 0)
		require.NoError(t, err)

		assert.Equal(t, data[:len(data)/2], readData)
	})

	t.Run("read-second-half", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		readData := make([]byte, len(data)/2)
		_, err = reader.ReadAt(readData, int64(len(data)/2))
		require.NoError(t, err)

		assert.Equal(t, data[len(data)/2:], readData)
	})

	t.Run("read-out-of-bounds", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		readData := make([]byte, len(data))
		_, err = reader.ReadAt(readData, int64(len(data)+1))
		require.Error(t, err)
	})

	t.Run("seek-from-start-and-read", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		_, err := reader.Seek(int64(len(data)/2), io.SeekStart)
		require.NoError(t, err)

		readData := make([]byte, len(data)/2)
		_, err = reader.Read(readData)
		require.NoError(t, err)

		assert.Equal(t, data[len(data)/2:], readData)
	})

	t.Run("seek-from-current-and-read", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		// Move offset to the 1/2 of file.
		_, err := reader.Seek(int64(len(data)/2), io.SeekStart)
		require.NoError(t, err)

		// Move offset to the 3/4 of file.
		_, err = reader.Seek(int64(len(data)/4), io.SeekCurrent)
		require.NoError(t, err)

		// Read last 1/4 of file.
		readData := make([]byte, len(data)/4)
		_, err = reader.Read(readData)
		require.NoError(t, err)

		assert.Equal(t, data[3*len(data)/4:], readData)
	})

	t.Run("seek-from-end-and-read", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		_, err := reader.Seek(-int64(len(data)), io.SeekEnd)
		require.NoError(t, err)

		readData := make([]byte, len(data))
		_, err = reader.Read(readData)
		require.NoError(t, err)

		assert.Equal(t, data, readData)
	})

	t.Run("invalid-seek", func(t *testing.T) {
		reader := NewReader(t.Context(), s3cli, path.Base(f.Name()))

		_, err := reader.Seek(-1, io.SeekStart)
		require.Error(t, err)

		_, err = reader.Seek(int64(len(data)+1), io.SeekStart)
		require.Error(t, err)

		_, err = reader.Seek(-1, io.SeekCurrent)
		require.Error(t, err)

		_, err = reader.Seek(int64(len(data)+1), io.SeekCurrent)
		require.Error(t, err)

		_, err = reader.Seek(-int64(len(data)+1), io.SeekEnd)
		require.Error(t, err)

		_, err = reader.Seek(1, io.SeekEnd)
		require.Error(t, err)
	})
}

func generateFileWithRandomContent(t *testing.T) (*os.File, []byte, func()) {
	t.Helper()

	f, err := os.CreateTemp("", "")
	require.NoError(t, err)

	data := []byte(uuid.NewString())

	_, err = f.Write(data)
	require.NoError(t, err)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	return f, data, func() {
		os.Remove(f.Name())
	}
}
