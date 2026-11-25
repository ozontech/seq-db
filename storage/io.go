package storage

import (
	"context"
	"io"
	"os"
)

type ImmutableFile interface {
	Name() string
	Stat() (os.FileInfo, error)

	io.Reader
	io.ReaderAt

	io.Seeker
	io.Closer
}

type Uploader interface {
	Upload(ctx context.Context, key string, file ImmutableFile) error
}
