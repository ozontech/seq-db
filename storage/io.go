package storage

import (
	"context"
	"io"
	"os"
)

type Type string

const (
	TypeLocal  Type = "local"
	TypeRemote Type = "remote"
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
	Upload(context.Context, ImmutableFile) error
}
