package frac

import (
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

const seqDBMagic = "SEQM"

type BlockInfo struct {
	Info *Info
}

func (b *BlockInfo) Pack(buf []byte) []byte {
	buf = append(buf, []byte(seqDBMagic)...)

	bin, err := json.Marshal(b.Info)
	if err != nil {
		logger.Fatal("info marshaling error", zap.Error(err))
	}

	buf = append(buf, bin...)
	return buf
}

func (b *BlockInfo) Unpack(data []byte) error {
	if len(data) < 4 || string(data[:4]) != seqDBMagic {
		return errors.New("seq-db index file header corrupted")
	}

	b.Info = &Info{}
	if err := json.Unmarshal(data[4:], b.Info); err != nil {
		return errors.New("stats unmarshaling error")
	}
	b.Info.MetaOnDisk = 0 // todo: make this correction on sealing and remove this next time

	return nil
}
