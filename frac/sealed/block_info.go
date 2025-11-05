package sealed

import (
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

const seqDBMagic = "SEQM"

type BlockInfo struct {
	Info *common.Info
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

	b.Info = &common.Info{}
	if err := json.Unmarshal(data[4:], b.Info); err != nil {
		return errors.New("stats unmarshaling error")
	}
	b.Info.MetaOnDisk = 0 // todo: make this correction on sealing and remove this next time

	// legacy format - MID in milliseconds
	if b.Info.BinaryDataVer < config.BinaryDataV2 {
		b.Info.From = seq.MillisToMID(uint64(b.Info.From))
		b.Info.To = seq.MillisToMID(uint64(b.Info.To))
	}

	return nil
}
