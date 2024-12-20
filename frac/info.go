package frac

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"time"

	"github.com/c2h5oh/datasize"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

const DistributionMaxInterval = 24 * time.Hour
const DistributionBucket = time.Minute
const DistributionSpreadThreshold = 10 * time.Minute

type BinaryDataVersion uint16

const (
	// BinaryDataV0 - initial version
	BinaryDataV0 BinaryDataVersion = iota
	// BinaryDataV1 - support RIDs encoded without varint
	BinaryDataV1
)

type Info struct {
	Path          string            `json:"name"`
	Ver           string            `json:"ver"`
	BinaryDataVer BinaryDataVersion `json:"binary_data_ver"`
	DocsTotal     uint32            `json:"docs_total"`
	DocsOnDisk    uint64            `json:"docs_on_disk"`  // how much compressed docs data is stored on disk
	DocsRaw       uint64            `json:"docs_raw"`      // how much raw docs data is appended
	MetaOnDisk    uint64            `json:"meta_on_disk"`  // how much compressed metadata is stored on disk
	IndexOnDisk   uint64            `json:"index_on_disk"` // how much compressed index data is stored on disk

	ConstRegularBlockSize uint64 `json:"const_regular_block_size"`
	ConstIDsPerBlock      uint64 `json:"const_ids_per_block"`
	ConstLIDBlockCap      uint64 `json:"const_lid_block_cap"`

	From         seq.MID               `json:"from"`
	To           seq.MID               `json:"to"`
	CreationTime uint64                `json:"creation_time"`
	SealingTime  uint64                `json:"sealing_time"`
	Distribution *seq.MIDsDistribution `json:"distribution"`
}

func NewInfo(ct time.Time, basePath string) Info {
	return Info{
		Ver:                   buildinfo.Version,
		BinaryDataVer:         BinaryDataV1,
		Path:                  basePath,
		From:                  math.MaxUint64,
		To:                    0,
		CreationTime:          uint64(ct.UnixMilli()),
		ConstIDsPerBlock:      consts.IDsPerBlock,
		ConstRegularBlockSize: consts.RegularBlockSize,
		ConstLIDBlockCap:      consts.LIDBlockCap,
	}
}

func (i *Info) String() string {
	return fmt.Sprintf(
		"raw docs=%s, disk docs=%s",
		datasize.ByteSize(i.DocsRaw).HR(),
		datasize.ByteSize(i.DocsOnDisk).HR(),
	)
}

func (i *Info) Load(data []byte) {
	err := json.Unmarshal(data, i)
	if err != nil {
		logger.Panic("stats unmarshalling error", zap.Error(err))
	}
}

func (i *Info) Save() []byte {
	result, err := json.Marshal(i)
	if err != nil {
		logger.Panic("stats marshaling error", zap.Error(err))
	}

	return result
}

func (i Info) Name() string {
	if i.Path == "" {
		return ""
	}
	return path.Base(i.Path)
}

func (i *Info) BuildDistribution(ids []seq.ID) {
	if !i.InitEmptyDistribution() {
		return
	}
	for _, id := range ids {
		i.Distribution.Add(id.MID)
	}
}

func (i *Info) InitEmptyDistribution() bool {
	from := time.UnixMilli(int64(i.From))
	creationTime := time.UnixMilli(int64(i.CreationTime))
	if creationTime.Sub(from) < DistributionSpreadThreshold { // no big spread in past
		return false
	}

	distTo := creationTime
	distFrom := from

	if distTo.Sub(distFrom) > DistributionMaxInterval {
		distFrom = distTo.Add(-DistributionMaxInterval)
	}

	i.Distribution = seq.NewMIDsDistribution(distFrom, distTo, DistributionBucket)
	return true
}

func (i Info) IsIntersecting(from, to seq.MID) bool {
	if i.DocsTotal == 0 { // don't include fresh active fraction
		return false
	}

	if to < i.From || i.To < from {
		return false
	}

	if i.Distribution == nil { // can't check distribution
		return true
	}

	// check with distribution
	return i.Distribution.IsIntersecting(from, to)
}

func InfoToString(info Info, fracType string) string {
	s := fmt.Sprintf(
		"%s fraction name=%s, creation time=%s, from=%s, to=%s, %s",
		fracType,
		info.Name(),
		time.UnixMilli(int64(info.CreationTime)).Format(consts.ESTimeFormat),
		info.From,
		info.To,
		info.String(),
	)
	return s
}
