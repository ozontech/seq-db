package sealed

import (
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
)

type PreloadedData struct {
	Info       *common.Info
	BlocksData BlocksData
	TokenTable token.Table
}

type BlocksData struct {
	IDsTable      seqids.Table
	LIDsTable     *lids.Table
	BlocksOffsets []uint64
}
