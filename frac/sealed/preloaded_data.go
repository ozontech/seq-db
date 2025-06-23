package sealed

import (
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed/ids"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
)

type PreloadedData struct {
	Info          *common.Info
	IDsTable      ids.Table
	LIDsTable     *lids.Table
	TokenTable    token.Table
	BlocksOffsets []uint64
}
