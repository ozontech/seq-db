package proxyapi

import (
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/proxy/bulk"
	"github.com/ozontech/seq-db/proxy/search"
)

type APIConfig struct {
	SearchTimeout  time.Duration
	ExportTimeout  time.Duration
	QueryRateLimit float64
	EsVersion      string
	// GatewayAddr is grpc-gateway client address. Used for debugging purposes.
	GatewayAddr string
}

type IngestorConfig struct {
	API    APIConfig
	Search search.Config
	Bulk   bulk.IngestorConfig
}

func (c *IngestorConfig) setDefaults() {
	if c.API.SearchTimeout == 0 {
		c.API.SearchTimeout = consts.DefaultSearchTimeout
		logger.Warn("wrong searchTimeout value (0) is fixed", zap.Duration("new_value", c.API.SearchTimeout))
	}
	if c.API.ExportTimeout == 0 {
		c.API.ExportTimeout = consts.DefaultExportTimeout
		logger.Warn("wrong exportTimeout value (0) is fixed", zap.Duration("new_value", c.API.ExportTimeout))
	}
	if c.Bulk.MaxInflightBulks == 0 {
		c.Bulk.MaxInflightBulks = consts.IngestorMaxInflightBulks
		logger.Warn("wrong maxInflightBulks value (0) is fixed", zap.Int("new_value", c.Bulk.MaxInflightBulks))
	}
}
