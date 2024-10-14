package util

import (
	"time"

	"go.uber.org/zap"
)

// ZapUint64AsSizeStr forms string zap.Field with val converted to size string.
func ZapUint64AsSizeStr(key string, val uint64) zap.Field {
	return zap.String(key, SizeStr(val))
}

// ZapFloat64WithPrec forms float64 zap.Field with removed numerics after prec
// digit.
func ZapFloat64WithPrec(key string, val float64, prec uint32) zap.Field {
	return zap.Float64(key, Float64ToPrec(val, prec))
}

// ZapDurationWithPrec converts duration to unit and forms float64 zap.Field
// with removed numerics after prec digit.
func ZapDurationWithPrec(
	key string, dur time.Duration, unit string, prec uint32,
) zap.Field {
	return ZapFloat64WithPrec(key, DurationToUnit(dur, unit), prec)
}

// ZapMsTsAsESTimeStr converts timestamp in milliseconds to ES time format
// string and forms string zap.Field.
func ZapMsTsAsESTimeStr(key string, ts uint64) zap.Field {
	return zap.String(key, MsTsToESFormat(ts))
}
