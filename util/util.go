package util

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
)

func ByteToStringUnsafe(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func StringToByteUnsafe(str string) []byte { // this works fine
	var buf = *(*[]byte)(unsafe.Pointer(&str))
	(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Cap = len(str)
	return buf
}

func SizeStr(bytes uint64) string {
	return datasize.ByteSize(bytes).HR()
}

func RunEvery(done <-chan struct{}, runInterval time.Duration, actionFn func()) {
	runTicker := time.NewTicker(runInterval)
	defer runTicker.Stop()

	actionFn() // first launch without delay

	for {
		select {
		case <-done:
			return
		case <-runTicker.C:
			actionFn()
		}
	}
}

func IdxFill(n int) []int {
	idx := make([]int, n)
	for i := 0; i < n; i++ {
		idx[i] = i
	}
	return idx
}

func IdxShuffle(n int) []int {
	idx := IdxFill(n)

	rand.Shuffle(n, func(i, j int) {
		idx[i], idx[j] = idx[j], idx[i]
	})

	return idx
}

// Float64ToPrec formats float64 by removing numberics after prec digit.
func Float64ToPrec(val float64, prec uint32) float64 {
	precDigit := math.Pow10(int(prec))
	return float64(int64(val*precDigit)) / precDigit
}

// SizeToUnit converts size in bytes to unit and returns as float64.
func SizeToUnit(sizeVal uint64, unit string) float64 {
	val := datasize.ByteSize(sizeVal)
	switch strings.ToLower(unit) {
	case "kb":
		return val.KBytes()
	case "mb":
		return val.MBytes()
	case "gb":
		return val.GBytes()
	case "tb":
		return val.TBytes()
	case "pb":
		return val.PBytes()
	case "eb":
		return val.EBytes()
	default:
		logger.Panic("unsupported unit", zap.String("unit", unit))
		panic("_")
	}
}

// DurationToUnit converts duration to unit and returns as float64.
func DurationToUnit(durationVal time.Duration, unit string) float64 {
	switch strings.ToLower(unit) {
	case "us":
		return float64(durationVal) / float64(time.Microsecond)
	case "ms":
		return float64(durationVal) / float64(time.Millisecond)
	case "s":
		return durationVal.Seconds()
	case "m":
		return durationVal.Minutes()
	case "h":
		return durationVal.Hours()
	default:
		logger.Panic("unsupported unit", zap.String("unit", unit))
		panic("_")
	}
}

// MsTsToESFormat converts timestamp in milliseconds to ES time format string.
func MsTsToESFormat(ts uint64) string {
	return time.UnixMilli(int64(ts)).Format(consts.ESTimeFormat)
}

func BinSearchInRange(from, to int, fn func(i int) bool) int {
	n := to - from + 1
	i := sort.Search(n, func(i int) bool { return fn(from + i) })
	return from + i
}

// IsCancelled is a faster way to check if the context has been canceled, compared to ctx.Err() != nil
func IsCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func EnsureSliceSize[T any](src []T, size int) []T {
	if cap(src) < size {
		return make([]T, size, max(2*cap(src), size))
	}
	return src[:size]
}
