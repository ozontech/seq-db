package util

import (
	"fmt"

	"github.com/alecthomas/units"
)

func ByteToHumanReadable(n uint64) string {
	switch {
	case n >= uint64(units.EiB):
		return fmt.Sprintf("%d EiB", n/uint64(units.EiB))
	case n >= uint64(units.PiB):
		return fmt.Sprintf("%d PiB", n/uint64(units.PiB))
	case n >= uint64(units.TiB):
		return fmt.Sprintf("%d TiB", n/uint64(units.TiB))
	case n >= uint64(units.GiB):
		return fmt.Sprintf("%d GiB", n/uint64(units.GiB))
	case n >= uint64(units.MiB):
		return fmt.Sprintf("%d MiB", n/uint64(units.MiB))
	case n >= uint64(units.KiB):
		return fmt.Sprintf("%d KiB", n/uint64(units.KiB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func ByteToEBytes(b int64) float64 {
	v := b / int64(units.EiB)
	r := b % int64(units.EiB)
	return float64(v) + float64(r)/float64(units.EiB)
}

func ByteToPBytes(b int64) float64 {
	v := b / int64(units.PiB)
	r := b % int64(units.PiB)
	return float64(v) + float64(r)/float64(units.PiB)
}

func ByteToTBytes(b int64) float64 {
	v := b / int64(units.TiB)
	r := b % int64(units.TiB)
	return float64(v) + float64(r)/float64(units.TiB)
}

func ByteToGBytes(b int64) float64 {
	v := b / int64(units.GiB)
	r := b % int64(units.GiB)
	return float64(v) + float64(r)/float64(units.GiB)
}

func ByteToMBytes(b int64) float64 {
	v := b / int64(units.MiB)
	r := b % int64(units.MiB)
	return float64(v) + float64(r)/float64(units.MiB)
}

func ByteToKBytes(b int64) float64 {
	v := b / int64(units.KiB)
	r := b % int64(units.KiB)
	return float64(v) + float64(r)/float64(units.KiB)
}
