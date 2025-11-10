package seq

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/ozontech/seq-db/util"
)

type ID struct {
	MID MID
	RID RID
}

type MID uint64 // nanoseconds part of ID
type RID uint64 // random part of ID
type LID uint32 // local id for a fraction

func (m MID) Time() time.Time {
	nanos := uint64(m)
	nanosPerSec := uint64(time.Second)
	secondsPart := nanos / nanosPerSec
	nanosPart := nanos - secondsPart*nanosPerSec
	return time.Unix(int64(secondsPart), int64(nanosPart))
}

func (d ID) String() string {
	return util.ByteToStringUnsafe(d.Bytes())
}

func (d ID) Equal(id ID) bool {
	return d.MID == id.MID && d.RID == id.RID
}

func (d ID) Time() string {
	return fmt.Sprintf("%d", d.MID)
}

func (d ID) Bytes() []byte {
	numBuf := make([]byte, 8)
	hexBuf := make([]byte, 64)

	binary.LittleEndian.PutUint64(numBuf, uint64(d.MID))
	n := hex.Encode(hexBuf, numBuf)

	final := append(make([]byte, 0), hexBuf[:n]...)
	final = append(final, '_')

	binary.LittleEndian.PutUint64(numBuf, uint64(d.RID))
	n = hex.Encode(hexBuf, numBuf)

	final = append(final, hexBuf[:n]...)

	return final
}

func LessOrEqual(a, b ID) bool {
	if a.MID == b.MID {
		return a.RID <= b.RID
	}
	return a.MID < b.MID
}

func Less(a, b ID) bool {
	if a.MID == b.MID {
		return a.RID < b.RID
	}
	return a.MID < b.MID
}

func FromString(x string) (ID, error) {
	id := ID{}
	if len(x) != 33 {
		return id, fmt.Errorf("wrong id len, should be 33")
	}

	mid, err := hex.DecodeString(x[:16])
	if err != nil {
		return id, err
	}

	rid, err := hex.DecodeString(x[17:])

	if err != nil {
		return id, err
	}

	delimiter := x[16]
	if delimiter == '-' {
		// legacy format, MID in millis
		id.MID = MillisToMID(binary.LittleEndian.Uint64(mid))
	} else if delimiter == '_' {
		id.MID = MID(binary.LittleEndian.Uint64(mid))
	} else {
		return id, fmt.Errorf("unknown delimiter %c", delimiter)
	}
	id.RID = RID(binary.LittleEndian.Uint64(rid))

	return id, nil
}

func SimpleID(i int64) ID {
	return ID{
		MID: MID(i),
		RID: 0,
	}
}

func MillisToMID(millis uint64) MID {
	if millis <= math.MaxUint64/uint64(time.Millisecond) {
		return MID(millis * uint64(time.Millisecond))
	} else {
		// math.MaxUint64/1000000 is 2554 year in unix time millisecond, so it's just an "infinite" future for us.
		// We can't scale it to nanoseconds, so we just leave it as it is
		return MID(millis)
	}
}

func TimeToMID(t time.Time) MID {
	return MID(t.UnixNano())
}

func DurationToMID(d time.Duration) MID {
	return MID(d)
}

func MIDToTime(t MID) time.Time {
	return t.Time()
}

func MIDToMillis(t MID) uint64 {
	return uint64(t) / uint64(time.Millisecond)
}

func MIDToCeilingMillis(t MID) uint64 {
	nanos := uint64(t)
	nanosPerMilli := uint64(time.Millisecond)
	millisFloorPart := nanos / uint64(time.Millisecond)
	nanosPart := nanos % nanosPerMilli
	if nanosPart != 0 {
		return millisFloorPart + 1
	} else {
		return millisFloorPart
	}
}

func MIDToDuration(t MID) time.Duration {
	return time.Duration(t)
}

func NewID(t time.Time, randomness uint64) ID {
	mid := TimeToMID(t)

	return ID{MID: mid, RID: RID(randomness)}
}

// String prints MID to ESFormat. Nanosecond part will not be printed.
func (m MID) String() string {
	return util.NsTsToESFormat(uint64(m))
}
