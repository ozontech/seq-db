package seq

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/ozontech/seq-db/util"
)

var (
	SystemMID    MID    = math.MaxUint64
	SystemRID    RID    = math.MaxUint64
	SystemID     ID     = ID{SystemMID, SystemRID}
	SystemDocPos DocPos = DocPos(0)
)

type ID struct {
	MID MID
	RID RID
}

type (
	MID uint64 // nanoseconds part of ID
	RID uint64 // random part of ID
	LID uint32 // local id for a fraction
)

func (m MID) Time() time.Time {
	nanosPerSecond := uint64(time.Second)
	return time.Unix(int64(uint64(m)/nanosPerSecond), int64(uint64(m)%nanosPerSecond))
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

func (d ID) Dec() ID {
	if d.RID != 0 {
		d.RID -= 1
	} else if d.MID != 0 {
		d.MID -= 1
		d.RID = math.MaxUint64
	}
	return d
}

func (d ID) Inc() ID {
	if d.RID != math.MaxUint64 {
		d.RID += 1
	} else if d.MID != math.MaxUint64 {
		d.MID += 1
		d.RID = 0
	}
	return d
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

	switch delimiter := x[16]; delimiter {
	case '_':
		id.MID = MID(binary.LittleEndian.Uint64(mid))
	case '-':
		// legacy format, MID in millis. Scale to nanoseconds
		id.MID = MillisToMID(binary.LittleEndian.Uint64(mid))
	default:
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

func MIDToSeconds(t MID) uint64 {
	return uint64(t) / uint64(time.Second)
}

func MIDToCeilingMillis(t MID) uint64 {
	millis := uint64(t) / uint64(time.Millisecond)
	nanosPartOfMilli := uint64(t) % uint64(time.Millisecond)
	if nanosPartOfMilli != 0 {
		millis += 1
	}
	return millis
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
