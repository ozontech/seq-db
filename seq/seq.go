package seq

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ozontech/seq-db/util"
)

type ID struct {
	MID MID
	RID RID
}

type MID uint64 // milliseconds part of ID
type RID uint64 // random part of ID
type LID uint32 // local id for a fraction

func (m MID) Time() time.Time {
	return time.UnixMilli(int64(m))
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
	final = append(final, '-')

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

	id.MID = MID(binary.LittleEndian.Uint64(mid))
	id.RID = RID(binary.LittleEndian.Uint64(rid))

	return id, nil
}

func SimpleID(i int) ID {
	return ID{
		MID: MID(i),
		RID: 0,
	}
}

func TimeToMID(t time.Time) MID {
	return MID(t.UnixNano() / int64(time.Millisecond))
}

func DurationToMID(d time.Duration) MID {
	return MID(d / time.Millisecond)
}

func MIDToTime(t MID) time.Time {
	return time.Unix(0, 0).Add(MIDToDuration(t))
}

func MIDToDuration(t MID) time.Duration {
	return time.Duration(t) * time.Millisecond
}

func NewID(t time.Time, randomness uint64) ID {
	mid := TimeToMID(t)

	return ID{MID: mid, RID: RID(randomness)}
}

func (m MID) String() string {
	return util.MsTsToESFormat(uint64(m))
}
