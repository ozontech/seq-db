package seq

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ozontech/seq-db/util"

	"github.com/ozontech/seq-db/consts"
)

const (
	NetN = 256 * 1024
)

type LID32Slice []uint32

func (p LID32Slice) Len() int           { return len(p) }
func (p LID32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p LID32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

type Multi interface {
	NextMono(n int) []LID
	Next(n int) ([]LID, []uint32)
	IsClosed() bool
	Close()
}

type Agg interface {
	Next(n int) ([]LID, []uint32)
	IsClosed() bool
	Close()
}

type MultiState struct {
	OpIndex uint32
	TID     uint32

	Pointer int32
	LIDs    []uint32
}

func (ms *MultiState) IsOver() bool {
	return ms.Pointer == -1
}

func (ms *MultiState) SetOver() {
	ms.Pointer = -1
}

func All(seq Multi) []LID {
	return AllWithN(seq, consts.DefaultComputeN)
}

func AllWithN(seq Multi, n int) []LID {
	buf := make([]LID, 0)
	for !seq.IsClosed() {
		l, _ := seq.Next(n)
		buf = append(buf, l...)
	}

	return buf
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

func ExtractMID(id ID) MID {
	return id.MID
}

func NewID(t time.Time, randomness uint64) ID {
	mid := TimeToMID(t)

	return ID{MID: mid, RID: RID(randomness)}
}

func (m MID) String() string {
	return util.MsTsToESFormat(uint64(m))
}
