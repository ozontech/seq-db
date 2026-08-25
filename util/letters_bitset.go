package util

// LettersBitset is a bitset of symbols present in token blocks. Allows to prune token blocks on wildcard search.
// It uses 26 bits for eng letters (case-insensitive) as well as two bits for first bytes of ru letters, and a single
// bit for everything else.
//
// The highest bit (bit 31) is reserved as the nil marker.
type LettersBitset uint32

const nilSet LettersBitset = 1 << 31

// Russian alphabet UTF-8 letters first bytes
const (
	reservedByteUtf8Ru1 = 0xD0
	reservedByteUtf8Ru2 = 0xD1
)

type LetterBitsetBuilder [30]bool

func (b *LetterBitsetBuilder) Add(token []byte) {
	for _, c := range token {
		switch {
		case c >= 'a' && c <= 'z':
			b[c-'a'] = true
		case c >= 'A' && c <= 'Z':
			b[c-'A'] = true
		case c >= '0' && c <= '9':
			b[26] = true
		case c == reservedByteUtf8Ru1:
			b[27] = true
		case c == reservedByteUtf8Ru2:
			b[28] = true
		default:
			b[29] = true
		}
	}
}

func (b *LetterBitsetBuilder) Build() LettersBitset {
	return NewLettersBitsetFromArray(*b)
}

func NewLettersBitsetNil() LettersBitset {
	return nilSet
}

func NewLettersBitsetFromArray(letters [30]bool) LettersBitset {
	var s LettersBitset
	for i, has := range letters {
		if has {
			s |= 1 << i
		}
	}
	return s
}

func NewLettersBitset(data ...[]byte) LettersBitset {
	var builder LetterBitsetBuilder
	for i := range data {
		builder.Add(data[i])
	}
	return builder.Build()
}

func (s LettersBitset) IsNil() bool {
	return (s & nilSet) != 0
}

func (s LettersBitset) ContainsAll(required LettersBitset) bool {
	return (s & required) == required
}
