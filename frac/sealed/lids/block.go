package lids

type Block struct {
	MinTID      uint32
	MaxTID      uint32
	IsContinued bool
	Chunks      Chunks
}

func (b *Block) GetExtForRegistry() (ext1, ext2 uint64) {
	if b.IsContinued {
		ext1 = 1
	}
	ext2 = uint64(b.MaxTID)<<32 | uint64(b.MinTID)
	return ext1, ext2
}
