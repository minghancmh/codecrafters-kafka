package types

import (
	"encoding/binary"
	"fmt"
)

type CompactString struct {
	Length  uint64
	Content string
}

func (cs *CompactString) FromBytes(data []byte) (consumed int, err error) {
	val, nbytes := binary.Uvarint(data)
	if nbytes <= 0 {
		return 0, fmt.Errorf("Invalid Uvarint encoding")
	}
	adjustedLength := val - 1
	cs.Length = adjustedLength
	cs.Content = string(data[nbytes : nbytes+int(adjustedLength)])
	return nbytes + int(adjustedLength), nil
}

func (cs *CompactString) ToBytes() []byte {
	buf := make([]byte, 0)
	buf = binary.AppendUvarint(buf, cs.Length+1)
	for _, ch := range cs.Content {
		buf = append(buf, byte(ch))
	}
	return buf
}
