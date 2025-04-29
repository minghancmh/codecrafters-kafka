package types

import (
	"encoding/binary"
	"fmt"
)

type CompactArray[T any] struct {
	Length   uint64
	Elements []T
}

func (ca *CompactArray[T]) FromBytes(data []byte, elementReader func([]byte) (T, int, error)) (consumed int, err error) {
	length, nbytes := binary.Uvarint(data)
	if nbytes <= 0 {
		return 0, fmt.Errorf("Invalid Uvarint encoding")
	}
	if length == 0 { // this is the case of a null array
		ca.Length = 0
		ca.Elements = nil
		return nbytes, nil
	}
	adjustedLength := length - 1
	ca.Length = adjustedLength
	offset := nbytes
	ca.Elements = make([]T, length)

	for i := 0; i < int(adjustedLength); i++ {
		elem, consumed, err := elementReader(data[offset:])
		if err != nil {
			return 0, err
		}
		ca.Elements[i] = elem
		offset += consumed
	}
	return offset, nil
}

func (ca *CompactArray[T]) ToBytes(elementWriter func(*T) []byte) []byte {
	buf := make([]byte, 0)

	buf = binary.AppendUvarint(buf, ca.Length+1)

	for _, elem := range ca.Elements {
		buf = append(buf, elementWriter(&elem)...)
	}
	return buf
}
