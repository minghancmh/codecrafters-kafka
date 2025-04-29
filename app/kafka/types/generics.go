package types

import "encoding/binary"

type UUID [16]byte
type Replica uint32

type Cursor struct {
	topic_name      string
	partition_index int32
	_tagged_fields  int8
}

func ReplicaArrayReader(data []byte) (Replica, int, error) {
	replica := Replica(binary.BigEndian.Uint32(data[0:]))
	return replica, 4, nil
}

func ReplicaElemWriter(r *Replica) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, uint32(*r))
	return buf
}

// func Uint32Reader(data []byte) (uint32, int, error) {
// 	elem := binary.BigEndian.Uint32(data[0:])
// 	return elem, 4, nil
// }

func UUIDReader(data []byte) (UUID, int, error) {
	buf := make([]byte, 16)
	copy(buf[:], data[0:16])
	return UUID(buf), 16, nil
}

func (u *UUID) ToBytes() []byte {
	return u[:]
}
