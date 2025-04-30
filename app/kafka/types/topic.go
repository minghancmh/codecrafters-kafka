package types

import (
	"encoding/binary"
	"fmt"
)

type TopicReq struct {
	Name      CompactString
	TagBuffer uint8
}

type TopicRes struct {
	ErrorCode                 uint16
	Name                      CompactString
	TopicID                   UUID
	IsInternal                uint8
	PartitionsArray           CompactArray[Partition]
	TopicAuthorizedOperations uint32
	TagBuffer                 byte
}

func TopicReqReader(data []byte) (TopicReq, int, error) {
	var treq TopicReq
	consumed, err := treq.Name.FromBytes(data)
	if err != nil {
		return TopicReq{}, 0, fmt.Errorf("Error decoding topicReq")
	}
	treq.TagBuffer = 0x0
	return treq, consumed+1, nil
}

func TopicResElemWriter(r *TopicRes) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, r.ErrorCode)
	buf = append(buf, r.Name.ToBytes()...)
	buf = append(buf, r.TopicID.ToBytes()...)
	buf = append(buf, r.IsInternal)
	buf = append(buf, r.PartitionsArray.ToBytes(partitionElemWriter)...)
	buf = binary.BigEndian.AppendUint32(buf, r.TopicAuthorizedOperations)
	buf = append(buf, r.TagBuffer)
	return buf
}
