package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/types"
)

type FetchRequestV16 struct {
	MessageSize uint32
	Header      requestHeader
	Body        FetchRequestV16Body
}

type FetchRequestV16Body struct {
	MaxWaitMs           uint32
	MinBytes            uint32
	MaxBytes            uint32
	IsolationLevel      uint8
	SessionId           uint32
	SessionEpoch        uint32
	Topics              types.CompactArray[FetchReqTopic]
	ForgottenTopicsData types.CompactArray[FetchReqTopic]
	TaggedFields        uint8
}

type FetchReqTopic struct {
	TopicId      types.UUID
	Partitions   types.CompactArray[FetchReqPartition]
	TaggedFields uint8
}

type FetchReqPartition struct {
	Partition          uint32
	CurrentLeaderEpoch uint32
	FetchOffset        uint64
	LastFetchedEpoch   uint32
	LogStartOffset     uint64
	PartitionMaxBytes  uint32
	TaggedFields       uint8
}

func DeserializeFetchRequestV16(data []byte, dest *FetchRequestV16) {
	dest.MessageSize = binary.BigEndian.Uint32(data[0:4])
	hdr, nbytes := deserializeRequestHeader(data[4:])
	dest.Header = hdr
	offset := 4 + nbytes
	body, nbytes := deserializeFetchRequestV16Body(data[offset:])
	dest.Body = body
}

func deserializeFetchRequestV16Body(data []byte) (FetchRequestV16Body, int) {
	var body FetchRequestV16Body
	body.MaxWaitMs = binary.BigEndian.Uint32(data[0:4])
	body.MinBytes = binary.BigEndian.Uint32(data[4:8])
	body.MaxBytes = binary.BigEndian.Uint32(data[8:12])
	body.IsolationLevel = data[12]
	body.SessionId = binary.BigEndian.Uint32(data[13:17])
	body.SessionEpoch = binary.BigEndian.Uint32(data[17:21])
	nbytes, err := body.Topics.FromBytes(data[21:], fetchReqTopicReader)
	if err != nil {
		log("Error decoding FetchReqTopic from TopicsData")
	}
	offset := 21 + nbytes
	nbytes, err = body.ForgottenTopicsData.FromBytes(data[offset:], fetchReqTopicReader)
	if err != nil {
		log("Error decoding FetchReqTopic from ForgottenTopicsData")
	}
	offset += nbytes
	body.TaggedFields = data[offset]
	return body, offset + 1
}

func fetchReqTopicReader(data []byte) (FetchReqTopic, int, error) {
	var res FetchReqTopic
	uuid, consumed, err := types.UUIDReader(data[0:])
	if err != nil {
		log("Failed to parse UUID")
	}
	res.TopicId = uuid
	offset := consumed
	nbytes, err := res.Partitions.FromBytes(data[offset:], fetchReqPartitionReader)
	if err != nil {
		log("Error decoding FetchReqPartitions")
	}
	offset += nbytes
	res.TaggedFields = data[offset]
	return res, offset + 1, nil
}

func fetchReqPartitionReader(data []byte) (FetchReqPartition, int, error) {
	var res FetchReqPartition
	res.Partition = binary.BigEndian.Uint32(data[0:4])
	res.CurrentLeaderEpoch = binary.BigEndian.Uint32(data[4:8])
	res.FetchOffset = binary.BigEndian.Uint64(data[8:16])
	res.LastFetchedEpoch = binary.BigEndian.Uint32(data[16:20])
	res.LogStartOffset = binary.BigEndian.Uint64(data[20:28])
	res.PartitionMaxBytes = binary.BigEndian.Uint32(data[28:32])
	res.TaggedFields = data[32]
	return res, 33, nil
}
