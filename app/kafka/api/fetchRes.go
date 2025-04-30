package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/types"
)

type FetchResponseV16 struct {
	Header responseHeaderV1
	Body   FetchResponseV16Body
}

type FetchResponseV16Body struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	SessionId      uint32
	Responses      types.CompactArray[FetchResTopic]
	TaggedFields   uint8
}

type FetchResTopic struct {
	TopicId      types.UUID
	Partitions   types.CompactArray[FetchResPartition]
	TaggedFields uint8
}

type FetchResPartition struct {
	PartitionIndex       uint32
	ErrorCode            uint16
	HighWatermark        uint64
	LastStableOffset     uint64
	LogStartOffset       uint64
	AbortedTransactions  types.CompactArray[AbortedTransaction]
	PreferredReadReplica uint32
	Records              byte // TODO: Underlying type is a RecordBatch according to ChatGPT, else just set to 0x0 for null
	TaggedFields         uint8
}

type AbortedTransaction struct {
	ProducerId   uint64
	FirstOffset  uint64
	TaggedFields uint8
}

func SerializeFetchResponseV16(res *FetchResponseV16) []byte {
	buf := make([]byte, 4)
	buf = append(buf, serializeResponseHeaderV1(&res.Header)...)
	buf = append(buf, serializeFetchResponseV16Body(&res.Body)...)
	msgSize := len(buf) - 4
	binary.BigEndian.PutUint32(buf[0:4], uint32(msgSize))
	return buf
}

func serializeFetchResponseV16Body(body *FetchResponseV16Body) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, body.ThrottleTimeMs)
	buf = binary.BigEndian.AppendUint16(buf, body.ErrorCode)
	buf = binary.BigEndian.AppendUint32(buf, body.SessionId)
	buf = append(buf, body.Responses.ToBytes(fetchResTopicWriter)...)
	buf = append(buf, body.TaggedFields)
	return buf
}

func fetchResTopicWriter(r *FetchResTopic) []byte {
	buf := make([]byte, 0)
	buf = append(buf, r.TopicId.ToBytes()...)
	buf = append(buf, r.Partitions.ToBytes(fetchResPartitionWriter)...)
	buf = append(buf, r.TaggedFields)
	return buf
}

func fetchResPartitionWriter(r *FetchResPartition) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, r.PartitionIndex)
	buf = binary.BigEndian.AppendUint16(buf, r.ErrorCode)
	buf = binary.BigEndian.AppendUint64(buf, r.HighWatermark)
	buf = binary.BigEndian.AppendUint64(buf, r.LastStableOffset)
	buf = binary.BigEndian.AppendUint64(buf, r.LogStartOffset)
	buf = append(buf, r.AbortedTransactions.ToBytes(abortedTransactionWriter)...)
	buf = binary.BigEndian.AppendUint32(buf, r.PreferredReadReplica)
	buf = append(buf, r.Records)
	buf = append(buf, r.TaggedFields)
	return buf
}

func abortedTransactionWriter(r *AbortedTransaction) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint64(buf, r.ProducerId)
	buf = binary.BigEndian.AppendUint64(buf, r.FirstOffset)
	buf = append(buf, r.TaggedFields)
	return buf
}
