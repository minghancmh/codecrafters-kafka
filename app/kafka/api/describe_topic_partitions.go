package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/types"
)

type DescribeTopicPartitionsRequest struct {
	messageSize uint32
	Header      requestHeader
	Body        describeTopicPartitionsRequestBody
}

type DescribeTopicPartitionsResponse struct {
	// messageSize uint32
	Header responseHeaderV1
	Body   describeTopicPartitionsResponseBody
}

type describeTopicPartitionsRequestBody struct {
	Topics                 types.CompactArray[types.TopicReq]
	responsePartitionLimit uint32
	cursor                 *types.Cursor
	tagBuffer              uint8
}

type describeTopicPartitionsResponseBody struct {
	ThrottleTime uint32
	TopicsArray  types.CompactArray[types.TopicRes]
	NextCursor   *types.Cursor
	TagBuffer    byte
}

func DeserializeTopicPartitionsRequest(data []byte, dest *DescribeTopicPartitionsRequest) {
	dest.messageSize = binary.BigEndian.Uint32(data[0:4])
	hdr, nbytes := deserializeRequestHeader(data[4:])
	dest.Header = hdr
	offset := 4 + nbytes
	body, nbytes := deserializeTopicPartitionsRequestBody(data[offset:])
	dest.Body = body
}

func deserializeTopicPartitionsRequestBody(data []byte) (describeTopicPartitionsRequestBody, int) {
	var res describeTopicPartitionsRequestBody
	nbytes, err := res.Topics.FromBytes(data[0:], types.TopicReqReader)
	if err != nil {
		log("error decoding TopicReq")
	}
	offset := nbytes
	res.responsePartitionLimit = binary.BigEndian.Uint32(data[offset : offset+4])

	// check next byte (Cursor): if 0xff, then cursor is null, if not then parse cursor
	if data[offset+4] == 0xff {
		// cursor is null
		res.cursor = nil
	}
	//  else {
	// TODO parse cursor

	// }
	res.tagBuffer = data[offset+5]
	return res, offset + 5
}

func SerializeDescribeTopicPartitionsResponse(res *DescribeTopicPartitionsResponse) []byte {
	buf := make([]byte, 4)
	buf = append(buf, serializeResponseHeaderV1(&res.Header)...)
	buf = append(buf, serializeDescribeTopicPartitionsResponseBody(&res.Body)...)
	return buf
}

func serializeResponseHeaderV1(hdr *responseHeaderV1) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, hdr.CorrelationID)
	buf = append(buf, hdr.TagBuffer)
	return buf
}

func serializeDescribeTopicPartitionsResponseBody(body *describeTopicPartitionsResponseBody) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, body.ThrottleTime)
	buf = append(buf, body.TopicsArray.ToBytes(types.TopicResElemWriter)...)
	buf = append(buf, 0xff) // TODO: hardcoding to 0xff for nil cursor now
	buf = append(buf, body.TagBuffer)
	return buf

}
