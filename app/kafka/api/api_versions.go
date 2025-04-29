package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/logger"
	"github.com/codecrafters-io/kafka-starter-go/app/kafka/types"
)

var log = logger.Log

type APIVersionsRequest struct {
	MessageSize uint32
	Header      requestHeader
	Body        apiVersionsRequestBody
}

type apiVersionsRequestBody struct {
	clientID              types.CompactString
	clientSoftwareVersion types.CompactString
	tagBuffer             byte
}

type APIVersionsResponse struct {
	// MessageSize uint32
	Header responseHeaderV0 // v0 is used for APIVersions
	Body   apiVersionsResponseBody
}

type apiVersionsResponseBody struct {
	ErrorCode        uint16
	ApiVersionsArray types.CompactArray[ApiVersionsElem]
	ThrottleTime     uint32
	TagBuffer        uint8
}

type ApiVersionsElem struct {
	ApiKey              uint16
	MinSupportedVersion uint16
	MaxSupportedVersion uint16
	TagBuffer           uint8
}

func SerializeAPIVersionsResponse(res *APIVersionsResponse) []byte {
	buf := make([]byte, 4)

	buf = append(buf, serializeResponseHeaderV0(&res.Header)...)
	buf = append(buf, serializeAPIVersionsResponseBody(&res.Body)...)

	// set the message size
	msgSize := len(buf) - 4
	binary.BigEndian.PutUint32(buf[0:4], uint32(msgSize))
	return buf

}

func serializeResponseHeaderV0(hdr *responseHeaderV0) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint32(buf, hdr.CorrelationID)
	return buf
}

func serializeAPIVersionsResponseBody(body *apiVersionsResponseBody) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, body.ErrorCode)
	buf = append(buf, body.ApiVersionsArray.ToBytes(apiVersionsElemWriter)...)
	buf = binary.BigEndian.AppendUint32(buf, body.ThrottleTime)
	buf = append(buf, body.TagBuffer) // tag buffer
	return buf
}

func DeserializeAPIVersionsRequest(data []byte, dest *APIVersionsRequest) {

	dest.MessageSize = binary.BigEndian.Uint32(data[0:4])
	hdr, nbytes := deserializeRequestHeader(data[4:])
	dest.Header = hdr
	offset := 4 + nbytes

	body, nbytes := deserializeApiVersionsRequestBody(data[offset:])
	dest.Body = body

}

func deserializeApiVersionsRequestBody(data []byte) (apiVersionsRequestBody, int) {
	var body apiVersionsRequestBody
	nbytes, err := body.clientID.FromBytes(data[0:])
	if err != nil {
		log("error decoding clientID")
	}
	offset := nbytes
	nbytes, err = body.clientSoftwareVersion.FromBytes(data[offset:])
	if err != nil {
		log("error decoding clientID")
	}
	offset += nbytes
	body.tagBuffer = data[offset]
	return body, offset + 1
}

func apiVersionsElemWriter(r *ApiVersionsElem) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, r.ApiKey)
	buf = binary.BigEndian.AppendUint16(buf, r.MinSupportedVersion)
	buf = binary.BigEndian.AppendUint16(buf, r.MaxSupportedVersion)
	buf = append(buf, r.TagBuffer)
	return buf
}
