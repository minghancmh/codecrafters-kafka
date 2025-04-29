package api

import (
	"encoding/binary"
)

type responseHeaderV1 struct {
	CorrelationID uint32
	TagBuffer     uint8
}

type responseHeaderV0 struct {
	CorrelationID uint32
}

type requestHeader struct {
	apiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientID      reqClientID
	tagBuffer     byte
}

type reqClientID struct {
	length  uint16
	content string
}

func deserializeRequestHeader(data []byte) (requestHeader, int) {
	var res requestHeader
	res.apiKey = binary.BigEndian.Uint16(data[0:2])
	res.ApiVersion = binary.BigEndian.Uint16(data[2:4])
	res.CorrelationID = binary.BigEndian.Uint32(data[4:8])
	clientID, nbytes := deserializeReqClientID(data[8:])
	res.ClientID = clientID
	res.tagBuffer = data[8+nbytes]
	return res, 8 + nbytes

}

func deserializeReqClientID(data []byte) (reqClientID, int) {
	var res reqClientID
	res.length = binary.BigEndian.Uint16(data[0:2])
	res.content = string(data[2 : 2+res.length])
	return res, 2 + int(res.length)
}
