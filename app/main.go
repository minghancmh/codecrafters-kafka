package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	// "time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var UNSUPPORTED_VERSION uint16 = 35
var UNKNOWN_TOPIC uint16 = 3

var DESCRIBE_TOPIC_PARTITIONS uint16 = 75

// DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor _tagged_fields
//   topics => name _tagged_fields
//     name => COMPACT_STRING
//   response_partition_limit => INT32
//   cursor => topic_name partition_index _tagged_fields
//     topic_name => COMPACT_STRING
//     partition_index => INT32

var API_VERSION uint16 = 18

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	// Example request message (all in big endian)
	//	00 00 00 23  // message_size:        35 			-> 4 byte
	//	00 12        // request_api_key:     18				-> 2 byte
	// 	00 04        // request_api_version: 4				-> 2 byte
	// 	6f 7f c6 61  // correlation_id:      1870644833		-> 4 byte

	buf := make([]byte, 1024)
	for {
		var currentMessageLength uint32 = 0

		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("read err:", err)
			os.Exit(1)
		}
		fmt.Printf("request: %s\n", buf)

		// parse the message
		// message_size :=  binary.BigEndian.Uint32(buf[0:4])
		requestAPIKey := binary.BigEndian.Uint16(buf[4:6])
		requestAPIVersion := binary.BigEndian.Uint16(buf[6:8])
		corrID := binary.BigEndian.Uint32(buf[8:12])

		response := make([]byte, 1024)
		currentMessageLength += setCorrelationId(&response, corrID)

		switch requestAPIKey {
		case API_VERSION:
			if requestAPIVersion > 4 {
				fmt.Printf("Requested API Version not supported: %d\n", requestAPIVersion)
				currentMessageLength += setErrorCode(&response, UNSUPPORTED_VERSION, 8)
				setMessageSize(&response, uint32(currentMessageLength))
				conn.Write(response)
				os.Exit(1)
			}

			currentMessageLength += setErrorCode(&response, 0, 8) // no error

			// Set the num_api_keys
			response[currentMessageLength+4] = 0x3
			currentMessageLength += 1

			// API Key 18 (APIVersions)
			// MinVersion: >= 0
			// MaxVersion: >= 4
			currentMessageLength += setAPIKey(&response, 18, 0, 5, currentMessageLength+4) // + 4 because of the message_size offset

			// API Key 75 (DescribeTopicPartitions)
			// MinVersion: >= 0
			// MaxVersion: >= 0
			currentMessageLength += setAPIKey(&response, 75, 0, 0, currentMessageLength+4)

			currentMessageLength += setThrottleTime(&response, 3735928559, currentMessageLength+4) // DEADBEEF for placeholder, +4 for message offset

			// set the tag buffer
			response[currentMessageLength+4] = 0x0
			currentMessageLength += 1

			setMessageSize(&response, currentMessageLength)

		case DESCRIBE_TOPIC_PARTITIONS:
			// parsing the request
			req := new(DescribeTopicPartitionsRequest)

			clientIdLength := binary.BigEndian.Uint16(buf[12:14])
			clientIdContents := make([]byte, clientIdLength)
			copy(clientIdContents[:], buf[14:14+clientIdLength])

			// byte buf[14+clientIdLength] is the tag_buffer

			topicsArrLength := int(buf[14+clientIdLength+1]) - 1
			offset := 16 + clientIdLength

			for i := 0; i < topicsArrLength; i++ {
				topicNameLength := int(buf[offset]) - 1
				offset += 1
				topicByteArr := make([]byte, topicNameLength)
				copy(topicByteArr[:], buf[offset:offset+uint16(topicNameLength)])
				offset += uint16(topicNameLength) + 1 // take care of the tag buffer

				topic := new(Topic)
				topic._tagged_fields = 0x0
				topic.name = string(topicByteArr)

				req.topics = append(req.topics, *topic)
			}

			req.response_partition_limit = int32(binary.BigEndian.Uint32(buf[offset : offset+4]))
			offset += 4

			// havent parsed cursor

			fmt.Println("request:", req)

			// formatting the response
			response := make([]byte, 1024)
			setMessageSize(&response, 41) // the response for DescribeTopicPartitions is always 41 bytes long
			var responseOffset uint32 = 4 // size of the message length
			responseOffset += setCorrelationId(&response, corrID)

			response[responseOffset] = 0x0 // tag buffer
			responseOffset += 1

			responseOffset += setThrottleTime(&response, 0, responseOffset)

			response[responseOffset] = byte(len(req.topics) + 1)
			responseOffset += 1

			responseOffset += setErrorCode(&response, UNKNOWN_TOPIC, responseOffset)

			for i := 0; i < len(req.topics); i++ {
				topicLength := len(req.topics[i].name)
				response[responseOffset] = byte(topicLength) + 1
				responseOffset += 1
				for _, char := range req.topics[i].name {
					response[responseOffset] = byte(char)
					responseOffset += 1
				}
			}

			topicId := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} // null id for now
			copy(response[responseOffset:responseOffset+16], topicId[:])
			responseOffset += 16

			response[responseOffset] = 0 // is internal
			responseOffset += 1

			response[responseOffset] = 0x1 // length of partitions array set to 0

			topicAuthorizedOperations := [4]byte{0x0, 0x0, 0xd, 0xf8} // refer to https://binspec.org/kafka-describe-topic-partitions-response-v0-unknown-topic?highlight=38-41
			copy(response[responseOffset:responseOffset+4], topicAuthorizedOperations[:])
			responseOffset += 4

			response[responseOffset] = 0 // tag buffer
			responseOffset += 1

			response[responseOffset] = 0xff // next cursor
			responseOffset += 1

			response[responseOffset] = 0
			responseOffset += 1

			setMessageSize(&response, responseOffset - 4)

			conn.Write(response[:responseOffset])

			continue
		default:
			fmt.Println("Unsupported case")
		}

		// messageSize(4byte) | correlationID(4byte) | Body...
		defer conn.Close() // we need to close the connection after function exit
		nbytes, err := conn.Write(response[:currentMessageLength+4])
		if err != nil {
			fmt.Println("Error writing response:", err)
			os.Exit(1)
		}
		fmt.Printf("Num Bytes Written: %v\n", nbytes)
	}
}

// Common Utils

func setMessageSize(buf *[]byte, msgSize uint32) uint32 {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, msgSize)
	if err != nil {
		fmt.Println("Setting msg size failed: ", err)
		return 0
	}
	tmpBytes := tmp.Bytes()
	copy((*buf)[0:4], tmpBytes)
	return 4
}

func setCorrelationId(buf *[]byte, corrId uint32) uint32 {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, corrId)
	if err != nil {
		fmt.Println("Setting correlation ID failed: ", err)
		return 0
	}
	tmpBytes := tmp.Bytes()
	copy((*buf)[4:8], tmpBytes)
	return 4
}

// APIVersions Response Utils

func setErrorCode(buf *[]byte, errorCode uint16, offset uint32) uint32 {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, errorCode)
	if err != nil {
		fmt.Println("Setting error code failed: ", err)
		return 0
	}
	tmpBytes := tmp.Bytes()
	copy((*buf)[offset:offset+2], tmpBytes)
	return 2
}

// ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms _tagged_fields
//
//	error_code => INT16
//	num_api_keys => byte (0x1 for 0 api_keys, 0x2 for 1 api_key, 0x3 for 2 api_key ... )
//	api_keys => api_key min_version max_version _tagged_fields (_tagged_fields need to set to 0x0 even if not used)
//	  api_key => INT16
//	  min_version => INT16
//	  max_version => INT16
//	throttle_time_ms => INT32
func setAPIKey(buf *[]byte, apiKey uint16, minVer uint16, maxVer uint16, offset uint32) uint32 {
	tmp := new(bytes.Buffer)

	err := binary.Write(tmp, binary.BigEndian, apiKey)
	err = binary.Write(tmp, binary.BigEndian, minVer)
	err = binary.Write(tmp, binary.BigEndian, maxVer)
	err = binary.Write(tmp, binary.BigEndian, uint8(0))
	if err != nil {
		fmt.Println("Setting APIVersionsAPIKey failed: ", err)
		return 0
	}

	tmpBytes := tmp.Bytes()

	copy((*buf)[offset:offset+7], tmpBytes)
	return 7
}

func setThrottleTime(buf *[]byte, throttleTime uint32, offset uint32) uint32 {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, throttleTime)
	if err != nil {
		fmt.Println("Setting ThrottleTime failed: ", err)
		return 0
	}

	tmpBytes := tmp.Bytes()

	copy((*buf)[offset:offset+4], tmpBytes)
	return 4
}

func setEnd(buf *[]byte, offset uint32) uint32 {
	tmp := []byte{0xd, 0xe, 0xa, 0xd, 0xd, 0xe, 0xa, 0xd} // end delimiter for easier debugging
	copy((*buf)[offset:offset+8], tmp)
	return 8
}

type Topic struct {
	name           string
	_tagged_fields int8
}
type Cursor struct {
	topic_name      string
	partition_index int32
	_tagged_fields  int8
}

type DescribeTopicPartitionsRequest struct {
	topics                   []Topic
	response_partition_limit int32
	cursor                   Cursor
	_tagged_fields           int8
}
