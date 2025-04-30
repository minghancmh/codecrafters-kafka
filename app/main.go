package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/api"
	"github.com/codecrafters-io/kafka-starter-go/app/kafka/logger"
	"github.com/codecrafters-io/kafka-starter-go/app/kafka/types"
	// "time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var log = logger.Log

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
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {

	buf := make([]byte, 1024)
	for {

		_, err := conn.Read(buf)
		if err != nil {
			log("read err:", err)
			os.Exit(1)
		}
		log("buf:", buf)

		// parse the message
		// messageSize := binary.BigEndian.Uint32(buf[0:4])
		// reqHeader.apiKey := binary.BigEndian.Uint16(buf[4:6])
		// reqHeader.apiVersion := binary.BigEndian.Uint16(buf[6:8])
		// reqHeader.correlationID := binary.BigEndian.Uint32(buf[8:12])

		apiKey := binary.BigEndian.Uint16(buf[4:6])

		switch apiKey {
		case API_VERSION:
			var req api.APIVersionsRequest
			api.DeserializeAPIVersionsRequest(buf[0:], &req)

			var res api.APIVersionsResponse
			res.Header.CorrelationID = req.Header.CorrelationID
			if req.Header.ApiVersion > 4 {
				log("Requested API Version not supported: %d\n", req.Header.ApiVersion)
				res.Body.ErrorCode = UNSUPPORTED_VERSION
				response := api.SerializeAPIVersionsResponse(&res)
				conn.Write(response)
				os.Exit(1)
			}


			res.Body.ErrorCode = 0
			res.Body.ApiVersionsArray.Elements = make([]api.ApiVersionsElem, 0)

			// API Key 18 (APIVersions)
			res.Body.ApiVersionsArray.Elements = append(res.Body.ApiVersionsArray.Elements, api.ApiVersionsElem{ApiKey: 18, MinSupportedVersion: 0, MaxSupportedVersion: 5, TagBuffer: 0})

			// API Key 75 (DescribeTopicPartitions)
			res.Body.ApiVersionsArray.Elements = append(res.Body.ApiVersionsArray.Elements, api.ApiVersionsElem{ApiKey: 75, MinSupportedVersion: 0, MaxSupportedVersion: 0, TagBuffer: 0})

			// API Key 1 (Fetch)
			res.Body.ApiVersionsArray.Elements = append(res.Body.ApiVersionsArray.Elements, api.ApiVersionsElem{ApiKey: 1, MinSupportedVersion: 0, MaxSupportedVersion: 16, TagBuffer: 0})


			res.Body.ApiVersionsArray.Length = uint64(len(res.Body.ApiVersionsArray.Elements))

			res.Body.ThrottleTime = 3735928559

			res.Body.TagBuffer = 0x0

			response := api.SerializeAPIVersionsResponse(&res)

			defer conn.Close() // we need to close the connection after function exit
			nbytes, err := conn.Write(response)
			if err != nil {
				log("Error writing response:", err)
				os.Exit(1)
			}
			fmt.Printf("Num Bytes Written: %v\n", nbytes)

		case DESCRIBE_TOPIC_PARTITIONS:
			var req api.DescribeTopicPartitionsRequest
			api.DeserializeTopicPartitionsRequest(buf[0:], &req)
			log("DESCRIBE_TOPIC_PARTITIONS: parsedRequest: %v", req)

			var res api.DescribeTopicPartitionsResponse
			res.Header.CorrelationID = req.Header.CorrelationID
			log("corrId: %v\n", res.Header.CorrelationID)
			res.Header.TagBuffer = 0x0
			res.Body.ThrottleTime = 0
			res.Body.TopicsArray.Elements = make([]types.TopicRes, 0)
			log("eleemnts:%v\n", res.Body.TopicsArray.Elements)

			partitionRecords, topicRecords := api.DescribeTopicPartitionsFromMetadataFile()
			log("reached line 124")

			for _, top := range req.Body.Topics.Elements {
				var elem types.TopicRes

				name := top.Name.Content
				tr, ok := topicRecords[name]
				if !ok {
					log("Unable to find topic with name: ", name)
					elem.ErrorCode = UNKNOWN_TOPIC
				}
				log("reached lined 132")
				partitionRecords, ok := partitionRecords[tr.TopicUUID]
				if !ok {
					log("Unable to find partitions for topic with UUID: %v", tr.TopicUUID)
				}

				elem.Name.Content = name
				elem.Name.Length = uint64(len(name))
				elem.TopicID = tr.TopicUUID
				elem.IsInternal = 0
				log("reached line 143")
				for idx, partitionRecord := range partitionRecords {
					var part types.Partition
					part.ErrorCode = 0
					part.PartitionIndex = uint32(idx)
					part.LeaderID = partitionRecord.Leader
					part.LeaderEpoch = partitionRecord.LeaderEpoch
					part.ReplicaNodes.Length = partitionRecord.ReplicaArray.Length
					part.ReplicaNodes.Elements = partitionRecord.ReplicaArray.Elements
					part.IsrNodes.Length = partitionRecord.InSyncReplicaArray.Length
					part.IsrNodes.Elements = partitionRecord.InSyncReplicaArray.Elements
					part.EligibleLeaderReplicas.Length = 0
					part.EligibleLeaderReplicas.Elements = make([]types.Replica, 0)
					part.LastKnownELRs.Length = 0
					part.LastKnownELRs.Elements = make([]types.Replica, 0)
					part.OfflineReplicas.Length = 0
					part.OfflineReplicas.Elements = make([]types.Replica, 0)
					part.TagBuffer = 0
					elem.PartitionsArray.Elements = append(elem.PartitionsArray.Elements, part)
					elem.PartitionsArray.Length += 1
				}
				log("reached line 163")
				elem.TopicAuthorizedOperations = 0x00000df8
				elem.TagBuffer = 0

				res.Body.TopicsArray.Elements = append(res.Body.TopicsArray.Elements, elem)
				res.Body.TopicsArray.Length += 1
				log("reached line 169")
			}
			log("topicsArray: %v", res.Body.TopicsArray)
			res.Body.NextCursor = nil
			res.Body.TagBuffer = 0
			log("DESCRIBE_TOPIC_PARTITIONS: res:", res)

			response := api.SerializeDescribeTopicPartitionsResponse(&res)
			log("DESCRIBE_TOPIC_PARTITIONS: serialized response:", response)

			defer conn.Close() // we need to close the connection after function exit
			nbytes, err := conn.Write(response)
			if err != nil {
				fmt.Println("Error writing response:", err)
				os.Exit(1)
			}
			fmt.Printf("Num Bytes Written: %v\n", nbytes)

		default:
			fmt.Println("Unsupported case")
		}

	}
}

// Common Utils

// func setMessageSize(buf *[]byte, msgSize uint32) uint32 {
// 	tmp := new(bytes.Buffer)
// 	err := binary.Write(tmp, binary.BigEndian, msgSize)
// 	if err != nil {
// 		fmt.Println("Setting msg size failed: ", err)
// 		return 0
// 	}
// 	tmpBytes := tmp.Bytes()
// 	copy((*buf)[0:4], tmpBytes)
// 	return 4
// }

// func setCorrelationId(buf *[]byte, corrId uint32) uint32 {
// 	tmp := new(bytes.Buffer)
// 	err := binary.Write(tmp, binary.BigEndian, corrId)
// 	if err != nil {
// 		fmt.Println("Setting correlation ID failed: ", err)
// 		return 0
// 	}
// 	tmpBytes := tmp.Bytes()
// 	copy((*buf)[4:8], tmpBytes)
// 	return 4
// }

// APIVersions Response Utils

// func setErrorCode(buf *[]byte, errorCode uint16, offset uint32) uint32 {
// 	tmp := new(bytes.Buffer)
// 	err := binary.Write(tmp, binary.BigEndian, errorCode)
// 	if err != nil {
// 		fmt.Println("Setting error code failed: ", err)
// 		return 0
// 	}
// 	tmpBytes := tmp.Bytes()
// 	copy((*buf)[offset:offset+2], tmpBytes)
// 	return 2
// }

// ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms _tagged_fields
//
//	error_code => INT16
//	num_api_keys => byte (0x1 for 0 api_keys, 0x2 for 1 api_key, 0x3 for 2 api_key ... )
//	api_keys => api_key min_version max_version _tagged_fields (_tagged_fields need to set to 0x0 even if not used)
//	  api_key => INT16
//	  min_version => INT16
//	  max_version => INT16
//	throttle_time_ms => INT32
// func setAPIKey(buf *[]byte, apiKey uint16, minVer uint16, maxVer uint16, offset uint32) uint32 {
// 	tmp := new(bytes.Buffer)

// 	err := binary.Write(tmp, binary.BigEndian, apiKey)
// 	err = binary.Write(tmp, binary.BigEndian, minVer)
// 	err = binary.Write(tmp, binary.BigEndian, maxVer)
// 	err = binary.Write(tmp, binary.BigEndian, uint8(0))
// 	if err != nil {
// 		fmt.Println("Setting APIVersionsAPIKey failed: ", err)
// 		return 0
// 	}

// 	tmpBytes := tmp.Bytes()

// 	copy((*buf)[offset:offset+7], tmpBytes)
// 	return 7
// }

// func setThrottleTime(buf *[]byte, throttleTime uint32, offset uint32) uint32 {
// 	tmp := new(bytes.Buffer)
// 	err := binary.Write(tmp, binary.BigEndian, throttleTime)
// 	if err != nil {
// 		fmt.Println("Setting ThrottleTime failed: ", err)
// 		return 0
// 	}

// 	tmpBytes := tmp.Bytes()

// 	copy((*buf)[offset:offset+4], tmpBytes)
// 	return 4
// }

// func setEnd(buf *[]byte, offset uint32) uint32 {
// 	tmp := []byte{0xd, 0xe, 0xa, 0xd, 0xd, 0xe, 0xa, 0xd} // end delimiter for easier debugging
// 	copy((*buf)[offset:offset+8], tmp)
// 	return 8
// }
