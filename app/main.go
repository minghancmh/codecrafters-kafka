package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
var FETCH_UNKNOWN_TOPIC uint16 = 100

var API_VERSION uint16 = 18
var DESCRIBE_TOPIC_PARTITIONS uint16 = 75
var FETCH uint16 = 1

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
		if err == io.EOF {
			log("connection closed by client. done reading.")
			os.Exit(0)
		}
		if err != nil {
			log("read err: %s", err)
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
			res.Body.ApiVersionsArray.Elements = append(res.Body.ApiVersionsArray.Elements, api.ApiVersionsElem{ApiKey: API_VERSION, MinSupportedVersion: 0, MaxSupportedVersion: 5, TagBuffer: 0})

			// API Key 75 (DescribeTopicPartitions)
			res.Body.ApiVersionsArray.Elements = append(res.Body.ApiVersionsArray.Elements, api.ApiVersionsElem{ApiKey: DESCRIBE_TOPIC_PARTITIONS, MinSupportedVersion: 0, MaxSupportedVersion: 0, TagBuffer: 0})

			// API Key 1 (Fetch)
			res.Body.ApiVersionsArray.Elements = append(res.Body.ApiVersionsArray.Elements, api.ApiVersionsElem{ApiKey: FETCH, MinSupportedVersion: 0, MaxSupportedVersion: 16, TagBuffer: 0})

			res.Body.ApiVersionsArray.Length = uint64(len(res.Body.ApiVersionsArray.Elements))

			res.Body.ThrottleTime = 3735928559 // 0xDEAFBEEF placeholder

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
			res.Header.TagBuffer = 0x0
			res.Body.ThrottleTime = 0
			res.Body.TopicsArray.Elements = make([]types.TopicRes, 0)

			partitionRecords, topicRecords := api.DescribeTopicPartitionsFromMetadataFile()

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
				elem.TopicAuthorizedOperations = 0x00000df8
				elem.TagBuffer = 0

				res.Body.TopicsArray.Elements = append(res.Body.TopicsArray.Elements, elem)
				res.Body.TopicsArray.Length += 1

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

		case FETCH:
			var req api.FetchRequestV16
			api.DeserializeFetchRequestV16(buf[0:], &req)
			log("req: %v", req)

			_, topicRecords := api.DescribeTopicPartitionsFromMetadataFile()
			topicUUIDtoTopicName := generateTopicUUIDtoTopicNamesMap(topicRecords)

			// log("TopicUUIDToRecordBatch: %v", topicUUIDtoRecordBatch)

			var res api.FetchResponseV16
			res.Header.CorrelationID = req.Header.CorrelationID
			res.Header.TagBuffer = 0

			res.Body.ThrottleTimeMs = 0
			res.Body.ErrorCode = 0
			res.Body.SessionId = 0

			for _, topic := range req.Body.Topics.Elements {
				var elem api.FetchResTopic
				elem.TopicId = topic.TopicId

				var partitionElem api.FetchResPartition
				partitionElem.PartitionIndex = 0

				tn, ok := topicUUIDtoTopicName[topic.TopicId]
				if !ok {
					log("Topic name does not exist")
					partitionElem.ErrorCode = FETCH_UNKNOWN_TOPIC
				} else {
					log("appending partition elem records")
					partitionElem.ErrorCode = 0
					filePath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-0/00000000000000000000.log", tn)
					rbArray := api.ReadLogFile(filePath)
					for _, rb := range rbArray {
						partitionElem.Records.Elements = append(partitionElem.Records.Elements, rb)
						partitionElem.Records.Length += uint64(len(rb))
					}

					// partitionElem.Records.Length = 200 // TODO: this represents bytes to read (?) change this
					log("partitionElem.Records.Elements: %v", partitionElem.Records.Elements)
				}


				elem.Partitions.Elements = append(elem.Partitions.Elements, partitionElem)
				elem.Partitions.Length = uint64(len(elem.Partitions.Elements))

				res.Body.Responses.Elements = append(res.Body.Responses.Elements, elem)
				res.Body.Responses.Length += 1
			}

			res.Body.TaggedFields = 0

			response := api.SerializeFetchResponseV16(&res)

			defer conn.Close()
			nbytes, err := conn.Write(response)
			if err != nil {
				log("Error writing response: %s", err)
				os.Exit(1)
			}
			log("NumBytes written: %v", nbytes)

		default:
			fmt.Println("Unsupported case")
		}

	}
}

func existsTopicID(tid types.UUID, topicMetadata map[string]api.TopicRecord) bool {
	for _, topic := range topicMetadata {
		if topic.TopicUUID == tid {
			return true
		}
	}
	return false
}

func generateTopicUUIDtoTopicNamesMap(topicMetadata map[string]api.TopicRecord) map[types.UUID]string {
	res := make(map[types.UUID]string)
	for _, topic := range topicMetadata {
		res[topic.TopicUUID] = topic.TopicName.Content
	}
	return res
}
