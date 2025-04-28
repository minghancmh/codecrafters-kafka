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
			defer conn.Close() // we need to close the connection after function exit
			nbytes, err := conn.Write(response[:currentMessageLength+4])
			if err != nil {
				fmt.Println("Error writing response:", err)
				os.Exit(1)
			}
			fmt.Printf("Num Bytes Written: %v\n", nbytes)

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
			var responseOffset uint32 = 4 // size of the message length
			responseOffset += setCorrelationId(&response, corrID)

			response[responseOffset] = 0x0 // tag buffer
			responseOffset += 1

			responseOffset += setThrottleTime(&response, 0, responseOffset)

			response[responseOffset] = byte(len(req.topics) + 1) // setting the length of the topics array
			responseOffset += 1

			responseOffset += setErrorCode(&response, UNKNOWN_TOPIC, responseOffset)

			for i := 0; i < len(req.topics); i++ {
				topicNameLength := len(req.topics[i].name)
				topicRecord := getTopicByName(req.topics[i].name)
				response[responseOffset] = byte(topicNameLength) + 1
				responseOffset += 1
				for _, char := range req.topics[i].name {
					response[responseOffset] = byte(char)
					responseOffset += 1
				}
				// topicId := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} // null id for now
				copy(response[responseOffset:responseOffset+16], topicRecord.topicUUID[:])
				responseOffset += 16

				response[responseOffset] = 0 // is internal
				responseOffset += 1

				response[responseOffset] = 0x1 // length of partitions array set to 0
				responseOffset += 1

				topicAuthorizedOperations := [4]byte{0x0, 0x0, 0xd, 0xf8} // refer to https://binspec.org/kafka-describe-topic-partitions-response-v0-unknown-topic?highlight=38-41
				copy(response[responseOffset:responseOffset+4], topicAuthorizedOperations[:])
				responseOffset += 4

				response[responseOffset] = 0 // tag buffer (topic)
				responseOffset += 1

			}
			response[responseOffset] = 0xff // next cursor
			responseOffset += 1

			response[responseOffset] = 0 // tag buffer (DescribeTopicPartitions Response Body v0)
			responseOffset += 1

			setMessageSize(&response, responseOffset-4)

			// messageSize(4byte) | correlationID(4byte) | Body...
			defer conn.Close() // we need to close the connection after function exit
			nbytes, err := conn.Write(response[:responseOffset])
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

// DescribeTopicPartitions Utils
func getTopicByName(name string) TopicRecord {
	path := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	dat, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading file at path: ", path)
	}

	var offset uint32 = 0

	for offset < uint32(len(dat)) {

		rb := readRecordBatch(dat[offset:], 0)
		lengthBatch := binary.BigEndian.Uint32(dat[offset+8:offset+12])
		offset = 12 + lengthBatch

		fmt.Println("[getTopicByName]: len(rb.records):", len(rb.records))

		for _, rec := range rb.records {
			val := rec.value
			switch val.isRecordValue() {
			case 0x2: // value record
				fmt.Println("[getTopicByName]: value record found")
				tr := val.(TopicRecord)
				if tr.topicName == name {
					return tr
				}

			case 0x3: // partition record
				// pr := val.(PartitionRecord)
				fmt.Println("[getTopicByName]: partition record parsing to be implemented")
			case 0xc:
				fmt.Println("[getTopicByName]: feature level record parsing to be implemented")
			default:
				fmt.Println("[getTopicByName]: no record type found")
			}

		}
	}
	return TopicRecord{}
}

func readRecordBatch(dat []byte, offset uint64) RecordBatch {
	rb := new(RecordBatch)
	rb.baseOffset = binary.BigEndian.Uint64(dat[offset : offset+8])
	fmt.Printf("baseOffset: %d\n", rb.baseOffset)

	rb.batchLength = binary.BigEndian.Uint32(dat[offset+8 : offset+12])
	fmt.Printf("batchLength: %d\n", rb.batchLength)

	rb.partitionLeaderEpoch = binary.BigEndian.Uint32(dat[offset+12 : offset+16])
	fmt.Printf("partitionLeaderEpoch: %d\n", rb.partitionLeaderEpoch)

	rb.magicByte = dat[offset+16]
	fmt.Printf("magicByte: %d\n", rb.magicByte)

	rb.crc = binary.BigEndian.Uint32(dat[offset+17 : offset+21])
	fmt.Printf("crc: %d\n", rb.crc)

	rb.attributes = binary.BigEndian.Uint16(dat[offset+21 : offset+23])
	fmt.Printf("attributes: %d\n", rb.attributes)

	rb.lastOffsetDelta = binary.BigEndian.Uint32(dat[offset+23 : offset+27])
	fmt.Printf("lastOffsetDelta: %d\n", rb.lastOffsetDelta)

	rb.baseTimestamp = binary.BigEndian.Uint64(dat[offset+27 : offset+35])
	fmt.Printf("baseTimestamp: %d\n", rb.baseTimestamp)

	rb.maxTimestamp = binary.BigEndian.Uint64(dat[offset+35 : offset+43])
	fmt.Printf("maxTimestamp: %d\n", rb.maxTimestamp)

	rb.producerID = binary.BigEndian.Uint64(dat[offset+43 : offset+51])
	fmt.Printf("producerID: %d\n", rb.producerID)

	rb.producerEpoch = binary.BigEndian.Uint16(dat[offset+51 : offset+53])
	fmt.Printf("producerEpoch: %d\n", rb.producerEpoch)

	rb.baseSequence = binary.BigEndian.Uint32(dat[offset+53 : offset+57])
	fmt.Printf("baseSequence: %d\n", rb.baseSequence)

	rb.recordsLength = binary.BigEndian.Uint32(dat[offset+57 : offset+61])
	fmt.Printf("recordsLength: %d\n", rb.recordsLength)

	// rb.baseOffset = binary.BigEndian.Uint64(dat[offset : offset+8])
	// rb.batchLength = binary.BigEndian.Uint32(dat[offset+8 : offset+12])
	// rb.partitionLeaderEpoch = binary.BigEndian.Uint32(dat[offset+12 : offset+16])
	// rb.magicByte = dat[offset+16]
	// rb.crc = binary.BigEndian.Uint32(dat[offset+17 : offset+21])
	// rb.attributes = binary.BigEndian.Uint16(dat[offset+21 : offset+23])
	// rb.lastOffsetDelta = binary.BigEndian.Uint32(dat[offset+23 : offset+27])
	// rb.baseTimestamp = binary.BigEndian.Uint64(dat[offset+27 : offset+35])
	// rb.maxTimestamp = binary.BigEndian.Uint64(dat[offset+35 : offset+43])
	// rb.producerID = binary.BigEndian.Uint64(dat[offset+43 : offset+51])
	// rb.producerEpoch = binary.BigEndian.Uint16(dat[offset+51 : offset+53])
	// rb.baseSequence = binary.BigEndian.Uint32(dat[offset+53 : offset+57])
	// rb.recordsLength = binary.BigEndian.Uint32(dat[offset+57 : offset+61])
	rb.records = getRecords(dat[offset+61:], rb.recordsLength)

	return *rb
}

func getRecords(dat []byte, recordsLength uint32) []Record {
	records := make([]Record, 0)
	var i uint32 = 0
	var offset uint8 = 0
	for i < recordsLength {
		// parse the record
		rec := new(Record)
		offset += getRecord(dat[offset:], rec)
		records = append(records, *rec)
		i++

	}
	return records

}

func getRecord(dat []byte, resPtr *Record) uint8 {
	fmt.Println("[getRecord]: Getting Record")
	res := *resPtr
	res.length = dat[0]
	res.attributes = dat[1]
	res.timestampDelta = dat[2]
	res.offsetDelta = dat[3]
	res.keyLength = zigzagDecode(dat[4])
	fmt.Println("[getRecord]: length:", res.length)
	fmt.Println("[getRecord]: attributes:", res.attributes)
	fmt.Println("[getRecord]: timestampDelta:", res.timestampDelta)
	fmt.Println("[getRecord]: offsetDelta:", res.offsetDelta)
	fmt.Println("[getRecord]: keyLength:", res.keyLength)

	if res.keyLength != -1 {
		res.key = make([]byte, res.keyLength)
		copy(res.key[:], dat[5:5+res.keyLength])
		fmt.Println("[getRecord]: key:", res.key)
		res.valueLength = int8(dat[5+res.keyLength])
		res.value = getRecordValue(dat[6+res.keyLength:])
	} else {
		res.valueLength = int8(dat[5])
		res.value = getRecordValue(dat[6:])
	}

	res.headersArrayCount = dat[res.length]
	fmt.Println("[getRecord]: headersArrayCount:", res.headersArrayCount)

	return res.length + 2 // to include the length field itself
}

func zigzagDecode(n uint8) int8 {
	return int8((n >> 1) ^ uint8((int8(n&1)<<7)>>7))
}

func getRecordValue(dat []byte) RecordValue {

	fmt.Println("[getRecordValue]: Getting Record Value")

	frameVer := dat[0]
	recordType := dat[1]
	fmt.Println("[getRecordValue]: frameVer:", frameVer)
	fmt.Println("[getRecordValue]: recordType:", recordType)

	switch recordType {
	case 0x2: // topic record
		return parseTopicRecord(dat[2:], frameVer)
	case 0x3: // partitionRecord
		return parsePartitionRecord(dat[2:], frameVer)
	case 0xc:
		return parseFeatureLevelRecord(dat[2:], frameVer)
	default:
		return nil
	}

}

func parseTopicRecord(dat []byte, frameVer uint8) TopicRecord {
	fmt.Println("[parseTopicRecord]: Parsing Topic Record")
	trPtr := new(TopicRecord)
	tr := *trPtr
	tr.frameVersion = frameVer
	tr.recordType = 0x2
	tr.version = dat[0]
	tr.nameLength = dat[1]
	tr.topicName = string(dat[2 : 2+tr.nameLength-1])
	copy(tr.topicUUID[:], dat[2+tr.nameLength-1:18+tr.nameLength-1])
	tr.taggedFieldsCount = dat[18+tr.nameLength-1]
	fmt.Println("[parseTopicRecord]: tr.frameVersion:", tr.frameVersion)
	fmt.Println("[parseTopicRecord]: tr.recordType:", tr.recordType)
	fmt.Println("[parseTopicRecord]: tr.version:", tr.version)
	fmt.Println("[parseTopicRecord]: tr.nameLength:", tr.nameLength)
	fmt.Println("[parseTopicRecord]: tr.topicName:", tr.topicName)
	fmt.Println("[parseTopicRecord]: tr.topicUUID:", tr.topicUUID)
	fmt.Println("[parseTopicRecord]: tr.taggedFieldsCount:", tr.taggedFieldsCount)

	return tr
}

func parsePartitionRecord(dat []byte, frameVer uint8) PartitionRecord {
	fmt.Println("[parsePartitionRecord]: Parsing Partition Record")

	prPtr := new(PartitionRecord)
	pr := *prPtr
	pr.frameVersion = frameVer
	pr.recordType = 0x3
	pr.version = dat[0]
	pr.partitionID = binary.BigEndian.Uint32(dat[1:5])
	copy(pr.topicUUID[:], dat[5:21])
	pr.lenReplicaArray = dat[21]
	pr.replicaArray = make([]uint32, 0)
	for i := 0; i < int(pr.lenReplicaArray)-1; i++ {
		pr.replicaArray = append(pr.replicaArray, binary.BigEndian.Uint32(dat[21+i*4:21+(i+1)*4]))
	}
	// final offset after appending to replica array -> (pr.lenReplicaArray - 1) * 4
	offset := (int(pr.lenReplicaArray) - 1) * 4
	pr.lenInSyncReplicaArray = dat[offset]
	pr.inSyncReplicaArray = make([]uint32, 0)
	for i := 0; i < int(pr.lenInSyncReplicaArray)-1; i++ {
		pr.inSyncReplicaArray = append(pr.inSyncReplicaArray, binary.BigEndian.Uint32(dat[offset+1+i*4:offset+1+(i+1*4)]))
	}
	offset = offset + 1 + (int(pr.lenInSyncReplicaArray)-1)*4
	pr.lenRemovingReplicasArray = dat[offset]
	pr.removingReplicasArray = make([]uint32, 0)
	for i := 0; i < int(pr.lenRemovingReplicasArray)-1; i++ {
		pr.removingReplicasArray = append(pr.removingReplicasArray, binary.BigEndian.Uint32(dat[offset+1+i*4:offset+1+(i+1)*4]))
	}
	offset = offset + 1 + (int(pr.lenRemovingReplicasArray)-1)*4

	pr.lenAddingReplicasArray = dat[offset]
	pr.addingReplicasArray = make([]uint32, 0)
	for i := 0; i < int(pr.lenAddingReplicasArray)-1; i++ {
		pr.addingReplicasArray = append(pr.addingReplicasArray, binary.BigEndian.Uint32(dat[offset+1+i*4:offset+1+(i+1)*4]))
	}
	offset = offset + 1 + (int(pr.lenAddingReplicasArray)-1)*4
	pr.leader = binary.BigEndian.Uint32(dat[offset : offset+4])
	pr.leaderEpoch = binary.BigEndian.Uint32(dat[offset+4 : offset+8])
	pr.partitionEpoch = binary.BigEndian.Uint32(dat[offset+8 : offset+12])
	pr.lenDirectoriesArray = dat[offset+12]
	pr.directoriesArray = make([][16]byte, 0)
	for i := 0; i < int(pr.lenDirectoriesArray)-1; i++ {
		var tmp [16]byte
		copy(tmp[:], dat[offset+12+i*16:offset+12+(i+1)*16])
		pr.directoriesArray = append(pr.directoriesArray, tmp)
	}
	offset = offset + 12 + (int(pr.lenDirectoriesArray)-1)*16
	pr.taggedFieldsCount = dat[offset]
	return pr

}

func parseFeatureLevelRecord(dat []byte, frameVer uint8) FeatureLevelRecord {
	fmt.Println("[parseFeatureLevelRecord]: Parsing FeatureLevelRecord")
	flPtr := new(FeatureLevelRecord)
	fl := *flPtr
	fl.frameVersion = frameVer
	fl.recordType = 0xc
	fl.version = dat[0]
	fl.nameLength = dat[1]
	fl.name = string(dat[2 : 2+fl.nameLength-1])
	fl.featureLevel = binary.BigEndian.Uint16(dat[2+fl.nameLength-1 : 3+fl.nameLength])
	fl.taggedFieldsCount = dat[3+fl.nameLength]

	fmt.Println("[parseFeatureLevelRecord]: frameVersion:", fl.frameVersion)
	fmt.Println("[parseFeatureLevelRecord]: recordType:", fl.recordType)
	fmt.Println("[parseFeatureLevelRecord]: version:", fl.version)
	fmt.Println("[parseFeatureLevelRecord]: nameLength:", fl.nameLength)
	fmt.Println("[parseFeatureLevelRecord]: name:", fl.name)
	fmt.Println("[parseFeatureLevelRecord]: featureLevel:", fl.featureLevel)
	fmt.Println("[parseFeatureLevelRecord]: taggedFieldsCount:", fl.taggedFieldsCount)

	return fl

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

type RecordBatch struct {
	baseOffset           uint64
	batchLength          uint32
	partitionLeaderEpoch uint32
	magicByte            byte
	crc                  uint32
	attributes           uint16
	lastOffsetDelta      uint32
	baseTimestamp        uint64
	maxTimestamp         uint64
	producerID           uint64
	producerEpoch        uint16
	baseSequence         uint32
	recordsLength        uint32
	records              []Record
}

type RecordValue interface {
	isRecordValue() uint8
}

func (TopicRecord) isRecordValue() uint8        { return 0x2 }
func (PartitionRecord) isRecordValue() uint8    { return 0x3 }
func (FeatureLevelRecord) isRecordValue() uint8 { return 0xc }

type Record struct {
	length            uint8 // from attributes to end of record
	attributes        uint8
	timestampDelta    uint8
	offsetDelta       uint8
	keyLength         int8
	key               []byte
	valueLength       int8
	value             RecordValue
	headersArrayCount uint8
}

type TopicRecord struct {
	frameVersion      uint8
	recordType        uint8
	version           uint8
	nameLength        uint8
	topicName         string
	topicUUID         [16]byte
	taggedFieldsCount uint8
}

type PartitionRecord struct {
	frameVersion             uint8
	recordType               uint8
	version                  uint8
	partitionID              uint32
	topicUUID                [16]byte
	lenReplicaArray          uint8
	replicaArray             []uint32
	lenInSyncReplicaArray    uint8
	inSyncReplicaArray       []uint32
	lenRemovingReplicasArray uint8
	removingReplicasArray    []uint32
	lenAddingReplicasArray   uint8
	addingReplicasArray      []uint32
	leader                   uint32
	leaderEpoch              uint32
	partitionEpoch           uint32
	lenDirectoriesArray      uint8
	directoriesArray         [][16]byte
	taggedFieldsCount        uint8
}

type FeatureLevelRecord struct {
	frameVersion      uint8
	recordType        uint8
	version           uint8
	nameLength        uint8
	name              string
	featureLevel      uint16
	taggedFieldsCount uint8
}
