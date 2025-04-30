package api

import (
	"encoding/binary"
	// "encoding/hex"
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/types"
)

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
	records              []record
}

type record struct {
	length            uint64 // from attributes to end of record
	attributes        uint8
	timestampDelta    uint8
	offsetDelta       uint8
	keyLength         int8
	key               []byte
	valueLength       int8
	value             recordValue
	headersArrayCount uint8
}

type recordValue interface {
	isRecordValue() uint8
}

func (TopicRecord) isRecordValue() uint8        { return 0x2 }
func (PartitionRecord) isRecordValue() uint8    { return 0x3 }
func (featureLevelRecord) isRecordValue() uint8 { return 0xc }

type TopicRecord struct {
	FrameVersion      uint8
	RecordType        uint8
	Version           uint8
	TopicName         types.CompactString
	TopicUUID         types.UUID
	TaggedFieldsCount uint8
}

type PartitionRecord struct {
	FrameVersion          uint8
	RecordType            uint8
	Version               uint8
	PartitionID           uint32
	TopicUUID             types.UUID
	ReplicaArray          types.CompactArray[types.Replica]
	InSyncReplicaArray    types.CompactArray[types.Replica]
	RemovingReplicasArray types.CompactArray[types.Replica]
	AddingReplicasArray   types.CompactArray[types.Replica]
	Leader                uint32
	LeaderEpoch           uint32
	PartitionEpoch        uint32
	DirectoriesArray      types.CompactArray[types.UUID]
	TaggedFieldsCount     uint8
}

type featureLevelRecord struct {
	frameVersion      uint8
	recordType        uint8
	version           uint8
	nameLength        uint8
	name              string
	featureLevel      uint16
	taggedFieldsCount uint8
}

// DescribeTopicPartitionsFromMetadataFile Utils
func DescribeTopicPartitionsFromMetadataFile() (map[types.UUID][]PartitionRecord, map[string]TopicRecord) {
	path := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	dat, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading file at path: ", path)
	}
	// log("dat:", dat)
	// log("Hexdump dat: %s\n", hex.Dump(dat))

	var offset uint32 = 0
	topicRecords := make(map[string]TopicRecord)               // topicName -> TopicRecord
	partitionRecords := make(map[types.UUID][]PartitionRecord) // topic UUID -> []PartitionRecord
	// topicUUIDtoRecordBatch := make(map[types.UUID][]byte)          // topicUUID -> serializedRecordBatch

	for offset < uint32(len(dat)) {
		log("Offset: %v\n", offset)
		rb := deserializeRecordBatch(dat[offset:], 0)
		lengthBatch := binary.BigEndian.Uint32(dat[offset+8 : offset+12])

		// fmt.Println("[describeTopicPartitions]: len(rb.records):", len(rb.records))

		for _, rec := range rb.records {
			// fmt.Println("[describeTopicPartitions]: rec:", rec)
			val := rec.value
			// fmt.Println("[describeTopicPartitions]: rec.value:", rec.value)

			switch val.isRecordValue() {
			case 0x2:
				// fmt.Println("[describeTopicPartitions]: Topic Record found!")
				tr := val.(TopicRecord)
				topicRecords[tr.TopicName.Content] = tr
				tr.TopicName.Length = uint64(len(tr.TopicName.Content))

				// topicUUIDtoRecordBatch[tr.TopicUUID] = dat[offset : offset+12+lengthBatch]

			case 0x3: // partition record
				// fmt.Println("[describeTopicPartitions]: Partition Record found!")
				pr := val.(PartitionRecord)
				_, ok := partitionRecords[pr.TopicUUID]
				if !ok {
					partitionRecords[pr.TopicUUID] = make([]PartitionRecord, 0)
				}
				partitionRecords[pr.TopicUUID] = append(partitionRecords[pr.TopicUUID], pr)

			case 0xc:
				// fmt.Println("[describeTopicPartitions]: feature level record parsing to be implemented")
			default:
				// fmt.Println("[describeTopicPartitions]: no record type found")
			}

		}
		offset += 12 + lengthBatch
	}
	return partitionRecords, topicRecords
}

func deserializeRecordBatch(dat []byte, offset uint64) RecordBatch {
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

	rb.records = getRecords(dat[offset+61:], rb.recordsLength)

	return *rb
}

func getRecords(dat []byte, recordsLength uint32) []record {
	records := make([]record, 0)
	var i uint32 = 0
	var offset uint64 = 0
	for i < recordsLength {
		// parse the record
		rec := new(record)
		offset += getRecord(dat[offset:], rec)
		fmt.Println("[getRecords]: rec:", *rec)
		records = append(records, *rec)
		// fmt.Println("[getRecords]: records: ", records)
		i++

	}
	return records

}

func getRecord(dat []byte, resPtr *record) uint64 {
	// fmt.Println("[getRecord]: Getting Record")
	// length := zigzagDecode(dat[0])
	tmplen, nbytes := binary.Uvarint(dat[0:])
	if nbytes <= 0 {
		log("Invalid Uvarint encoding!")
	}
	resPtr.length = uint64(zigzagDecode(tmplen))
	log("resPtr.length:", resPtr.length)
	offset := nbytes

	resPtr.attributes = dat[offset]
	resPtr.timestampDelta = dat[offset+1]
	resPtr.offsetDelta = dat[offset+2]
	resPtr.keyLength = int8(zigzagDecode(uint64(dat[offset+3])))
	fmt.Println("[getRecord]: length:", resPtr.length)
	fmt.Println("[getRecord]: attributes:", resPtr.attributes)
	fmt.Println("[getRecord]: timestampDelta:", resPtr.timestampDelta)
	fmt.Println("[getRecord]: offsetDelta:", resPtr.offsetDelta)
	fmt.Println("[getRecord]: keyLength:", resPtr.keyLength)

	if resPtr.keyLength != -1 {
		log("Key length:", resPtr.keyLength)
		resPtr.key = make([]byte, resPtr.keyLength)
		copy(resPtr.key[:], dat[offset+4:offset+4+int(resPtr.keyLength)])
		log("Key:", resPtr.key)
		offset = offset + 4 + int(resPtr.keyLength)

		tmplen, nbytes := binary.Uvarint(dat[offset:])
		if nbytes <= 0 {
			log("Invalid decode of value length!")
		}
		resPtr.valueLength = int8(zigzagDecode(tmplen))
		offset = offset + nbytes
		log("valueLength: %v", resPtr.valueLength)
		resPtr.value = getRecordValue(dat[offset:])
		log("value: %v", resPtr.value)

		resPtr.valueLength = int8(dat[offset+5+int(resPtr.keyLength)])
		resPtr.value = getRecordValue(dat[6+resPtr.keyLength:])
	} else {
		tmplen, nbytes := binary.Uvarint(dat[offset+4:])
		if nbytes <= 0 {
			log("Invalid decode of value length!")
		}
		resPtr.valueLength = int8(zigzagDecode(tmplen))
		offset = offset + 4 + nbytes
		log("valueLength: %v", resPtr.valueLength)
		resPtr.value = getRecordValue(dat[offset:])
		log("value: %v", resPtr.value)
	}

	resPtr.headersArrayCount = dat[resPtr.length]
	// fmt.Println("[getRecord]: headersArrayCount:", resPtr.headersArrayCount)
	// fmt.Println("[getRecord]: resPtr: ", resPtr)

	return uint64(resPtr.length + uint64(nbytes))
}

func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ uint64((int64(n&1)<<63)>>63))
}

func getRecordValue(dat []byte) recordValue {

	// fmt.Println("[getRecordValue]: Getting Record Value")

	frameVer := dat[0]
	recordType := dat[1]
	fmt.Println("[getRecordValue]: frameVer:", frameVer)
	fmt.Println("[getRecordValue]: recordType:", recordType)

	switch recordType {
	case 0x2: // topic record
		return parseTopicRecord(dat[2:], frameVer)
	case 0x3: // partitionRecord
		return deserializePartitionRecord(dat[2:], frameVer)
	case 0xc:
		return parseFeatureLevelRecord(dat[2:], frameVer)
	default:
		return nil
	}

}

func parseTopicRecord(dat []byte, frameVer uint8) TopicRecord {
	// fmt.Println("[parseTopicRecord]: Parsing Topic Record")
	trPtr := new(TopicRecord)
	trPtr.FrameVersion = frameVer
	trPtr.RecordType = 0x2
	trPtr.Version = dat[0]

	nbytes, err := trPtr.TopicName.FromBytes(dat[1:])
	if err != nil {
		log("Error parsing topic name")
	}
	log("topicName: %s", trPtr.TopicName.Content)
	offset := 1 + nbytes

	copy(trPtr.TopicUUID[:], dat[offset:offset+16])
	trPtr.TaggedFieldsCount = dat[offset+16]
	// fmt.Println("[parseTopicRecord]: trPtr.frameVersion:", trPtr.frameVersion)
	// fmt.Println("[parseTopicRecord]: trPtr.recordType:", trPtr.recordType)
	// fmt.Println("[parseTopicRecord]: trPtr.version:", trPtr.version)
	// fmt.Println("[parseTopicRecord]: trPtr.topicName:", trPtr.topicName)
	// fmt.Println("[parseTopicRecord]: trPtr.topicUUID:", trPtr.TopicUUID)
	// fmt.Println("[parseTopicRecord]: trPtr.taggedFieldsCount:", trPtr.taggedFieldsCount)

	return *trPtr
}

func deserializePartitionRecord(dat []byte, frameVer uint8) PartitionRecord {
	// fmt.Println("[parsePartitionRecord]: Parsing Partition Record")

	prPtr := new(PartitionRecord)
	prPtr.FrameVersion = frameVer
	prPtr.RecordType = 0x3
	prPtr.Version = dat[0]
	prPtr.PartitionID = binary.BigEndian.Uint32(dat[1:5])
	// log("FrameVersion: %v", prPtr.FrameVersion)
	// log("RecordType: %v", prPtr.RecordType)
	// log("Version: %v", prPtr.Version)
	// log("PartitionID: %v", prPtr.PartitionID)
	copy(prPtr.TopicUUID[:], dat[5:21])
	// log("topicUUID: %v", prPtr.TopicUUID)

	nbytes, err := prPtr.ReplicaArray.FromBytes(dat[21:], types.ReplicaArrayReader)
	if err != nil {
		log("error decoding replica array")
	}
	// log("ReplicaArray:", prPtr.ReplicaArray)
	offset := 21 + nbytes

	nbytes, err = prPtr.InSyncReplicaArray.FromBytes(dat[offset:], types.ReplicaArrayReader)
	if err != nil {
		log("error decoding insync replica array")
	}
	// log("InSyncReplicaArray:", prPtr.InSyncReplicaArray)
	offset += nbytes

	nbytes, err = prPtr.RemovingReplicasArray.FromBytes(dat[offset:], types.ReplicaArrayReader)
	if err != nil {
		log("error decoding removing replica array")
	}
	// log("RemovingReplicasArray:", prPtr.RemovingReplicasArray)
	offset += nbytes

	nbytes, err = prPtr.AddingReplicasArray.FromBytes(dat[offset:], types.ReplicaArrayReader)
	if err != nil {
		log("error decoding adding replica array")
	}
	// log("AddingReplicasArray:", prPtr.AddingReplicasArray)
	offset += nbytes

	prPtr.Leader = binary.BigEndian.Uint32(dat[offset : offset+4])
	prPtr.LeaderEpoch = binary.BigEndian.Uint32(dat[offset+4 : offset+8])
	prPtr.PartitionEpoch = binary.BigEndian.Uint32(dat[offset+8 : offset+12])
	// log("Leader: %v", prPtr.Leader)
	// log("LeaderEpoch: %v", prPtr.LeaderEpoch)
	// log("PartitionEpoch: %v", prPtr.PartitionEpoch)

	offset = offset + 12
	nbytes, err = prPtr.DirectoriesArray.FromBytes(dat[offset:], types.UUIDReader)
	// log("DirectoriesArray: %v", prPtr.DirectoriesArray)

	offset += nbytes
	prPtr.TaggedFieldsCount = dat[offset]
	// log("TaggedFieldsCount: %v", prPtr.TaggedFieldsCount)
	return *prPtr

}

func parseFeatureLevelRecord(dat []byte, frameVer uint8) featureLevelRecord {
	// fmt.Println("[parseFeatureLevelRecord]: Parsing FeatureLevelRecord")
	flPtr := new(featureLevelRecord)
	fl := *flPtr
	fl.frameVersion = frameVer
	fl.recordType = 0xc
	fl.version = dat[0]
	fl.nameLength = dat[1]
	fl.name = string(dat[2 : 2+fl.nameLength-1])
	fl.featureLevel = binary.BigEndian.Uint16(dat[2+fl.nameLength-1 : 3+fl.nameLength])
	fl.taggedFieldsCount = dat[3+fl.nameLength]

	// fmt.Println("[parseFeatureLevelRecord]: frameVersion:", fl.frameVersion)
	// fmt.Println("[parseFeatureLevelRecord]: recordType:", fl.recordType)
	// fmt.Println("[parseFeatureLevelRecord]: version:", fl.version)
	// fmt.Println("[parseFeatureLevelRecord]: nameLength:", fl.nameLength)
	// fmt.Println("[parseFeatureLevelRecord]: name:", fl.name)
	// fmt.Println("[parseFeatureLevelRecord]: featureLevel:", fl.featureLevel)
	// fmt.Println("[parseFeatureLevelRecord]: taggedFieldsCount:", fl.taggedFieldsCount)

	return fl

}

func ReadLogFile(path string) map[types.UUID][]byte {
	log("===================READING LOG FILE===================")
	dat, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading file at path: ", path)
	}
	// log("dat:", dat)
	// log("Hexdump dat: %s\n", hex.Dump(dat))

	var offset uint32 = 0
	// topicRecords := make(map[string]TopicRecord)               // topicName -> TopicRecord
	// partitionRecords := make(map[types.UUID][]PartitionRecord) // topic UUID -> []PartitionRecord
	topicUUIDtoRecordBatch := make(map[types.UUID][]byte)      // topicUUID -> serializedRecordBatch

	for offset < uint32(len(dat)) {
		log("Offset: %v\n", offset)
		rb := deserializeRecordBatch(dat[offset:], 0)
		lengthBatch := binary.BigEndian.Uint32(dat[offset+8 : offset+12])
		log("rb:", rb)

		// fmt.Println("[describeTopicPartitions]: len(rb.records):", len(rb.records))

		// for _, rec := range rb.records {
		// 	// fmt.Println("[describeTopicPartitions]: rec:", rec)
		// 	val := rec.value
		// 	// fmt.Println("[describeTopicPartitions]: rec.value:", rec.value)

		// 	switch val.isRecordValue() {
		// 	case 0x2:
		// 		// fmt.Println("[describeTopicPartitions]: Topic Record found!")
		// 		tr := val.(TopicRecord)
		// 		topicRecords[tr.TopicName.Content] = tr
		// 		tr.TopicName.Length = uint64(len(tr.TopicName.Content))

		// 		// topicUUIDtoRecordBatch[tr.TopicUUID] = dat[offset : offset+12+lengthBatch]

		// 	case 0x3: // partition record
		// 		// fmt.Println("[describeTopicPartitions]: Partition Record found!")
		// 		pr := val.(PartitionRecord)
		// 		_, ok := partitionRecords[pr.TopicUUID]
		// 		if !ok {
		// 			partitionRecords[pr.TopicUUID] = make([]PartitionRecord, 0)
		// 		}
		// 		partitionRecords[pr.TopicUUID] = append(partitionRecords[pr.TopicUUID], pr)

		// 	case 0xc:
		// 		// fmt.Println("[describeTopicPartitions]: feature level record parsing to be implemented")
		// 	default:
		// 		// fmt.Println("[describeTopicPartitions]: no record type found")
		// 	}

		// }
		offset += 12 + lengthBatch
	}
	return topicUUIDtoRecordBatch
}
