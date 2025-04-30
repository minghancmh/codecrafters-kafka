package api

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

func ReadLogFile(path string) [][]byte {
	log("===================READING LOG FILE===================")
	dat, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading file at path: ", path)
	}
	log("dat:", dat)
	log("Hexdump dat: %s\n", hex.Dump(dat))

	var offset uint32 = 0
	// topicRecords := make(map[string]TopicRecord)               // topicName -> TopicRecord
	// partitionRecords := make(map[types.UUID][]PartitionRecord) // topic UUID -> []PartitionRecord
	res := make([][]byte, 0)

	for offset < uint32(len(dat)) {
		log("Offset: %v\n", offset)
		rb := deserializeRecordBatchMessageLogFile(dat[offset:], 0)
		lengthBatch := binary.BigEndian.Uint32(dat[offset+8 : offset+12])
		log("rb:", rb)
		res = append(res, dat[offset:offset+12+lengthBatch])

		offset += 12 + lengthBatch
	}
	return res
}
func deserializeRecordBatchMessageLogFile(dat []byte, offset uint64) RecordBatch {
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

	rb.records = getRecordsMessageLogFile(dat[offset+61:], rb.recordsLength)

	return *rb
}

func getRecordsMessageLogFile(dat []byte, recordsLength uint32) []record {
	records := make([]record, 0)
	var i uint32 = 0
	var offset uint64 = 0
	for i < recordsLength {
		// parse the record
		rec := new(record)
		offset += getRecordMessageLogFile(dat[offset:], rec)
		fmt.Println("[getRecords]: rec:", *rec)
		records = append(records, *rec)
		// fmt.Println("[getRecords]: records: ", records)
		i++

	}
	return records

}
func getRecordMessageLogFile(dat []byte, resPtr *record) uint64 {
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
		// resPtr.value = getRecordValue(dat[offset:])
		resPtr.value = MessageRecord{string(dat[offset : offset+int(resPtr.valueLength)])}
		log("value: %v", resPtr.value)
	}

	resPtr.headersArrayCount = dat[resPtr.length]
	// fmt.Println("[getRecord]: headersArrayCount:", resPtr.headersArrayCount)
	fmt.Println("[getRecord]: resPtr: ", resPtr)

	return uint64(resPtr.length + uint64(nbytes))
}
