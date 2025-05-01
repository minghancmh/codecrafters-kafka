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

	log("Hexdump dat: %s\n", hex.Dump(dat))

	var offset uint32 = 0
	res := make([][]byte, 0)

	for offset < uint32(len(dat)) {
		lengthBatch := binary.BigEndian.Uint32(dat[offset+8 : offset+12])
		res = append(res, dat[offset:offset+12+lengthBatch])
		offset += 12 + lengthBatch
	}
	return res
}
func deserializeRecordBatchMessageLogFile(dat []byte, offset uint64) RecordBatch {
	rb := new(RecordBatch)
	rb.baseOffset = binary.BigEndian.Uint64(dat[offset : offset+8])
	rb.batchLength = binary.BigEndian.Uint32(dat[offset+8 : offset+12])
	rb.partitionLeaderEpoch = binary.BigEndian.Uint32(dat[offset+12 : offset+16])
	rb.magicByte = dat[offset+16]
	rb.crc = binary.BigEndian.Uint32(dat[offset+17 : offset+21])
	rb.attributes = binary.BigEndian.Uint16(dat[offset+21 : offset+23])
	rb.lastOffsetDelta = binary.BigEndian.Uint32(dat[offset+23 : offset+27])
	rb.baseTimestamp = binary.BigEndian.Uint64(dat[offset+27 : offset+35])
	rb.maxTimestamp = binary.BigEndian.Uint64(dat[offset+35 : offset+43])
	rb.producerID = binary.BigEndian.Uint64(dat[offset+43 : offset+51])
	rb.producerEpoch = binary.BigEndian.Uint16(dat[offset+51 : offset+53])
	rb.baseSequence = binary.BigEndian.Uint32(dat[offset+53 : offset+57])
	rb.recordsLength = binary.BigEndian.Uint32(dat[offset+57 : offset+61])
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
		records = append(records, *rec)
		i++

	}
	return records

}
func getRecordMessageLogFile(dat []byte, resPtr *record) uint64 {
	tmplen, nbytes := binary.Uvarint(dat[0:])
	if nbytes <= 0 {
		log("Invalid Uvarint encoding!")
	}
	resPtr.length = uint64(zigzag64Decode(tmplen))
	offset := nbytes

	resPtr.attributes = dat[offset]
	resPtr.timestampDelta = dat[offset+1]
	resPtr.offsetDelta = dat[offset+2]
	resPtr.keyLength = int8(zigzag64Decode(uint64(dat[offset+3])))

	if resPtr.keyLength != -1 {
		resPtr.key = make([]byte, resPtr.keyLength)
		copy(resPtr.key[:], dat[offset+4:offset+4+int(resPtr.keyLength)])
		offset = offset + 4 + int(resPtr.keyLength)

		tmplen, nbytes := binary.Uvarint(dat[offset:])
		if nbytes <= 0 {
			log("Invalid decode of value length!")
		}
		resPtr.valueLength = int8(zigzag64Decode(tmplen))
		offset = offset + nbytes
		resPtr.value = getRecordValue(dat[offset:])
		log("value: %v", resPtr.value)

		resPtr.valueLength = int8(dat[offset+5+int(resPtr.keyLength)])
		resPtr.value = getRecordValue(dat[6+resPtr.keyLength:])
	} else {
		tmplen, nbytes := binary.Uvarint(dat[offset+4:])
		if nbytes <= 0 {
			log("Invalid decode of value length!")
		}
		resPtr.valueLength = int8(zigzag64Decode(tmplen))
		offset = offset + 4 + nbytes
		resPtr.value = MessageRecord{string(dat[offset : offset+int(resPtr.valueLength)])}
		log("value: %v", resPtr.value)
	}

	resPtr.headersArrayCount = dat[resPtr.length]

	return uint64(resPtr.length + uint64(nbytes))
}
