package main

import (
	"fmt"
	"net"
	"os"
	"encoding/binary"
	"bytes"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var UNSUPPORTED_VERSION uint16 = 35

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}


	// Example request message (all in big endian)
	//	00 00 00 23  // message_size:        35 			-> 4 byte
	//	00 12        // request_api_key:     18				-> 2 byte
	// 	00 04        // request_api_version: 4				-> 2 byte
	// 	6f 7f c6 61  // correlation_id:      1870644833		-> 4 byte

	buf := make([]byte, 1024)


	_, err = conn.Read(buf)
	if err != nil {
		fmt.Println("read err:", err)
		os.Exit(1)
	}
	fmt.Printf("request: %s\n", buf)

	// parse the message
	// message_size :=  binary.BigEndian.Uint32(buf[0:4])	
	// requestAPIKey := binary.BigEndian.Uint16(buf[4:6])
	requestAPIVersion :=  binary.BigEndian.Uint16(buf[6:8])
	corrID := binary.BigEndian.Uint32(buf[8:12])

	response := make([]byte, 1024)

	setMessageSize(&response, 3735928559) // DEADBEEF for placeholder
	setCorrelationId(&response, corrID)

	fmt.Printf("response: %x\n", response)




	if requestAPIVersion > 4 {
		fmt.Printf("Requested API Version not supported: %d\n", requestAPIVersion)
		setErrorCode(&response, UNSUPPORTED_VERSION)
		conn.Write(response)
		os.Exit(1)
	}


	// messageSize(4byte) | correlationID(4byte) | Body...
	defer conn.Close() // we need to close the connection after function exit
	conn.Write(response)
}

func setMessageSize(buf *[]byte, msgSize uint32) {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, msgSize)
	if err != nil {
		fmt.Println("Setting msg size failed: ", err)
		return
	}
	tmpBytes := tmp.Bytes()
	copy((*buf)[0:4], tmpBytes)
	return
}

func setCorrelationId(buf *[]byte, corrId uint32) {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, corrId)
	if err != nil {
		fmt.Println("Setting correlation ID failed: ", err)
		return
	}
	tmpBytes := tmp.Bytes()
	copy((*buf)[4:8], tmpBytes)
}

func setErrorCode(buf *[]byte, errorCode uint16) {
	tmp := new(bytes.Buffer)
	err := binary.Write(tmp, binary.BigEndian, errorCode)
	if err != nil {
		fmt.Println("Setting error code failed: ", err)
		return
	}
	tmpBytes := tmp.Bytes()
	copy((*buf)[8:10], tmpBytes)
}

