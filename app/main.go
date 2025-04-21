package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

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


	// Example request message
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

	corrID := buf[8:12]

	fmt.Printf("corrId: %x\n", corrID)

	// messageSize(4byte) | correlationID(4byte) | Body...
	defer conn.Close() // we need to close the connection after function exit
	conn.Write([]byte{1, 1, 1, 1, buf[8], buf[9], buf[10], buf[11]})
}
