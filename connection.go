package tarantool

import (
	"net"
	"fmt"
	"github.com/vmihailenco/msgpack"
	// "atomic"
	"bytes"
	// "encoding/binary"
)

const (
	PacketLengthBytes = 5
)

type Connection struct {
	connection net.Conn
	requestId  int32
	Greeting   *Greeting
}

type Greeting struct {
	version string
	auth    string
}

func Connect(addr string) (conn *Connection, err error) {
	fmt.Printf("Connecting to %s ...\n", addr)
	connection, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}

	fmt.Println("Connected ...")

	conn = &Connection{ connection, 0, &Greeting{} }
	err = conn.handShake()

	return
}

func (conn *Connection) handShake() (err error) {
	fmt.Printf("Greeting ... ")
	greeting := make([]byte, 128)
	_, err = conn.connection.Read(greeting)
	if err != nil {
		fmt.Println("Error")
		return
	}
	conn.Greeting.version = bytes.NewBuffer(greeting[:64]).String()
	conn.Greeting.auth = bytes.NewBuffer(greeting[64:]).String()

	fmt.Println("Success")
	fmt.Println("Version:", conn.Greeting.version)
	return
}

func (conn *Connection) write(data *bytes.Buffer) (err error) {
	_, err = conn.connection.Write(data.Bytes())
	return
}

func (conn *Connection) read() (response []byte, err error){
	length := make([]byte, PacketLengthBytes)
	_, err = conn.connection.Read(length)
	if err != nil {
		return
	}

	var length_uint uint32
    err = msgpack.Unmarshal(length, &length_uint)
	if err != nil {
		return
	}

	response = make([]byte, length_uint)
	if(length_uint > 0){
		_, err = conn.connection.Read(response)
	}

	return
}

func (conn *Connection) nextRequestId() (requestId int32) {
	conn.requestId = conn.requestId + 1
	return conn.requestId
}
