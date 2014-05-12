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

type Request struct {
	conn      *Connection
	msgLength []byte
	header    []byte
	body      []byte
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
	conn.Greeting.version = bytes.NewBuffer(greeting[0:64]).String()
	conn.Greeting.auth = bytes.NewBuffer(greeting[64:128]).String()

	fmt.Println("Success")
	fmt.Println("Version:", conn.Greeting.version)
	return
}

func (conn *Connection) Read() (response []byte, err error){
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

func (conn *Connection) Write(data *bytes.Buffer) (err error) {
	_, err = conn.connection.Write(data.Bytes())
	return
}

func (conn *Connection) NextRequestId() (requestId int32) {
	conn.requestId = conn.requestId + 1
	return conn.requestId
}

func (request *Request) setHeader(reqCode int32) (err error) {
	requestId := request.conn.NextRequestId()
	header := map[int32]int32{}
	header[KeyCode] = reqCode
	header[KeySync] = requestId

	msg, err := msgpack.Marshal(header)
	request.header = msg
	return
}

func (request *Request) setMsgLength() (err error) {
	lngth := uint32(len(request.header) + len(request.body))
	msgLength, err := msgpack.Marshal(lngth)
	if err != nil {
		return
	}
	request.msgLength = msgLength
	return
}

func (request *Request) pack() (packet *bytes.Buffer, err error) {
	packet = new(bytes.Buffer)
	err = request.setMsgLength()
	if err != nil {
		return
	}

	packet.Write(request.msgLength)
	packet.Write(request.header)
	packet.Write(request.body)
	return
}

func (conn *Connection) request(reqCode int32, body []byte) (resp *Response, err error) {
	resp = &Response{}
	request := &Request{}
	request.conn = conn
	err = request.setHeader(reqCode)
	if err != nil {
		return
	}

	if body != nil {
		request.body = body
	}
	
	err = request.setMsgLength()
	if err != nil {
		return
	}

	msg, err := request.pack()
	fmt.Println("BYTES", msg.Bytes())
	if err != nil {
		return
	}

	err = conn.Write(msg)
	if err != nil {
		return
	}

	resp_bytes, err := conn.Read()
	if err != nil {
		return
	}

	err = unpack(resp, resp_bytes)

	return
}

func (conn *Connection) Ping() (resp *Response, err error) {
	resp, err = conn.request(PingRequest, nil)
	return
}

func (conn *Connection) Select(spaceNo, indexNo, offset, limit uint32, iterator string, key []interface{}) (resp *Response, err error) {

	body := make(map[int]interface{})
	body[KeySpaceNo]  = spaceNo
	body[KeyIndexNo]  = indexNo
	// body[KeyIterator] = 0
	body[KeyOffset]   = offset
	body[KeyLimit]    = limit
	body[KeyKey]      = key
	body_msg, err := msgpack.Marshal(body)
	if err != nil {
		return
	}
	resp, err = conn.request(SelectRequest, body_msg)
	return
}

func (conn *Connection) Insert(spaceNo uint32, tuple []interface{}) (resp *Response, err error) {
	body := make(map[int]interface{})
	body[KeySpaceNo]  = spaceNo
	body[KeyTuple]    = tuple
	body_msg, err := msgpack.Marshal(body)
	if err != nil {
		return
	}
	resp, err = conn.request(InsertRequest, body_msg)
	return
}

func (conn *Connection) Replace(spaceNo uint32, tuple []interface{}) (resp *Response, err error) {
	return
}

func (conn *Connection) Delete(spaceNo, indexNo uint32, key []interface{}) (resp *Response, err error) {
	return
}

func (conn *Connection) Update(spaceNo, indexNo uint32, key, tuple []interface{}) (resp *Response, err error) {
	return
}

func (conn *Connection) Call(functionName string, tuple []interface{}) (resp *Response, err error) {
	return
}


func (conn *Connection) Auth(key, tuple []interface{}) (resp *Response, err error) {
	return
}
