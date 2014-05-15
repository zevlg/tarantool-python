package tarantool

import (
	"net"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"sync/atomic"
	"bytes"
	"sync"
)

type Connection struct {
	connection net.Conn
	mutex      *sync.Mutex
	requestId  uint64
	Greeting   *Greeting
	requests   map[uint64]chan *Response
	packets    chan []byte
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
	connection.(*net.TCPConn).SetNoDelay(true)

	fmt.Println("Connected ...")

	conn = &Connection{ connection, &sync.Mutex{}, 0, &Greeting{}, make(map[uint64]chan *Response), make(chan []byte) }
	err = conn.handShake()

	go conn.writer()
	go conn.reader()

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

func (conn *Connection) writer(){
	var (
		err error
		packet []byte
	)
	for {
		packet = <- conn.packets
		err = conn.write(packet)
		if err != nil {
			panic(err)
		}
	}
}

func (conn *Connection) reader() {
	var (
		err error
		resp_bytes []byte
	)
	for {
		resp_bytes, err = conn.read()
		if err != nil {
			panic(err)
		}

		resp := NewResponse(resp_bytes)
		respChan := conn.requests[resp.RequestId]
		conn.mutex.Lock()
		delete(conn.requests, resp.RequestId)
		conn.mutex.Unlock()
		respChan <- resp
	}
}

func (conn *Connection) write(data []byte) (err error) {
	l, err := conn.connection.Write(data)
	if l != len(data) {
		panic("Wrong length writed")
	}
	return
}

func (conn *Connection) read() (response []byte, err error){
	var length_uint uint32
	var l, tl int
	length := make([]byte, PacketLengthBytes)	

	tl = 0
	for tl < int(PacketLengthBytes) {
		l, err = conn.connection.Read(length[tl:])
		tl += l
		if err != nil {
			return
		}
	}

    err = msgpack.Unmarshal(length, &length_uint)
	if err != nil {
		return
	}

	response = make([]byte, length_uint)
	if(length_uint > 0){
		tl = 0
		for tl < int(length_uint) {
			l, err = conn.connection.Read(response[tl:])
			tl += l
			if err != nil {
				return
			}
		}
	}

	return
}

func (conn *Connection) nextRequestId() (requestId uint64) {
	conn.requestId = atomic.AddUint64(&conn.requestId, 1)
	return conn.requestId
}
