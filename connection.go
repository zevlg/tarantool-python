package tarantool

import (
	"net"
	"github.com/vmihailenco/msgpack"
	"sync/atomic"
	"bytes"
	"sync"
)

type Connection struct {
	addr       string
	connection net.Conn
	mutex      *sync.Mutex
	requestId  uint32
	Greeting   *Greeting
	requests   map[uint32]chan *Response
	packets    chan []byte
	opts       Opts
	state      uint
}

type Greeting struct {
	version string
	auth    string
}

type Opts {
	pingInterval int // seconds
	timeout int      // microseconds
	reconnect bool
}

const (
	stConnecting = iota
	stEstablished
	stBroken
)

func Connect(addr string, opts Opts) (conn *Connection, err error) {

	conn = &Connection{
		addr: addr,
		connection: nil,
		mutex: &sync.Mutex{},
		requestId: 0,
		greeting: &Greeting{},
		requests: make(map[uint32]chan *Response),
		packets: make(chan []byte),
		opts: opts,
	}

	err = co.dial()
	if err != nil {
		return
	}

	go conn.writer()
	go conn.reader()

	return
}

func (conn *Connection) dial() (err error) {
	connection, err := net.Dial("tcp", conn.addr)
	if err != nil {
		return
	}
	connection.(*net.TCPConn).SetNoDelay(true)
	conn.connection = connection
	greeting := make([]byte, 128)
	// TODO: read all
	_, err = conn.connection.Read(greeting)
	if err != nil {
		return
	}
	conn.Greeting.version = bytes.NewBuffer(greeting[:64]).String()
	conn.Greeting.auth = bytes.NewBuffer(greeting[64:]).String()
	return
}

func (conn *Connection) getConnection() (connection net.Conn, err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.connection == nil {
		err = conn.dial()
	}
	return conn.connection, err
}

func (conn *Connection) closeConnection(err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	conn.connection.Close()
	conn.connection = nil
	for requestId, respChan := range(conn.requests) {
		respChan <- NewNetErrResponse(err)
		delete(conn.requests, requestId)
		close(respChan)
	}
}

func (conn *Connection) writer() {
	var (
		err error
		packet []byte
		connection net.Conn
	)
	for {
		packet = <- conn.packets
		connetion = conn.getConnection()
		err = write(connection, packet)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
	}
}

func (conn *Connection) reader() {
	var (
		err error
		resp_bytes []byte
		connection net.Conn
	)
	for {
		connection = conn.getConnection()
		resp_bytes, err = read(connection)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
		resp := NewResponse(resp_bytes)
		respChan := conn.requests[resp.RequestId]
		conn.mutex.Lock()
		delete(conn.requests, resp.RequestId)
		conn.mutex.Unlock()
		if respChan != nil {
			respChan <- resp
		} else {
			log.Printf("tarantool: unexpected requestId (%d) in response", uint(resp.RequestId))
		}
	}
}

func write(connection net.Conn, data []byte) (err error) {
	l, err := connection.Write(data)
	if err != nil {
		return
	}
	if l != len(data) {
		panic("Wrong length writed")
	}
	return
}

func read(connection net.Conn) (response []byte, err error){
	var length_uint uint32
	var l, tl int
	length := make([]byte, PacketLengthBytes)

	tl = 0
	for tl < int(PacketLengthBytes) {
		l, err = connection.Read(length[tl:])
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
	if length_uint > 0 {
		tl = 0
		for tl < int(length_uint) {
			l, err = connection.Read(response[tl:])
			tl += l
			if err != nil {
				return
			}
		}
	}

	return
}

func (conn *Connection) nextRequestId() (requestId uint32) {
	return atomic.AddUint32(&conn.requestId, 1)
}

func (conn *Connection) Close() (err error) {
	// TODO close all pending responses
	conn.closeConnection(errors.New("connection closed"))
	return conn.connection.Close()
}

