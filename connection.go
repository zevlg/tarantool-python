package tarantool

import (
	"net"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync/atomic"
	"bytes"
	"sync"
	"time"
	"log"
	"errors"
)

type Connection struct {
	addr       string
	connection net.Conn
	mutex      *sync.Mutex
	requestId  uint32
	Greeting   *Greeting
	requests   map[uint32]chan responseAndError
	packets    chan []byte
	opts       Opts
	closed     bool
}

type Greeting struct {
	version string
	auth    string
}

type Opts struct {
	Timeout      time.Duration // milliseconds
	Reconnect    time.Duration // milliseconds
}


func Connect(addr string, opts Opts) (conn *Connection, err error) {

	conn = &Connection{
		addr: addr,
		connection: nil,
		mutex: &sync.Mutex{},
		requestId: 0,
		Greeting: &Greeting{},
		requests: make(map[uint32]chan responseAndError),
		packets: make(chan []byte),
		opts: opts,
	}

	err = conn.dial()
	if err != nil {
		return
	}

	go conn.writer()
	go conn.reader()
	return
}

func (conn *Connection) Close() (err error) {
	conn.closed = true;
	err = conn.closeConnection(errors.New("client closed connection"))
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

func (conn *Connection) getConnection() (connection net.Conn) {
	if c := conn.connection; c != nil {
		return c
	}
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	for conn.connection == nil {
		if conn.closed {
			return nil
		}
		err := conn.dial()
		if err == nil {
			break
		} else if conn.opts.Reconnect > 0 {
			time.Sleep(conn.opts.Reconnect)
		} else {
			return nil
		}
	}
	return conn.connection
}

func (conn *Connection) closeConnection(neterr error) (err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.connection == nil {
		return
	}
	err = conn.connection.Close()
	conn.connection = nil
	for requestId, respChan := range(conn.requests) {
		respChan <- responseAndError{nil, neterr}
		delete(conn.requests, requestId)
		close(respChan)
	}
	return
}

func (conn *Connection) writer() {
	for {
		packet := <-conn.packets
		connection := conn.getConnection()
		if connection == nil {
			return
		}
		err := write(connection, packet)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
	}
}

func (conn *Connection) reader() {
	for {
		connection := conn.getConnection()
		if connection == nil {
			return
		}
		resp_bytes, err := read(connection)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
		resp := NewResponse(resp_bytes)
		conn.mutex.Lock()
		respChan := conn.requests[resp.RequestId]
		delete(conn.requests, resp.RequestId)
		conn.mutex.Unlock()
		if respChan != nil {
			respChan <- responseAndError{resp, nil}
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
