package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Connection struct {
	addr       string
	connection net.Conn
	r          io.Reader
	w          *bufio.Writer
	mutex      *sync.Mutex
	requestId  uint32
	Greeting   *Greeting
	requests   map[uint32]*responseAndError
	packets    chan []byte
	control    chan struct{}
	opts       Opts
	closed     bool
}

type Greeting struct {
	version string
	auth    string
}

type Opts struct {
	Timeout   time.Duration // milliseconds
	Reconnect time.Duration // milliseconds
}

func Connect(addr string, opts Opts) (conn *Connection, err error) {

	conn = &Connection{
		addr:       addr,
		connection: nil,
		mutex:      &sync.Mutex{},
		requestId:  0,
		Greeting:   &Greeting{},
		requests:   make(map[uint32]*responseAndError),
		packets:    make(chan []byte, 64),
		control:    make(chan struct{}),
		opts:       opts,
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
	conn.closed = true
	close(conn.control)
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
	conn.r = bufio.NewReaderSize(conn.connection, 128*1024)
	conn.w = bufio.NewWriter(conn.connection)
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

func (conn *Connection) createConnection() (r io.Reader, w *bufio.Writer) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	for conn.connection == nil {
		if conn.closed {
			return
		}
		err := conn.dial()
		if err == nil {
			break
		} else if conn.opts.Reconnect > 0 {
			time.Sleep(conn.opts.Reconnect)
		} else {
			return
		}
	}
	return conn.r, conn.w
}

func (conn *Connection) closeConnection(neterr error) (err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.connection == nil {
		return
	}
	err = conn.connection.Close()
	conn.connection = nil
	for rid, resp := range conn.requests {
		resp.e = neterr
		close(resp.c)
		delete(conn.requests, rid)
	}
	return
}

func (conn *Connection) writer() {
	var w *bufio.Writer
	for {
		var packet []byte
		select {
		case packet = <-conn.packets:
		default:
			if w = conn.w; w != nil {
				if err := w.Flush(); err != nil {
					conn.closeConnection(err)
				}
			}
			select {
			case packet = <-conn.packets:
			case <-conn.control:
				return
			}
		}
		if packet == nil {
			return
		}
		if w = conn.w; w == nil {
			if _, w = conn.createConnection(); w == nil {
				return
			}
		}
		if err := write(w, packet); err != nil {
			conn.closeConnection(err)
			continue
		}
	}
}

func (conn *Connection) reader() {
	var length [PacketLengthBytes]byte
	var r io.Reader
	for {
		if r = conn.r; r == nil {
			if r, _ = conn.createConnection(); r == nil {
				return
			}
		}
		resp_bytes, err := read(length[:], r)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
		resp := NewResponse(resp_bytes)
		conn.mutex.Lock()
		r := conn.requests[resp.RequestId]
		delete(conn.requests, resp.RequestId)
		conn.mutex.Unlock()
		if r != nil {
			r.r = resp
			close(r.c)
		} else {
			log.Printf("tarantool: unexpected requestId (%d) in response", uint(resp.RequestId))
		}
	}
}

func write(connection io.Writer, data []byte) (err error) {
	l, err := connection.Write(data)
	if err != nil {
		return
	}
	if l != len(data) {
		panic("Wrong length writed")
	}
	return
}

func read(length []byte, connection io.Reader) (response []byte, err error) {
	var length_uint uint32
	var l, tl int

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
