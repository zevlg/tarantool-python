package tarantool

import (
	"github.com/vmihailenco/msgpack"
	"errors"
	// "fmt"
	// "bytes"

)

const (
	SelectRequest    = 1
	InsertRequest    = 2
	ReplaceRequest   = 3
	UpdateRequest    = 4
	DeleteRequest    = 5
	CallRequest      = 6
	AuthRequest      = 7
	PingRequest      = 64
	SubscribeRequest = 66

	KeyCode          = 0x00
	KeySync          = 0x01
	KeySpaceNo       = 0x10
	KeyIndexNo       = 0x11
	KeyLimit         = 0x12
	KeyOffset        = 0x13
	KeyIterator      = 0x14
	KeyKey           = 0x20
	KeyTuple         = 0x21
	KeyFunctionName  = 0x22
	KeyData          = 0x30
	KeyError         = 0x31
)

type Space struct {
	conn    *Connection
	SpaceNo uint32
}

type Response struct {
	Header map[int32]interface{}
	Body   map[int32]interface{}
}

func (conn *Connection) Space(spaceNo uint32) (space *Space){
	return &Space{ conn, spaceNo }
}

func unpack(resp *Response, resp_bytes []byte) (err error) {
	msgpack.Unmarshal(resp_bytes, &resp.Header, &resp.Body)
	code := resp.Header[KeyCode].(uint64)

	if code != uint64(0) {
		errText := resp.Body[KeyError].(string)
		err = errors.New(errText)
	}

	return 
}

func (space *Space) Ping() (resp *Response, err error) {
	return space.conn.Ping()
}

func (space *Space) Select() (resp *Response, err error) {
	return
}