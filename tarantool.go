package tarantool

import (
	// "github.com/vmihailenco/msgpack"
	// "errors"
	// "fmt"
	// "bytes"

)

type Space struct {
	conn    *Connection
	SpaceNo uint32
}


func (conn *Connection) Space(spaceNo uint32) (space *Space){
	return &Space{ conn, spaceNo }
}

func (space *Space) Ping() (resp *Response, err error) {
	return space.conn.Ping()
}

func (space *Space) Select() (resp *Response, err error) {
	return
}