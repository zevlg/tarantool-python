package tarantool

import (
	"errors"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

type Request struct {
	conn        *Connection
	requestId   uint32
	requestCode int32
	body        map[int]interface{}
}

type Future struct {
	conn *Connection
	id   uint32
	r    responseAndError
	t    *time.Timer
	tc   <-chan time.Time
}

func (conn *Connection) NewRequest(requestCode int32) (req *Request) {
	req = &Request{}
	req.conn = conn
	req.requestId = conn.nextRequestId()
	req.requestCode = requestCode
	req.body = make(map[int]interface{})

	return
}

func (conn *Connection) Ping() (resp *Response, err error) {
	request := conn.NewRequest(PingRequest)
	resp, err = request.perform()
	return
}

func (r *Request) fillSearch(spaceNo, indexNo uint32, key []interface{}) {
	r.body[KeySpaceNo] = spaceNo
	r.body[KeyIndexNo] = indexNo
	r.body[KeyKey] = key
}
func (r *Request) fillIterator(offset, limit, iterator uint32) {
	r.body[KeyIterator] = iterator
	r.body[KeyOffset] = offset
	r.body[KeyLimit] = limit
}

func (r *Request) fillInsert(spaceNo uint32, tuple []interface{}) {
	r.body[KeySpaceNo] = spaceNo
	r.body[KeyTuple] = tuple
}

func (conn *Connection) Select(spaceNo, indexNo, offset, limit, iterator uint32, key []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(SelectRequest)
	request.fillSearch(spaceNo, indexNo, key)
	request.fillIterator(offset, limit, iterator)
	resp, err = request.perform()
	return
}

func (conn *Connection) Insert(spaceNo uint32, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(InsertRequest)
	request.fillInsert(spaceNo, tuple)
	resp, err = request.perform()
	return
}

func (conn *Connection) Replace(spaceNo uint32, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(ReplaceRequest)
	request.fillInsert(spaceNo, tuple)
	resp, err = request.perform()
	return
}

func (conn *Connection) Delete(spaceNo, indexNo uint32, key []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(DeleteRequest)
	request.fillSearch(spaceNo, indexNo, key)
	resp, err = request.perform()
	return
}

func (conn *Connection) Update(spaceNo, indexNo uint32, key, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(UpdateRequest)
	request.fillSearch(spaceNo, indexNo, key)
	request.body[KeyTuple] = tuple
	resp, err = request.perform()
	return
}

func (conn *Connection) Call(functionName string, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(CallRequest)
	request.body[KeyFunctionName] = functionName
	request.body[KeyTuple] = tuple
	resp, err = request.perform()
	return
}

func (conn *Connection) SelectAsync(spaceNo, indexNo, offset, limit, iterator uint32, key []interface{}) *Future {
	request := conn.NewRequest(SelectRequest)
	request.fillSearch(spaceNo, indexNo, key)
	request.fillIterator(offset, limit, iterator)
	return request.future()
}

func (conn *Connection) InsertAsync(spaceNo uint32, tuple []interface{}) *Future {
	request := conn.NewRequest(InsertRequest)
	request.fillInsert(spaceNo, tuple)
	return request.future()
}

func (conn *Connection) ReplaceAsync(spaceNo uint32, tuple []interface{}) *Future {
	request := conn.NewRequest(ReplaceRequest)
	request.fillInsert(spaceNo, tuple)
	return request.future()
}

func (conn *Connection) DeleteAsync(spaceNo, indexNo uint32, key []interface{}) *Future {
	request := conn.NewRequest(DeleteRequest)
	request.fillSearch(spaceNo, indexNo, key)
	return request.future()
}

func (conn *Connection) UpdateAsync(spaceNo, indexNo uint32, key, tuple []interface{}) *Future {
	request := conn.NewRequest(UpdateRequest)
	request.fillSearch(spaceNo, indexNo, key)
	request.body[KeyTuple] = tuple
	return request.future()
}

func (conn *Connection) CallAsync(functionName string, tuple []interface{}) *Future {
	request := conn.NewRequest(CallRequest)
	request.body[KeyFunctionName] = functionName
	request.body[KeyTuple] = tuple
	return request.future()
}

//
// To be implemented
//
func (conn *Connection) Auth(key, tuple []interface{}) (resp *Response, err error) {
	return
}

//
// private
//

func (req *Request) perform() (resp *Response, err error) {
	var packet []byte
	if packet, err = req.pack(); err != nil {
		return
	}

	r := responseAndError{c: make(chan struct{})}

	req.conn.mutex.Lock()
	if req.conn.closed {
		req.conn.mutex.Unlock()
		return nil, errors.New("using closed connection")
	}
	req.conn.requests[req.requestId] = &r
	req.conn.mutex.Unlock()

	req.conn.packets <- (packet)

	if req.conn.opts.Timeout > 0 {
		timer := time.NewTimer(req.conn.opts.Timeout)
		select {
		case <-r.c:
			timer.Stop()
			resp, err = r.r, r.e
			break
		case <-timer.C:
			req.conn.mutex.Lock()
			delete(req.conn.requests, req.requestId)
			req.conn.mutex.Unlock()
			resp = nil
			err = errors.New("client timeout")
		}
	} else {
		<-r.c
		resp, err = r.r, r.e
	}

	if resp != nil && resp.Error != "" {
		err = Error{resp.Code, resp.Error}
	}
	return
}

func (req *Request) pack() (packet []byte, err error) {
	var header, body, packetLength []byte

	msg_header := make(map[int]interface{})
	msg_header[KeyCode] = req.requestCode
	msg_header[KeySync] = req.requestId

	header, err = msgpack.Marshal(msg_header)
	if err != nil {
		return
	}

	body, err = msgpack.Marshal(req.body)
	if err != nil {
		return
	}

	length := uint32(len(header) + len(body))
	packetLength, err = msgpack.Marshal(length)
	if err != nil {
		return
	}

	packet = append(packet, packetLength...)
	packet = append(packet, header...)
	packet = append(packet, body...)
	return
}

func (req *Request) future() (f *Future) {
	f = &Future{
		conn: req.conn,
		id:   req.requestId,
		r:    responseAndError{c: make(chan struct{})},
	}
	var packet []byte
	if packet, f.r.e = req.pack(); f.r.e != nil {
		return
	}

	req.conn.mutex.Lock()
	if req.conn.closed {
		req.conn.mutex.Unlock()
		f.r.e = errors.New("using closed connection")
		return
	}
	req.conn.requests[req.requestId] = &f.r
	req.conn.mutex.Unlock()
	req.conn.packets <- (packet)

	if req.conn.opts.Timeout > 0 {
		f.t = time.NewTimer(req.conn.opts.Timeout)
		f.tc = f.t.C
	}
	return
}

func (f *Future) Get() (*Response, error) {
	select {
	case <-f.r.c:
	default:
		select {
		case <-f.r.c:
		case <-f.tc:
			f.conn.mutex.Lock()
			delete(f.conn.requests, f.id)
			f.conn.mutex.Unlock()
			f.r.r = nil
			f.r.e = errors.New("client timeout")
			close(f.r.c)
		}
	}
	if f.t != nil {
		f.t.Stop()
		f.t = nil
		f.tc = nil
	}
	return f.r.r, f.r.e
}
