package tarantool

import(
	"bytes"
	"github.com/vmihailenco/msgpack"
)

type Request struct {
	conn        *Connection
	requestId   int32
	requestCode int32
	body        map[int]interface{}
}

func (conn *Connection) NewRequest(requestCode int32) (req *Request) {
	req = &Request{}
	req.conn        = conn
	req.requestId   = conn.nextRequestId()
	req.requestCode = requestCode
	req.body        = make(map[int]interface{})

	return
}

func (conn *Connection) Ping() (resp *Response, err error) {
	request := conn.NewRequest(PingRequest)
	resp, err = request.perform()
	return
}

func (conn *Connection) Select(spaceNo, indexNo, offset, limit uint32, iterator string, key []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(SelectRequest)

	request.body[KeySpaceNo]  = spaceNo
	request.body[KeyIndexNo]  = indexNo
	request.body[KeyIterator] = iterator
	request.body[KeyOffset]   = offset
	request.body[KeyLimit]    = limit
	request.body[KeyKey]      = key
	
	resp, err = request.perform()
	return
}

func (conn *Connection) Insert(spaceNo uint32, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(InsertRequest)

	request.body[KeySpaceNo] = spaceNo
	request.body[KeyTuple]   = tuple

	resp, err = request.perform()
	return
}

func (conn *Connection) Replace(spaceNo uint32, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(ReplaceRequest)

	request.body[KeySpaceNo] = spaceNo
	request.body[KeyTuple]   = tuple

	resp, err = request.perform()
	return
}

func (conn *Connection) Delete(spaceNo, indexNo uint32, key []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(DeleteRequest)

	request.body[KeySpaceNo] = spaceNo
	request.body[KeyIndexNo] = indexNo
	request.body[KeyKey]     = key

	resp, err = request.perform()
	return
}

func (conn *Connection) Update(spaceNo, indexNo uint32, key, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(UpdateRequest)

	request.body[KeySpaceNo] = spaceNo
	request.body[KeyIndexNo] = indexNo
	request.body[KeyKey]     = key
	request.body[KeyTuple]   = tuple

	resp, err = request.perform()
	return
}

func (conn *Connection) Call(functionName string, tuple []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(CallRequest)

	request.body[KeyFunctionName] = functionName
	request.body[KeyTuple]        = tuple

	resp, err = request.perform()
	return
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
	packet, err := req.pack()
	if err != nil {
		return
	}

	err = req.conn.write(packet)
	if err != nil {
		return
	}

	resp_bytes, err := req.conn.read()
	if err != nil {
		return
	}

	resp, err = NewResponse(resp_bytes)

	return
}

func (req *Request) pack() (packet *bytes.Buffer, err error) {
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

	packet = new(bytes.Buffer)
	packet.Write(packetLength)
	packet.Write(header)
	packet.Write(body)

	return
}

