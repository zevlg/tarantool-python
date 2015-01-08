package tarantool

import(
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Response struct {
	RequestId uint32
	Code      uint32
	Error     string
	Data      []interface{}
}

type responseAndError struct {
	resp *Response
	err  error
}

func NewResponse(bytes []byte) (resp *Response) {
	var header, body map[int32]interface{}
	resp = &Response{}

	msgpack.Unmarshal(bytes, &header, &body)
	resp.RequestId = uint32(header[KeySync].(uint64))
	resp.Code = uint32(header[KeyCode].(uint64))
	if body[KeyData] != nil {
		data := body[KeyData].([]interface{})
		resp.Data = make([]interface{}, len(data))
		for i, v := range(data) {
			resp.Data[i] = v.([]interface{})
		}
	}

	if resp.Code != OkCode {
		resp.Error = body[KeyError].(string)
	}

	return
}

func (resp *Response) GoString (str string) {
	str = fmt.Sprintf("<%d %d '%s'>\n", resp.RequestId, resp.Code, resp.Error)
	for t := range(resp.Data) {
		str += fmt.Sprintf("%v\n", t)
	}
	return
}
