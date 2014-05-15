package tarantool

import(
	"github.com/vmihailenco/msgpack"
)

type Response struct {
	RequestId uint64
	Code      uint64
	Error     string
	Data      []interface{}
}

func NewResponse(bytes []byte) (resp *Response) {
	var header, body map[int32]interface{}
	resp = &Response{}

	msgpack.Unmarshal(bytes, &header, &body)
	resp.RequestId = header[KeySync].(uint64)
	resp.Code = header[KeyCode].(uint64)
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