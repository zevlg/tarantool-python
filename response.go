package tarantool

import(
	"errors"
	"github.com/vmihailenco/msgpack"
)

type Response struct {
	Code uint64
	Data []interface{}
}

type Resp struct {
	number int
	string1 string
	string2 string
	string3 string
	string4 string
	string5 string
	string6 string
}

func NewResponse(bytes []byte) (resp *Response, err error) {
	var header, body map[int32]interface{}
	resp = &Response{}

	msgpack.Unmarshal(bytes, &header, &body)
	resp.Code = header[KeyCode].(uint64)
	if body[KeyData] != nil {
		data := body[KeyData].([]interface{})
		resp.Data = make([]interface{}, len(data))
		for i, v := range(data) {
			resp.Data[i] = v.([]interface{})
		}
	}

	if resp.Code != OkCode {
		errText := body[KeyError].(string)
		err = errors.New(errText)
	}

	return 
}