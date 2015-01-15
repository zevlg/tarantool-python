package tarantool

import (
	"errors"
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
)

type Response struct {
	RequestId uint32
	Code      uint32
	Error     error
	Data      []interface{}
}

type responseAndError struct {
	c chan struct{}
	b []byte
	r Response
}

type smallBuf struct {
	b []byte
	p int
}

func (s *smallBuf) Read(d []byte) (l int, err error) {
	l = len(s.b) - s.p
	if l == 0 && len(d) > 0 {
		return 0, io.EOF
	}
	if l > len(d) {
		l = len(d)
	}
	copy(d, s.b[s.p:])
	s.p += l
	return l, nil
}

func (s *smallBuf) ReadByte() (b byte, err error) {
	if s.p == len(s.b) {
		return 0, io.EOF
	}
	b = s.b[s.p]
	s.p++
	return b, nil
}

func (s *smallBuf) UnreadByte() error {
	if s.p == 0 {
		return errors.New("Could not unread")
	}
	s.p--
	return nil
}

func (s *smallBuf) Len() int {
	return len(s.b) - s.p
}

func (s *smallBuf) Bytes() []byte {
	if len(s.b) > s.p {
		return s.b[s.p:]
	}
	return nil
}

func (r *Response) fill(b []byte) []byte {
	var l int
	s := smallBuf{b: b}
	d := msgpack.NewDecoder(&s)
	if l, r.Error = d.DecodeMapLen(); r.Error != nil {
		return nil
	}
	for ; l > 0; l-- {
		var cd int
		if cd, r.Error = d.DecodeInt(); r.Error != nil {
			return nil
		}
		switch cd {
		case KeySync:
			if r.RequestId, r.Error = d.DecodeUint32(); r.Error != nil {
				return nil
			}
		case KeyCode:
			if r.Code, r.Error = d.DecodeUint32(); r.Error != nil {
				return nil
			}
		}
	}
	return s.Bytes()
}

func (resp *Response) String() (str string) {
	if resp.Code == OkCode {
		return fmt.Sprintf("<%d OK %v>", resp.RequestId, resp.Data)
	} else {
		return fmt.Sprintf("<%d ERR 0x%x %s>", resp.RequestId, resp.Code, resp.Error)
	}
}

func (r *responseAndError) get() (*Response, error) {
	if r.r.Error != nil {
		return &r.r, r.r.Error
	}
	if len(r.b) > 0 {
		var body map[int]interface{}
		d := msgpack.NewDecoder(&smallBuf{b: r.b})

		if r.r.Error = d.Decode(&body); r.r.Error != nil {
			r.b = nil
			return nil, r.r.Error
		}

		if body[KeyData] != nil {
			data := body[KeyData].([]interface{})
			r.r.Data = make([]interface{}, len(data))
			for i, v := range data {
				r.r.Data[i] = v.([]interface{})
			}
		}

		if r.r.Code != OkCode {
			r.r.Error = Error{r.r.Code, body[KeyError].(string)}
		}
		r.b = nil
	}

	return &r.r, r.r.Error
}

func (r *responseAndError) getTyped(res interface{}) error {
	if r.r.Error != nil {
		return r.r.Error
	}
	if len(r.b) > 0 {
		var l int
		d := msgpack.NewDecoder(&smallBuf{b: r.b})
		if l, r.r.Error = d.DecodeMapLen(); r.r.Error != nil {
			r.b = nil
			return r.r.Error
		}

		for ; l > 0; l-- {
			var cd int
			if cd, r.r.Error = d.DecodeInt(); r.r.Error != nil {
				r.b = nil
				return r.r.Error
			}
			switch cd {
			case KeyData:
				if r.r.Error = d.Decode(res); r.r.Error != nil {
					r.b = nil
					return r.r.Error
				}
			case KeyError:
				var str string
				if str, r.r.Error = d.DecodeString(); r.r.Error == nil {
					r.r.Error = Error{
						r.r.Code,
						str,
					}
				}
				r.b = nil
				return r.r.Error
			}
		}
	}

	return r.r.Error
}
