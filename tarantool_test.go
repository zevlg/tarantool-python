package tarantool

import (
	"fmt"
	"testing"
)

var server = "127.0.0.1:3013"
var spaceNo = uint32(512)
var indexNo = uint32(0)
var limit = uint32(10)
var offset = uint32(0)
var iterator = IterAll
var key = []interface{}{12}
var tuple1 = []interface{}{12, "Hello World", "Olga"}
var tuple2 = []interface{}{12, "Hello Mars", "Anna"}
var upd_tuple = []interface{}{[]interface{}{"=", 1, "Hello Moon"}, []interface{}{"#", 2, 1}}

var functionName = "box.cfg()"
var functionTuple = []interface{}{"box.schema.SPACE_ID"}

func BenchmarkClientSerial(b *testing.B) {
	var err error

	client, err := Connect(server, Opts{})
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i++ {
		_, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
		if err != nil {
			b.Errorf("No connection available")
		}

	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	client, err := Connect(server, Opts{})
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i += 10 {
		var fs [10]*Future
		for j := 0; j < 10; j++ {
			fs[j] = client.SelectAsync(spaceNo, indexNo, offset, limit, iterator, key)
		}
		for j := 0; j < 10; j++ {
			_, err = fs[j].Get()
			if err != nil {
				b.Errorf("No connection available")
			}
		}

	}
}

func BenchmarkClientParrallel(b *testing.B) {
	client, err := Connect(server, Opts{})
	if err != nil {
		b.Errorf("No connection available")
	}

	_, err = client.Replace(spaceNo, tuple1)
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
			if err != nil {
				b.Errorf("No connection available")
			}
		}
	})
}

func TestClient(t *testing.T) {
	client, err := Connect(server, Opts{})
	if err != nil {
		t.Errorf("No connection available")
	}

	var resp *Response

	resp, err = client.Ping()
	fmt.Println("Ping")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Insert(spaceNo, tuple1)
	fmt.Println("Insert")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("Select")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Replace(spaceNo, tuple2)
	fmt.Println("Replace")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("Select")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Update(spaceNo, indexNo, key, upd_tuple)
	fmt.Println("Update")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("Select")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	responses := make(chan *Response)
	cnt1 := 50
	cnt2 := 500
	for j := 0; j < cnt1; j++ {
		for i := 0; i < cnt2; i++ {
			go func() {
				resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
				responses <- resp
			}()
		}
		for i := 0; i < cnt2; i++ {
			resp = <-responses
			// fmt.Println(resp)
		}
	}

	resp, err = client.Delete(spaceNo, indexNo, key)
	fmt.Println("Delete")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

	resp, err = client.Call(functionName, functionTuple)
	fmt.Println("Call")
	fmt.Println("ERROR", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("----")

}
