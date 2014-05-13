package tarantool

import(
	"testing"
	"fmt"
	// "bytes"
	// "github.com/vmihailenco/msgpack"
)

func TestClient(t *testing.T) {
	server    := "127.0.0.1:3013"
	spaceNo   := uint32(514)
	indexNo   := uint32(0)
	limit     := uint32(10)
	offset    := uint32(0)
	iterator  := ""
	key       := []interface{}{ 12 }
	tuple1    := []interface{}{ 12, "Hello World", "Olga" }
	tuple2    := []interface{}{ 12, "Hello Mars", "Anna" }
	upd_tuple := []interface{}{ []interface{}{ "=", 1, "Hello Moon" }, []interface{}{ "#", 2, 1 } }

	functionName  := "box.cfg()"
	functionTuple := []interface{}{ "box.schema.SPACE_ID" }


	client, err := Connect(server)
	if err != nil {
		t.Errorf("No connection available")
	}

	space := client.Space(spaceNo)
	
	resp, err := space.Ping()
	if err != nil {
		t.Errorf("Can't Ping", err)
	}

	fmt.Println("Ping")
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