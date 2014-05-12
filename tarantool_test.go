package tarantool

import(
	"testing"
	"fmt"
	// "bytes"
	// "github.com/vmihailenco/msgpack"
)

func TestClient(t *testing.T) {
	server   := "127.0.0.1:3013"
	spaceNo  := uint32(514)
	indexNo  := uint32(0)
	limit    := uint32(10)
	offset   := uint32(0)
	key      := []interface{}{ 12 }
	tuple    := []interface{}{ uint32(12), "hello", "world", "and", "again", "and", "again" }
	iterator := ""

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
	fmt.Println("Header", resp.Header)
	fmt.Println("Body", resp.Body)

	fmt.Println("----")
	fmt.Println("Select")
	resp, err = client.Select(spaceNo, indexNo, offset, limit, iterator, key)
	fmt.Println("ERROR", err)
	fmt.Println("Header", resp.Header)
	fmt.Println("Body", resp.Body)

	fmt.Println("----")
	fmt.Println("Insert")
	resp, err = client.Insert(spaceNo, tuple)
	fmt.Println("ERROR", err)
	fmt.Println("Header", resp.Header)
	fmt.Println("Body", resp.Body)
}