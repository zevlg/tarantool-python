package tarantool

const (
	SelectRequest    = 1
	InsertRequest    = 2
	ReplaceRequest   = 3
	UpdateRequest    = 4
	DeleteRequest    = 5
	CallRequest      = 6
	AuthRequest      = 7
	PingRequest      = 64
	SubscribeRequest = 66

	KeyCode          = 0x00
	KeySync          = 0x01
	KeySpaceNo       = 0x10
	KeyIndexNo       = 0x11
	KeyLimit         = 0x12
	KeyOffset        = 0x13
	KeyIterator      = 0x14
	KeyKey           = 0x20
	KeyTuple         = 0x21
	KeyFunctionName  = 0x22
	KeyData          = 0x30
	KeyError         = 0x31

	OkCode           = uint32(0)

	PacketLengthBytes = 5
)
