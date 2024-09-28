package common

import (
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/proto"
)

/*
	each message sent over the network should implement this interface
*/

type Serializable interface {
	Marshal(io.Writer) error
	Unmarshal(io.Reader) error
	New() Serializable
}

/*
	A struct that allocates a unique uint8 for each message type
*/

type MessageCode struct {
	WriteRequest    uint8
	ReadRequest     uint8
	WriteResponse   uint8
	ReadResponse	uint8
	PrintLog        uint8
	PrepareRequest  uint8
	PromiseReply    uint8
	ProposeRequest  uint8
	AcceptReply     uint8
	ReadPrepare     uint8
	ReadPromise    	uint8
}

/*
	A static function which assigns a unique uint8 to each message type
*/

func GetRPCCodes() MessageCode {
	return MessageCode {
		WriteRequest:    1,
		ReadRequest:     2,
		WriteResponse:   3,
		ReadResponse:	 4,
		PrintLog:       5,
		PrepareRequest:  6,
		PromiseReply:    7,
		ProposeRequest:  8,
		AcceptReply:     9,
		ReadPrepare:    10,
		ReadPromise:    11,
	}
}

func marshalMessage(wire io.Writer, m proto.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	lengthWritten := len(data)
	var b [8]byte
	bs := b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(lengthWritten))
	_, err = wire.Write(bs)
	if err != nil {
		return err
	}
	_, err = wire.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func unmarshalMessage(wire io.Reader, m proto.Message) error {
	var b [8]byte
	bs := b[:8]
	_, err := io.ReadFull(wire, bs)
	if err != nil {
		return err
	}
	numBytes := binary.LittleEndian.Uint64(bs)
	data := make([]byte, numBytes)
	length, err := io.ReadFull(wire, data)
	if err != nil {
		return err
	}
	err = proto.Unmarshal(data[:length], m)
	if err != nil {
		return err
	}
	return nil
}

// ReadRequest wrapper

func (t *ReadRequest) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *ReadRequest) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *ReadRequest) New() Serializable {
	return new(ReadRequest)
}

// ReadPrepare wrapper

func (t *ReadPrepare) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *ReadPrepare) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *ReadPrepare) New() Serializable {
	return new(ReadPrepare)
}

// ReadPromise wrapper

func (t *ReadPromise) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *ReadPromise) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *ReadPromise) New() Serializable {
	return new(ReadPromise)
}


// ReadResponse wrapper

func (t *ReadResponse) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *ReadResponse) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *ReadResponse) New() Serializable {
	return new(ReadResponse)
}

// WriteRequest wrapper

func (t *WriteRequest) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *WriteRequest) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *WriteRequest) New() Serializable {
	return new(WriteRequest)
}

// WriteResponse wrapper

func (t *WriteResponse) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *WriteResponse) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *WriteResponse) New() Serializable {
	return new(WriteResponse)
}

// PrintLog wrapper

func (t *PrintLog) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *PrintLog) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *PrintLog) New() Serializable {
	return new(PrintLog)
}

// PrepareRequest wrapper

func (t *PrepareRequest) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *PrepareRequest) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *PrepareRequest) New() Serializable {
	return new(PrepareRequest)
}

// PromiseReply wrapper

func (t *PromiseReply) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *PromiseReply) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *PromiseReply) New() Serializable {
	return new(PromiseReply)
}

// ProposeRequest wrapper

func (t *ProposeRequest) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *ProposeRequest) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *ProposeRequest) New() Serializable {
	return new(ProposeRequest)
}

// AcceptReply wrapper

func (t *AcceptReply) Marshal(wire io.Writer) error {
	return marshalMessage(wire, t)
}

func (t *AcceptReply) Unmarshal(wire io.Reader) error {
	return unmarshalMessage(wire, t)
}

func (t *AcceptReply) New() Serializable {
	return new(AcceptReply)
}