package filesystem

import (
	"encoding/binary"
)

type MessageType byte

const (
	MessageMkdir MessageType = iota + 100
	MessageWrite
	MessageRead
	MessageDelete

	fsMax
)

const (
	MessageHeaderSize = 4 + 4 + 4 + 8 + 1 + 4
)

type Message struct {
	ID        uint32
	From      []byte
	To        []byte
	Timestamp int64
	Type      MessageType
	Payload   []byte
}

func (m Message) Encode() ([]byte, error) {
	b := make([]byte, 0, MessageHeaderSize+len(m.From)+len(m.To)+len(m.Payload))
	b = binary.LittleEndian.AppendUint32(b, m.ID)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.From)))
	b = append(b, m.From...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.To)))
	b = append(b, m.To...)
	b = binary.LittleEndian.AppendUint64(b, uint64(m.Timestamp))
	return b, nil
}
func DecodeMessage(b []byte) (Message, error) {
	m := Message{}
	var offset uint32 = 0
	m.ID = binary.LittleEndian.Uint32(b)
	offset += 4
	fromSize := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	m.From = make([]byte, fromSize)
	copy(m.From, b[offset:offset+fromSize])
	offset += fromSize
	toSize := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	m.To = make([]byte, toSize)
	copy(m.To, b[offset:offset+toSize])
	offset += toSize
	m.Timestamp = int64(binary.LittleEndian.Uint64(b[offset:]))
	return m, nil
}
