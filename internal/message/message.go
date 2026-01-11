package message

import (
	"encoding/binary"
	"os"
)

type MessageType byte

const (
	MessageMkdir MessageType = iota
	MessageWrite
	MessageRead
	MessageDelete
)

const (
	MessageIdSize       = 4
	MessageFromLen      = 4
	MessageToLen        = 4
	MessageTimestampLen = 8
	MessageHeaderSize   = MessageIdSize + MessageFromLen + MessageToLen + MessageTimestampLen
)

type Message struct {
	ID        uint32
	From      []byte
	To        []byte
	Timestamp int64
}

func (m Message) Encode() ([]byte, error) {
	b := make([]byte, 0, MessageHeaderSize+len(m.From)+len(m.To))

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

type MkdirMessage struct {
	Message
	DirPath []byte
	Perm    os.FileMode // uint32
}

func (m MkdirMessage) Encode() ([]byte, error) {
	msgBuf, err := m.Message.Encode()
	if err != nil {
		return nil, err
	}
	b := make([]byte, len(msgBuf), 4+4+len(msgBuf)+len(m.DirPath))
	copy(b, msgBuf)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.DirPath)))
	b = append(b, m.DirPath...)
	b = binary.LittleEndian.AppendUint32(b, uint32(m.Perm))
	return b, nil
}

func DecodeMkdirMessage(b []byte) (MkdirMessage, error) {

	mkdirMsg := MkdirMessage{}

	msg, err := DecodeMessage(b)
	if err != nil {
		return MkdirMessage{}, err
	}
	mkdirMsg.Message = msg
	offset := MessageHeaderSize + len(msg.From) + len(msg.To)
	dirPathSize := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	mkdirMsg.DirPath = make([]byte, dirPathSize)
	copy(mkdirMsg.DirPath, b[offset:offset+int(dirPathSize)])
	offset += int(dirPathSize)
	mkdirMsg.Perm = os.FileMode(binary.LittleEndian.Uint32(b[offset:]))
	return mkdirMsg, nil
}
