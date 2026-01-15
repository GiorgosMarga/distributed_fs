package filesystem

import (
	"encoding/binary"
	"fmt"
	"io/fs"
)

type MessageType byte

const (
	MessageMkdir MessageType = iota + 100
	MessageWrite
	MessageRead
	MessageDelete
	MessageMetadata
	MessageAck
	fsMax
)

const (
	// id + len(from) + len(to) + ts + type + len(payload)
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
	b = append(b, byte(m.Type))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Payload)))
	b = append(b, m.Payload...)
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
	offset += 8
	m.Type = MessageType(b[offset])
	offset += 1
	payloadLen := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	m.Payload = make([]byte, payloadLen)
	copy(m.Payload, b[offset:])
	return m, nil
}

type Response struct {
	RespForId uint64 //the id of the message that this message responds to
	Payload   []byte
	Success   bool
}

func (resp Response) Encode() []byte {
	b := make([]byte, 0, 13+len(resp.Payload))
	b = binary.LittleEndian.AppendUint64(b, resp.RespForId)
	if resp.Success {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	fmt.Println(resp)
	if len(resp.Payload) == 0 {
		return b[:9]
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(resp.Payload)))
	b = append(b, resp.Payload...)

	return b
}

func DecodeAck(b []byte) (Response, error) {
	resp := Response{}
	offset := 0
	resp.RespForId = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	if b[offset] == 1 {
		resp.Success = true
	} else {
		resp.Success = false
	}
	offset += 1
	if offset < len(b) {
		fmt.Println("here")
		payloadLen := binary.LittleEndian.Uint32(b[offset:])
		offset += 4
		resp.Payload = make([]byte, payloadLen)
		copy(resp.Payload, b[offset:offset+int(payloadLen)])
	}

	return resp, nil
}

type MkdirMessage struct {
	Path        []byte
	Permissions fs.FileMode
}

func (m MkdirMessage) Encode() []byte {
	b := make([]byte, 0, 4+len(m.Path)+4)
	offset := 0
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Path)))
	offset += 4
	b = append(b, m.Path...)
	offset += len(m.Path)
	b = binary.LittleEndian.AppendUint32(b, uint32(m.Permissions))
	return b
}

func DecodeMkdirMsg(b []byte) (MkdirMessage, error) {
	msg := MkdirMessage{}
	pathLen := binary.LittleEndian.Uint32(b)
	msg.Path = make([]byte, pathLen)
	copy(msg.Path, b[4:4+pathLen])
	msg.Permissions = fs.FileMode(binary.LittleEndian.Uint32(b[4+pathLen:]))
	return msg, nil
}

type DeleteMessage struct {
	Path []byte
}

func (m DeleteMessage) Encode() []byte {
	b := make([]byte, 0, 4+len(m.Path))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Path)))
	b = append(b, m.Path...)
	return b
}

func DecodeDeleteMsg(b []byte) (DeleteMessage, error) {
	msg := DeleteMessage{}
	pathLen := binary.LittleEndian.Uint32(b)
	msg.Path = make([]byte, pathLen)
	copy(msg.Path, b[4:4+pathLen])
	return msg, nil
}

type ReadMessage struct {
	Path []byte
}

func (m ReadMessage) Encode() []byte {
	b := make([]byte, 0, 4+len(m.Path))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Path)))
	b = append(b, m.Path...)
	return b
}

func DecodeReadMsg(b []byte) (ReadMessage, error) {
	msg := ReadMessage{}
	pathLen := binary.LittleEndian.Uint32(b)
	msg.Path = make([]byte, pathLen)
	copy(msg.Path, b[4:4+pathLen])
	return msg, nil
}

type WriteMessage struct {
	Path  []byte
	Chunk []byte
}

func (m WriteMessage) Encode() []byte {
	b := make([]byte, 0, 4+len(m.Path)+4+len(m.Chunk))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Path)))
	b = append(b, m.Path...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Chunk)))
	b = append(b, m.Chunk...)
	return b
}

func DecodeWriteMsg(b []byte) (WriteMessage, error) {
	msg := WriteMessage{}
	var offset uint32 = 0
	pathLen := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	msg.Path = make([]byte, pathLen)
	copy(msg.Path, b[offset:offset+pathLen])
	offset += pathLen
	chunkLen := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	msg.Chunk = make([]byte, chunkLen)
	copy(msg.Chunk, b[offset:offset+chunkLen])
	return msg, nil
}

type MetadataMessage struct {
	MetadataEntry
}

func (m MetadataMessage) Encode() []byte {
	b := make([]byte, 0)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Name)))
	b = append(b, []byte(m.Name)...)

	b = binary.LittleEndian.AppendUint64(b, m.Size)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.ChunkIDs)))
	for _, chunkId := range m.ChunkIDs {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(chunkId)))
		b = append(b, []byte(chunkId)...)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Replicas)))
	for chunkId, replica := range m.Replicas {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(chunkId)))
		b = append(b, []byte(chunkId)...)

		b = binary.LittleEndian.AppendUint32(b, uint32(len(replica)))
		for _, serverId := range replica {
			b = binary.LittleEndian.AppendUint32(b, uint32(len(serverId)))
			b = append(b, []byte(serverId)...)
		}
	}
	return b
}

func DecodeMetadataMsg(b []byte) (MetadataMessage, error) {
	metadata := MetadataMessage{}
	var offset uint32 = 0
	nameSize := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	metadata.Name = string(b[offset : offset+nameSize])
	offset += nameSize

	metadata.Size = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	chunkIdsSize := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	metadata.ChunkIDs = make([]string, 0, chunkIdsSize)

	for range chunkIdsSize {
		chunkSize := binary.LittleEndian.Uint32(b[offset:])
		offset += 4
		metadata.ChunkIDs = append(metadata.ChunkIDs, string(b[offset:offset+chunkSize]))
		offset += chunkSize
	}

	replicasLen := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	metadata.Replicas = make(map[string][]string, replicasLen)
	for range replicasLen {
		chunkIdLen := binary.LittleEndian.Uint32(b[offset:])
		offset += 4
		filename := string(b[offset : offset+chunkIdLen])
		offset += chunkIdLen

		replicas := binary.LittleEndian.Uint32(b[offset:])
		offset += 4
		serverIds := make([]string, 0, replicas)
		for range replicas {
			serverSize := binary.LittleEndian.Uint32(b[offset:])
			offset += 4
			serverIds = append(serverIds, string(b[offset:offset+serverSize]))
			offset += serverSize
		}
		metadata.Replicas[filename] = serverIds
	}
	return metadata, nil
}
