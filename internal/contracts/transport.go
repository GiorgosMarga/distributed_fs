package contracts

import "math/rand"

type PacketType byte

const (
	RaftPacket PacketType = iota
	FSPacket
	HandshakePacket
	LeavePacket
)

type TransportMessage struct {
	Id         uint64
	From       string
	To         string
	PacketType PacketType
	Payload    []byte
}

func NewTransportMsg(from, to string, pcktType PacketType, payload []byte) TransportMessage {
	return TransportMessage{
		From:       from,
		To:         to,
		PacketType: pcktType,
		Payload:    payload,
		Id:         rand.Uint64(),
	}
}
