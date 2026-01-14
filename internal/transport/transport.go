package transport

import (
	"io"
)

type PacketType byte
type TransportMessage struct {
	Id         uint64
	From       string
	To         string
	PacketType PacketType
	Payload    []byte
}
type Transport interface {
	ListenAndAccept() error
	Close() error
	Send(io.Writer, TransportMessage) error
	Consume() <-chan TransportMessage
	Connect(string) error
}
