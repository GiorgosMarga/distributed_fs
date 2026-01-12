package transport

type PacketType byte
type TransportMessage struct {
	From       string
	To         string
	PacketType PacketType
	Payload    []byte
}
type Transport interface {
	ListenAndAccept() error
	Consume() chan TransportMessage
}
