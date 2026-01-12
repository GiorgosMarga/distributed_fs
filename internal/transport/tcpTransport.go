package transport

import (
	"fmt"
	"io"
	"net"
	"sync"
)


const (
	RaftPacket PacketType = iota
	FSPacket
)

type Peer struct {
	address   string
	conn      net.Conn
	isInbound bool
}

type TCPTransport struct {
	ln         net.Listener
	address    string
	serializer Serializer
	msgChan    chan TransportMessage
	peers      map[string]Peer
	mu         *sync.Mutex
}

func NewTCPTransport(address string, serializer Serializer) *TCPTransport {
	return &TCPTransport{
		address:    address,
		serializer: serializer,
		msgChan:    make(chan TransportMessage),
		peers:      make(map[string]Peer),
		mu:         &sync.Mutex{},
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.ln, err = net.Listen("tcp", t.address)
	if err != nil {
		return err
	}

	go t.acceptLoop()
	return nil
}

func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			continue
		}

		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()

	peerAddress := conn.RemoteAddr().String()

	t.mu.Lock()
	t.peers[peerAddress] = Peer{
		address:   peerAddress,
		conn:      conn,
		isInbound: true,
	}
	t.mu.Unlock()

	for {
		msg, err := t.serializer.Decode(conn)
		if err != nil {
			if err == io.EOF {
				return // Connection closed gracefully
			}
			fmt.Println(err)
			continue
		}
		t.msgChan <- msg
	}

}
func (t *TCPTransport) Consume() chan TransportMessage {
	return t.msgChan
}
