package transport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

var (
	ErrPeerNotFound = errors.New("peer doesnt exist")
)

type Peer struct {
	address   string
	conn      net.Conn
	isInbound bool
}

const (
	RaftPacket PacketType = iota
	FSPacket
	HandshakePacket
)

type OnNewPeerFunc func(Peer) error
type HandshakeFunc func(net.Conn) (string, error)

func DefaultHandshake(conn net.Conn) (string, error) {
	return conn.RemoteAddr().String(), nil
}

func DefaultOnNewPeer(_ Peer) error {
	return nil
}

type TCPTransportOpts struct {
	Serializer Serializer
	OnNewPeer  OnNewPeerFunc
	Handshake  HandshakeFunc
}
type TCPTransport struct {
	TCPTransportOpts
	ln      net.Listener
	address string
	msgChan chan TransportMessage
	mu      *sync.Mutex
}

func NewTCPTransport(address string, opts TCPTransportOpts) *TCPTransport {
	t := &TCPTransport{
		address: address,
		msgChan: make(chan TransportMessage),
		mu:      &sync.Mutex{},
	}
	if opts.OnNewPeer == nil {
		opts.OnNewPeer = DefaultOnNewPeer
	}
	if opts.Handshake == nil {
		opts.Handshake = DefaultHandshake
	}
	t.TCPTransportOpts = opts
	return t
}

func (t *TCPTransport) Close() error {
	fmt.Printf("[%s]: Closing...\n", t.address)
	return t.ln.Close()
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
	fmt.Printf("[%s]: Server is listening...\n", t.address)
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			continue
		}

		go t.handleConn(conn, true)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, isInbound bool) {
	defer conn.Close()

	remoteAddr, err := t.Handshake(conn)
	if err != nil {
		fmt.Printf("[%s]: Error handshaking (%s): %s\n", t.address, conn.RemoteAddr().String(), err)
		return
	}

	peer := Peer{
		conn:      conn,
		isInbound: isInbound,
		address:   remoteAddr,
	}
	// i initiated the connection
	if err := t.OnNewPeer(peer); err != nil {
		fmt.Printf("[%s]: on new peer error: %s\n", t.address, err)
		return
	}

	for {
		msg, err := t.Serializer.Decode(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				// eof -> the other end closed the connection
				// errclosed -> we internally closed the connection. (ex. onPeersHandler)
				return
			}
			continue
		}
		t.msgChan <- msg
	}

}
func (t *TCPTransport) Consume() <-chan TransportMessage {
	return t.msgChan
}

func (t *TCPTransport) Send(w io.Writer, msg TransportMessage) error {
	return t.Serializer.Encode(w, msg)
}

func (t *TCPTransport) Connect(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	go t.handleConn(conn, false)
	return nil
}


