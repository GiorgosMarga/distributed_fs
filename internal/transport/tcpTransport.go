package transport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/GiorgosMarga/dfs/internal/contracts"
)

var (
	ErrPeerNotFound = errors.New("peer doesnt exist")
)

type Peer struct {
	TransportAddress string
	Conn             net.Conn
	IsInbound        bool
}

type OnNewPeerFunc func(conn net.Conn, isInbound bool) error

// TODO: not used right now, (handshake should be included on the new peer)
type HandshakeFunc func(net.Conn) (string, error)

func DefaultHandshake(conn net.Conn) (string, error) {
	return conn.RemoteAddr().String(), nil
}

func DefaultOnNewPeer(_ net.Conn, _ bool) error {
	return nil
}

type TCPTransportOpts struct {
	Serializer Serializer
	OnNewPeer  OnNewPeerFunc
	// Handshake HandshakeFunc
}
type TCPTransport struct {
	TCPTransportOpts
	ln      net.Listener
	address string
	msgChan chan contracts.TransportMessage
	mu      *sync.Mutex
}

func NewTCPTransport(address string, opts TCPTransportOpts) *TCPTransport {
	t := &TCPTransport{
		address: address,
		msgChan: make(chan contracts.TransportMessage),
		mu:      &sync.Mutex{},
	}
	if opts.OnNewPeer == nil {
		opts.OnNewPeer = DefaultOnNewPeer
	}
	// if opts.Handshake == nil {
	// 	opts.Handshake = DefaultHandshake
	// }
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

	// i initiated the connection
	if err := t.OnNewPeer(conn, isInbound); err != nil {
		fmt.Printf("[%s]: Error registering peer: %s\n", t.address, err)
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
func (t *TCPTransport) Consume() <-chan contracts.TransportMessage {
	return t.msgChan
}

func (t *TCPTransport) Send(w io.Writer, msg contracts.TransportMessage) error {
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
