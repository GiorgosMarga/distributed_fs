package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/GiorgosMarga/dfs/internal/message"
)

type Server struct {
	listenAddr string
	ln         net.Listener
}

func NewServer(listenAddr string) *Server {
	return &Server{
		listenAddr: listenAddr,
	}
}

// Start creates a listener and then blocks to accept new connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			fmt.Printf("[%s]: Error accepting connection %s\n", s.listenAddr, err)
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	var size uint32
	for {
		if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			fmt.Printf("[%s]: Error reading message size from %s: %s\n", s.listenAddr, conn.RemoteAddr(), err)
		}

		msgBug := make([]byte, size)
		if _, err := io.ReadFull(conn, msgBug); err != nil {
			fmt.Printf("[%s]: Error reading message from %s: %s\n", s.listenAddr, conn.RemoteAddr(), err)
			continue
		}
		switch message.MessageType(msgBug[0]) {
		case message.MessageMkdir:
			fmt.Println("MKDIR")
		case message.MessageWrite:
			fmt.Println("WRT")
		case message.MessageRead:
			fmt.Println("RD")
		case message.MessageDelete:
			fmt.Println("DLT")
		}
	}

}

func (s *Server) handleMkdirMsg(bufMsg []byte) error {
	
}
