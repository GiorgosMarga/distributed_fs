package transport

import (
	"fmt"
	"sync"
	"time"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
	"github.com/GiorgosMarga/dfs/internal/raft"
)

type RaftResponse struct {
	err   error
	index uint64
}

type RaftRequest struct {
	command []byte
	done    chan RaftResponse
}

type Server struct {
	transport       Transport
	pendingRequests map[uint64]chan RaftResponse
	raft            *raft.Raft
	mu              *sync.Mutex
}

type ServerOpts struct {
	address    string
	serializer Serializer
}

func NewServer(listenAddr string, serverOpts ServerOpts) *Server {
	return &Server{
		transport:       NewTCPTransport(listenAddr, NewGOBSerializer()),
		raft:            raft.NewRaft(0),
		mu:              &sync.Mutex{},
		pendingRequests: make(map[uint64]chan RaftResponse),
	}
}

func (s *Server) Start() error {
	if err := s.transport.ListenAndAccept(); err != nil {
		return err
	}

	go s.handleCommittedEntries()
	for transportMessage := range s.transport.Consume() {
		switch transportMessage.PacketType {
		case RaftPacket:
			s.raft.InboundCh <- transportMessage.Payload
		case FSPacket:
			fsMessage, err := filesystem.DecodeMessage(transportMessage.Payload)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if s.raft.IsLeader() {
				go s.handleClientCommand(fsMessage)
			} else {
				// redirect msg to leader
			}
		default:
			fmt.Printf("[%s]: Unknown packet type\n", "ser")
		}

	}
	return nil
}
func (s *Server) handleCommittedEntries() {
	for entry := range s.raft.ApplyCh {
		err := s.applyCommand(entry.Data)
		s.mu.Lock()
		clientWaiter, exists := s.pendingRequests[entry.Index]
		if exists {
			// 3. Send the result back to the specific client handler
			clientWaiter <- RaftResponse{err: err, index: entry.Index}
			delete(s.pendingRequests, entry.Index)
		}
		s.mu.Unlock()
	}
}
func (s *Server) applyCommand(cmd []byte) error {
	_ = cmd
	return nil
}
func (s *Server) handleClientCommand(msg filesystem.Message) error {
	index := s.raft.Propose(msg.Payload)
	respCh := make(chan RaftResponse, 1)
	s.mu.Lock()
	s.pendingRequests[index] = respCh
	s.mu.Unlock()
	// Ensure we clean up if we timeout
	defer func() {
		s.mu.Lock()
		delete(s.pendingRequests, index)
		s.mu.Unlock()
	}()
	select {
	case resp := <-respCh:
		return resp.err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout: cluster took too long to agree")
	}
}
func (s *Server) handleMkdirMsg(bufMsg []byte) error {
	return nil
}
