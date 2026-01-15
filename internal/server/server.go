package server

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
	"github.com/GiorgosMarga/dfs/internal/raft"
	"github.com/GiorgosMarga/dfs/internal/transport"
)

type Server struct {
	ServerOpts
	transport       transport.Transport
	pendingRequests map[uint64]chan RequestResponse
	peers           map[string]transport.Peer
	raftPeers       map[uint64]string
	raft            *raft.Raft
	readyCond       *sync.Cond
	mu              *sync.Mutex
	dfs             filesystem.DistributedFileSystem
}

type ServerOpts struct {
	Address    string
	Serializer transport.Serializer
}

type RequestResponse struct {
	Data []byte
	Err  error
}

// Handshake is the onHandshake function that is required in the transport layer.
// It sends the peer id and the address of the server
func (s *Server) Handshake(conn net.Conn) (string, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, s.raft.Id)
	msg := transport.TransportMessage{
		Id:         rand.Uint64(),
		To:         conn.RemoteAddr().String(),
		From:       s.Address,
		PacketType: transport.HandshakePacket,
		Payload:    b,
	}
	if err := s.Serializer.Encode(conn, msg); err != nil {
		return "", err
	}
	resp, err := s.Serializer.Decode(conn)
	if err != nil {
		return "", err
	}
	peerRaftId := binary.LittleEndian.Uint64(resp.Payload)

	s.mu.Lock()
	if _, exists := s.raftPeers[peerRaftId]; !exists {
		s.raftPeers[peerRaftId] = resp.From
		s.raft.AddPeer(peerRaftId)
	}
	s.mu.Unlock()
	return resp.From, nil
}

// OnNewPeer is the onNewPeer function required in the transport layer.
// Its job is to keep one connection if 2 servers try to connect to each other
func (s *Server) OnNewPeer(peer transport.Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingPeer, exists := s.peers[peer.Address]
	if exists {
		// TIEBRAKER: the node with the "higher" address string keeps the outbound
		// this ensures that both nodes make the same decision independently
		if s.Address > existingPeer.Address {
			// i am higher, i keep the outbound
			if peer.IsInbound {
				// if peer is inbound it means the existing peer is the outbound, the one i want to keep
				return fmt.Errorf("duplicate connection: yielding to outbound")
			}
		} else {
			if !peer.IsInbound {
				// if peer is outbound it means the existing peer is the inbound, the one i want to keep
				return fmt.Errorf("duplicate connection: yielding to inbound")
			}
		}
		existingPeer.Conn.Close()
	}
	s.peers[peer.Address] = peer
	if len(s.peers)+1 >= raft.MinPeers {
		s.readyCond.Signal()
	}
	fmt.Printf("[%s]: New Peer %s\n", s.Address, peer.Address)
	return nil
}

// New creates a new server.
func New(serverOpts ServerOpts) (*Server, error) {
	if serverOpts.Address == "" {
		serverOpts.Address = fmt.Sprintf(":%d", rand.Intn(1000)+3000) // from 3000 -> 4000
	}
	if serverOpts.Serializer == nil {
		serverOpts.Serializer = transport.NewGOBSerializer()
	}
	dfs, err := filesystem.NewDFS(serverOpts.Address)
	if err != nil {
		return nil, err
	}
	s := &Server{
		readyCond:       sync.NewCond(&sync.Mutex{}),
		peers:           make(map[string]transport.Peer),
		raft:            raft.NewRaft(rand.Uint64()),
		mu:              &sync.Mutex{},
		pendingRequests: make(map[uint64]chan RequestResponse),
		raftPeers:       make(map[uint64]string),
		ServerOpts:      serverOpts,
		dfs:             dfs,
	}
	s.transport = transport.NewTCPTransport(serverOpts.Address, transport.TCPTransportOpts{
		Serializer: serverOpts.Serializer,
		OnNewPeer:  s.OnNewPeer,
		Handshake:  s.Handshake,
	})
	return s, nil
}

// Close gracefully stops the server by stopping the trasnport and raft routines
func (s *Server) Close() error {
	// s.raft.Close()
	return s.transport.Close()
}

// Start is a blocking function that starts consuming messages from the transport.
// It waits untill there are enough peers to start the routines.
// It also starts the http server, the entry point of clients requests
func (s *Server) Start() error {
	if err := s.transport.ListenAndAccept(); err != nil {
		return err
	}
	// Wait until we have enough peers before starting Raft
	s.readyCond.L.Lock()
	for len(s.peers)+1 < raft.MinPeers { // wait for at least 2 peers
		s.readyCond.Wait()
	}
	s.readyCond.L.Unlock()

	go s.startRaft()
	go s.clientHandler()

	for transportMessage := range s.transport.Consume() {
		switch transportMessage.PacketType {
		case transport.RaftPacket:
			s.raft.InboundCh <- transportMessage.Payload
		case transport.FSPacket:
			fsMessage, err := filesystem.DecodeMessage(transportMessage.Payload)
			fmt.Printf("[%s]: %+v\n", s.Address, fsMessage)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if err := s.handleFsMsgs(fsMessage); err != nil {
				fmt.Println(err)
			}
		default:
			fmt.Printf("[%s]: Unknown packet type\n", "ser")
		}

	}
	return nil
}

// handleFsMsgs handles all incoming filesystem messages
func (s *Server) handleFsMsgs(msg filesystem.Message) error {
	switch msg.Type {
	case filesystem.MessageWrite:
		err := s.handleWriteMsg(msg.Payload)
		return s.sendFsMsg(filesystem.Response{RespForId: uint64(msg.ID), Success: err != nil}, filesystem.MessageAck, string(msg.From))
	case filesystem.MessageAck:
		return s.handleAckMsg(msg.Payload)
	case filesystem.MessageDelete:
		return s.handleDeleteMsg(msg.Payload)
	case filesystem.MessageMetadata:
		return s.handleMetadataMsg(msg.Payload)
	case filesystem.MessageMkdir:
		return s.handleMkdirMsg(msg.Payload)
	case filesystem.MessageRead:
		b, err := s.handleReadMsg(msg.Payload)
		return s.sendFsMsg(filesystem.Response{RespForId: uint64(msg.ID), Success: err != nil, Payload: b}, filesystem.MessageAck, string(msg.From))
	default:
		fmt.Println("Unknown fs type")
	}
	return nil
}

func (s *Server) handleAckMsg(msgBuf []byte) error {
	fmt.Println("Received ack message")
	ack, err := filesystem.DecodeAck(msgBuf)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, exists := s.pendingRequests[ack.RespForId]
	if !exists {
		return nil
	}
	// todo: change this
	ch <- RequestResponse{
		Data: ack.Payload,
		Err:  nil,
	}
	return nil
}
func (s *Server) Bootstrap(addresses ...string) error {

	for _, address := range addresses {
		if err := s.transport.Connect(address); err != nil {
			fmt.Printf("[%s]: Error connecting with: %s\n", s.Address, address)
		}
	}
	return nil
}

// startRaft starts the raft routine and is also responsible got handling commited entries
func (s *Server) startRaft() {
	go s.handleCommittedEntries()
	go s.raft.Run()
	// this for loop is responsible for sending raft messages to other peers
	for raftMessage := range s.raft.OutboundCh {
		payload, err := raftMessage.Encode()
		if err != nil {
			fmt.Println(err)
			continue
		}
		to, ok := s.raftPeers[raftMessage.To]
		if !ok {
			fmt.Printf("[%s]: Peer %d not found from Raft map %v\n", s.Address, raftMessage.To, s.raftPeers)
			continue
		}
		// encapsulate raft message in a transport message
		transportMsg := transport.TransportMessage{
			From:       s.Address,
			To:         to,
			PacketType: transport.RaftPacket,
			Payload:    payload,
		}
		peer, exists := s.peers[to]
		if !exists {
			fmt.Printf("[%s]: Peer not found\n", s.Address)
			continue
		}
		// send message to peer
		if err := s.transport.Send(peer.Conn, transportMsg); err != nil {
			fmt.Println(err)
			continue
		}
	}
}

func (s *Server) handleCommittedEntries() {
	for entry := range s.raft.ApplyCh {
		err := s.applyCommand(entry.Data)
		s.mu.Lock()
		clientWaiter, exists := s.pendingRequests[entry.Index]
		if exists {
			clientWaiter <- RequestResponse{
				Err: err,
			}
		}
		s.mu.Unlock()
	}
}

func (s *Server) applyCommand(cmd []byte) error {
	fsMessage, err := filesystem.DecodeMessage(cmd)
	if err != nil {
		return err
	}
	return s.handleFsMsgs(fsMessage)
}
func (s *Server) proposeCommand(msg filesystem.Message) error {
	t, err := msg.Encode()
	if err != nil {
		return err
	}
	index := s.raft.Propose(t)
	respCh := make(chan RequestResponse, 1)
	s.mu.Lock()
	s.pendingRequests[index] = respCh
	s.mu.Unlock()
	// Ensure we clean up if we timeout2
	defer func() {
		s.mu.Lock()
		delete(s.pendingRequests, index)
		s.mu.Unlock()
	}()
	select {
	case resp := <-respCh:
		return resp.Err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout: cluster took too long to agree")
	}
}
func (s *Server) handleMkdirMsg(bufMsg []byte) error {
	mkdirMsg, err := filesystem.DecodeMkdirMsg(bufMsg)
	if err != nil {
		return err
	}
	fmt.Printf("[%s]: Mkdir message: %+v\n", s.Address, mkdirMsg)
	return nil
}
func (s *Server) handleMetadataMsg(bufMsg []byte) error {
	// this msg has been replicated to all raft users
	metadataMsg, err := filesystem.DecodeMetadataMsg(bufMsg)
	if err != nil {
		return err
	}

	err = s.dfs.InsertMetadata(metadataMsg.Name, metadataMsg.MetadataEntry)
	fmt.Printf("[%s]: Metadata message: %+v\n", s.Address, metadataMsg)
	return nil
}
func (s *Server) handleDeleteMsg(bufMsg []byte) error {
	deleteMsg, err := filesystem.DecodeDeleteMsg(bufMsg)
	if err != nil {
		return err
	}
	fmt.Printf("Delete message: %+v\n", deleteMsg)
	return nil
}

func (s *Server) handleReadMsg(bufMsg []byte) ([]byte, error) {
	readMsg, err := filesystem.DecodeReadMsg(bufMsg)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Read message: %+v\n", readMsg)
	return s.dfs.Read(string(readMsg.Path))
}

func (s *Server) handleWriteMsg(bufMsg []byte) error {
	writeMsg, err := filesystem.DecodeWriteMsg(bufMsg)
	if err != nil {
		return err
	}
	fmt.Printf("Write message: %+v\n", writeMsg)
	_, err = s.dfs.Write(string(writeMsg.Path), writeMsg.Chunk)

	return err
}

func (s *Server) sendFsMsgWithAck(payload filesystem.FSMessage, msgType filesystem.MessageType, to string) ([]byte, error) {
	respCh := make(chan RequestResponse, 1)
	fsMsg := filesystem.Message{
		ID:        rand.Uint32(),
		From:      []byte(s.Address),
		To:        []byte(to),
		Timestamp: time.Now().Unix(),
		Type:      msgType,
		Payload:   payload.Encode(),
	}
	encoded, err := fsMsg.Encode()
	if err != nil {
		return nil, err
	}
	peer, exists := s.peers[to]
	if !exists {
		return nil, fmt.Errorf("not found")
	}
	s.mu.Lock()
	s.pendingRequests[uint64(fsMsg.ID)] = respCh
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pendingRequests, uint64(fsMsg.ID))
		s.mu.Unlock()
	}()

	if err := s.transport.Send(peer.Conn, transport.TransportMessage{
		From:       s.Address,
		To:         peer.Address,
		PacketType: transport.FSPacket,
		Payload:    encoded,
	}); err != nil {
		return nil, err
	}
	select {
	case resp := <-respCh:
		return resp.Data, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout")
	}

}

func (s *Server) sendFsMsg(payload filesystem.FSMessage, msgType filesystem.MessageType, to string) error {
	fsMsg := filesystem.Message{
		ID:        rand.Uint32(),
		From:      []byte(s.Address),
		To:        []byte(to),
		Timestamp: time.Now().Unix(),
		Type:      msgType,
		Payload:   payload.Encode(),
	}
	encoded, err := fsMsg.Encode()
	if err != nil {
		return err
	}
	fmt.Println("to", s.Address, to)
	s.mu.Lock()
	peer, exists := s.peers[to]
	s.mu.Unlock()
	if !exists {
		return fmt.Errorf("not found")
	}

	return s.transport.Send(peer.Conn, transport.TransportMessage{
		From:       s.Address,
		To:         peer.Address,
		PacketType: transport.FSPacket,
		Payload:    encoded,
	})

}
