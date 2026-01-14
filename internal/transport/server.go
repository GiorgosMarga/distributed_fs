package transport

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
	"github.com/GiorgosMarga/dfs/internal/raft"
)

type Server struct {
	ServerOpts
	transport       Transport
	pendingRequests map[uint64]chan error
	peers           map[string]Peer
	raftPeers       map[uint64]string
	raft            *raft.Raft
	readyCond       *sync.Cond
	mu              *sync.Mutex
	dfs             filesystem.FileSystem
}

type ServerOpts struct {
	Address    string
	Serializer Serializer
}

func (s *Server) Handshake(conn net.Conn) (string, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, s.raft.Id)
	msg := TransportMessage{
		From:       s.Address,
		PacketType: HandshakePacket,
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
	s.raftPeers[peerRaftId] = resp.From
	s.raft.AddPeer(peerRaftId)
	s.mu.Unlock()
	return resp.From, nil
}
func (s *Server) OnNewPeer(peer Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingPeer, exists := s.peers[peer.address]
	if exists {
		// TIEBRAKER: the node with the "higher" address string keeps the outbound
		// this ensures that both nodes make the same decision independently
		if s.Address > existingPeer.address {
			// i am higher, i keep the outbound
			if peer.isInbound {
				// if peer is inbound it means the existing peer is the outbound, the one i want to keep
				return fmt.Errorf("duplicate connection: yielding to outbound")
			}
		} else {
			if !peer.isInbound {
				// if peer is outbound it means the existing peer is the inbound, the one i want to keep
				return fmt.Errorf("duplicate connection: yielding to inbound")
			}
		}
		existingPeer.conn.Close()
	}
	s.peers[peer.address] = peer
	if len(s.peers)+1 >= raft.MinPeers {
		s.readyCond.Signal()
	}
	fmt.Printf("[%s]: New Peer %s\n", s.Address, peer.address)
	return nil
}
func NewServer(serverOpts ServerOpts) *Server {
	if serverOpts.Address == "" {
		serverOpts.Address = fmt.Sprintf(":%d", rand.Intn(1000)+3000) // from 3000 -> 4000
	}
	if serverOpts.Serializer == nil {
		serverOpts.Serializer = NewGOBSerializer()
	}
	dfs, _ := filesystem.NewDFS(serverOpts.Address)
	s := &Server{
		readyCond:       sync.NewCond(&sync.Mutex{}),
		peers:           make(map[string]Peer),
		raft:            raft.NewRaft(rand.Uint64()),
		mu:              &sync.Mutex{},
		pendingRequests: make(map[uint64]chan error),
		raftPeers:       make(map[uint64]string),
		ServerOpts:      serverOpts,
		dfs:             dfs,
	}
	s.transport = NewTCPTransport(serverOpts.Address, TCPTransportOpts{
		Serializer: serverOpts.Serializer,
		OnNewPeer:  s.OnNewPeer,
		Handshake:  s.Handshake,
	})
	return s
}

func (s *Server) Close() error {
	return s.transport.Close()
}
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
	go s.handleCommittedEntries()
	go s.clientHandler()
	for transportMessage := range s.transport.Consume() {
		switch transportMessage.PacketType {
		case RaftPacket:
			s.raft.InboundCh <- transportMessage.Payload
		case FSPacket:
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

func (s *Server) handleFsMsgs(msg filesystem.Message) error {
	switch msg.Type {
	case filesystem.MessageWrite:
		err := s.handleWriteMsg(msg.Payload)
		return s.sendFsMsg(filesystem.ACK{Id: uint64(msg.ID), Success: err != nil}, filesystem.MessageAck, string(msg.From))
	case filesystem.MessageAck:
		return s.handleAckMsg(msg.Payload)
	default:
		if s.raft.IsLeader() {
			go s.handleClientCommand(msg)
		}
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
	ch, exists := s.pendingRequests[ack.Id]
	if !exists {
		return nil
	}
	close(ch)
	// if ack.Success {
	// 	ch <- nil
	// } else {
	// 	ch <- fmt.Errorf("err")
	// }
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
func (s *Server) startRaft() {
	time.Sleep(1 * time.Second)
	go s.raft.Run()
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
		transportMsg := TransportMessage{
			From:       s.Address,
			To:         to,
			PacketType: RaftPacket,
			Payload:    payload,
		}
		peer, exists := s.peers[to]
		if !exists {
			fmt.Printf("[%s]: Peer not found\n", s.Address)
			continue
		}
		if err := s.transport.Send(peer.conn, transportMsg); err != nil {
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
			clientWaiter <- err
		}
		s.mu.Unlock()
	}
}
func (s *Server) applyCommand(cmd []byte) error {
	fsMessage, err := filesystem.DecodeMessage(cmd)
	if err != nil {
		return err
	}
	switch fsMessage.Type {
	case filesystem.MessageMkdir:
		if err := s.handleMkdirMsg(fsMessage.Payload); err != nil {
			return err
		}
	case filesystem.MessageDelete:
		if err := s.handleDeleteMsg(fsMessage.Payload); err != nil {
			return err
		}
	case filesystem.MessageRead:
		if err := s.handleReadMsg(fsMessage.Payload); err != nil {
			return err
		}
	case filesystem.MessageMetadata:
		if err := s.handleMetadataMsg(fsMessage.Payload); err != nil {
			return err
		}
	default:
		fmt.Println("Unknown message type")
	}

	return nil
}
func (s *Server) handleClientCommand(msg filesystem.Message) error {
	t, err := msg.Encode()
	if err != nil {
		return err
	}
	index := s.raft.Propose(t)
	respCh := make(chan error, 1)
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
	case err := <-respCh:
		return err
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
	metadataMsg, err := filesystem.DecodeMetadataMsg(bufMsg)
	if err != nil {
		return err
	}
	for i := range metadataMsg.Servers {
		if err := s.dfs.MapChunkId(string(metadataMsg.ChunkIds[i]), string(metadataMsg.Servers[i])); err != nil {
			return err
		}
	}
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

func (s *Server) handleReadMsg(bufMsg []byte) error {
	readMsg, err := filesystem.DecodeReadMsg(bufMsg)
	if err != nil {
		return err
	}
	fmt.Printf("Read message: %+v\n", readMsg)
	return nil
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

func (s *Server) sendFsMsgWithAck(payload filesystem.FSMessage, msgType filesystem.MessageType, to string) error {
	respCh := make(chan error, 1)
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
	peer, exists := s.peers[to]
	if !exists {
		return fmt.Errorf("not found")
	}
	s.mu.Lock()
	s.pendingRequests[uint64(fsMsg.ID)] = respCh
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pendingRequests, uint64(fsMsg.ID))
		s.mu.Unlock()
	}()

	if err := s.transport.Send(peer.conn, TransportMessage{
		From:       s.Address,
		To:         peer.address,
		PacketType: FSPacket,
		Payload:    encoded,
	}); err != nil {
		return err
	}
	select {
	case err := <-respCh:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout")
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

	return s.transport.Send(peer.conn, TransportMessage{
		From:       s.Address,
		To:         peer.address,
		PacketType: FSPacket,
		Payload:    encoded,
	})

}
