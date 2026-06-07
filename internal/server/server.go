package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/GiorgosMarga/dfs/internal/contracts"
	"github.com/GiorgosMarga/dfs/internal/filesystem"
	"github.com/GiorgosMarga/dfs/internal/raft"
	"github.com/GiorgosMarga/dfs/internal/transport"
)

const (
	MaxErrors = 5
)

// TODO: leader no-op after election
// DONE: send leave message when leaving the cluster
// TODO: make server & raft persistent
// TODO: implement a metadata request message (follower -> leader)
// DONE: refactor peers maps
// TODO: on new peer send existing peers for raft

type NodeInfo struct {
	transport.Peer
	Id       uint64
	HttpAddr string
}
type Server struct {
	ServerOpts
	quitCh          chan struct{}
	transport       transport.Transport
	pendingRequests map[uint64]chan RequestResponse
	peers           map[uint64]*NodeInfo
	errorPeers      map[uint64]int
	raft            *raft.Raft
	readyCond       *sync.Cond
	mu              *sync.Mutex
	dfs             filesystem.DistributedFileSystem
}

type ServerOpts struct {
	Id               uint64
	BootstrapAddress []string
	TransportAddress string
	HttpAddress      string
	Serializer       transport.Serializer
}

type RequestResponse struct {
	Data []byte
	Err  error
}

type HandshakeData struct {
	Id       uint64 // server id, its also used as the raft Id
	HttpAddr string
	TcpAddr  string
}

// TODO: make handshake and on new peer into one function
// It sends the peer id and the address of the server
func (s *Server) handshake(conn net.Conn) (*HandshakeData, error) {
	b := make([]byte, 8+len(s.HttpAddress)) // uint64 + address
	binary.LittleEndian.PutUint64(b, s.raft.Id)
	copy(b[8:], []byte(s.HttpAddress))
	msg := contracts.NewTransportMsg(s.TransportAddress, conn.RemoteAddr().String(), contracts.HandshakePacket, b)
	if err := s.Serializer.Encode(conn, msg); err != nil {
		return nil, err
	}
	resp, err := s.Serializer.Decode(conn)
	if err != nil {
		return nil, err
	}
	peerId := binary.LittleEndian.Uint64(resp.Payload)
	peerHttpAddress := resp.Payload[8:]

	data := &HandshakeData{
		Id:       peerId,
		HttpAddr: string(peerHttpAddress),
		TcpAddr:  resp.From,
	}
	return data, nil
}

// OnNewPeer is the onNewPeer function required in the transport layer.
// Its job is to keep one connection if 2 servers try to connect to each other
func (s *Server) OnNewPeer(conn net.Conn, isInbound bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := s.handshake(conn)
	if err != nil {
		return err
	}

	existingPeer, exists := s.peers[data.Id]
	if exists {
		// TIEBRAKER: the node with the "higher" address string keeps the outbound
		// this ensures that both nodes make the same decision independently
		if s.TransportAddress > existingPeer.TransportAddress {
			// i am higher, i keep the outbound
			if isInbound {
				// if peer is inbound it means the existing peer is the outbound, the one i want to keep
				return fmt.Errorf("duplicate connection: yielding to outbound")
			}
		} else {
			if !isInbound {
				// if peer is outbound it means the existing peer is the inbound, the one i want to keep
				return fmt.Errorf("duplicate connection: yielding to inbound")
			}
		}
		existingPeer.Conn.Close()
	}
	s.peers[data.Id] = &NodeInfo{
		Peer: transport.Peer{
			TransportAddress: data.TcpAddr,
			Conn:             conn,
			IsInbound:        isInbound,
		},
		Id:       data.Id,
		HttpAddr: data.HttpAddr,
	}
	s.raft.AddPeer(data.Id)
	if len(s.peers)+1 >= raft.MinPeers {
		s.readyCond.Signal()
	}
	fmt.Printf("[%s]: New Peer %s\n", s.TransportAddress, data.HttpAddr)
	return nil
}

// New creates a new server.
func New(serverOpts ServerOpts) (*Server, error) {
	if serverOpts.Id == 0 {
		serverOpts.Id = rand.Uint64()
	}

	// TODO: this is kinda wrong
	if serverOpts.TransportAddress == "" {
		serverOpts.TransportAddress = fmt.Sprintf(":%d", rand.Intn(1000)+3000) // from 3000 -> 4000
	}
	if serverOpts.HttpAddress == "" {
		serverOpts.HttpAddress = fmt.Sprintf(":%d", rand.Intn(1000)+3000) // from 3000 -> 4000
	}
	if serverOpts.Serializer == nil {
		serverOpts.Serializer = transport.NewGOBSerializer()
	}
	dfs, err := filesystem.NewDFS(serverOpts.TransportAddress)
	if err != nil {
		return nil, err
	}
	s := &Server{
		readyCond:       sync.NewCond(&sync.Mutex{}),
		peers:           make(map[uint64]*NodeInfo),
		raft:            raft.NewRaft(serverOpts.Id),
		mu:              &sync.Mutex{},
		pendingRequests: make(map[uint64]chan RequestResponse),
		errorPeers:      make(map[uint64]int),
		ServerOpts:      serverOpts,
		dfs:             dfs,
		quitCh:          make(chan struct{}),
	}
	s.transport = transport.NewTCPTransport(serverOpts.TransportAddress, transport.TCPTransportOpts{
		Serializer: serverOpts.Serializer,
		OnNewPeer:  s.OnNewPeer,
	})
	return s, nil
}

// Close gracefully stops the server by stopping the trasnport and raft routines
func (s *Server) Close() error {
	if s.raft.IsLeader() {
		clusterNodes := make([]uint64, len(s.peers)-1)
		for idx, peer := range s.peers {
			clusterNodes[idx] = peer.Id
		}

		clusterConfig := contracts.ClusterConfig{
			ClusterNodes: clusterNodes,
		}
		buf, _ := clusterConfig.Encode()
		serverCmd := contracts.ServerCommand{
			Type:    contracts.CmdClusterChange,
			Payload: buf,
		}
		cmdBuf, _ := serverCmd.Encode()
		s.raft.ProposeConfigChange(cmdBuf)
	} else {

		s.mu.Lock()
		leader, exists := s.peers[s.raft.LeaderId]
		s.mu.Unlock()

		if !exists {
			return fmt.Errorf("leader was not found")
		}

		msg := contracts.NewTransportMsg(s.TransportAddress, leader.TransportAddress, contracts.LeavePacket, nil)

		if err := s.transport.Send(leader.Conn, msg); err != nil {
			return err
		}
	}

	fmt.Println("Waiting for apply loop")

	<-s.quitCh
	fmt.Printf("closed")

	s.readyCond.Broadcast()
	_ = s.transport.Close()
	_ = s.raft.Close()
	return nil
}

// Start is a blocking function that starts consuming messages from the transport.
// It waits untill there are enough peers to start the routines.
// It also starts the http server, the entry point of clients requests
func (s *Server) Start() error {
	if err := s.transport.ListenAndAccept(); err != nil {
		return err
	}

	for _, addr := range s.BootstrapAddress {
		if err := s.transport.Connect(addr); err != nil {
			fmt.Printf("[%s]: Error connecting with %q: %s\n", s.TransportAddress, addr, err)
		}
	}

	// Wait until we have enough peers before starting Raft
	s.readyCond.L.Lock()
	for len(s.peers)+1 < raft.MinPeers {
		select {
		case <-s.quitCh:
			fmt.Printf("[%s]: Terminating...\n", s.TransportAddress)
			return nil
		default:
			// wait for at least 2 peers
			s.readyCond.Wait()
		}
	}
	s.readyCond.L.Unlock()
	fmt.Printf("[%s]: Ready to start...\n", s.TransportAddress)
	go s.startRaft()
	go s.clientHandler()

	for {
		select {
		case transportMessage := <-s.transport.Consume():
			switch transportMessage.PacketType {
			case contracts.RaftPacket:
				s.raft.InboundCh <- transportMessage.Payload
			case contracts.FSPacket:
				fsMessage, err := contracts.DecodeMessage(transportMessage.Payload)
				if err != nil {
					fmt.Printf("[%s]: FSPacket error: %s\n", s.TransportAddress, err)
					continue
				}
				if err := s.handleFsMsg(fsMessage); err != nil {
					fmt.Printf("[%s]: FSMessage error: %s\n", s.TransportAddress, err)
					continue
				}
			case contracts.LeavePacket:
				s.mu.Lock()
				clusterNodes := make([]uint64, 1, len(s.peers)-1)
				clusterNodes[0] = s.Id
				for _, peer := range s.peers {
					if peer.TransportAddress == transportMessage.From {
						continue
					}
					clusterNodes = append(clusterNodes, peer.Id)
				}

				clusterConfig := contracts.ClusterConfig{
					ClusterNodes: clusterNodes,
				}
				buf, _ := clusterConfig.Encode()
				serverCmd := contracts.ServerCommand{
					Type:    contracts.CmdClusterChange,
					Payload: buf,
				}
				cmdBuf, _ := serverCmd.Encode()
				s.raft.ProposeConfigChange(cmdBuf)
				s.mu.Unlock()

			default:
				fmt.Printf("[%s]: Unknown packet type\n", s.TransportAddress)
			}
		case <-s.quitCh:
			fmt.Printf("[%s]: Stopping....\n", s.TransportAddress)
			return nil

		}
	}
}

// removePeer removes a peer from peers & raft. NOTE: this is not thread-safe
func (s *Server) removePeer(peerId uint64) error {
	delete(s.peers, peerId)
	s.raft.RemovePeer(peerId)
	return fmt.Errorf("not found")
}

// handleFsMsg handles all incoming filesystem messages
func (s *Server) handleFsMsg(msg contracts.FsMessage) error {
	switch msg.Type {
	case contracts.MessageWrite:
		err := s.handleWriteMsg(msg.Payload)
		return s.sendFsMsg(contracts.Response{RespForId: uint64(msg.ID), Success: err == nil}, contracts.MessageAck, string(msg.From))
	case contracts.MessageAck:
		return s.handleAckMsg(msg.Payload)
	case contracts.MessageDelete:
		return s.handleDeleteMsg(msg.Payload)
	case contracts.MessageMetadata:
		return s.handleMetadataMsg(msg.Payload)
	case contracts.MessageMkdir:
		return s.handleMkdirMsg(msg.Payload)
	case contracts.MessageRead:
		b, err := s.handleReadMsg(msg.Payload)
		return s.sendFsMsg(contracts.Response{RespForId: uint64(msg.ID), Success: err == nil, Payload: b}, contracts.MessageAck, string(msg.From))
	default:
		fmt.Println("Unknown fs type")
	}
	return nil
}

func (s *Server) handleAckMsg(msgBuf []byte) error {
	ack, err := contracts.DecodeAck(msgBuf)
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

func (s *Server) Read(path string) ([]byte, error) {
	fileEntry, err := s.dfs.ReadMetadata(path)
	if err != nil {
		return nil, err
	}

	fmt.Printf("File entry: %+v\n", fileEntry)

	var (
		fileBuf = new(bytes.Buffer)
	)
	for _, chunkId := range fileEntry.ChunkIDs {
		servers := fileEntry.Replicas[chunkId]
		var data []byte
		for _, server := range servers {
			if server == s.TransportAddress {
				// read from locally
				data, err = s.dfs.Read(chunkId)
				if err != nil {
					fmt.Println(err)
					continue
				}
				break
			} else {
				data, err = s.sendFsMsgWithAck(contracts.ReadMessage{Path: []byte(chunkId)}, contracts.MessageRead, server)
				if err != nil {
					fmt.Println(err)
					continue
				}
				break
			}
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("[%s]: Unable to read chunk: %s\n", s.TransportAddress, chunkId)
		}
		fileBuf.Write(data)

	}
	return fileBuf.Bytes(), nil
}
func (s *Server) Bootstrap(addresses ...string) error {

	for _, address := range addresses {
		if err := s.transport.Connect(address); err != nil {
			fmt.Printf("[%s]: Error connecting with: %s\n", s.TransportAddress, address)
		}
	}
	return nil
}

// startRaft starts the raft routine and is also responsible got handling commited entries
func (s *Server) startRaft() {
	go s.handleApplyCommands()
	go s.raft.Run()
	// this for loop is responsible for sending raft messages to other peers
	for raftMessage := range s.raft.OutboundCh {
		payload, err := raftMessage.Encode()
		if err != nil {
			fmt.Println(err)
			continue
		}
		to, ok := s.peers[raftMessage.To]
		if !ok {
			fmt.Printf("[%s]: Peer %d not found from Raft map %v %T\n", s.TransportAddress, raftMessage.To, s.peers, raftMessage)
			continue
		}
		// encapsulate raft message in a transport message
		transportMsg := contracts.NewTransportMsg(s.TransportAddress, to.TransportAddress, contracts.RaftPacket, payload)
		peer, exists := s.peers[to.Id]
		if !exists {
			fmt.Printf("[%s]: Peer not found %+v\n", s.TransportAddress, raftMessage)
			continue
		}
		// send message to peer
		if err := s.transport.Send(peer.Conn, transportMsg); err != nil {
			if errors.Is(err, net.ErrClosed) {
				s.raft.RemovePeer(peer.Id)
			}else{
				fmt.Println(err)
			}
			continue
		}
	}
}

func (s *Server) handleApplyCommands() {
	for cmd := range s.raft.Consume() {
		serverMessage, _ := contracts.DecodeServerCommand(cmd)
		switch serverMessage.Type {
		case contracts.CmdFsMessage:
			fsMsg, _ := contracts.DecodeMessage(serverMessage.Payload)
			if err := s.handleFsMsg(fsMsg); err != nil {
				fmt.Printf("[%d]: Error handling filesystem msg: %s\n", s.Id, err)
			}
		case contracts.CmdClusterChange:
			msg, err := contracts.DecodeClusterConfig(serverMessage.Payload)
			if err != nil {
				fmt.Printf("[%d]: Error handling cluster msg: %s\n", s.Id, err)
				continue
			}
			fmt.Println("Cluster nodes:", msg.ClusterNodes)

			if !slices.Contains(msg.ClusterNodes, s.Id) {
				close(s.quitCh)
				return
			}
			fmt.Println(msg.ClusterNodes)

		default:
			fmt.Printf("Invalid message type")
		}
	}
}

func (s *Server) proposeCommand(msg contracts.FsMessage) error {
	t, _ := msg.Encode()
	serverCmd := contracts.ServerCommand{
		Type:    contracts.CmdFsMessage,
		Payload: t,
	}
	b, _ := serverCmd.Encode()
	s.raft.Propose(b)
	return nil
}

func (s *Server) handleMkdirMsg(bufMsg []byte) error {
	mkdirMsg, err := contracts.DecodeMkdirMsg(bufMsg)
	if err != nil {

		return err
	}
	fmt.Printf("[%s]: Mkdir message: %+v\n", s.TransportAddress, mkdirMsg)
	return nil
}
func (s *Server) handleMetadataMsg(bufMsg []byte) error {
	// this msg has been replicated to all raft users
	metadataMsg, err := contracts.DecodeMetadataMsg(bufMsg)
	if err != nil {
		return err
	}

	err = s.dfs.InsertMetadata(metadataMsg.Name, filesystem.MetadataEntry{
		Name:     metadataMsg.Name,
		Size:     metadataMsg.Size,
		ChunkIDs: metadataMsg.ChunkIDs,
		Replicas: metadataMsg.Replicas,
	})
	fmt.Printf("[%s]: Metadata message: %+v\n", s.TransportAddress, metadataMsg)
	return nil
}
func (s *Server) handleDeleteMsg(bufMsg []byte) error {
	deleteMsg, err := contracts.DecodeDeleteMsg(bufMsg)
	if err != nil {
		return err
	}
	fmt.Printf("Delete message: %+v\n", deleteMsg)
	return s.dfs.Delete(string(deleteMsg.Path))
}

func (s *Server) handleReadMsg(bufMsg []byte) ([]byte, error) {
	readMsg, err := contracts.DecodeReadMsg(bufMsg)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Read message: %+v\n", readMsg)
	return s.dfs.Read(string(readMsg.Path))
}

func (s *Server) handleWriteMsg(bufMsg []byte) error {
	writeMsg, err := contracts.DecodeWriteMsg(bufMsg)
	if err != nil {
		return err
	}
	fmt.Printf("Write message: %+v\n", writeMsg)
	_, err = s.dfs.Write(string(writeMsg.Path), writeMsg.Chunk)

	return err
}

func (s *Server) sendFsMsgWithAck(payload filesystem.FSMessage, msgType contracts.MessageType, to string) ([]byte, error) {
	respCh := make(chan RequestResponse, 1)
	fsMsg := contracts.FsMessage{
		ID:        rand.Uint32(),
		From:      []byte(s.TransportAddress),
		To:        []byte(to),
		Timestamp: time.Now().Unix(),
		Type:      msgType,
		Payload:   payload.Encode(),
	}
	encoded, err := fsMsg.Encode()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	var peer *NodeInfo
	for _, p := range s.peers {
		if p.TransportAddress == to {
			peer = p
			break
		}
	}
	if peer == nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("not found")
	}
	s.pendingRequests[uint64(fsMsg.ID)] = respCh
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pendingRequests, uint64(fsMsg.ID))
		s.mu.Unlock()
	}()
	msg := contracts.NewTransportMsg(s.TransportAddress, peer.TransportAddress, contracts.FSPacket, encoded)
	if err := s.transport.Send(peer.Conn, msg); err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp.Data, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout")
	}

}

func (s *Server) sendFsMsg(payload filesystem.FSMessage, msgType contracts.MessageType, to string) error {
	fsMsg := contracts.FsMessage{
		ID:        rand.Uint32(),
		From:      []byte(s.TransportAddress),
		To:        []byte(to),
		Timestamp: time.Now().Unix(),
		Type:      msgType,
		Payload:   payload.Encode(),
	}
	encoded, err := fsMsg.Encode()
	if err != nil {
		return err
	}
	s.mu.Lock()
	var peer *NodeInfo
	for _, p := range s.peers {
		if p.TransportAddress == to {
			peer = p
			break
		}
	}
	if peer == nil {
		return fmt.Errorf("not found")
	}
	s.mu.Unlock()

	msg := contracts.NewTransportMsg(s.TransportAddress, peer.TransportAddress, contracts.FSPacket, encoded)
	return s.transport.Send(peer.Conn, msg)

}
