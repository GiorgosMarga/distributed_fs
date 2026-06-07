package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/GiorgosMarga/dfs/internal/contracts"
	"github.com/google/uuid"
)

// TODO: add graceful shutdown
func (s *Server) clientHandler() {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /mkdir", s.mkdirHandler)
	mux.HandleFunc("POST /write", s.writeHandler)
	mux.HandleFunc("POST /read", s.readHandler)
	mux.HandleFunc("DELETE /delete", s.deleteHandler)

	fmt.Printf("[%d]: serving http on %s...\n", s.Id, s.HttpAddress)
	server := http.Server{Handler: mux, Addr: s.HttpAddress}
	go server.ListenAndServe()
	<-s.quitCh
	fmt.Println("Quit chan")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		fmt.Println(err)
	}
}

func (s *Server) mkdirHandler(w http.ResponseWriter, r *http.Request) {
	if !s.raft.IsLeader() {
		s.mu.Lock()

		leaderNode, exists := s.peers[s.raft.LeaderId]
		if !exists {
			// write error
			fmt.Println("Leader doesnt exist")
			w.WriteHeader(http.StatusInternalServerError)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		http.Redirect(w, r, fmt.Sprintf("http://localhost%s/mkdir", leaderNode.HttpAddr), http.StatusTemporaryRedirect)
		return
	}
	mkdirMsg := contracts.MkdirMessage{}

	if err := json.NewDecoder(r.Body).Decode(&mkdirMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}
	// TODO: fix THIS
	fsMsg := contracts.FsMessage{
		ID:        rand.Uint32(),
		From:      []byte(s.TransportAddress),
		Timestamp: time.Now().Unix(),
		Type:      contracts.MessageMkdir,
		Payload:   mkdirMsg.Encode(),
	}
	if err := s.proposeCommand(fsMsg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(fmt.Appendf(nil, "Created folder %s", mkdirMsg.Path))
}

func (s *Server) writeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Write handler", s.raft.LeaderId)
	if !s.raft.IsLeader() {
		s.mu.Lock()
		leaderNode, exists := s.peers[s.raft.LeaderId]
		if !exists {
			// write error
			fmt.Println("Leader doesnt exist")
			w.WriteHeader(http.StatusInternalServerError)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		fmt.Printf("Redirecting to %s\n", fmt.Sprintf("http://localhost%s/write", leaderNode.HttpAddr))
		http.Redirect(w, r, fmt.Sprintf("http://localhost%s/write", leaderNode.HttpAddr), http.StatusTemporaryRedirect)
		return
	}
	writeMsg := contracts.WriteMessage{}
	if err := json.NewDecoder(r.Body).Decode(&writeMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}
	allPeers := make([]string, 0, len(s.peers))
	for _, p := range s.peers {
		allPeers = append(allPeers, p.TransportAddress)
	}

	allPeers = append(allPeers, s.TransportAddress)

	chunks := s.dfs.SplitIntoChunks(writeMsg.Chunk, 1024)
	chunkIds := make([]string, 0, len(chunks))
	// chunkids -> [server1,server2]
	replicas := make(map[string][]string)

	for i, chunk := range chunks {
		peerOne := allPeers[i%len(allPeers)]
		peerTwo := allPeers[(i+2)%len(allPeers)]
		// assume writes dont fail
		chunkId := uuid.New().String()
		if err := s.sendWriteMsg(chunk, []byte(chunkId), []byte(peerOne)); err != nil {
			fmt.Println(err)
		}
		if err := s.sendWriteMsg(chunk, []byte(chunkId), []byte(peerTwo)); err != nil {
			fmt.Println(err)
		}
		chunkIds = append(chunkIds, chunkId)
		replicas[chunkId] = []string{peerOne, peerTwo}
	}

	metadataMsg := contracts.MetadataMessage{
		Name:     string(writeMsg.Path),
		Size:     uint64(len(writeMsg.Chunk)),
		ChunkIDs: chunkIds,
		Replicas: replicas,
	}
	if err := s.proposeCommand(contracts.FsMessage{
		ID:        rand.Uint32(),
		From:      []byte(s.TransportAddress),
		Timestamp: time.Now().Unix(),
		Type:      contracts.MessageMetadata,
		Payload:   metadataMsg.Encode(),
	}); err != nil {
		fmt.Println(err)
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(metadataMsg); err != nil {
		fmt.Println(err)
	}
}

func (s *Server) sendWriteMsg(chunk, id, to []byte) error {
	if string(to) == s.TransportAddress {
		_, err := s.dfs.Write(string(id), chunk)
		return err
	}

	writeMsg := contracts.WriteMessage{
		Path:  id,
		Chunk: chunk,
	}

	_, err := s.sendFsMsgWithAck(writeMsg, contracts.MessageWrite, string(to))
	return err
}

func (s *Server) readHandler(w http.ResponseWriter, r *http.Request) {
	// ask for metadata from leader
	// if !s.raft.IsLeader() {
	// 	leader, exists := s.peers[s.raft.LeaderId]
	// 	if !exists {
	// 		w.WriteHeader(500)
	// 		w.Write([]byte("not leader\n"))
	// 		return
	// 	}
	// 	b, _ := io.ReadAll(r.Body)
	// 	if err := s.transport.Send(leader.Conn, transport.TransportMessage{
	// 		Id:         rand.Uint64(),
	// 		From:       s.TransportAddress,
	// 		To:         leader.TransportAddress,
	// 		PacketType: transport.FSPacket,
	// 		Payload:    b,
	// 	}); err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	return
	// }
	readMsg := contracts.ReadMessage{}
	if err := json.NewDecoder(r.Body).Decode(&readMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	fmt.Println(readMsg)

	content, err := s.Read(string(readMsg.Path))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if !s.raft.IsLeader() {
		s.mu.Lock()
		leaderNode, exists := s.peers[s.raft.LeaderId]
		if !exists {
			fmt.Println("Leader doesnt exist")
			w.WriteHeader(http.StatusInternalServerError)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		http.Redirect(w, r, fmt.Sprintf("http://localhost%s/delete", leaderNode.HttpAddr), http.StatusTemporaryRedirect)
		return
	}

	deleteMsg := contracts.DeleteMessage{}
	if err := json.NewDecoder(r.Body).Decode(&deleteMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	fsMsg := contracts.FsMessage{
		ID:        rand.Uint32(),
		From:      []byte(s.TransportAddress),
		Timestamp: time.Now().Unix(),
		Type:      contracts.MessageDelete,
		Payload:   deleteMsg.Encode(),
	}
	if err := s.proposeCommand(fsMsg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(fmt.Appendf(nil, "Deleted %s", deleteMsg.Path))
}
