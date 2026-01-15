package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"slices"
	"time"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
	"github.com/google/uuid"
)

func (s *Server) clientHandler() {
	availablePorts := map[string]int{
		":3000": 4000,
		":3001": 4001,
		":3002": 4002,
	}
	port := availablePorts[s.Address]
	mux := http.NewServeMux()
	mux.HandleFunc("POST /mkdir", s.mkdirHandler)
	mux.HandleFunc("POST /write", s.writeHandler)
	mux.HandleFunc("POST /read", s.readHandler)

	fmt.Printf("[%s]: serving http on %d %v\n", s.Address, port, s.raft.IsLeader())
	server := http.Server{Handler: mux, Addr: fmt.Sprintf(":%d", port)}
	go server.ListenAndServe()
}

func (s *Server) mkdirHandler(w http.ResponseWriter, r *http.Request) {
	mkdirMsg := filesystem.MkdirMessage{}

	if err := json.NewDecoder(r.Body).Decode(&mkdirMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}
	fmt.Printf("mkdir %+v\n", mkdirMsg)
	fsMsg := filesystem.Message{
		ID:        rand.Uint32(),
		From:      []byte(r.RemoteAddr),
		To:        []byte(s.Address),
		Timestamp: time.Now().Unix(),
		Type:      filesystem.MessageMkdir,
		Payload:   mkdirMsg.Encode(),
	}
	if err := s.proposeCommand(fsMsg); err != nil {
		w.WriteHeader(500)
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	w.Write(fmt.Appendf(nil, "Created folder %s", mkdirMsg.Path))
}

func (s *Server) writeHandler(w http.ResponseWriter, r *http.Request) {
	if !s.raft.IsLeader() {
		// redirect to leader
		// http.Redirect(w,r,)
		w.WriteHeader(500)
		w.Write([]byte("not leader\n"))
		return
	}
	writeMsg := filesystem.WriteMessage{}
	if err := json.NewDecoder(r.Body).Decode(&writeMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}
	allPeers := make([]string, 0, len(s.peers))
	for k := range s.peers {
		allPeers = append(allPeers, k)
	}

	allPeers = append(allPeers, s.Address)

	chunks := s.dfs.SplitIntoChunks(writeMsg.Chunk, 1024)
	chunkIds := make([]string, 0, len(chunks))
	fmt.Println(chunks)
	// chunkid -> [server1,server2]
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

	metadataMsg := filesystem.MetadataMessage{
		MetadataEntry: filesystem.MetadataEntry{
			Name:     string(writeMsg.Path),
			Size:     uint64(len(writeMsg.Chunk)),
			ChunkIDs: chunkIds,
			Replicas: replicas,
		},
	}
	if err := s.proposeCommand(filesystem.Message{
		ID:        rand.Uint32(),
		From:      []byte(s.Address),
		To:        []byte{},
		Timestamp: time.Now().Unix(),
		Type:      filesystem.MessageMetadata,
		Payload:   metadataMsg.Encode(),
	}); err != nil {
		fmt.Println(err)
	}

}

func (s *Server) sendWriteMsg(chunk, id, to []byte) error {
	if string(to) == s.Address {
		_, err := s.dfs.Write(string(id), chunk)
		return err
	}

	writeMsg := filesystem.WriteMessage{
		Path:  id,
		Chunk: chunk,
	}

	_, err := s.sendFsMsgWithAck(writeMsg, filesystem.MessageWrite, string(to))
	return err
}

func (s *Server) readHandler(w http.ResponseWriter, r *http.Request) {
	if !s.raft.IsLeader() {
		// redirect to leader
		// http.Redirect(w,r,)
		w.WriteHeader(500)
		w.Write([]byte("not leader\n"))
		return
	}
	readMsg := filesystem.ReadMessage{}
	if err := json.NewDecoder(r.Body).Decode(&readMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	fileEntry, err := s.dfs.ReadMetadata(string(readMsg.Path))
	if err != nil {
		fmt.Println(err)
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	var (
		fileBuf = new(bytes.Buffer)
		data    []byte
	)
	for _, chunkId := range fileEntry.ChunkIDs {
		servers := fileEntry.Replicas[chunkId]
		if slices.Contains(servers, s.Address) {
			data, err = s.dfs.Read(chunkId)
			if err != nil {
				fmt.Println(err)
			}

		} else {
			data, err = s.sendFsMsgWithAck(filesystem.ReadMessage{Path: []byte(chunkId)}, filesystem.MessageRead, servers[0])
			if err != nil {
				fmt.Println(err)
			}
		}
		fileBuf.Write(data)
	}
	w.Write(fileBuf.Bytes())
}
