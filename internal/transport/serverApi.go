package transport

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
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
	if err := s.handleClientCommand(fsMsg); err != nil {
		w.WriteHeader(500)
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	w.Write(fmt.Appendf(nil, "Created folder %s", mkdirMsg.Path))
}

func (s *Server) writeHandler(w http.ResponseWriter, r *http.Request) {
	if !s.raft.IsLeader() {
		w.WriteHeader(500)
		w.Write([]byte("not leader\n"))
		return
	}
	writeMsg := filesystem.WriteMessage{}

	if err := json.NewDecoder(r.Body).Decode(&writeMsg); err != nil {
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	chunkSize := 10
	totalChunks := len(writeMsg.Chunk) / chunkSize
	chunkIds := make([][]byte, totalChunks)
	servers := make([][]byte, totalChunks)
	keys := make([]string, 0, len(s.peers))
	for k := range s.peers {
		keys = append(keys, k)
	}
	keys = append(keys, s.Address)

	for i := range totalChunks {
		peerId := keys[i%len(keys)]
		chunk := writeMsg.Chunk[i*chunkSize : min(chunkSize*(i+1), len(writeMsg.Chunk))]
		chunkIds[i] = fmt.Appendf(nil, "%d", i)
		servers[i] = []byte(peerId)

		writeMsg := filesystem.WriteMessage{
			Path:  chunkIds[i],
			Chunk: chunk,
		}

		if peerId == s.Address {
			if _, err := s.dfs.Write(string(chunkIds[i]), chunk); err != nil {
				fmt.Println(err)
			}
		} else {
			if err := s.sendFsMsgWithAck(writeMsg, filesystem.MessageWrite, peerId); err != nil {
				fmt.Println(err)
			}
		}

		fmt.Printf("Writing chunk %d to %s\n", i, peerId)
	}

	metadataMsg := filesystem.MetadataMessage{
		ChunkIds: chunkIds,
		Servers:  servers,
	}

	fsMsg := filesystem.Message{
		ID:        rand.Uint32(),
		From:      []byte(r.RemoteAddr),
		To:        []byte(s.Address),
		Timestamp: time.Now().Unix(),
		Type:      filesystem.MessageMetadata,
		Payload:   metadataMsg.Encode(),
	}

	if err := s.handleClientCommand(fsMsg); err != nil {
		w.WriteHeader(500)
		w.Write(fmt.Appendf(nil, "%s", err))
		return
	}

	w.Write(fmt.Appendf(nil, "Wrote file %s", writeMsg.Path))
}
