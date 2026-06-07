package transport

import (
	"encoding/gob"
	"io"
	"sync"

	"github.com/GiorgosMarga/dfs/internal/contracts"
)

type Serializer interface {
	Encode(io.Writer, contracts.TransportMessage) error
	Decode(io.Reader) (contracts.TransportMessage, error)
}

type GOBSerializer struct {
	encoders map[io.Writer]*gob.Encoder
	decoders map[io.Reader]*gob.Decoder

	mu *sync.Mutex
}

func NewGOBSerializer() *GOBSerializer {
	return &GOBSerializer{
		encoders: make(map[io.Writer]*gob.Encoder),
		decoders: make(map[io.Reader]*gob.Decoder),
		mu:       &sync.Mutex{},
	}
}

func (g *GOBSerializer) Encode(w io.Writer, msg contracts.TransportMessage) error {
	g.mu.Lock()
	enc, ok := g.encoders[w]
	if !ok {
		enc = gob.NewEncoder(w)
		g.encoders[w] = enc
	}
	g.mu.Unlock()
	return enc.Encode(msg)
}

func (g *GOBSerializer) Decode(r io.Reader) (contracts.TransportMessage, error) {
	g.mu.Lock()
	dec, ok := g.decoders[r]
	if !ok {
		dec = gob.NewDecoder(r)
		g.decoders[r] = dec
	}
	g.mu.Unlock()

	var msg contracts.TransportMessage
	err := dec.Decode(&msg)
	return msg, err
}
