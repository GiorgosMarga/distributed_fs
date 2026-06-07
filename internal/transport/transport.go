package transport

import (
	"io"

	"github.com/GiorgosMarga/dfs/internal/contracts"
)

type Transport interface {
	ListenAndAccept() error
	Close() error
	Send(io.Writer, contracts.TransportMessage) error
	Consume() <-chan contracts.TransportMessage
	Connect(string) error
}
