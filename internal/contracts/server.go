package contracts

import (
	"encoding/binary"
	"fmt"
)

type CommandType byte

const (
	CmdFsMessage CommandType = iota + 200
	CmdClusterChange
)

type ServerCommand struct {
	Type    CommandType
	Payload []byte
}

func (sc ServerCommand) Encode() ([]byte, error) {
	b := make([]byte, 1+4+len(sc.Payload))
	offset := 0
	b[offset] = byte(sc.Type)
	offset += 1
	binary.LittleEndian.PutUint32(b[1:], uint32(len(sc.Payload)))
	offset += 4
	copy(b[offset:], sc.Payload)
	return b, nil
}
func DecodeServerCommand(b []byte) (ServerCommand, error) {
	offset := 0
	sc := ServerCommand{
		Type: CommandType(b[offset]),
	}
	offset += 1
	payloadSize := binary.LittleEndian.Uint32(b[offset:])
	offset += 4
	sc.Payload = make([]byte, payloadSize)
	copy(sc.Payload, b[5:])

	return sc, nil
}

type ClusterConfig struct {
	ClusterNodes []uint64
}

func (cc ClusterConfig) Encode() ([]byte, error) {
	b := make([]byte, 0, 8*len(cc.ClusterNodes))
	for _, nId := range cc.ClusterNodes {
		b = binary.LittleEndian.AppendUint64(b, nId)
	}
	return b, nil
}

func DecodeClusterConfig(b []byte) (ClusterConfig, error) {
	if len(b)%8 != 0 {
		return ClusterConfig{}, fmt.Errorf("invalid buffer")
	}
	cc := ClusterConfig{
		ClusterNodes: make([]uint64, 0, len(b)/8),
	}
	offset := 0
	for offset < len(b) {
		id := binary.LittleEndian.Uint64(b[offset:])
		cc.ClusterNodes = append(cc.ClusterNodes, id)
		offset += 8
	}
	return cc, nil
}
