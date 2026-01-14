package filesystem

import (
	"io/fs"
)
// TODO: fix this
type FSMessage interface {
	Encode() []byte
	// Decode([]byte) (any, error)
}

type FileSystem interface {
	Write(string, []byte) (int, error)
	Mkdir(string, fs.FileMode) error
	Read(string) ([]byte, error)
	Delete(string) error
	MapChunkId(string, string) error
	// Ls(string) []byte
}
