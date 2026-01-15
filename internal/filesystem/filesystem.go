package filesystem

import (
	"io/fs"
)

// TODO: fix this
type FSMessage interface {
	Encode() []byte
	// Decode([]byte) (any, error)
}

type DistributedFileSystem interface {
	Write(string, []byte) (int, error)
	Mkdir(string, fs.FileMode) error
	Read(string) ([]byte, error)
	Delete(string) error
	InsertMetadata(string, MetadataEntry) error
	ReadMetadata(string) (MetadataEntry, error)
	SplitIntoChunks([]byte, int) [][]byte
	// Ls(string) []byte
}
