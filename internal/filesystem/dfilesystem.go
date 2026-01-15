package filesystem

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
)

type DFS struct {
	root        string
	fileCatalog map[string]MetadataEntry
}

type MetadataEntry struct {
	Name     string
	Size     uint64
	ChunkIDs []string
	// chunk id => [server1, server2...]
	Replicas map[string][]string
}

func NewDFS(root string) (*DFS, error) {
	if err := os.Mkdir(root, 0755); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return &DFS{
		root:        root,
		fileCatalog: make(map[string]MetadataEntry),
	}, nil
}

func (dfs *DFS) Mkdir(dirPath string, perm fs.FileMode) error {
	fullPath := path.Join(dfs.root, dirPath)
	return os.Mkdir(fullPath, perm)
}
func (dfs *DFS) Write(filePath string, data []byte) (int, error) {
	fullPath := path.Join(dfs.root, filePath)
	fd, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	return fd.Write(data)
}

func (dfs *DFS) Read(filePath string) ([]byte, error) {
	fullPath := path.Join(dfs.root, filePath)
	fd, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(fd)
}
func (dfs *DFS) Delete(filePath string) error {
	fullPath := path.Join(dfs.root, filePath)
	return os.Remove(fullPath)
}

func (dfs *DFS) InsertMetadata(filename string, metadata MetadataEntry) error {
	dfs.fileCatalog[filename] = metadata
	return nil
}
func (dfs *DFS) ReadMetadata(filename string) (MetadataEntry, error) {
	metadata, exists := dfs.fileCatalog[filename]
	if !exists {
		return MetadataEntry{}, fmt.Errorf("metadata not found")
	}
	return metadata, nil
}

func (dfs *DFS) SplitIntoChunks(file []byte, chunkSize int) [][]byte {
	totalChunks := len(file) / chunkSize
	chunks := make([][]byte, totalChunks)
	for chunk := range totalChunks {
		chunks[chunk] = make([]byte, chunkSize)
		copy(chunks[chunk], file[chunk*chunkSize:min((chunk+1)*chunkSize, len(file))])
	}
	return chunks
}
