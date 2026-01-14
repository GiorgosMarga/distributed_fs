package filesystem

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
)

type DFS struct {
	root     string
	filesMap map[string]string
}

func NewDFS(root string) (*DFS, error) {
	if err := os.Mkdir(root, 0755); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return &DFS{
		root:     root,
		filesMap: make(map[string]string),
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

func (dfs *DFS) MapChunkId(chunkId, serverId string) error {
	dfs.filesMap[chunkId] = serverId
	return nil
}
