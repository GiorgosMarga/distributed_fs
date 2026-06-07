package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/GiorgosMarga/dfs/internal/contracts"
)

const defaultServerAddr = "http://localhost:4000"

func main() {
	var (
		op         string
		serverAddr string
		name       string
		data       string
		perm       string
	)

	flag.StringVar(&op, "op", "write", "Operation: mkdir, write, read, delete")
	flag.StringVar(&serverAddr, "server", defaultServerAddr, "Server address, for example http://localhost:4000")
	flag.StringVar(&name, "name", "", "File or directory name")
	flag.StringVar(&data, "data", "", "Inline file contents for write")
	flag.StringVar(&perm, "perm", "0755", "Directory permissions for mkdir")
	flag.Parse()

	if op == "" || name == "" {
		printUsage()
		os.Exit(2)
	}

	var code int
	switch op {
	case "mkdir":
		code = mkdir(serverAddr, name, perm)
	case "write":
		code = write(serverAddr, name, data)
	case "read":
		code = read(serverAddr, name)
	case "delete":
		code = deleteFile(serverAddr, name)
	default:
		fmt.Fprintf(os.Stderr, "unknown op: %s\n\n", op)
		printUsage()
		code = 2
	}
	os.Exit(code)
}

func mkdir(serverAddr, name, perm string) int {
	mode, err := parseOctalMode(perm)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	respBody, status, err := sendJSONRequest(http.MethodPost, endpoint(serverAddr, "/mkdir"), contracts.MkdirMessage{
		Path:        []byte(name),
		Permissions: mode,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	fmt.Println(strings.TrimSpace(string(respBody)))
	if status >= http.StatusBadRequest {
		return 1
	}
	return 0
}

func write(serverAddr, name, data string) int {
	content := []byte(data)
	if len(content) == 0 {
		var err error
		content, err = os.ReadFile(name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	respBody, status, err := sendJSONRequest(http.MethodPost, endpoint(serverAddr, "/write"), contracts.WriteMessage{
		Path:  []byte(name),
		Chunk: content,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	fmt.Println(strings.TrimSpace(string(respBody)))
	if status >= http.StatusBadRequest {
		return 1
	}
	return 0
}

func read(serverAddr, name string) int {
	respBody, status, err := sendJSONRequest(http.MethodPost, endpoint(serverAddr, "/read"), contracts.ReadMessage{
		Path: []byte(name),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if status >= http.StatusBadRequest {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(string(respBody)))
		return 1
	}

	fmt.Print(string(respBody))
	return 0
}

func deleteFile(serverAddr, name string) int {
	respBody, status, err := sendJSONRequest(http.MethodDelete, endpoint(serverAddr, "/delete"), contracts.DeleteMessage{
		Path: []byte(name),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	fmt.Println(strings.TrimSpace(string(respBody)))
	if status >= http.StatusBadRequest {
		return 1
	}
	return 0
}

func sendJSONRequest(method, url string, payload any) ([]byte, int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return respBody, resp.StatusCode, nil
}

func endpoint(serverAddr, path string) string {
	return strings.TrimRight(normalizeServerAddr(serverAddr), "/") + path
}

func normalizeServerAddr(serverAddr string) string {
	if serverAddr == "" {
		return defaultServerAddr
	}
	if strings.HasPrefix(serverAddr, "http://") || strings.HasPrefix(serverAddr, "https://") {
		return serverAddr
	}
	if strings.HasPrefix(serverAddr, ":") {
		return "http://localhost" + serverAddr
	}
	return "http://" + serverAddr
}

func parseOctalMode(raw string) (fs.FileMode, error) {
	value, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid permissions %q: %w", raw, err)
	}
	return fs.FileMode(value), nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `dfs client

Usage:
  dfs-client -op mkdir  -name docs      -server http://localhost:4000 -perm 0755
  dfs-client -op write  -name file.txt  -server http://localhost:4000
  dfs-client -op read   -name file.txt  -server http://localhost:4000
  dfs-client -op delete -name file.txt  -server http://localhost:4000

Notes:
  - write reads local contents from ./<name> unless -data is set
`)
}
