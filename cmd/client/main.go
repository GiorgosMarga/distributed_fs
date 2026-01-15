package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
)

func main() {
	client := http.Client{}

	availablePorts := []int{4000, 4001, 4002}
	for _, port := range availablePorts {
		b := new(bytes.Buffer)
		content, err := os.ReadFile("file.txt")
		if err != nil {
			log.Fatal(err)
		}

		mkdirmsg := filesystem.WriteMessage{
			Path:  []byte("test.txt"),
			Chunk: content,
		}

		if err := json.NewEncoder(b).Encode(mkdirmsg); err != nil {
			log.Fatal(err)
		}

		resp, err := client.Post(fmt.Sprintf("http://localhost:%d/write", port), "application/json", b)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 500 {
			continue
		}
		break
	}

	for _, port := range availablePorts {
		b := new(bytes.Buffer)
		mkdirmsg := filesystem.ReadMessage{
			Path: []byte("test.txt"),
		}

		if err := json.NewEncoder(b).Encode(mkdirmsg); err != nil {
			log.Fatal(err)
		}

		resp, err := client.Post(fmt.Sprintf("http://localhost:%d/read", port), "application/json", b)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == 500 {
			continue
		}
		f, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(string(f))

		break
	}
}
