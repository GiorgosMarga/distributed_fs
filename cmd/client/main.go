package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/GiorgosMarga/dfs/internal/filesystem"
)

func main() {
	client := http.Client{}

	availablePorts := []int{4000, 4001, 4002}
	for _, port := range availablePorts {
		b := new(bytes.Buffer)

		mkdirmsg := filesystem.WriteMessage{
			Path:  []byte("test.txt"),
			Chunk: []byte("12345678901234567890123456789012345678901234567890"),
		}

		if err := json.NewEncoder(b).Encode(mkdirmsg); err != nil {
			log.Fatal(err)
		}

		resp, err := client.Post(fmt.Sprintf("http://localhost:%d/write", port), "application/json", b)
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode == 500 {
			fmt.Println("500")
			continue
		}
		break
	}
}
