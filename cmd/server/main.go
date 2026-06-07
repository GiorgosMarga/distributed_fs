package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/GiorgosMarga/dfs/internal/server"
)

func main() {
	serverOpts := server.ServerOpts{}

	flag.StringVar(&serverOpts.TransportAddress, "transportAddress", ":3000", "Listening address of the node.")
	flag.StringVar(&serverOpts.HttpAddress, "httpAddress", ":4000", "Listening address of the http api.")
	flag.Uint64Var(&serverOpts.Id, "nodeId", rand.Uint64(), "Node id.")
	flag.Func("connectWith", "Bootstrap nodes seperated by a comma.", func(s string) error {
		serverOpts.BootstrapAddress = strings.Split(s, ",")
		return nil
	})

	flag.Parse()

	s, err := server.New(serverOpts)
	if err != nil {
		log.Fatal(err)
	}
	quitCh := make(chan os.Signal, 1)

	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quitCh
		fmt.Println("Terminating....")
		if err := s.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	log.Fatal(s.Start())
}
