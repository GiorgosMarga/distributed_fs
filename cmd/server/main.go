package main

import (
	"log"
	"time"

	"github.com/GiorgosMarga/dfs/internal/transport"
)

func main() {
	s1 := transport.NewServer(transport.ServerOpts{
		Address:    ":3000",
		Serializer: transport.NewGOBSerializer(),
	})
	s2 := transport.NewServer(transport.ServerOpts{
		Address:    ":3001",
		Serializer: transport.NewGOBSerializer(),
	})
	s3 := transport.NewServer(transport.ServerOpts{
		Address:    ":3002",
		Serializer: transport.NewGOBSerializer(),
	})

	go func() {
		if err := s1.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := s2.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	go func() {
		if err := s3.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	time.Sleep(1 * time.Second)

	if err := s2.Bootstrap(":3000", ":3002"); err != nil {
		log.Fatal(err)
	}
	if err := s3.Bootstrap(":3000", ":3001"); err != nil {
		log.Fatal(err)
	}
	time.Sleep(1000 * time.Second)
}
