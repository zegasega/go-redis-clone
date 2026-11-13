package main

import (
	"log"

	"github.com/zegasega/go-redis-clone/internal/server"
)

func main() {
	config := server.Config{
		ListenAddr: ":5001",
	}

	tcp := server.NewTCPServer(config)

	if err := tcp.Run(); err != nil {
		log.Fatal(err)
	}
}
