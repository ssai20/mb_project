package main

import (
	"MBProject/api"
	"log"
	"os"
)

func main() {
	server, err := api.NewServer()
	if err != nil {
		log.Printf("Failed to create server: %v", err)
		os.Exit(1)
	}
	server.SetupRouters()
	server.Start()

}
