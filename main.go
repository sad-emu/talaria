package main

import (
	"flag"
	"log"
	"os"

	"talaria/config"
)

const VERSION = "0.0.1"

func main() {
	cfgPath := flag.String("config", "talaria.yml", "path to YAML config file")
	flag.Parse()

	log.Printf("talaria version %s starting", VERSION)

	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		f, ferr := os.OpenFile("crash.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if ferr == nil {
			log.SetOutput(f)
			defer f.Close()
		}
		log.Fatalf("failed to load config: %v", err)
	}

	node, err := NewNode(cfg)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("node stopped: %v", err)
	}
}
