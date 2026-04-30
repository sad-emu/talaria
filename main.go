package main

import (
	"context"
	"flag"
	"time"

	"talaria/api"
	"talaria/config"
	"talaria/hodos"
	"talaria/persistence"
	"talaria/utils"
)

const VERSION = "0.0.1"

func main() {
	cfgPath := flag.String("config", "talaria.yml", "path to YAML config file")
	apiListen := flag.String("api-listen", "127.0.0.1:8080", "HTTP API listen address")
	flag.Parse()

	utils.Infof("talaria version %s starting", VERSION)

	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		utils.SetupLogger(&config.LogConfig{Filename: "crash.txt", Level: "ERROR"})
		utils.Fatalf("failed to load config: %v", err)
	}

	utils.SetupLogger(&cfg.GlobalLog)

	var store persistence.TransferStore
	if len(cfg.Hodos) > 0 {
		store, err = persistence.OpenTransferStore(context.Background(), persistence.Config{
			Backend:    persistence.Backend(cfg.Persistence.Backend),
			SQLitePath: cfg.Persistence.SQLitePath,
		})
		if err != nil {
			utils.Fatalf("open persistence store failed: %v", err)
		}
		defer store.Close()
	}

	apiServer := api.NewServerWithProgress(*apiListen, store)
	if len(cfg.Hodos) > 0 {
		chunkSizes := make(map[string]int, len(cfg.Hodos))
		for _, hc := range cfg.Hodos {
			if hc.Dropoff.S3 != nil {
				chunkSizes[hc.Name] = hc.Dropoff.S3.MultipartChunkSizeMB
			}
		}
		apiServer.WithHodosChunkSizes(chunkSizes)
	}
	if err := apiServer.StartBackground(); err != nil {
		utils.Fatalf("failed to start API server: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := apiServer.Shutdown(shutdownCtx); err != nil {
			utils.Errorf("API server shutdown failed: %v", err)
		}
	}()

	if len(cfg.Hodos) > 0 {
		go func() {
			count, herr := hodos.RunConfigured(context.Background(), cfg.Hodos, store)
			if herr != nil {
				utils.Fatalf("hodos run failed: %v", herr)
			}
			utils.Infof("hodos completed, processed %d item(s)", count)
		}()
	}

	node, err := NewNode(cfg)
	if err != nil {
		utils.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		utils.Fatalf("node stopped: %v", err)
	}
}
