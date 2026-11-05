package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	kv "github.com/Yukinoshita03/raft-kv"
	"go.etcd.io/raft/v3"
)

type peerFlags []string

func (p *peerFlags) String() string {
	return strings.Join(*p, ",")
}

func (p *peerFlags) Set(value string) error {
	*p = append(*p, value)
	return nil
}

func main() {
	id := flag.Uint64("id", 0, "this node's Raft ID")
	addr := flag.String("raft-addr", "127.0.0.1:8081", "local Raft HTTP listen address")
	var peerArgs peerFlags
	flag.Var(&peerArgs, "peer", "other node as ID=http://host:port; repeat once per peer")
	flag.Parse()

	if *id == 0 {
		log.Fatal("-id must be a non-zero Raft node ID")
	}

	peers, peerURLs, err := parsePeers(*id, peerArgs)
	if err != nil {
		log.Fatal(err)
	}
	if len(peers) < 2 {
		log.Fatal("provide at least two -peer values for a three-node cluster")
	}

	raftNode := kv.NewRaftNode(*id, peers, kv.NewStore())
	transport := kv.NewHTTPTransport(peerURLs)
	server := kv.NewRaftHTTPServer(*addr, raftNode)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("Raft node %d listening on %s", *id, *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Raft HTTP server stopped: %v", err)
			stop()
		}
	}()

	runErr := raftNode.Run(ctx, transport)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shut down Raft HTTP server: %v", err)
	}
	if runErr != nil {
		log.Fatal(runErr)
	}
}

func parsePeers(localID uint64, args []string) ([]raft.Peer, map[uint64]string, error) {
	peers := make([]raft.Peer, 0, len(args))
	peerURLs := make(map[uint64]string, len(args))

	for _, arg := range args {
		idText, peerURL, ok := strings.Cut(arg, "=")
		if !ok || peerURL == "" {
			return nil, nil, fmt.Errorf("invalid peer %q: expected ID=http://host:port", arg)
		}
		// 兼容旧版完整的 /raft 地址和基础 URL。
		peerURL = strings.TrimSuffix(strings.TrimRight(peerURL, "/"), "/raft")
		peerURL = strings.TrimRight(peerURL, "/")

		id, err := strconv.ParseUint(idText, 10, 64)
		if err != nil || id == 0 {
			return nil, nil, fmt.Errorf("invalid peer ID in %q", arg)
		}
		if id == localID {
			return nil, nil, fmt.Errorf("peer ID %d is the local node ID", id)
		}
		if _, exists := peerURLs[id]; exists {
			return nil, nil, fmt.Errorf("duplicate peer ID %d", id)
		}

		peers = append(peers, raft.Peer{ID: id})
		peerURLs[id] = peerURL
	}

	// StartNode 的初始成员配置需要包含本节点。
	peers = append(peers, raft.Peer{ID: localID})
	return peers, peerURLs, nil
}
