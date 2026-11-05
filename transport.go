package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type Transport interface {
	Send(ctx context.Context, msg *raftpb.Message) error
	SendResult(ctx context.Context, result WriteResult) error
	Run(ctx context.Context)
}

var ErrTransportQueueFull = errors.New("raft transport queue is full")

type outboundMessage struct {
	url         string
	contentType string
	data        []byte
}

type HTTPTransport struct {
	peerURLs map[uint64]string
	client   *http.Client
	outgoing chan outboundMessage
}

func NewHTTPTransport(peerURLs map[uint64]string) *HTTPTransport {
	return &HTTPTransport{
		peerURLs: peerURLs,
		client:   http.DefaultClient,
		outgoing: make(chan outboundMessage, 256),
	}
}
func (t *HTTPTransport) Send(ctx context.Context, msg *raftpb.Message) error {
	peerURL, ok := t.peerURLs[msg.GetTo()]
	if !ok {
		return fmt.Errorf("unknown raft peer %d", msg.GetTo())
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return t.enqueue(ctx, outboundMessage{
		url:         peerURL + "/raft",
		contentType: "application/octet-stream",
		data:        data,
	})
}

func (t *HTTPTransport) SendResult(ctx context.Context, result WriteResult) error {
	peerURL, ok := t.peerURLs[result.OriginNodeID]
	if !ok {
		return fmt.Errorf("unknown result destination node %d", result.OriginNodeID)
	}

	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return t.enqueue(ctx, outboundMessage{
		url:         peerURL + "/result",
		contentType: "application/json",
		data:        data,
	})
}

func (t *HTTPTransport) enqueue(ctx context.Context, msg outboundMessage) error {
	select {
	case t.outgoing <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrTransportQueueFull
	}
}

func (t *HTTPTransport) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-t.outgoing:
			if err := t.sendHTTP(ctx, msg); err != nil {
				log.Printf("send HTTP message to %s: %v", msg.url, err)
			}
		}
	}
}

func (t *HTTPTransport) sendHTTP(ctx context.Context, msg outboundMessage) error {
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		msg.url,
		bytes.NewReader(msg.data),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", msg.contentType)

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("HTTP peer %s returned %s", msg.url, resp.Status)
	}
	return nil
}
