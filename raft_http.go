package kv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const clientRequestTimeout = 10 * time.Second

type WriteResult struct {
	OriginNodeID uint64 `json:"origin_node_id"`
	RequestID    string `json:"request_id"`
	Error        string `json:"error,omitempty"`
}

type putRequest struct {
	RequestID string `json:"request_id"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type putResponse struct {
	RequestID string `json:"request_id"`
	Applied   bool   `json:"applied"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (n *RaftNode) HandleRaftMessage(c *gin.Context) {
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "read message: " + err.Error()})
		return
	}

	msg := new(raftpb.Message)
	if err := proto.Unmarshal(data, msg); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "decode message: " + err.Error()})
		return
	}

	if err := n.Step(c.Request.Context(), msg); err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{Error: "step message: " + err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

func (n *RaftNode) HandlePut(c *gin.Context) {
	var request putRequest
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "decode PUT body: " + err.Error()})
		return
	}
	if request.Key == "" {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "key must not be empty"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), clientRequestTimeout)
	defer cancel()
	requestID, err := n.ProposeAndWait(ctx, Command{
		RequestID: request.RequestID,
		Op:        "put",
		Key:       request.Key,
		Value:     request.Value,
	})
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		c.JSON(status, errorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, putResponse{RequestID: requestID, Applied: true})
}

func (n *RaftNode) HandleGet(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "key query parameter is required"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), clientRequestTimeout)
	defer cancel()
	value, found, err := n.LinearizableGet(ctx, key)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		c.JSON(status, errorResponse{Error: err.Error()})
		return
	}
	c.JSON(http.StatusOK, getResponse{Key: key, Value: value, Found: found})
}

func (n *RaftNode) HandleWriteResult(c *gin.Context) {
	var result WriteResult
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	if err := c.ShouldBindJSON(&result); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "decode write result: " + err.Error()})
		return
	}
	if result.OriginNodeID != n.id || result.RequestID == "" {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: fmt.Sprintf("write result does not belong to node %d", n.id),
		})
		return
	}

	n.CompleteWriteResult(result)
	c.Status(http.StatusNoContent)
}

func NewRaftHTTPServer(addr string, node *RaftNode) *http.Server {
	router := gin.New()
	router.Use(gin.Recovery())
	router.PUT("/kv", node.HandlePut)
	router.GET("/kv", node.HandleGet)
	router.POST("/raft", node.HandleRaftMessage)
	router.POST("/result", node.HandleWriteResult)

	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}
