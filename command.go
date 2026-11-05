package kv

import "encoding/json"

type Command struct {
	RequestID    string `json:"request_id"`
	OriginNodeID uint64 `json:"origin_node_id"`
	Op           string `json:"op"`
	Key          string `json:"key"`
	Value        string `json:"value"`
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
