# raft-kv
基于 Raft 共识库构建的 Go 分布式 KV 学习项目

## 启动三节点 Raft 集群

在三个终端分别启动一个节点：

```sh
go run ./cmd/raftkv -id 1 -raft-addr 127.0.0.1:8081 \
  -peer 2=http://127.0.0.1:8082 \
  -peer 3=http://127.0.0.1:8083
```

```sh
go run ./cmd/raftkv -id 2 -raft-addr 127.0.0.1:8082 \
  -peer 1=http://127.0.0.1:8081 \
  -peer 3=http://127.0.0.1:8083
```

```sh
go run ./cmd/raftkv -id 3 -raft-addr 127.0.0.1:8083 \
  -peer 1=http://127.0.0.1:8081 \
  -peer 2=http://127.0.0.1:8082
```

`/kv` 是客户端接口。PUT 命令发到任意节点后，会在日志提交并由 leader 应用后返回；GET 使用 Raft `ReadIndex` 确认安全读位置，并等本地状态机应用到该位置后再读取。

```sh
curl -X PUT http://127.0.0.1:8082/kv \
  -H 'Content-Type: application/json' \
  -d '{"key":"x","value":"3"}'

curl 'http://127.0.0.1:8083/kv?key=x'
```

Raft 日志和 KV 数据目前都只在内存中，进程重启后不会恢复。
