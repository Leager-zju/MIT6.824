package shardkv

import "6.824/shardctrler"

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNotReady"
)

type status int

const (
	// everyone
	Ready status = iota // 一切就绪
	// new owner
	NeedPull           // 表明该分片等待从其他 group 处拉取
	ReadyButNeedSendGC // 就绪，但需要通知其他 group 进行 GC
	// old owner
	Waiting // 表明该分片等待被其他 group 拉取 + 通知 GC
)

type Shard struct {
	KVs         map[string]string
	ShardStatus status
}

type RequestInfo struct {
	RequestID int
	Err       Err
}

// cmd
type OperationCommand struct {
	Op        string
	Key       string
	Value     string
	ClerkId   int64
	RequestId int
	Ch        chan *Reply
}

type Reply struct {
	Err   Err
	Value string
}

type ShardCommand struct {
	Op              string
	Shard           *Shard
	Sid             int
	LastRequestInfo map[int64]RequestInfo
}

type ConfigCommand struct {
	LastConfig shardctrler.Config
	NewConfig  shardctrler.Config
}

type EmptyCommand struct {
}

// rpc
type RPCArgs struct {
	Op        string
	ShardId   int
	ConfigNum int
}

type RPCReply struct {
	Err             Err
	Shard           Shard
	ConfigNum       int
	LastRequestInfo map[int64]RequestInfo
}
