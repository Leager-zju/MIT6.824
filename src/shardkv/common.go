package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicated  = "ErrDuplicated"
	ErrScale       = "ErrScale"
)

type Err string

type Shard struct {
	KVs     map[string]string
	Version int
}

type Args struct {
	Key       string
	Value     string
	Op        string
	ClerkId   int64
	RequestId int
	Ch        chan *Reply

	Data interface{}
}

type Reply struct {
	Err   Err
	Value string
}

type ShardExchangeArgs struct {
	ShardId int
	Version int
}

type ShardExchangeReply struct {
	Err   Err
	Shard Shard
}

type ConfigInfo struct {
	LastConfig shardctrler.Config
	NewConfig  shardctrler.Config
}

type ShardInfo struct {
	Shard Shard
	Sid   int
}
