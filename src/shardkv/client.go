package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	ClerkId   int64
	RequestId int

	volatileLeader map[int]int // gid -> leader
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	return &Clerk{
		sm:             shardctrler.MakeClerk(ctrlers),
		make_end:       make_end,
		ClerkId:        nrand(),
		RequestId:      0,
		volatileLeader: make(map[int]int),
	}
}

func (ck *Clerk) SendRequest(Command *OperationCommand) string {
	Command.ClerkId, Command.RequestId = ck.ClerkId, ck.RequestId

	for {
		shard := key2shard(Command.Key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok { // 若 gid 在当前 config 中
			if _, ok := ck.volatileLeader[gid]; !ok {
				ck.volatileLeader[gid] = 0
			}

			guard := ck.volatileLeader[gid]
			leader := guard

			for {
				reply := new(Reply)
				srv := ck.make_end(servers[leader])
				ok := srv.Call("ShardKV.HandleRequest", Command, reply)
				DPrintf("[client] %s %s, %s -> %d send to %s and get reply %+v", Command.Op, Command.Key, Command.Value, shard, servers[leader], reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) { // OK
					ck.volatileLeader[gid] = leader
					ck.RequestId++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup { // arrive but WrongGroup
					break
				} else { // not arrive / arrive but WrongLeader
					leader = (leader + 1) % len(ck.config.Groups[gid])
					if leader == guard { // all servers failed
						break
					}
					continue
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.SendRequest(&OperationCommand{
		Key: key,
		Op:  "Get",
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.SendRequest(&OperationCommand{
		Key:   key,
		Value: value,
		Op:    "Put",
	})
}
func (ck *Clerk) Append(key string, value string) {
	ck.SendRequest(&OperationCommand{
		Key:   key,
		Value: value,
		Op:    "Append",
	})
}
