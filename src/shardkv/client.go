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

// which shard is a key in?
// please use this function,
// and please do not change it.
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
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	return &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		ClerkId:   nrand(),
		RequestId: 0,
	}
}

func (ck *Clerk) SendRequest(args *Args) string {
	args.ClerkId, args.RequestId = ck.ClerkId, ck.RequestId

	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		DPrintf("key %s -> shard %d :%d %d %d", args.Key, shard, gid, args.ClerkId, args.RequestId)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				reply := new(Reply)
				srv := ck.make_end(servers[si])
				ok := srv.Call("ShardKV.HandleRequest", args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.RequestId++
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.SendRequest(&Args{
		Key: key,
		Op:  "Get",
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.SendRequest(&Args{
		Key:   key,
		Value: value,
		Op:    "Put",
	})
}
func (ck *Clerk) Append(key string, value string) {
	ck.SendRequest(&Args{
		Key:   key,
		Value: value,
		Op:    "Append",
	})
}
