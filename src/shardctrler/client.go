package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers        []*labrpc.ClientEnd
	ClerkId        int64
	RequestId      int
	volatileLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:        servers,
		ClerkId:        nrand(),
		RequestId:      0,
		volatileLeader: 0,
	}
}

func (ck *Clerk) SendRequest(args *Args) Config {
	args.ClerkId, args.RequestId = ck.ClerkId, ck.RequestId
	for {
		reply := &Reply{}
		ok := ck.servers[ck.volatileLeader].Call("ShardCtrler.HandleRequest", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.RequestId++
			return reply.Config
		}
		ck.volatileLeader = (ck.volatileLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.SendRequest(&Args{
		Op:      Join,
		Servers: servers,
	})
}

func (ck *Clerk) Leave(gids []int) {
	ck.SendRequest(&Args{
		Op:   Leave,
		GIDs: gids,
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.SendRequest(&Args{
		Op:    Move,
		Shard: shard,
		GIDs:  []int{gid},
	})
}

func (ck *Clerk) Query(num int) Config {
	return ck.SendRequest(&Args{
		Op:  Query,
		Num: num,
	})
}
