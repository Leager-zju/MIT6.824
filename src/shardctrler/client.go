package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/src/labrpc"
)

type Clerk struct {
	servers        []*labrpc.ClientEnd
	ClerkID        int64
	RequestID      int
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
		ClerkID:        nrand(),
		RequestID:      0,
		volatileLeader: 0,
	}
}

func (ck *Clerk) SendRequest(args *Args) Config {
	args.ClerkID, args.RequestID = ck.ClerkID, ck.RequestID
	for {
		reply := &Reply{}
		ok := ck.servers[ck.volatileLeader].Call("ShardCtrler.HandleRequest", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.RequestID++
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
		ck.volatileLeader = (ck.volatileLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.SendRequest(&Args{
		Op:  Query,
		Num: num,
	})
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
