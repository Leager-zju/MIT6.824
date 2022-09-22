package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/src/labrpc"
)

type Clerk struct {
	mu             sync.Mutex
	servers        []*labrpc.ClientEnd
	volatileLeader int
	RequestId      uint32
	ClerkId        uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:        servers,
		volatileLeader: 0,
		RequestId:      0,
		ClerkId:        uint32(nrand()),
	}

	return ck
}

func (ck *Clerk) SendRequest(args *Args) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args.RequestId, args.ClerkId = ck.RequestId, ck.ClerkId

	for {
		reply := &Reply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.HandleRequest", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.RequestId++
			return reply.Value
		}
		ck.volatileLeader = (ck.volatileLeader + 1) % len(ck.servers)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
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
