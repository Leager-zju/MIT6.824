package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"6.824/src/labrpc"
)

type Clerk struct {
	mu             sync.Mutex
	servers        []*labrpc.ClientEnd
	volatileLeader int
	// You will have to modify this struct.
	RequestId uint32
	ClerkId   uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.volatileLeader = 0
	ck.RequestId = 0
	ck.ClerkId = uint32(nrand())

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &GetArgs{
		Key: key,
	}

	DPrintf("[%d] client new GET %s\n", ck.ClerkId%1000, key)

	for {
		reply := &GetReply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.Get", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			return reply.Value
		}
		ck.volatileLeader = (ck.volatileLeader + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	DPrintf("[%d] client new %s KEY: '%s' VALUE: '%s' REQUESTID: %d\n", ck.ClerkId%1000, op, key, value, ck.RequestId)

	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestId: atomic.LoadUint32(&ck.RequestId),
		ClerkId:   ck.ClerkId,
	}

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			atomic.AddUint32(&ck.RequestId, 1)
			return
		}
		ck.volatileLeader = (ck.volatileLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
