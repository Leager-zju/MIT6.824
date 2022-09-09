package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"6.824/src/labrpc"
)

const null int = -1

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
	ck.volatileLeader = null
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

	if ck.volatileLeader != null {
		args := &GetArgs{
			Key: key,
		}
		reply := &GetReply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.Get", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			return reply.Value
		}
	}

	for {
		for i := range ck.servers {
			args := &GetArgs{
				Key: key,
			}
			reply := &GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", args, reply)
			if ok && reply.Err != ErrWrongLeader {
				ck.volatileLeader = i
				return reply.Value
			}
		}
	}

	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	if ck.volatileLeader != null {
		args := &PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			Info: Info{
				RequestId: ck.RequestId,
				ClerkId:   ck.ClerkId,
			},
		}
		reply := &PutAppendReply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err != ErrWrongLeader {
			if reply.Err != ErrDuplicate {
				atomic.AddUint32(&ck.RequestId, 1)
			}
			return
		}
	}

	for {
		total := 0
		success := false
		for i := range ck.servers {
			go func(server int) {
				args := &PutAppendArgs{
					Key:   key,
					Value: value,
					Op:    op,
				}
				reply := &PutAppendReply{}
				ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
				if ok && reply.Err != ErrWrongLeader {
					ck.volatileLeader = server
					mu.Lock()
					success = true
					mu.Unlock()

					if reply.Err != ErrDuplicate {
						atomic.AddUint32(&ck.RequestId, 1)
					}
				}
				mu.Lock()
				total++
				mu.Unlock()
				cond.Signal()
			}(i)
		}
		mu.Lock()
		for !success && total < len(ck.servers) {
			cond.Wait()
		}
		mu.Unlock()
		if success {
			return
		}
	}

	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
