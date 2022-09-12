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
	args := &GetArgs{
		Key: key,
	}

	// fmt.Printf("[%d]    Get Key: {%v}\n", ck.ClerkId%1000, key)

	for ck.volatileLeader != null {
		reply := &GetReply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.Get", args, reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				return reply.Value
			} else {
				ck.volatileLeader = null
			}
		}
	}

	var success uint32 = 0
	ch := make(chan string, 1)

	for i := range ck.servers {
		go func(server int) {
			for {
				if atomic.LoadUint32(&success) == 1 {
					return
				}
				reply := &GetReply{}
				ok := ck.servers[server].Call("KVServer.Get", args, reply)
				if ok {
					if reply.Err != ErrWrongLeader {
						ck.volatileLeader = server
						atomic.StoreUint32(&success, 1)
						ch <- reply.Value
						return
					}
				}
			}
		}(i)
	}

	return <-ch
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// fmt.Printf("[%d, %d]    %v Key: {%v} Value:{%v}\n", ck.ClerkId%1000, ck.RequestId, op, key, value)
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestId: atomic.LoadUint32(&ck.RequestId),
		ClerkId:   ck.ClerkId,
	}

	for ck.volatileLeader != null {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.volatileLeader].Call("KVServer.PutAppend", args, reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				// fmt.Println(ck.ClerkId%1000, "P/A at", reply.ServerID)
				atomic.AddUint32(&ck.RequestId, 1)
				return
			} else {
				ck.volatileLeader = null
			}
		}
	}

	var success uint32 = 0
	ch := make(chan interface{}, 1)
	for i := range ck.servers {
		go func(server int) {
			for {
				if atomic.LoadUint32(&success) == 1 {
					return
				}
				reply := &PutAppendReply{}
				ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
				if ok && reply.Err != ErrWrongLeader {
					ck.volatileLeader = server
					// fmt.Println(ck.ClerkId%1000, "P/A at", reply.ServerID)
					atomic.StoreUint32(&success, 1)
					atomic.AddUint32(&ck.RequestId, 1)
					ch <- 0
					return
				}
			}
		}(i)
	}
	<-ch
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
