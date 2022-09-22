package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

// const Debug = true

const Debug = false

const ExecutionTimeOut = 500 * time.Millisecond

type ReplyContext struct {
	RequestID uint32
	Err       Err
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KVs)
	e.Encode(kv.lastReplyContext)
	e.Encode(kv.lastapplied)
	data := w.Bytes()
	return data
}

func (kv *KVServer) ApplySnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if d.Decode(&kv.KVs) != nil || d.Decode(&kv.lastReplyContext) != nil || d.Decode(&kv.lastapplied) != nil {
		log.Fatalf("ApplySnapshot Decode Error\n")
	}
}

func (kv *KVServer) NeedSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

type KVServer struct {
	mu        sync.RWMutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	dead int32

	maxraftstate int // snapshot if log grows this big
	lastapplied  int

	KVs              map[string]string
	lastReplyContext map[uint32]*ReplyContext // clerkID -> {RequestID, reply} (Put or Append)

	snapshotCond *sync.Cond
}

func (kv *KVServer) isDuplicated(RequestID, ClerkID uint32) bool {
	replyContext, ok := kv.lastReplyContext[ClerkID]
	return ok && replyContext.RequestID >= RequestID
}

func (kv *KVServer) HandleRequest(args *Args, reply *Reply) {
	kv.mu.Lock()
	if args.Op != "Get" && kv.isDuplicated(args.RequestId, args.ClerkId) {
		reply.Err = kv.lastReplyContext[args.ClerkId].Err
		kv.mu.Unlock()
		return
	}
	args.Ch = make(chan *Reply)
	idx, _, isleader := kv.rf.Start(*args)

	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf("[%d] client %d new Request %+v at idx %d", kv.me, args.ClerkId%1000, args, idx)
	ch := args.Ch
	kv.mu.Unlock()

	select {
	case <-time.After(ExecutionTimeOut):
		reply.Err = ErrWrongLeader
		DPrintf("[%d] %+v timeout", kv.me, args)
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		DPrintf("[%d] %+v success, get reply {%+v}", kv.me, args, reply)
	}

}

func (kv *KVServer) Applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex > kv.lastapplied {
				kv.ApplyCommand(msg)
				kv.lastapplied = msg.CommandIndex
				if kv.NeedSnapshot() {
					DPrintf("[%d] snapshot", kv.me)
					data := kv.MakeSnapshot()
					go kv.rf.Snapshot(msg.CommandIndex, data)
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.ApplySnapshot(msg.Snapshot)
		}
	}
}

func (kv *KVServer) ApplyCommand(msg raft.ApplyMsg) {
	command := msg.Command.(Args)
	ch := command.Ch
	reply := new(Reply)

	if command.Op == "Get" {
		val, ok := kv.KVs[command.Key]
		if ok {
			reply.Value, reply.Err = val, OK
		} else {
			reply.Value, reply.Err = "", ErrNoKey
		}
	} else if kv.isDuplicated(command.RequestId, command.ClerkId) {
		reply.Err = kv.lastReplyContext[command.ClerkId].Err
	} else {
		if command.Op == "Put" {
			kv.KVs[command.Key] = command.Value
			reply.Err = OK
		} else if command.Op == "Append" {
			_, ok := kv.KVs[command.Key]
			if ok {
				kv.KVs[command.Key] += command.Value
				reply.Err = OK
			} else {
				kv.KVs[command.Key] = command.Value
				reply.Err = ErrNoKey
			}
		} else {
			log.Fatalf("command.op error!")
		}

		kv.lastReplyContext[command.ClerkId] = &ReplyContext{
			RequestID: command.RequestId,
			Err:       reply.Err,
		}
	}

	DPrintf("[%d] try to ApplyCommand{%+v} and Get reply {%+v}", kv.me, msg, reply)

	if kv.rf.GetRaftState() == raft.Leader && kv.rf.GetCurrentTerm() == msg.CommandTerm {
		go func(reply_ *Reply) { ch <- reply_ }(reply)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Killed() instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Args{})

	kv := &KVServer{
		me:               me,
		applyCh:          make(chan raft.ApplyMsg),
		persister:        persister,
		maxraftstate:     maxraftstate,
		KVs:              make(map[string]string),
		lastReplyContext: make(map[uint32]*ReplyContext),
		snapshotCond:     sync.NewCond(new(sync.Mutex)),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ApplySnapshot(kv.persister.ReadSnapshot())

	go kv.Applier()

	return kv
}
