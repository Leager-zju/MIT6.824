package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

// const Debug = false

const ExecutionTimeOut = 500 * time.Millisecond

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
	e.Encode(kv.lastRequestInfo)
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

	if d.Decode(&kv.KVs) != nil || d.Decode(&kv.lastRequestInfo) != nil || d.Decode(&kv.lastapplied) != nil {
		log.Fatalf("ApplySnapshot Decode Error\n")
	}
}

func (kv *KVServer) NeedSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

type RequestInfo struct {
	RequestID uint32
	Err       Err
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

	KVs             map[string]string
	lastRequestInfo map[uint32]*RequestInfo // clerkID -> {RequestID, Err} (Put or Append)
}

func (kv *KVServer) isDuplicated(RequestID, ClerkID uint32) bool {
	lastRequestInfo, ok := kv.lastRequestInfo[ClerkID]
	return ok && lastRequestInfo.RequestID >= RequestID
}

func (kv *KVServer) HandleRequest(args *Args, reply *Reply) {
	kv.mu.Lock()
	if args.Op != "Get" && kv.isDuplicated(args.RequestId, args.ClerkId) {
		reply.Err = kv.lastRequestInfo[args.ClerkId].Err
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
		reply.Err = kv.lastRequestInfo[command.ClerkId].Err
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

		kv.lastRequestInfo[command.ClerkId] = &RequestInfo{
			RequestID: command.RequestId,
			Err:       reply.Err,
		}
	}

	DPrintf("[%d] try to ApplyCommand{%+v} and Get reply {%+v}", kv.me, msg, reply)

	if kv.rf.GetRaftState() == raft.Leader && kv.rf.GetCurrentTerm() == msg.CommandTerm {
		go func(reply_ *Reply) { ch <- reply_ }(reply)
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want Go's RPC library to marshall/unmarshall.
	labgob.Register(Args{})

	kv := &KVServer{
		me:              me,
		applyCh:         make(chan raft.ApplyMsg),
		persister:       persister,
		maxraftstate:    maxraftstate,
		KVs:             make(map[string]string),
		lastRequestInfo: make(map[uint32]*RequestInfo),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ApplySnapshot(kv.persister.ReadSnapshot())

	go kv.Applier()

	return kv
}
