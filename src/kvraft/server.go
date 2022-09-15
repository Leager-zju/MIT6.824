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

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) GetChannel(index int) chan string {
	if _, ok := kv.applierCh[index]; !ok {
		kv.applierCh[index] = make(chan string, 1)
	}

	return kv.applierCh[index]
}

func (kv *KVServer) MakeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	keys := make([]string, 0)
	vals := make([]string, 0)
	for k, v := range kv.KVs {
		keys = append(keys, k)
		vals = append(vals, v)
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.baseIndex)
	e.Encode(keys)
	e.Encode(vals)
	data := w.Bytes()
	return data
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	dead int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	baseIndex    int

	KVs         map[string]string
	lastapplied map[uint32]uint32
	applierCh   map[int]chan string
}

// A Get for a non-existent key should return an empty string.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	reply.Err = OK
	idx, _, isleader := kv.rf.Start(*args)

	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := kv.GetChannel(idx)
	kv.mu.Unlock()

	select {
	case <-time.After(ExecutionTimeOut):
		DPrintf("[%d] %v timeout\n", kv.me, args)
		reply.Err = ErrWrongLeader
	case val := <-ch:
		DPrintf("[%d] %v success ---> %s\n", kv.me, args, val)
		reply.Value = val
	}
}

// An Append to a non-existent key should act like Put.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	reply.Err = OK
	idx, _, isleader := kv.rf.Start(*args)

	if !isleader {
		DPrintf("[%d] %v wrongLeader\n", kv.me, args)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.GetChannel(idx)
	kv.mu.Unlock()

	select {
	case <-time.After(ExecutionTimeOut):
		DPrintf("[%d] %v timeout\n", kv.me, args)
		reply.Err = ErrWrongLeader
	case err := <-ch:
		DPrintf("[%d] %v success\n", kv.me, args)
		reply.Err = Err(err)
	}
}

func (kv *KVServer) Worker() {
	for !kv.killed() {
		msg := <-kv.applyCh
		raftSize := kv.persister.RaftStateSize()
		// log.Printf("BaseIndex is %d and MaxRaftState is %d\n", kv.baseIndex, kv.maxraftstate)
		if kv.maxraftstate != -1 && raftSize-kv.baseIndex >= kv.maxraftstate {
			kv.baseIndex = raftSize
			log.Printf("[%d] Snapshot at %d", kv.me, kv.baseIndex)
			go kv.rf.Snapshot(msg.CommandIndex, kv.MakeSnapshot())
		}
		if msg.CommandValid {
			kv.ApplyCommand(msg)
		} else if msg.SnapshotValid {
			kv.ApplySnapshot()
		}
	}
}

func (kv *KVServer) ApplyCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	index := msg.CommandIndex

	switch c := msg.Command.(type) {
	case GetArgs:
		if _, isleader := kv.rf.GetState(); isleader {
			ch := kv.GetChannel(index)
			if val, ok := kv.KVs[c.Key]; ok {
				ch <- val
			} else {
				ch <- ""
			}
		}
	case PutAppendArgs:
		// If duplicated, return immediately
		if ReqID, ok := kv.lastapplied[c.ClerkId]; ok && c.RequestId <= ReqID {
			ch := kv.GetChannel(index)
			DPrintf("[%v] is duplicated %d %d\n", c, c.RequestId, ReqID)
			ch <- string(ErrDuplicate)
			return
		}

		kv.lastapplied[c.ClerkId] = c.RequestId
		if val, ok := kv.KVs[c.Key]; ok && c.Op == "Append" {
			kv.KVs[c.Key] = val + c.Value
		} else {
			kv.KVs[c.Key] = c.Value
		}

		if _, isleader := kv.rf.GetState(); isleader {
			ch := kv.GetChannel(index)
			ch <- string(OK)
		}
	}
}

func (kv *KVServer) ApplySnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var keys []string
	var vals []string

	if d.Decode(&kv.baseIndex) != nil || d.Decode(&keys) != nil || d.Decode(&vals) != nil {
		log.Fatalf("ApplySnapshot Decode Error\n")
	} else {
		for i, k := range keys {
			v := vals[i]
			kv.KVs[k] = v
		}
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
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := &KVServer{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		persister:    persister,
		maxraftstate: maxraftstate,
		baseIndex:    0,
		KVs:          make(map[string]string),
		lastapplied:  make(map[uint32]uint32),
		applierCh:    make(map[int]chan string),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ApplySnapshot()

	go kv.Worker()

	return kv
}
