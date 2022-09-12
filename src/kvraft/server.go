package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Void struct{}

var void Void

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string

	Feedback bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	KVs         map[string]string
	lastapplied map[uint32]uint32

	clientCh sync.Map
	// Your definitions here.
}

// A Get for a non-existent key should return an empty string.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	idx, _, isleader := kv.rf.Start(Op{Operation: "Get", Key: args.Key, Feedback: true})

	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		ch, ok := kv.clientCh.Load(idx)
		if ok {
			reply.Value = (<-ch.(chan interface{})).(string)
			break
		}
	}

}

// An Append to a non-existent key should act like Put.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	reply.ServerID = kv.me

	if ReqID, ok := kv.lastapplied[args.ClerkId]; ok {
		if args.RequestId <= ReqID {
			reply.Err = ErrDuplicate
			return
		}
	}

	idx, _, isleader := kv.rf.Start(Op{Operation: args.Op, Key: args.Key, Value: args.Value, Feedback: true})

	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		_, ok := kv.clientCh.Load(idx)
		if ok {
			break
		}
	}

	kv.lastapplied[args.ClerkId] = args.RequestId
	// fmt.Printf("[%d] P/A Key: %v, Value: %v at %d\n", kv.me, args.Key, args.Value, idx)
}

func (kv *KVServer) Worker() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.Apply(msg.CommandIndex, msg.Command)
		}
	}
}

func (kv *KVServer) Apply(index int, cmd interface{}) {
	op := cmd.(Op)
	kv.clientCh.Store(index, make(chan interface{}, 1))
	ch, _ := kv.clientCh.Load(index)

	switch op.Operation {
	case "Get":
		if op.Feedback {
			if val, ok := kv.KVs[op.Key]; ok {
				ch.(chan interface{}) <- val
			} else {
				ch.(chan interface{}) <- ""
			}
		}
	case "Put":
		kv.KVs[op.Key] = op.Value
		if op.Feedback {
			ch.(chan interface{}) <- ""
		}
	case "Append":
		val, ok := kv.KVs[op.Key]
		if ok {
			kv.KVs[op.Key] = val + op.Value
		} else {
			kv.KVs[op.Key] = op.Value
		}

		if op.Feedback {
			ch.(chan interface{}) <- ""
		}
	}

	if op.Feedback {
		// fmt.Printf("[%v] is applied at %d\n", cmd, index)
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
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		maxraftstate: maxraftstate,
		KVs:          make(map[string]string),
		lastapplied:  make(map[uint32]uint32),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Worker()
	// You may need initialization code here.

	return kv
}
