package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"

	"6.824/labgob"
	"6.824/raft"
)

// const Debug = false

const Debug = true

const ExecutionTimeOut = 500 * time.Millisecond

const NewConfigQueryTimeOut = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type RequestInfo struct {
	RequestID int
	Err       Err
}

type ShardKV struct {
	mu        sync.RWMutex
	me        int
	mck       *shardctrler.Clerk
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	make_end  func(string) *labrpc.ClientEnd
	gid       int
	ctrlers   []*labrpc.ClientEnd
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big
	lastapplied  int

	Shards          [shardctrler.NShards]Shard
	lastRequestInfo map[int64]*RequestInfo

	// 对这俩的修改要加锁(?)
	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config // currentConfig.Num 其实就相当于 raft 层的 Term

	CP_Cond *sync.Cond
	SP_Cond *sync.Cond
	GC_Cond *sync.Cond

	dead int32

	RequestId  int
	CtrlLeader int
}

func (kv *ShardKV) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Shards)
	e.Encode(kv.lastRequestInfo)
	e.Encode(kv.lastapplied)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) ApplySnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if d.Decode(&kv.Shards) != nil || d.Decode(&kv.lastRequestInfo) != nil || d.Decode(&kv.lastapplied) != nil || d.Decode(&kv.lastConfig) != nil || d.Decode(&kv.currentConfig) != nil {
		log.Fatalf("ApplySnapshot Decode Error\n")
	}
}

func (kv *ShardKV) NeedSnapshot() bool { // raftstate 是否超过阈值，需要执行快照
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *ShardKV) isDuplicated(RequestID int, ClerkID int64) bool { // 当前命令是否已执行
	lastRequestInfo, ok := kv.lastRequestInfo[ClerkID]
	return ok && lastRequestInfo.RequestID >= RequestID
}

func (kv *ShardKV) HandleRequest(args *Args, reply *Reply) {
	kv.mu.Lock()
	if args.Op != "Get" && kv.isDuplicated(args.RequestId, args.ClerkId) {
		DPrintf("args %+v duplicated", args)
		reply.Err = kv.lastRequestInfo[args.ClerkId].Err
		kv.mu.Unlock()
		return
	}

	args.Ch = make(chan *Reply)
	_, _, isleader := kv.rf.Start(*args)

	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := args.Ch
	kv.mu.Unlock()

	select {
	case <-time.After(ExecutionTimeOut):
		reply.Err = ErrWrongLeader
		DPrintf("[%d %d] %+v timeout", kv.gid, kv.me, args)
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		DPrintf("[%d %d] %+v success, get reply {%+v}", kv.gid, kv.me, args, reply)
	}
}

func (kv *ShardKV) Applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex > kv.lastapplied {
				DPrintf("[%d %d] Get Command ---- %+v", kv.gid, kv.me, msg)

				command := msg.Command.(Args)

				switch command.Op {
				case "ApplyConfig":
					kv.ApplyConfig(command.Data.(ConfigInfo))
				case "InsertShard":
					kv.InsertShard(command.Data.(ShardInfo))
				// case "DeleteShard":
				default:
					kv.ApplyCommand(msg)
				}

				kv.lastapplied = msg.CommandIndex
				if kv.NeedSnapshot() {
					DPrintf("[%d %d] Snapshot", kv.gid, kv.me)
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

func (kv *ShardKV) ApplyCommand(msg raft.ApplyMsg) {
	command := msg.Command.(Args)
	ch := command.Ch
	reply := new(Reply)

	shardId := key2shard(command.Key)

	if !kv.OwnShardAndValid(shardId) {
		reply.Err = ErrWrongGroup
	} else if command.Op == "Get" {
		val, ok := kv.Shards[shardId].KVs[command.Key]
		if ok {
			reply.Value, reply.Err = val, OK
		} else {
			reply.Value, reply.Err = "", ErrNoKey
		}
	} else if kv.isDuplicated(command.RequestId, command.ClerkId) {
		reply.Err = kv.lastRequestInfo[command.ClerkId].Err
	} else {
		if command.Op == "Put" {
			kv.Shards[shardId].KVs[command.Key] = command.Value
			reply.Err = OK
		} else if command.Op == "Append" {
			_, ok := kv.Shards[shardId].KVs[command.Key]
			if ok {
				kv.Shards[shardId].KVs[command.Key] += command.Value
				reply.Err = OK
			} else {
				kv.Shards[shardId].KVs[command.Key] = command.Value
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

	DPrintf("[%d %d] try to ApplyCommand{%+v} and Get reply {%+v}", kv.gid, kv.me, msg, reply)
	DPrintf("[%d %d] KVs: %+v", kv.gid, kv.me, kv.Shards[shardId].KVs)

	if kv.rf.GetRaftState() == raft.Leader && kv.rf.GetCurrentTerm() == msg.CommandTerm {
		go func(reply_ *Reply) { ch <- reply_ }(reply)
	}
}

func (kv *ShardKV) GarbageCollector() {
	// TODO
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Args{})
	labgob.Register(ConfigInfo{})
	labgob.Register(ShardInfo{})

	kv := &ShardKV{
		me:              me,
		mck:             shardctrler.MakeClerk(ctrlers),
		make_end:        make_end,
		gid:             gid,
		ctrlers:         ctrlers,
		persister:       persister,
		maxraftstate:    maxraftstate,
		lastapplied:     0,
		lastRequestInfo: make(map[int64]*RequestInfo),
		lastConfig:      shardctrler.Config{Num: 0},
		currentConfig:   shardctrler.Config{Num: 0},
		dead:            0,
		RequestId:       0,
		CtrlLeader:      0,
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ApplySnapshot(kv.persister.ReadSnapshot())

	for i := range kv.Shards {
		kv.Shards[i].KVs = make(map[string]string)
	}

	kv.CP_Cond = sync.NewCond(&sync.Mutex{})
	kv.SP_Cond = sync.NewCond(&sync.Mutex{})
	kv.GC_Cond = sync.NewCond(&sync.Mutex{})

	go kv.Applier()
	go kv.ConfigPuller()
	go kv.ShardPuller()
	go kv.GarbageCollector()

	return kv
}
