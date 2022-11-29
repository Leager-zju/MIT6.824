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

const Debug = false

const ExecutionTimeOut = 500 * time.Millisecond

const NewConfigQueryTimeOut = 100 * time.Millisecond

const ShardPullerTimeOut = 100 * time.Millisecond

const GarbageCollectorTimeOut = 100 * time.Millisecond

const EmptyEntryDetectorTimeOut = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu       sync.RWMutex
	me       int
	mck      *shardctrler.Clerk
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd

	maxraftstate int // snapshot if log grows this big
	lastapplied  int

	Shards          [shardctrler.NShards]Shard
	lastRequestInfo map[int64]*RequestInfo

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	dead int32
}

func (kv *ShardKV) MakeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Shards)
	e.Encode(kv.lastRequestInfo)
	e.Encode(kv.lastapplied)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	data := w.Bytes()
	DPrintf("[%d %d] Make Snapshot, Shard: %+v", kv.gid, kv.me, kv.Shards)
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) ApplySnapshot(index, term int, data []byte) {
	if data == nil || len(data) < 1 || (index != -1 && index <= kv.lastapplied) {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards [shardctrler.NShards]Shard
	if d.Decode(&shards) != nil || d.Decode(&kv.lastRequestInfo) != nil || d.Decode(&kv.lastapplied) != nil || d.Decode(&kv.lastConfig) != nil || d.Decode(&kv.currentConfig) != nil {
		log.Fatalf("ApplySnapshot Decode Error\n")
	}
	for sid, shard := range shards {
		for k, v := range shard.KVs {
			kv.Shards[sid].KVs[k] = v
		}
		kv.Shards[sid].ShardStatus = shard.ShardStatus
	}
	DPrintf("[%d %d] Apply Snapshot (%d, %d) Shards %+v", kv.gid, kv.me, index, term, kv.Shards)
}

// raftstate 是否超过阈值，需要执行快照
func (kv *ShardKV) NeedSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

// 当前命令是否已执行
func (kv *ShardKV) isDuplicated(RequestID int, ClerkID int64) bool {
	lastRequestInfo, ok := kv.lastRequestInfo[ClerkID]
	return ok && lastRequestInfo.RequestID >= RequestID
}

// 当前配置分配给 kv 该分片
func (kv *ShardKV) OwnShard(shardId int) bool {
	// locked
	return kv.currentConfig.Shards[shardId] == kv.gid
}

// 是否需要向其它 group 拉取
func (kv *ShardKV) NeedPull(shardId int) bool {
	// locked
	return kv.OwnShard(shardId) && kv.Shards[shardId].ShardStatus == NeedPull
}

// 分片数据是否能被客户端访问
func (kv *ShardKV) ReadyForServer(shardId int) bool {
	// locked
	return kv.OwnShard(shardId) && (kv.Shards[shardId].ShardStatus == Ready || kv.Shards[shardId].ShardStatus == ReadyButNeedSendGC)
}

// 是否满足拉取 config 条件
func (kv *ShardKV) ReadyForConfigPuller(shardId int) bool {
	// locked
	return kv.Shards[shardId].ShardStatus == Ready
}

func (kv *ShardKV) ReadyButNeedSendGC(shardId int) bool {
	// locked
	return kv.Shards[shardId].ShardStatus == ReadyButNeedSendGC
}

func (kv *ShardKV) HandleRequest(args *OperationCommand, reply *Reply) {
	kv.mu.Lock()
	if args.Op != "Get" && kv.isDuplicated(args.RequestId, args.ClerkId) {
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
		DPrintf("[%d %d] %s (%s, %s) timeout", kv.gid, kv.me, args.Op, args.Key, args.Value)
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		DPrintf("[%d %d] %s (%s, %s) success, get reply {%+v}", kv.gid, kv.me, args.Op, args.Key, args.Value, reply)
	}
}

func (kv *ShardKV) Applier() {
	// goroutine
	for !kv.killed() {
		for msg := range kv.applyCh {
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex > kv.lastapplied {
					kv.lastapplied = msg.CommandIndex
					DPrintf("[%d %d] Get Command %+v", kv.gid, kv.me, msg.Command)

					switch msg.Command.(type) {
					case OperationCommand:
						kv.ApplyCommand(msg)
					case ConfigCommand:
						kv.ApplyUpdateConfigCommand(msg)
					case ShardCommand:
						kv.ApplyShardCommand(msg)
					case EmptyCommand:
						kv.ApplyEmptyCommand()
					default:
						panic("Undefined Command Type!")
					}

					if kv.NeedSnapshot() {
						kv.MakeSnapshot(msg.CommandIndex)
					}
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				kv.ApplySnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) ApplyCommand(msg raft.ApplyMsg) {
	// locked
	command := msg.Command.(OperationCommand)
	ch := command.Ch
	reply := new(Reply)

	shardId := key2shard(command.Key)

	if !kv.ReadyForServer(shardId) {
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

		DPrintf("[%d %d] %s (%s, %s) success and the value become %s", kv.gid, kv.me, command.Op, command.Key, command.Value, kv.Shards[shardId].KVs[command.Key])

		kv.lastRequestInfo[command.ClerkId] = &RequestInfo{
			RequestID: command.RequestId,
			Err:       reply.Err,
		}
	}

	DPrintf("[%d %d] Apply Command #%d %s (%s, %s) and reply %+v", kv.gid, kv.me, msg.CommandIndex, command.Op, command.Key, command.Value, reply)
	if kv.rf.GetRaftState() == raft.Leader && kv.rf.GetCurrentTerm() == msg.CommandTerm {
		go func(reply_ *Reply) {
			ch <- reply_
		}(reply)
	}
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
	labgob.Register(OperationCommand{})
	labgob.Register(ConfigCommand{})
	labgob.Register(ShardCommand{})
	labgob.Register(EmptyCommand{})

	kv := &ShardKV{
		me:              me,
		mck:             shardctrler.MakeClerk(ctrlers),
		make_end:        make_end,
		gid:             gid,
		ctrlers:         ctrlers,
		maxraftstate:    maxraftstate,
		lastapplied:     0,
		lastRequestInfo: make(map[int64]*RequestInfo),
		lastConfig:      shardctrler.Config{Num: 0},
		currentConfig:   shardctrler.Config{Num: 0},
		dead:            0,
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.GroupId = kv.gid

	for i := range kv.Shards {
		kv.Shards[i].KVs = make(map[string]string)
	}
	kv.ApplySnapshot(-1, -1, persister.ReadSnapshot())

	go kv.Applier()
	go kv.ConfigPuller()
	go kv.ShardPuller()
	go kv.GarbageCollector()
	go kv.EmptyEntryDetector()

	return kv
}
