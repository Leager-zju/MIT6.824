package shardctrler

import (
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

const ExecutionTimeOut = 500 * time.Millisecond

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

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft

	lastRequestInfo map[int64]*RequestInfo // clerkID -> requestID

	configs []Config // indexed by config num
}

const (
	Join int = iota
	Leave
	Move
	Query
)

func (sc *ShardCtrler) isDuplicated(RequestID int, ClerkID int64) bool {
	lastRequestInfo, ok := sc.lastRequestInfo[ClerkID]
	return ok && lastRequestInfo.RequestID >= RequestID
}

func (sc *ShardCtrler) makeNewConfig() *Config {
	lastconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{
		Num:    lastconfig.Num + 1,
		Shards: lastconfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, members := range lastconfig.Groups {
		newconfig.Groups[gid] = members
	}
	return &newconfig
}

func (sc *ShardCtrler) shuffleShard(config *Config) {
	N := len(config.Groups)
	if N == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	allocated := 0
	NumofShards := make(map[int]int)
	for gid := range config.Groups {
		NumofShards[gid] = 0
	}
	for _, gid := range config.Shards {
		if _, ok := config.Groups[gid]; ok {
			allocated++
			NumofShards[gid]++
		}
	}

	for {
		WhoHasTheMostShards, WhoHasTheLeastShards := -1, -1
		MaxmShards, MinmShards := -1, NShards+1

		for gid, num := range NumofShards {
			if num > MaxmShards {
				MaxmShards = num
				WhoHasTheMostShards = gid
			}
			if num < MinmShards {
				MinmShards = num
				WhoHasTheLeastShards = gid
			}
		}
		if allocated == NShards && MaxmShards < MinmShards+2 {
			break
		}

		for sid, gid := range config.Shards {
			if _, ok := config.Groups[gid]; !ok { // gid 已离开，直接分给拥有分片最少的 group
				config.Shards[sid] = WhoHasTheLeastShards
				allocated++
				NumofShards[WhoHasTheLeastShards]++
				break
			} else if gid == WhoHasTheMostShards && allocated == NShards {
				config.Shards[sid] = WhoHasTheLeastShards
				NumofShards[WhoHasTheMostShards]--
				NumofShards[WhoHasTheLeastShards]++
				break
			}
		}
	}

}

func (sc *ShardCtrler) HandleRequest(args *Args, reply *Reply) {
	sc.mu.Lock()

	if args.Op != Query && sc.isDuplicated(args.RequestId, args.ClerkId) {
		reply.Err = sc.lastRequestInfo[args.ClerkId].Err
		sc.mu.Unlock()
		return
	}

	args.Ch = make(chan *Reply)
	_, _, isleader := sc.Raft().Start(*args)

	if !isleader {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	ch := args.Ch
	sc.mu.Unlock()

	select {
	case <-time.After(ExecutionTimeOut):
		reply.Err = ErrWrongLeader
		// DPrintf("[ShardCtrler]: %+v timeout", args)
	case result := <-ch:
		reply.Config, reply.Err = result.Config, result.Err
		// DPrintf("[ShardCtrler]: %+v success, get reply {%+v}", args, reply)
	}
}

func (sc *ShardCtrler) Applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			sc.ApplyCommand(msg)
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) ApplyCommand(msg raft.ApplyMsg) {
	args := msg.Command.(Args)
	ch := args.Ch
	reply := new(Reply)

	if args.Op == Query {
		reply.Err = OK
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	} else if sc.isDuplicated(args.RequestId, args.ClerkId) {
		reply.Err = sc.lastRequestInfo[args.ClerkId].Err
	} else {
		newconfig := sc.makeNewConfig()

		switch args.Op {
		case Move:
			newconfig.Shards[args.Shard] = args.GIDs[0]
		case Join:
			for gid, members := range args.Servers {
				newconfig.Groups[gid] = members
			}
			sc.shuffleShard(newconfig)
		case Leave:
			for _, gid := range args.GIDs {
				delete(newconfig.Groups, gid)
			}
			sc.shuffleShard(newconfig)
		}
		DPrintf("[ShardCtrler]: NewConfig %+v", newconfig)
		sc.configs = append(sc.configs, *newconfig)
		sc.lastRequestInfo[args.ClerkId] = &RequestInfo{
			RequestID: args.RequestId,
			Err:       reply.Err,
		}
	}

	if sc.rf.GetRaftState() == raft.Leader && sc.rf.GetCurrentTerm() == msg.CommandTerm {
		go func(reply_ *Reply) { ch <- reply_ }(reply)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastRequestInfo = make(map[int64]*RequestInfo)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Args{})

	// Your code here.
	go sc.Applier()

	return sc
}
