package shardctrler

import (
	"log"
	"sort"
	"sync"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft

	lastRequestInfo map[int64]int // clerkID -> requestID
	// Your data here.

	configs []Config // indexed by config num
}

const (
	Join int = iota
	Leave
	Move
	Query
)

// const Debug = false
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) isDuplicated(ClerkID int64, RequestID int) bool {
	lastRequest, ok := sc.lastRequestInfo[ClerkID]
	return ok && lastRequest >= RequestID
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
	// DPrintf("new config before shuffle: %v", config)
	// defer DPrintf("shuffle result: %v", config)

	N := len(config.Groups)

	if N == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	gids := make([]int, 0)
	shardPerGroup := NShards / N
	index := 0

	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, gid := range gids {
		for i := 0; i < shardPerGroup; i++ {
			config.Shards[index] = gid
			index++
		}
	}
	for _, gid := range gids {
		if index >= NShards {
			break
		}
		config.Shards[index] = gid
		index++
	}
}

func (sc *ShardCtrler) HandleRequest(args *Args, reply *Reply) {
	sc.mu.Lock()

	if args.Op != Query && sc.isDuplicated(args.ClerkID, args.RequestID) {
		reply.Err = ErrDuplicated
		sc.mu.Unlock()
		return
	}

	args.ch = make(chan *Reply)
	_, _, isleader := sc.Raft().Start(*args)

	if !isleader {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	ch := args.ch
	sc.mu.Unlock()

	result := <-ch
	reply.Err, reply.Config = result.Err, result.Config
}

func (sc *ShardCtrler) Worker() {
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
	reply := new(Reply)
	ch := args.ch

	if args.Op == Query {
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	} else if sc.isDuplicated(args.ClerkID, args.RequestID) {
		reply.Err = ErrDuplicated
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
		sc.configs = append(sc.configs, *newconfig)
		sc.lastRequestInfo[args.ClerkID] = args.RequestID
	}

	if sc.rf.GetRaftState() == raft.Leader && sc.rf.GetCurrentTerm() == msg.CommandTerm {
		go func() { ch <- reply }()
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastRequestInfo = make(map[int64]int)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Args{})

	// Your code here.
	go sc.Worker()

	return sc
}
