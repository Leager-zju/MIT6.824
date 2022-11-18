package shardkv

import (
	"sync"
	"time"

	"6.824/raft"
)

func (kv *ShardKV) HandleBetweenGroupRequest(args *RPCArgs, reply *RPCReply) {
	// RPC
	if kv.rf.GetRaftState() != raft.Leader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	sid, configNum := args.ShardId, args.ConfigNum

	defer DPrintf("[%d %d] get %+v and reply %+v", kv.gid, kv.me, args, reply)
	if configNum > kv.currentConfig.Num {
		reply.Err = ErrNotReady
		return
	}

	if configNum < kv.currentConfig.Num {
		reply.Err = ErrOldRequest
		return
	}

	switch args.Op {
	case "PullShard":
		kv.Shards[sid].ShardStatus = NeedBeGC

		shard := kv.Shards[sid]
		reply.Shard.KVs = make(map[string]string)
		reply.LastRequestInfo = make(map[int64]RequestInfo)
		for k, v := range shard.KVs {
			reply.Shard.KVs[k] = v
		}
		for k, v := range kv.lastRequestInfo {
			reply.LastRequestInfo[k] = v
		}
	case "GarbageCollect":
		go kv.rf.Start(ShardCommand{
			Op:              "GarbageCollect",
			Shard:           nil,
			Sid:             sid,
			LastRequestInfo: nil,
		})
	}

	reply.ConfigNum = configNum
	reply.Err = OK
}

func (kv *ShardKV) ApplyShardCommand(msg raft.ApplyMsg) {
	Command := msg.Command.(ShardCommand)
	switch Command.Op {
	case "InsertShard":
		kv.InsertShard(Command)
	case "UpdateStatus":
		kv.UpdateShardStatus(Command)
	case "GarbageCollect":
		kv.GarbageCollect(Command)
	default:
		panic("Undefined Command!")
	}
}

func (kv *ShardKV) InsertShard(Command ShardCommand) {
	// locked
	newshard, sid := Command.Shard, Command.Sid
	for k, v := range newshard.KVs {
		kv.Shards[sid].KVs[k] = v
	}
	for clientId, lastrequest := range Command.LastRequestInfo {
		Rinfo, ok := kv.lastRequestInfo[clientId]
		if !ok || Rinfo.RequestID < lastrequest.RequestID {
			kv.lastRequestInfo[clientId] = lastrequest
		}
	}
	kv.Shards[sid].ShardStatus = ReadyButNeedSendGC
	DPrintf("[%d %d] insert %d-th Shard %+v so the Shards become %+v", kv.gid, kv.me, Command.Sid, Command.Shard, kv.Shards)
}

func (kv *ShardKV) UpdateShardStatus(Command ShardCommand) {
	// locked
	sid := Command.Sid
	kv.Shards[sid].ShardStatus = Ready
}

func (kv *ShardKV) GarbageCollect(Command ShardCommand) {
	// locked
	sid := Command.Sid
	kv.Shards[sid].KVs = make(map[string]string)
	kv.Shards[sid].ShardStatus = Ready
}

func (kv *ShardKV) ShardPuller() {
	// goroutine
	for !kv.killed() {
		if kv.rf.GetRaftState() == raft.Leader {
			kv.mu.RLock()
			configNum := kv.currentConfig.Num // 捎带当前 config.num，防止收到过期回复
			var wg sync.WaitGroup
			for sid, gid := range kv.lastConfig.Shards {
				if kv.OwnShard(sid) && kv.Shards[sid].ShardStatus == NeedPull {
					// 向前任持有者拉取
					wg.Add(1)
					others := kv.lastConfig.Groups[gid]
					args := &RPCArgs{
						Op:        "PullShard",
						ShardId:   sid,
						ConfigNum: configNum,
					}
					DPrintf("[%d %d] pull shard %d from %d with args %+v", kv.gid, kv.me, sid, gid, args)

					// Send RPC and Pull Shard
					go func(sid, gid int, others []string) {
						index := 0
						for {
							end := kv.make_end(others[index])
							reply := &RPCReply{}
							ok := end.Call("ShardKV.HandleBetweenGroupRequest", args, reply)
							if ok && (reply.Err == OK || reply.Err == ErrNotReady) {
								DPrintf("[%d %d] pull shard %d from %d and get reply %+v", kv.gid, kv.me, sid, gid, reply)
								// update step-by-step
								if reply.ConfigNum == kv.currentConfig.Num && kv.Shards[sid].ShardStatus == NeedPull {
									kv.rf.Start(ShardCommand{
										Op:              "InsertShard",
										Shard:           &reply.Shard,
										Sid:             sid,
										LastRequestInfo: reply.LastRequestInfo,
									})
								}
								wg.Done()
								return
							} // else: FailSend / ErrWrongLeader / ErrScale
							index = (index + 1) % len(others)
						} // end for
					}(sid, gid, others)
				}
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(NewShardPullerTimeOut)
	}
}

func (kv *ShardKV) GarbageCollector() {
	// locked
	for !kv.killed() {
		if kv.rf.GetRaftState() == raft.Leader {
			kv.mu.RLock()
			configNum := kv.currentConfig.Num // 捎带当前 config.num，防止收到过期回复
			var wg sync.WaitGroup
			for sid, gid := range kv.lastConfig.Shards {
				if kv.ReadyButNeedSendGC(sid) {
					// 向前任持有者发送 GC 请求
					wg.Add(1)
					others := kv.lastConfig.Groups[gid]
					args := &RPCArgs{
						Op:        "GarbageCollect",
						ShardId:   sid,
						ConfigNum: configNum,
					}
					DPrintf("[%d %d] send GC %d to %d with args %+v", kv.gid, kv.me, sid, gid, args)

					// Send RPC and make GC
					go func(sid, gid int, others []string) {
						index := 0
						for {
							end := kv.make_end(others[index])
							reply := &RPCReply{}
							ok := end.Call("ShardKV.HandleBetweenGroupRequest", args, reply)
							if ok && (reply.Err == OK || reply.Err == ErrNotReady) {
								DPrintf("[%d %d] send GC %d to %d and get reply %+v", kv.gid, kv.me, sid, gid, reply)
								if reply.ConfigNum == kv.currentConfig.Num && kv.Shards[sid].ShardStatus == ReadyButNeedSendGC {
									kv.rf.Start(ShardCommand{
										Op:              "UpdateStatus",
										Shard:           nil,
										Sid:             sid,
										LastRequestInfo: nil,
									})
								}
								wg.Done()
								return
							} // else: FailSend / ErrWrongLeader / ErrScale
							index = (index + 1) % len(others)
						} // end for
					}(sid, gid, others)
				}
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(NewGarbageCollectorTimeOut)
	}
}
