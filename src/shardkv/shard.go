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

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	defer DPrintf("[%d %d] get %+v and reply %+v", kv.gid, kv.me, args, reply)

	sid, configNum := args.ShardId, args.ConfigNum
	if configNum > kv.currentConfig.Num {
		reply.ConfigNum, reply.Err = kv.currentConfig.Num, ErrNotReady
		return
	}

	switch args.Op {
	case "PullShard":
		reply.Shard.KVs = make(map[string]string)
		for k, v := range kv.Shards[sid].KVs {
			reply.Shard.KVs[k] = v
		}

		reply.LastRequestInfo = make(map[int64]RequestInfo)
		for clerkId, lastrequestInfo := range kv.lastRequestInfo {
			reply.LastRequestInfo[clerkId] = *lastrequestInfo
		}

	case "GarbageCollect":
		kv.rf.Start(ShardCommand{
			Op:              "GarbageCollect",
			Shard:           nil,
			ConfigNum:       configNum,
			Sid:             sid,
			LastRequestInfo: nil,
		})
	}

	reply.ConfigNum, reply.Err = configNum, OK
}

func (kv *ShardKV) ApplyShardCommand(msg raft.ApplyMsg) {
	Command := msg.Command.(ShardCommand)
	switch Command.Op {
	case "InsertShard":
		kv.InsertShard(Command)
	case "GarbageCollect":
		kv.GarbageCollect(Command)
	default:
		panic("Undefined Command!")
	}
}

func (kv *ShardKV) ApplyEmptyCommand() {
	// DPrintf("EMPTY COMMAND")
}

func (kv *ShardKV) InsertShard(Command ShardCommand) {
	// locked
	configNum, newshard, sid := Command.ConfigNum, Command.Shard, Command.Sid
	if configNum == kv.currentConfig.Num && kv.Shards[sid].ShardStatus == NeedPull {
		for k, v := range newshard.KVs {
			kv.Shards[sid].KVs[k] = v
		}
		for clientId, lastrequestInfo := range Command.LastRequestInfo {
			Rinfo, ok := kv.lastRequestInfo[clientId]
			if !ok || Rinfo.RequestID < lastrequestInfo.RequestID {
				kv.lastRequestInfo[clientId] = &RequestInfo{
					lastrequestInfo.RequestID,
					lastrequestInfo.Err,
				}
			}
		}
		kv.Shards[sid].ShardStatus = ReadyButNeedSendGC
		// DPrintf("[%d %d] insert %d-th Shard %+v", kv.gid, kv.me, Command.Sid, Command.Shard)
	}
}

func (kv *ShardKV) GarbageCollect(Command ShardCommand) {
	// locked
	configNum, sid := Command.ConfigNum, Command.Sid
	if configNum == kv.currentConfig.Num {
		if kv.Shards[sid].ShardStatus == ReadyButNeedSendGC {
			kv.Shards[sid].ShardStatus = Ready
		} else if kv.Shards[sid].ShardStatus == Waiting {
			kv.Shards[sid].KVs = make(map[string]string)
			kv.Shards[sid].ShardStatus = Ready
		}
	}
}

func (kv *ShardKV) ShardPuller() {
	// goroutine
	for !kv.killed() {
		if kv.rf.GetRaftState() == raft.Leader {
			kv.mu.RLock()
			configNum := kv.currentConfig.Num // 捎带当前 config.num，防止收到过期回复
			var wg sync.WaitGroup
			for sid, gid := range kv.lastConfig.Shards {
				if kv.NeedPull(sid) { // 向前任持有者拉取 shard
					wg.Add(1)
					// Send RPC and Pull Shard
					go func(sid, gid int, others []string) {
						defer wg.Done()
						index := 0
						args := &RPCArgs{
							Op:        "PullShard",
							ShardId:   sid,
							ConfigNum: configNum,
						}
						// DPrintf("[%d %d] pull shard %d from %d with args %+v", kv.gid, kv.me, sid, gid, args)
						for {
							end := kv.make_end(others[index])
							reply := &RPCReply{}
							ok := end.Call("ShardKV.HandleBetweenGroupRequest", args, reply)
							if ok && reply.Err == OK {
								DPrintf("[%d %d] pull shard %d from %d and get KVs: %+v", kv.gid, kv.me, sid, gid, reply.Shard.KVs)
								kv.rf.Start(ShardCommand{
									Op:              "InsertShard",
									Shard:           &reply.Shard,
									ConfigNum:       configNum,
									Sid:             sid,
									LastRequestInfo: reply.LastRequestInfo,
								})
								return
							} // else: FailSend / ErrWrongLeader / ErrNotReady
							index = (index + 1) % len(others)
						} // end for
					}(sid, gid, kv.lastConfig.Groups[gid])
				}
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(ShardPullerTimeOut)
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
				if kv.ReadyButNeedSendGC(sid) { // 向前任持有者发送 GC 请求
					wg.Add(1)
					// Send RPC and make GC
					go func(sid, gid int, others []string) {
						defer wg.Done()
						index := 0
						args := &RPCArgs{
							Op:        "GarbageCollect",
							ShardId:   sid,
							ConfigNum: configNum,
						}
						// DPrintf("[%d %d] send GC %d to %d at Config %d", kv.gid, kv.me, sid, gid, configNum)
						for {
							end := kv.make_end(others[index])
							reply := &RPCReply{}
							ok := end.Call("ShardKV.HandleBetweenGroupRequest", args, reply)
							if ok && reply.Err == OK {
								DPrintf("[%d %d] send GC %d to %d success", kv.gid, kv.me, sid, gid)
								kv.rf.Start(ShardCommand{
									Op:              "GarbageCollect",
									Shard:           nil,
									ConfigNum:       configNum,
									Sid:             sid,
									LastRequestInfo: nil,
								})
								return
							} // else: FailSend / ErrWrongLeader / ErrNotReady
							index = (index + 1) % len(others)
						} // end for
					}(sid, gid, kv.lastConfig.Groups[gid])
				}
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(GarbageCollectorTimeOut)
	}
}

func (kv *ShardKV) EmptyEntryDetector() {
	for !kv.killed() {
		if kv.rf.GetRaftState() == raft.Leader {
			if !kv.rf.HasLogAtCurrentTerm() {
				kv.rf.Start(EmptyCommand{})
			}
		}
		time.Sleep(EmptyEntryDetectorTimeOut)
	}
}
