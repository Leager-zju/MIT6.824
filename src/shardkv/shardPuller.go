package shardkv

import (
	"sync"

	"6.824/raft"
)

func (kv *ShardKV) HandleShardPullRequest(args *ShardExchangeArgs, reply *ShardExchangeReply) {
	DPrintf("[%d %d] get request %+v", kv.gid, kv.me, args)
	if kv.rf.GetRaftState() != raft.Leader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := kv.Shards[args.ShardId]
	if shard.Version != args.Version {
		reply.Err = ErrScale
		return
	}

	reply.Shard.KVs = make(map[string]string)
	for k, v := range shard.KVs {
		reply.Shard.KVs[k] = v
	}
	reply.Shard.Version = shard.Version + 1
	reply.Err = OK
}

func (kv *ShardKV) InsertShard(shardInfo ShardInfo) { // locked
	newshard, sid := shardInfo.Shard, shardInfo.Sid
	if kv.Shards[sid].Version < newshard.Version {
		kv.Shards[sid].KVs = make(map[string]string)
		for k, v := range newshard.KVs {
			kv.Shards[sid].KVs[k] = v
		}
		kv.Shards[sid].Version = newshard.Version
		kv.CP_Cond.Signal()
	}
}

func (kv *ShardKV) ShardPuller() {
	kv.SP_Cond.L.Lock()
	defer kv.SP_Cond.L.Unlock()
	for !kv.killed() {
		for !kv.NeedPullShard() {
			kv.SP_Cond.Wait()
		}

		if kv.rf.GetRaftState() == raft.Leader {

			configNum := kv.lastConfig.Num
			var wg sync.WaitGroup
			DPrintf("[%d %d] start pull shard", kv.gid, kv.me)
			for sid, gid := range kv.lastConfig.Shards {
				if kv.NeedPull(sid) {
					// 向前任持有者拉取
					wg.Add(1)
					go func(sid int, gid int) {
						others := kv.currentConfig.Groups[gid]
						args := &ShardExchangeArgs{ // ask for Shard that Version == Version
							ShardId: sid,
							Version: configNum,
						}

						DPrintf("[%d %d] pull shard %d from %d with args %+v", kv.gid, kv.me, sid, gid, args)
						for {
							for _, other := range others {
								end := kv.make_end(other)

								reply := &ShardExchangeReply{}
								ok := end.Call("ShardKV.HandleShardPullRequest", args, reply)
								if ok && reply.Err == OK {
									// update step-by-step
									DPrintf("[%d %d] pull shard %d from %d and get reply %+v", kv.gid, kv.me, sid, gid, reply)

									if reply.Shard.Version > kv.Shards[sid].Version {
										args := &Args{
											ClerkId: int64(kv.me),
											Op:      "InsertShard",
											Data:    ShardInfo{reply.Shard, sid},
										}

										kv.rf.Start(*args)
									}

									wg.Done()
									return
								}
							}
						}
					}(sid, gid)

				}
			}

			wg.Wait()

		}
	}
}

func (kv *ShardKV) OwnShard(shardId int) bool { // 当前配置分配给 kv 该分片
	// locked
	return kv.currentConfig.Shards[shardId] == kv.gid
}

func (kv *ShardKV) Valid(shardId int) bool { // 分片是否准备完毕数据
	// locked
	return kv.Shards[shardId].Version == kv.currentConfig.Num
}

func (kv *ShardKV) OwnShardAndValid(shardId int) bool { // 当前配置分配给 kv 该分片且数据准备完毕
	// locked
	return kv.OwnShard(shardId) && kv.Valid(shardId)
}

func (kv *ShardKV) NeedPullShard() bool { // 当且仅当 所有持有的 shard 的 Version 与当前 config 编号一致时，无需拉取 shard
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for sid := range kv.currentConfig.Shards {
		if kv.OwnShard(sid) && !kv.Valid(sid) {
			return true
		}
	}
	return false
}

func (kv *ShardKV) NeedPull(shardId int) bool { // 当且仅当 所有持有的 shard 的 Version 与当前 config 编号一致时，无需拉取 shard
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.OwnShard(shardId) && !kv.Valid(shardId)
}
