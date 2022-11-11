package shardkv

import (
	"time"

	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) ApplyConfig(configInfo ConfigInfo) { // locked

	lastconfig, newconfig := configInfo.LastConfig, configInfo.NewConfig

	if newconfig.Num > kv.currentConfig.Num {
		kv.lastConfig = lastconfig
		kv.currentConfig = newconfig

		if newconfig.Num == 1 { // 首个配置，所有 shardkv 都是第一次更新，不需要去拉取 shard

			for sid := range newconfig.Shards {
				if kv.OwnShard(sid) {
					kv.Shards[sid].Version = 1
				}
			}

		} else { // need update shards

			// 仍持有的直接更新 version，已分配但未持有的等到拉取 shard 完成后更新 Version

			for sid, gid := range kv.lastConfig.Shards {
				if gid == kv.gid && kv.OwnShard(sid) {
					kv.Shards[sid].Version = kv.currentConfig.Num
				}
			}

		}

		DPrintf("[%d %d] apply config %+v and update Shards -> %v", kv.gid, kv.me, newconfig, kv.Shards)

		kv.SP_Cond.Signal()
	}
}

func (kv *ShardKV) GetNewConfig() shardctrler.Config {
	return kv.mck.Query(kv.currentConfig.Num + 1)
}

// 定期拉取配置
func (kv *ShardKV) ConfigPuller() {
	kv.CP_Cond.L.Lock()
	defer kv.CP_Cond.L.Unlock()
	for !kv.killed() {
		for kv.NeedPullShard() {
			kv.CP_Cond.Wait()
		}
		newconfig := kv.GetNewConfig()
		if newconfig.Num > kv.currentConfig.Num && kv.rf.GetRaftState() == raft.Leader {
			configInfo := ConfigInfo{
				LastConfig: kv.currentConfig,
				NewConfig:  newconfig,
			}
			if kv.lastConfig.Num == 0 && newconfig.Num > 1 { // 第一次加入集群，需初始化 lastconfig
				configInfo.LastConfig = kv.mck.Query(newconfig.Num - 1)
			}

			DPrintf("[%d %d] send msg %+v", kv.gid, kv.me, configInfo)

			args := &Args{
				Op:   "ApplyConfig",
				Data: configInfo,
			}
			kv.rf.Start(*args)
		}
		time.Sleep(NewConfigQueryTimeOut)
	}
}
