package shardkv

import (
	"time"

	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) GetNewConfig() shardctrler.Config {
	return kv.mck.Query(kv.currentConfig.Num + 1)
}

func (kv *ShardKV) ApplyUpdateConfigCommand(msg raft.ApplyMsg) {
	// locked
	Command := msg.Command.(ConfigCommand)
	lastconfig, newconfig := Command.LastConfig, Command.NewConfig
	if newconfig.Num == kv.currentConfig.Num+1 {
		kv.lastConfig = lastconfig
		kv.currentConfig = newconfig

		if newconfig.Num > 1 {
			// config 发生变化：
			// 1. 分配且仍持有的保持 Ready
			// 2. 分配但未持有的变为 NeedPull，等到拉取 shard 完成后变为 Valid
			// 3. 持有但未分配的变为 Waiting，等待其他组 pull 后被回收
			for sid, gid := range kv.lastConfig.Shards {
				if gid != kv.gid && kv.OwnShard(sid) {
					kv.Shards[sid].ShardStatus = NeedPull
				}
				if gid == kv.gid && !kv.OwnShard(sid) {
					kv.Shards[sid].ShardStatus = Waiting
				}
			}
		}

		DPrintf("[%d %d] Apply Config: last %+v, new %+v", kv.gid, kv.me, lastconfig, newconfig)
	}
}

// 周期性拉取配置
func (kv *ShardKV) ConfigPuller() {
	// goroutine
	for !kv.killed() {
		if kv.rf.GetRaftState() == raft.Leader {
			kv.mu.RLock()
			CanPullConfig := true
			lastConfig, configNum := kv.currentConfig, kv.currentConfig.Num
			for sid := range kv.currentConfig.Shards {
				if !kv.ReadyForConfigPuller(sid) {
					CanPullConfig = false
					break
				}
			}
			kv.mu.RUnlock()

			if CanPullConfig {
				newconfig := kv.GetNewConfig()
				if newconfig.Num > configNum {
					if configNum == 0 && newconfig.Num > 1 { // 第一次加入集群，需初始化 lastconfig
						lastConfig = kv.mck.Query(newconfig.Num - 1)
					}
					kv.rf.Start(ConfigCommand{
						LastConfig: lastConfig,
						NewConfig:  newconfig,
					})
				}
			}
		}
		time.Sleep(NewConfigQueryTimeOut)
	}
}
