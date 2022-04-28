package shardkv

import "6.824/shardctrler"

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type OpKV struct {
	Shard     int
	CfgNum    int
	RequestID string
	ClientID  string
	OpType    string
	Key       string
	Value     string
}

type KVResult struct {
	Shard     int
	RequestID string
	ClientID  string
	Value     string
	Err
}

func (k KVResult) ToCache() KvOpCache {
	return KvOpCache{RequestID: k.RequestID, Err: k.Err}
}

type KvOpCache struct {
	RequestID string
	Err
}

type OpReCfgStart struct {
	Config shardctrler.Config
}

type OpDeleteShard struct {
	Num    int
	GID    int
	Shards []int // gid -> shard
}

type OpDeleteShardRes struct {
	Num int
	GID int
	Err Err
}

type OpMoveShard struct {
	Num    int
	GID    int
	Shards []int
}

type OpMoveShardRes struct {
	Num        int
	GID        int
	ShardData  map[int]*KvShard
	ShardCache map[int]*Cache
	Err        Err
}

type OpUpdateShard struct {
	GID        int
	Local      bool
	NewConfig  shardctrler.Config
	ShardData  map[int]*KvShard
	ShardCache map[int]*Cache
}
