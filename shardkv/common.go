package shardkv

import (
	"fmt"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	Debug = false

	Server = "SER"
	Client = "ClI"
)

var (
	DebugStart = time.Now()

	ErrOk          = Err("OK")
	ErrWrongLeader = Err("Wrong Leader")
	ErrWrongGroup  = Err("Wrong Group")
	ErrTimeout     = Err("Timeout")
	ErrNetwork     = Err("Network Failed")
	ErrNoKey       = Err("NoKey")
	ErrNotReady    = Err("Shard Not Ready")
	ErrKill        = Err("ServerKilled")
)

type Err string

type BaseArg struct {
	RequestID string
	ClientID  string
	Shard     int
	CfgNum    int
}

type BaseReply struct {
	RequestID string
	Err
}

// Put or Append
type PutAppendArgs struct {
	BaseArg
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	BaseReply
}

type GetArgs struct {
	BaseArg
	Key string
}

type GetReply struct {
	BaseReply
	Value string
}

type DeleteShardArgs struct {
	Num    int
	GID    int
	Shards []int
}

type DeleteShardReply struct {
	Err Err
}

type MoveShardArgs struct {
	Num    int
	GID    int
	Shards []int
}

type MoveShardReply struct {
	ShardData  map[int]*KvShard
	ShardCache map[int]*Cache
	Err        Err
}

type KvShard struct {
	M      map[string]string
	CfgNum int
	Stop   bool
}

func NewKVShard(CfgNum int) *KvShard {
	return &KvShard{
		M:      map[string]string{},
		CfgNum: CfgNum,
	}
}

func (s *KvShard) Get(key string) (string, bool) {
	val, ok := s.M[key]
	return val, ok
}

func (s *KvShard) Put(key, val string) {
	s.M[key] = val
}

func (s *KvShard) Append(key, val string) {
	if old, ok := s.M[key]; ok {
		s.M[key] = old + val
	} else {
		s.M[key] = val
	}
}

// Clone will initialize stop as false
func (s *KvShard) Clone() *KvShard {
	return &KvShard{
		M:      shardCopy(s.M),
		CfgNum: s.CfgNum,
		Stop:   false,
	}
}

type Logger struct {
	Type string
	ID   string
	GID  string
}

func NewServerLogger(_type, gid, id string) *Logger {
	log.SetLevel(log.InfoLevel)
	return &Logger{
		Type: _type,
		ID:   id,
		GID:  gid,
	}
}

func NewClientLogger(_type, id string) *Logger {
	log.SetLevel(log.InfoLevel)
	return &Logger{
		Type: _type,
		ID:   id,
	}
}

func (l *Logger) Tracef(format string, args ...interface{}) {
	if Debug {
		log.Trace(l.prefix() + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Infof(format string, args ...interface{}) {
	if Debug {
		log.Info(l.prefix() + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	if Debug {
		log.Info(l.prefix() + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	if Debug {
		log.Error(l.prefix() + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) prefix() string {
	if l.Type == Server {
		return fmt.Sprintf("%06d %s %v-%v ", time.Since(DebugStart).Milliseconds(), l.Type, l.GID, l.ID)
	} else if l.Type == Client {
		return fmt.Sprintf("%06d %s-%v ", time.Since(DebugStart).Milliseconds(), l.Type, l.ID)
	} else {
		return ""
	}
}

func shardCopy(s map[string]string) map[string]string {
	_copy := map[string]string{}
	for k, v := range s {
		_copy[k] = v
	}
	return _copy
}

func sortedKeys[V any](m map[int]V) (keys []int) {
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func copySlice[V int](arr []V) []V {
	a := make([]V, len(arr))
	copy(a, arr)
	return a
}
