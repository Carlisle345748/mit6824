package shardctrler

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"math"
	"sort"
	"sync"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.

const (
	NShards = 10
	Debug   = false

	Server = "SER-CTL"
	Client = "CLI-CTL"
)

var (
	ErrOk          = Err("OK")
	ErrWrongLeader = Err("WrongLeader")
	ErrNetwork     = Err("Network Fail")
	ErrNoConfig    = Err("Config Not Exist")
	ErrTimeout     = Err("Timeout")
	ErrKill        = Err("Server Killed")

	DebugStart = time.Now()
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) Clone() Config {
	_copy := Config{
		Num:    c.Num,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	for i, shard := range c.Shards {
		_copy.Shards[i] = shard
	}
	for k, v := range c.Groups {
		_copy.Groups[k] = make([]string, len(v))
		copy(_copy.Groups[k], v)
	}
	return _copy
}

func (c Config) Move(gid, shard int) Config {
	cfg := c.Clone()
	cfg.Num = c.Num + 1
	cfg.Shards[shard] = gid
	return cfg
}

func (c Config) Leave(gids []int) Config {
	cfg := c.Clone()
	cfg.Num = c.Num + 1
	var shards2reassign []int
	gid2leave := map[int]bool{}   // gid -> is leaving?
	gid2shards := map[int][]int{} // gid -> []shard
	for _, gid := range gids {
		delete(cfg.Groups, gid)
		gid2leave[gid] = true
	}
	// No available replica group
	if len(cfg.Groups) == 0 {
		cfg.Shards = [NShards]int{}
		return cfg
	}
	for _, gid := range sortedKeys(cfg.Groups) {
		gid2shards[gid] = []int{}
	}
	for shard, gid := range cfg.Shards {
		if gid2leave[gid] {
			shards2reassign = append(shards2reassign, shard)
		} else {
			gid2shards[gid] = append(gid2shards[gid], shard)
		}
	}
	// No shards need to be reassigned
	if len(shards2reassign) == 0 {
		return cfg
	}
	for _, shard := range shards2reassign {
		minGid := c.minLoadGroup(gid2shards)
		gid2shards[minGid] = append(gid2shards[minGid], shard)
		cfg.Shards[shard] = minGid
	}
	return cfg
}

func (c Config) Join(servers map[int][]string) Config {
	cfg := c.Clone()
	cfg.Num += 1
	gid2shards := map[int][]int{} // gid -> []shard
	for gid, name := range servers {
		cfg.Groups[gid] = name
	}
	if len(cfg.Groups) > 10 {
		return cfg
	}
	// ReBalance
	for _, gid := range sortedKeys(cfg.Groups) {
		gid2shards[gid] = []int{}
	}
	for shard, gid := range cfg.Shards {
		gid2shards[gid] = append(gid2shards[gid], shard)
	}
	threshold := 1
	if (len(cfg.Groups) % NShards) == 0 {
		threshold = 0
	}
	for {
		toGid := c.minLoadGroup(gid2shards)
		fromGid := c.maxLoadGroup(gid2shards)
		if len(gid2shards[0]) > 0 {
			fromGid = 0
		}
		if len(gid2shards[fromGid])-len(gid2shards[toGid]) <= threshold && fromGid != 0 {
			break
		}
		maxCount := len(gid2shards[fromGid])
		shard := gid2shards[fromGid][maxCount-1]
		gid2shards[fromGid] = gid2shards[fromGid][:maxCount-1]
		gid2shards[toGid] = append(gid2shards[toGid], shard)
		cfg.Shards[shard] = toGid
	}
	return cfg
}

func (c Config) minLoadGroup(nums map[int][]int) int {
	if len(nums) == 0 {
		panic("no available replica group")
	}
	var minK = math.MaxInt
	var minV = math.MaxInt
	for _, k := range sortedKeys(nums) {
		if len(nums[k]) < minV && k != 0 {
			minK = k
			minV = len(nums[k])
		}
	}
	return minK
}

func (c Config) maxLoadGroup(nums map[int][]int) int {
	if len(nums) == 0 {
		panic("no available replica group")
	}
	var maxK = math.MinInt
	var maxV = math.MinInt
	for _, k := range sortedKeys(nums) {
		if len(nums[k]) > maxV && k != 0 {
			maxK = k
			maxV = len(nums[k])
		}
	}
	return maxK
}

func (c Config) String() string {
	str := fmt.Sprintf("%d %v [", c.Num, c.Shards)
	for _, k := range sortedKeys(c.Groups) {
		str += fmt.Sprint(k) + " "
	}
	if str[len(str)-1] != '[' {
		str = str[:len(str)-1]
	}
	return str + "]"
}

type Err string

type Args interface {
	RequestID() string
	ClientID() string
}

type BaseArg struct {
	RID string
	CID string
}

func (arg BaseArg) RequestID() string {
	return arg.RID
}

func (arg BaseArg) ClientID() string {
	return arg.CID
}

type BaseReply struct {
	RequestID string
	Err
}

type JoinArgs struct {
	BaseArg
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	BaseReply
}

type LeaveArgs struct {
	BaseArg
	GIDs []int
}

type LeaveReply struct {
	BaseReply
}

type MoveArgs struct {
	BaseArg
	Shard int
	GID   int
}

type MoveReply struct {
	BaseReply
}

type QueryArgs struct {
	BaseArg
	Num int // desired config number
}

type QueryReply struct {
	BaseReply
	Config Config
}

type OpResult struct {
	ClientID  string
	RequestID string
	Err       Err
	Config    Config
}

type OpCache struct {
	RequestID string
	Err       Err
}

type Caches struct {
	Caches map[string]*Cache // client_id -> cache
}

func (c *Caches) Get(clientID, requestID string) (OpCache, bool) {
	if _, ok := c.Caches[clientID]; !ok {
		return OpCache{}, false
	}
	return c.Caches[clientID].Get(requestID)
}

func (c *Caches) Add(result OpResult) {
	if _, ok := c.Caches[result.ClientID]; !ok {
		c.Caches[result.ClientID] = &Cache{Size: 5}
	}
	c.Caches[result.ClientID].Add(OpCache{
		RequestID: result.RequestID,
		Err:       result.Err,
	})
}

type Cache struct {
	Ops  []OpCache
	Size int
}

func (q *Cache) Add(op OpCache) {
	q.Ops = append(q.Ops, op)
	if len(q.Ops) > q.Size {
		q.Ops = q.Ops[1:]
	}
}

func (q *Cache) Get(requestID string) (OpCache, bool) {
	for i := 0; i < len(q.Ops); i++ {
		if q.Ops[i].RequestID == requestID {
			return q.Ops[i], true
		}
	}
	return OpCache{}, false
}

type ReadyCh struct {
	mu  sync.RWMutex
	chs map[int]chan OpResult
}

func (ch *ReadyCh) GetOrCreate(index int) chan OpResult {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, ok := ch.chs[index]; !ok {
		ch.chs[index] = make(chan OpResult, 1)
	}
	return ch.chs[index]
}

func (ch *ReadyCh) Get(index int) (chan OpResult, bool) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	if readyCh, ok := ch.chs[index]; ok {
		return readyCh, ok
	}
	return nil, false
}

func (ch *ReadyCh) Delete(index int) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	delete(ch.chs, index)
}

type Logger struct {
	Type string
	ID   string
}

func NewLogger(_type, id string) *Logger {
	log.SetLevel(log.InfoLevel)
	return &Logger{
		Type: _type,
		ID:   id,
	}
}

func (l *Logger) Tracef(format string, args ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("%06d %s-%v ", time.Since(DebugStart).Milliseconds(), l.Type, l.ID)
		log.Trace(prefix + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Infof(format string, args ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("%06d %s-%v ", time.Since(DebugStart).Milliseconds(), l.Type, l.ID)
		log.Info(prefix + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("%06d %s-%v ", time.Since(DebugStart).Milliseconds(), l.Type, l.ID)
		log.Warn(prefix + fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("%06d %s-%v ", time.Since(DebugStart).Milliseconds(), l.Type, l.ID)
		log.Error(prefix + fmt.Sprintf(format, args...))
	}
}

func sortedKeys[V any](m map[int]V) []int {
	keys := make([]int, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
