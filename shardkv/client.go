package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"

	"go.uber.org/atomic"
)

var clientIDCounter = atomic.NewInt64(-1)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	id         string
	sm         *shardctrler.Clerk
	mu         sync.RWMutex
	config     shardctrler.Config
	leaders    sync.Map
	roundRobin *atomic.Int64
	ridCounter *atomic.Int64
	make_end   func(string) *labrpc.ClientEnd
	logger     *Logger
	// You will have to modify this struct.
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	cid := strconv.FormatInt(clientIDCounter.Inc(), 10)
	ck := &Clerk{
		id: cid,
		sm: shardctrler.MakeClerk(ctrlers),
		config: shardctrler.Config{
			Shards: [10]int{},
			Groups: map[int][]string{},
		},
		ridCounter: atomic.NewInt64(0),
		roundRobin: atomic.NewInt64(0),
		make_end:   make_end,
		logger:     NewClientLogger(Client, cid),
	}
	ck.sm = shardctrler.MakeClerk(ctrlers)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	requestID := strconv.FormatInt(ck.ridCounter.Inc(), 10)
	for {
		ck.mu.RLock()
		args := GetArgs{
			BaseArg: BaseArg{
				RequestID: requestID,
				ClientID:  ck.id,
				Shard:     key2shard(key),
				CfgNum:    ck.config.Num,
			},
			Key: key,
		}
		gid := ck.config.Shards[args.Shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for i := 0; i < len(servers)+1; i++ {
				si := ck.pick(gid)
				srv := ck.make_end(servers[si])
				ck.logger.Infof("request_id=%s send Get to %s shard=%d",
					args.RequestID, servers[si], args.Shard)
				var reply GetReply
				if ok := srv.Call("ShardKV.Get", &args, &reply); !ok {
					reply.Err = ErrNetwork
				}
				if reply.Err == ErrOk || reply.Err == ErrNoKey {
					ck.leaders.Store(gid, si)
					ck.logger.Infof("request_id=%s GET success", reply.RequestID)
					ck.mu.RUnlock()
					return reply.Value
				}
				ck.leaders.Store(gid, -1)
				if reply.Err == ErrWrongGroup {
					ck.logger.Infof("request_id=%s GET failed: %s", reply.RequestID, reply.Err)
					break
				}
				if reply.Err != ErrNetwork {
					ck.logger.Infof("request_id=%s GET failed: %s", reply.RequestID, reply.Err)
				}
			}
		}
		ck.mu.RUnlock()
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.mu.Lock()
		ck.config = ck.sm.Query(-1).Clone()
		ck.mu.Unlock()
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestID := strconv.FormatInt(ck.ridCounter.Inc(), 10)
	for {
		ck.mu.RLock()
		args := PutAppendArgs{
			BaseArg: BaseArg{
				RequestID: requestID,
				ClientID:  ck.id,
				Shard:     key2shard(key),
				CfgNum:    ck.config.Num,
			},
			Key:   key,
			Value: value,
			Op:    op,
		}
		gid := ck.config.Shards[args.Shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for i := 0; i < len(servers)+1; i++ {
				si := ck.pick(gid)
				srv := ck.make_end(servers[si])
				ck.logger.Infof("request_id=%s send %s to %s shard=%d",
					args.RequestID, op, servers[si], args.Shard)
				var reply PutAppendReply
				if ok := srv.Call("ShardKV.PutAppend", &args, &reply); !ok {
					reply.Err = ErrNetwork
				}
				if reply.Err == ErrOk || reply.Err == ErrNoKey {
					ck.leaders.Store(gid, si)
					ck.logger.Infof("request_id=%s %s success", reply.RequestID, op)
					ck.mu.RUnlock()
					return
				}
				ck.leaders.Store(gid, -1)
				if reply.Err == ErrWrongGroup {
					ck.logger.Infof("request_id=%s %s failed: %s", reply.RequestID, op, reply.Err)
					break
				}
				if reply.Err != ErrNetwork {
					ck.logger.Infof("request_id=%s %s failed: %s", reply.RequestID, op, reply.Err)
				}
			}
		}
		ck.mu.RUnlock()
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.mu.Lock()
		ck.config = ck.sm.Query(-1).Clone()
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

func (ck *Clerk) pick(gid int) int {
	if leader, ok := ck.leaders.Load(gid); ok && leader.(int) != -1 {
		return leader.(int)
	}
	return int(ck.roundRobin.Inc() % int64(len(ck.config.Groups[gid])))
}
