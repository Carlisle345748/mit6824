package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"crypto/rand"
	"go.uber.org/atomic"
	"math/big"
	"strconv"
	"time"
)

var clientIDCounter = atomic.NewInt64(-1)
var requestIDCounter = atomic.NewInt64(-1)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	id         string
	leader     *atomic.Int64
	roundRobin *atomic.Int64
	logger     *Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leader = atomic.NewInt64(-1)
	ck.roundRobin = atomic.NewInt64(0)
	ck.id = strconv.FormatInt(clientIDCounter.Inc(), 10)
	ck.logger = NewLogger(Client, ck.id)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		BaseArg: BaseArg{
			RID: strconv.FormatInt(requestIDCounter.Inc(), 10),
			CID: ck.id,
		},
		Num: num,
	}
	ck.logger.Infof("request_id=%s send Query, num=%d", args.RequestID(), num)

	for {
		srv := ck.pick()
		var reply QueryReply
		if ok := ck.servers[srv].Call("ShardCtrler.Query", args, &reply); !ok {
			reply.Err = ErrNetwork
		}
		if reply.Err == ErrOk {
			ck.leader.Store(srv)
			ck.logger.Infof("request_id=%s send Query success", args.RequestID())
			return reply.Config
		}
		ck.leader.Store(-1)
		ck.logger.Infof("request_id=%s send Query failed: %s", args.RequestID(), reply.Err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		BaseArg: BaseArg{
			RID: strconv.FormatInt(requestIDCounter.Inc(), 10),
			CID: ck.id,
		},
		Servers: servers,
	}
	ck.logger.Infof("request_id=%s send Join, servers=%v+", args.RequestID(), servers)

	for {
		srv := ck.pick()
		var reply JoinReply
		if ok := ck.servers[srv].Call("ShardCtrler.Join", args, &reply); !ok {
			reply.Err = ErrNetwork
		}
		if reply.Err == ErrOk {
			ck.logger.Infof("request_id=%s send Join success", args.RequestID())
			return
		}
		ck.leader.Store(-1)
		ck.logger.Infof("request_id=%s send Join failed: %v", args.RequestID(), reply.Err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		BaseArg: BaseArg{
			RID: strconv.FormatInt(requestIDCounter.Inc(), 10),
			CID: ck.id,
		},
		GIDs: gids,
	}
	ck.logger.Infof("request_id=%s send Leave, gids=%v", args.RequestID(), gids)

	for {
		// try each known server.
		srv := ck.pick()
		var reply LeaveReply
		if ok := ck.servers[srv].Call("ShardCtrler.Leave", args, &reply); !ok {
			reply.Err = ErrNetwork
		}
		if reply.Err == ErrOk {
			ck.leader.Store(srv)
			ck.logger.Infof("request_id=%s send Leave success", args.RequestID())
			return
		}
		ck.leader.Store(-1)
		ck.logger.Infof("request_id=%s send Leave failed: %v", args.RequestID(), reply.Err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		BaseArg: BaseArg{
			RID: strconv.FormatInt(requestIDCounter.Inc(), 10),
			CID: ck.id,
		},
		Shard: shard,
		GID:   gid,
	}
	ck.logger.Infof("request_id=%s send Move, shard=%d gid=%d", args.RequestID(), shard, gid)

	for {
		// try each known server.
		srv := ck.pick()
		var reply MoveReply
		if ok := ck.servers[srv].Call("ShardCtrler.Move", args, &reply); !ok {
			reply.Err = ErrNetwork
		}
		if reply.Err == ErrOk {
			ck.leader.Store(srv)
			ck.logger.Infof("request_id=%s send Move success", args.RequestID())
			return
		}
		ck.leader.Store(-1)
		ck.logger.Infof("request_id=%s send Move failed: %v", args.RequestID(), reply.Err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) pick() int64 {
	if ck.leader.Load() != -1 {
		return ck.leader.Load()
	}
	return ck.roundRobin.Inc() % int64(len(ck.servers))
}
