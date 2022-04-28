package kvraft

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"time"

	"6.824/labrpc"

	"go.uber.org/atomic"
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
	ck.leader = atomic.NewInt64(-1)
	ck.roundRobin = atomic.NewInt64(0)
	ck.id = strconv.FormatInt(clientIDCounter.Inc(), 10)
	ck.logger = NewLogger(Client, ck.id)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	requestID := strconv.Itoa(int(requestIDCounter.Inc()))
	ck.logger.Infof("request_id=%s send GET, key=%s", requestID, key)
	for {
		arg := GetArgs{
			BaseArg: BaseArg{RequestID: requestID, ClientID: ck.id},
			Key:     key,
		}
		reply := GetReply{}
		server := ck.pick()
		if ok := ck.servers[server].Call("KVServer.Get", &arg, &reply); !ok {
			reply.Err = ErrNetwork
		}
		// Get non-exist key
		if reply.Err == ErrNoKey {
			ck.leader.Store(server)
			ck.logger.Infof("Get failed: %v", reply.Err)
			return ""
		}
		if reply.Err == ErrOk {
			ck.leader.Store(server)
			ck.logger.Infof("GET success, request_id=%s", arg.RequestID)
			return reply.Value
		}
		ck.leader.Store(-1)
		ck.logger.Infof("Get failed: %v", reply.Err)
		if reply.Err != ErrWrongLeader {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestID := strconv.Itoa(int(requestIDCounter.Inc()))
	ck.logger.Infof("request_id=%s send PutAppend, key=%s, val=%s", requestID, key, value)
	for {
		arg := PutAppendArgs{
			BaseArg: BaseArg{RequestID: requestID, ClientID: ck.id},
			Key:     key,
			Value:   value,
			Op:      op,
		}
		reply := PutAppendReply{}
		server := ck.pick()
		if ok := ck.servers[server].Call("KVServer.PutAppend", &arg, &reply); !ok {
			reply.Err = ErrNetwork
		}
		if reply.Err == ErrOk {
			ck.leader.Store(server)
			ck.logger.Infof("PutAppend success, request_id=%s", arg.RequestID)
			return
		}
		ck.leader.Store(-1)
		ck.logger.Infof("PutAppend failed: %v, request_id=%s", reply.Err, arg.RequestID)
		if reply.Err != ErrWrongLeader {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OPPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

func (ck *Clerk) pick() int64 {
	if ck.leader.Load() != -1 {
		return ck.leader.Load()
	}
	return ck.roundRobin.Inc() % int64(len(ck.servers))
}
