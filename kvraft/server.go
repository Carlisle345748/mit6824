package kvraft

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Server = "SER"
	Client = "CLI"

	OPGet    = "GET"
	OPPut    = "PUT"
	OpAppend = "APPEND"
)

type Op struct {
	Type      string
	Key       string
	Data      string
	ClientID  string
	RequestID string
}

type OpResult struct {
	ClientID  string
	RequestID string
	Val       string
	Err       Err
}

type OpCache struct {
	RequestID string
	Err       Err
}

type Caches struct {
	Caches map[string]*Cache
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

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	close   chan struct{}

	maxraftstate int // snapshot if log grows this big

	kvMap        map[string]string
	opCaches     Caches
	readyCh      *ReadyCh
	committedIdx int
	snapTicker   *time.Ticker
	persister    *raft.Persister

	logger *Logger
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.RequestID = args.RequestID
	opRes := kv.requestHandler(args.BaseArg, Op{
		Type:      OPGet,
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	reply.Err = opRes.Err
	if opRes.Err == ErrOk {
		reply.Value = opRes.Val
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.RequestID = args.RequestID
	opRes := kv.requestHandler(args.BaseArg, Op{
		Type:      args.Op,
		Key:       args.Key,
		Data:      args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	reply.Err = opRes.Err
}

func (kv *KVServer) requestHandler(args BaseArg, op Op) OpResult {
	// Add Raft log
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{Err: ErrWrongLeader, RequestID: args.RequestID}
	}
	select {
	case opRes := <-kv.readyCh.GetOrCreate(index):
		if opRes.RequestID == args.RequestID &&
			opRes.ClientID == args.ClientID {
			kv.readyCh.Delete(index)
			return opRes
		}
		kv.logger.Infof("request_id=%s, lose leadership", args.RequestID)
		return OpResult{Err: ErrWrongLeader, RequestID: args.RequestID}
	case <-time.After(500 * time.Millisecond):
		return OpResult{Err: ErrTimeout, RequestID: args.RequestID}
	case <-kv.close:
		return OpResult{Err: ErrKill, RequestID: args.RequestID}
	}
}

func (kv *KVServer) applier() {
	for {
		select {
		case <-kv.close:
			return
		case <-kv.snapTicker.C:
			if kv.maxraftstate > 0 &&
				kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot()
			}
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.applyMsg(msg)
			} else if msg.SnapshotValid {
				kv.applySnapshot(msg)
			} else {
				kv.logger.Errorf("invalid message %+v", msg)
			}
		}
	}
}

func (kv *KVServer) applyMsg(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	res := OpResult{RequestID: op.RequestID, ClientID: op.ClientID}

	switch op.Type {
	case OPGet:
		val, ok := kv.kvMap[op.Key]
		if !ok {
			res.Err = ErrNoKey
			kv.logger.Warnf("request_id=%s, apply GET of %s failed, key=%s", op.RequestID, op.ClientID, op.Key)
			break
		}
		res.Err = ErrOk
		res.Val = val
		kv.logger.Infof("request_id=%s, idx=%d apply GET of %s, key=%s", op.RequestID, msg.CommandIndex, op.ClientID, op.Key)
	case OPPut:
		if cache, ok := kv.opCaches.Get(op.ClientID, op.RequestID); ok {
			res.Err = cache.Err
			break
		}
		kv.kvMap[op.Key] = op.Data
		res.Err = ErrOk
		kv.opCaches.Add(res)
		kv.logger.Infof("request_id=%s, idx=%d apply PUT of %s, key=%s, val=%s",
			op.RequestID, msg.CommandIndex, op.ClientID, op.Key, op.Data)
	case OpAppend:
		if cache, ok := kv.opCaches.Get(op.ClientID, op.RequestID); ok {
			res.Err = cache.Err
			break
		}
		val, ok := kv.kvMap[op.Key]
		if ok {
			kv.kvMap[op.Key] = val + op.Data
		} else {
			kv.kvMap[op.Key] = op.Data
		}
		res.Err = ErrOk
		kv.opCaches.Add(res)
		kv.logger.Infof("request_id=%s, idx=%d apply APPEND of %s, key=%s, val=%s",
			op.RequestID, msg.CommandIndex, op.ClientID, op.Key, op.Data)
	default:
		kv.logger.Errorf("unrecognized arg type, request_id=%s", op.RequestID)
	}

	if readyCh, ok := kv.readyCh.Get(msg.CommandIndex); ok {
		readyCh <- res
	}
	if msg.CommandIndex > kv.committedIdx {
		kv.committedIdx = msg.CommandIndex
	}
}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {
	if msg.SnapshotIndex <= kv.committedIdx {
		kv.logger.Warnf("discard outdated snapshot: SnapshotIndex=%d maxCommittedIdx=%d",
			msg.SnapshotIndex, kv.committedIdx)
		return
	}
	kv.readSnapshot(msg.Snapshot)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	// bootstrap on start
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kvMap := map[string]string{}
	opCaches := Caches{}
	committedIdx := 0
	if d.Decode(&kvMap) != nil || d.Decode(&opCaches) != nil || d.Decode(&committedIdx) != nil {
		kv.logger.Errorf("decode snapshot failed")
	} else {
		kv.kvMap = kvMap
		kv.opCaches = opCaches
		kv.committedIdx = committedIdx
		kv.logger.Infof("install snapshot index=%d", committedIdx)
	}
}

func (kv *KVServer) snapshot() {
	var state []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.kvMap); err != nil {
		kv.logger.Errorf("encode kvMap fail")
	}
	if err := e.Encode(kv.opCaches); err != nil {
		kv.logger.Errorf("encode opCaches fail")
	}
	if err := e.Encode(kv.committedIdx); err != nil {
		kv.logger.Errorf("encode committedIdx fail")
	}
	state = w.Bytes()
	kv.rf.Snapshot(kv.committedIdx, state)
	kv.logger.Infof("save snapshot index=%d", kv.committedIdx)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.snapTicker.Stop()
	close(kv.close)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Run() {
	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.applier()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		applyCh:      applyCh,
		close:        make(chan struct{}),
		maxraftstate: maxraftstate,
		kvMap:        map[string]string{},
		opCaches:     Caches{Caches: map[string]*Cache{}},
		readyCh:      &ReadyCh{chs: map[int]chan OpResult{}},
		persister:    persister,
		snapTicker:   time.NewTicker(50 * time.Millisecond),
		logger:       NewLogger(Server, strconv.Itoa(me)),
		rf:           raft.Make(servers, me, persister, applyCh),
	}
	kv.Run()

	return kv
}
