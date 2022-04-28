package shardkv

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"

	"go.uber.org/atomic"
)

type ShardKV struct {
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk

	dead  *atomic.Bool // set by Kill()
	close chan struct{}

	readyCh      *ReadyCh
	opCaches     *OpKVCache
	committedIdx int
	lastSnapIdx  int
	persister    *raft.Persister
	cfgMu        sync.RWMutex
	curConfig    shardctrler.Config
	logger       *Logger
	kvMap        map[int]*KvShard
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.RequestID = args.RequestID
	opRes := kv.kvRequestHandler(args.BaseArg, OpKV{
		Shard:     args.Shard,
		CfgNum:    args.CfgNum,
		OpType:    OpGet,
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	reply.Err = opRes.Err
	if opRes.Err == ErrOk {
		reply.Value = opRes.Value
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.RequestID = args.RequestID
	opRes := kv.kvRequestHandler(args.BaseArg, OpKV{
		Shard:     args.Shard,
		CfgNum:    args.CfgNum,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	reply.Err = opRes.Err
}

func (kv *ShardKV) appendLogAndWait(op any) (any, Err) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, ErrWrongLeader
	}
	ready := kv.readyCh.GetOrCreate(index)
	defer kv.readyCh.Delete(index)
	select {
	case opRes := <-ready:
		return opRes, ErrOk
	case <-time.After(500 * time.Millisecond):
		return nil, ErrTimeout
	case <-kv.close:
		return nil, ErrKill
	}
}

func (kv *ShardKV) kvRequestHandler(args BaseArg, op OpKV) KVResult {
	opRes, err := kv.appendLogAndWait(op)
	if err != ErrOk {
		return KVResult{Err: err}
	}
	res, ok := opRes.(KVResult)
	if !ok || res.RequestID != args.RequestID || res.ClientID != args.ClientID {
		kv.logger.Infof("request_id=%s lose leadership", args.RequestID)
		return KVResult{Err: ErrWrongLeader}
	}
	return res
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	opRes, err := kv.appendLogAndWait(OpMoveShard{
		Num:    args.Num,
		GID:    args.GID,
		Shards: copySlice(args.Shards),
	})
	if err != ErrOk {
		reply.Err = err
		return
	}
	res, ok := opRes.(OpMoveShardRes)
	if !ok || res.Num != args.Num || res.GID != args.GID {
		reply.Err = ErrWrongLeader
		kv.logger.Infof("MoveShardRes lose leadership num=%d gid=%d", args.Num, args.GID)
		return
	}
	reply.Err = res.Err
	reply.ShardData = res.ShardData
	reply.ShardCache = res.ShardCache
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	opRes, err := kv.appendLogAndWait(OpDeleteShard{
		Num:    args.Num,
		GID:    args.GID,
		Shards: copySlice(args.Shards),
	})
	if err != ErrOk {
		reply.Err = err
		return
	}
	res, ok := opRes.(OpDeleteShardRes)
	if !ok || res.Num != args.Num || res.GID != args.GID {
		reply.Err = ErrWrongLeader
		kv.logger.Infof("DeleteShard lose leadership num=%d gid=%d", args.Num, args.GID)
		return
	}
	reply.Err = res.Err
}

func (kv *ShardKV) fetchConfig() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-kv.close:
			return
		case <-ticker.C:
			cfg := kv.mck.Query(-1)

			kv.cfgMu.RLock()
			shouldReCfg := cfg.Num > kv.curConfig.Num
			cfgNum := -1
			if cfg.Num > kv.curConfig.Num+1 {
				cfgNum = kv.curConfig.Num + 1
			}
			kv.cfgMu.RUnlock()

			if cfgNum != -1 {
				cfg = kv.mck.Query(cfgNum)
			}
			if shouldReCfg {
				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.prepareReCfg(cfg)
				}
			}
		}
	}
}

func (kv *ShardKV) applier() {
	snapTicker := time.NewTicker(100 * time.Millisecond)
	defer snapTicker.Stop()
	for {
		select {
		case <-kv.close:
			return
		case <-snapTicker.C:
			if kv.maxraftstate > 0 && kv.committedIdx > kv.lastSnapIdx &&
				kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot()
			}
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if msg.CommandIndex > kv.committedIdx {
					kv.committedIdx = msg.CommandIndex
				}
				switch msg.Command.(type) {
				case OpKV:
					kv.applyKV(msg)
				case OpDeleteShard:
					kv.applyDeleteShard(msg)
				case OpMoveShard:
					kv.applyMoveShard(msg)
				case OpUpdateShard:
					kv.applyUpdateShard(msg)
				default:
					kv.logger.Errorf("invalid command %+v", msg)
				}
			} else if msg.SnapshotValid {
				kv.applySnapshot(msg)
			} else {
				kv.logger.Errorf("invalid message %+v", msg)
			}
		}
	}
}

func (kv *ShardKV) prepareReCfg(cfg shardctrler.Config) {
	kv.logger.Infof("start prepareReCfg num=%d %v", cfg.Num, cfg)
	wg := sync.WaitGroup{}
	pullList := map[int][]int{}    // gid -> shards
	stayList := map[int]*KvShard{} // gid -> empty shards

	kv.cfgMu.RLock()
	for shard, gid := range cfg.Shards {
		if gid == kv.gid {
			if kv.curConfig.Num != 0 && kv.curConfig.Shards[shard] != gid {
				shardOwnerGid := kv.curConfig.Shards[shard]
				pullList[shardOwnerGid] = append(pullList[shardOwnerGid], shard)
			} else {
				stayList[shard] = &KvShard{}
			}
		}
	}
	kv.logger.Infof("pull list: %+v", pullList)
	kv.logger.Infof("stay list: %v", sortedKeys(stayList))
	for gid, shards := range pullList {
		wg.Add(1)
		go kv.sendPullShard(&wg, gid, kv.curConfig.Groups[gid], cfg, shards)
	}
	kv.rf.Start(OpUpdateShard{
		GID:       kv.gid,
		Local:     true,
		NewConfig: cfg,
		ShardData: stayList,
	})
	kv.cfgMu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) sendPullShard(wg *sync.WaitGroup, gid int, servers []string, cfg shardctrler.Config, shards []int) {
	defer wg.Done()
	req := MoveShardArgs{
		Num:    cfg.Num,
		GID:    kv.gid,
		Shards: copySlice(shards),
	}
	for {
		for _, srv := range servers {
			kv.logger.Infof("send MoveShard to %s num=%d", srv, cfg.Num)
			rsp := MoveShardReply{}
			if ok := kv.make_end(srv).Call("ShardKV.MoveShard", &req, &rsp); !ok {
				rsp.Err = ErrNetwork
			}
			if rsp.Err == ErrOk {
				op := OpUpdateShard{
					GID:        gid,
					NewConfig:  cfg,
					ShardData:  map[int]*KvShard{},
					ShardCache: map[int]*Cache{},
				}
				for shard := range rsp.ShardData {
					op.ShardData[shard] = rsp.ShardData[shard].Clone()
					op.ShardCache[shard] = rsp.ShardCache[shard].Clone()
				}
				if _, _, isLeader := kv.rf.Start(op); isLeader {
					kv.logger.Infof("pull shard from %s success: %+v", srv, sortedKeys(rsp.ShardData))
				}
				return
			}
			kv.logger.Infof("pull shard from %s failed: %s", srv, rsp.Err)
		}
		select {
		case <-kv.close:
			kv.logger.Infof("stop MoveShard to %v num=%d on killed", servers, cfg.Num)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (kv *ShardKV) sendDeleteShard(servers []string, cfgNum int, shards []int) {
	req := DeleteShardArgs{
		Num:    cfgNum,
		GID:    kv.gid,
		Shards: copySlice(shards),
	}
	for {
		for _, srv := range servers {
			kv.logger.Infof("send DeleteShard to %s num=%d", srv, cfgNum)
			rsp := DeleteShardReply{}
			if ok := kv.make_end(srv).Call("ShardKV.DeleteShard", &req, &rsp); !ok {
				rsp.Err = ErrNetwork
			}
			if rsp.Err == ErrOk {
				return
			}
			kv.logger.Infof("send DeleteShard %s failed: %s", srv, rsp.Err)
		}
		select {
		case <-kv.close:
			kv.logger.Infof("stop DeleteShard to %v num=%d on killed", servers, cfgNum)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (kv *ShardKV) applyDeleteShard(msg raft.ApplyMsg) {
	kv.cfgMu.RLock()
	defer kv.cfgMu.RUnlock()

	op := msg.Command.(OpDeleteShard)
	res := OpDeleteShardRes{Num: op.Num, GID: op.GID}

	if kv.curConfig.Num < op.Num {
		res.Err = ErrNotReady
	} else {
		for _, shard := range op.Shards {
			if _, ok := kv.kvMap[shard]; ok {
				if kv.kvMap[shard].CfgNum == op.Num {
					delete(kv.kvMap, shard)
					kv.opCaches.DeleteShard(shard)
					kv.logger.Infof("idx=%d num=%d apply delete shard %d",
						msg.CommandIndex, op.Num, shard)
				}
			}
		}
		res.Err = ErrOk
	}

	kv.readyCh.SendIfExist(msg.CommandIndex, res)
}

func (kv *ShardKV) applyMoveShard(msg raft.ApplyMsg) {
	kv.cfgMu.RLock()
	defer kv.cfgMu.RUnlock()

	op := msg.Command.(OpMoveShard)
	res := OpMoveShardRes{Num: op.Num, GID: op.GID,
		ShardData:  map[int]*KvShard{},
		ShardCache: map[int]*Cache{}}

	if kv.curConfig.Num < op.Num {
		res.Err = ErrNotReady
	} else {
		for _, shard := range op.Shards {
			if _, ok := kv.kvMap[shard]; ok {
				res.ShardData[shard] = kv.kvMap[shard].Clone()
				if op.Num == kv.curConfig.Num {
					kv.kvMap[shard].Stop = true // Stop accepting new request
					kv.logger.Infof("idx=%d stop shard %d", msg.CommandIndex, shard)
				}
				kv.logger.Infof("idx=%d transfer shard %d to %d %+v %+v",
					msg.CommandIndex, shard, op.GID, kv.kvMap[shard], kv.opCaches.ToMap(shard)[shard])
			}
		}
		res.ShardCache = kv.opCaches.ToMap(op.Shards...)
		res.Err = ErrOk
	}

	kv.readyCh.SendIfExist(msg.CommandIndex, res)
}

func (kv *ShardKV) applyUpdateShard(msg raft.ApplyMsg) {
	kv.cfgMu.Lock()
	defer kv.cfgMu.Unlock()

	op := msg.Command.(OpUpdateShard)
	if kv.curConfig.Num >= op.NewConfig.Num {
		kv.logger.Infof("idx=%d drop outdated install shard num=%d", msg.CommandIndex, op.NewConfig.Num)
		return
	} else if kv.curConfig.Num+1 == op.NewConfig.Num {
		for shard := range op.ShardData {
			if op.Local {
				if _, ok := kv.kvMap[shard]; !ok {
					kv.kvMap[shard] = NewKVShard(0)
				}
				kv.kvMap[shard].CfgNum = op.NewConfig.Num
				continue
			}
			kvShard, ok := kv.kvMap[shard]
			if !ok || kvShard.CfgNum < op.ShardData[shard].CfgNum {
				kv.kvMap[shard] = op.ShardData[shard].Clone()
				kv.kvMap[shard].CfgNum = op.NewConfig.Num
				kv.opCaches.AddShard(shard, op.ShardCache[shard].Clone())
				kv.logger.Infof("idx=%d install shard %d num=%d %v %+v",
					msg.CommandIndex, shard, op.NewConfig.Num, op.ShardData[shard], op.ShardCache[shard])
			} else {
				kv.logger.Infof("idx=%d install shard drop shard %d cur=%d new=%d",
					msg.CommandIndex, shard, kv.kvMap[shard].CfgNum, op.ShardData[shard].CfgNum)
			}
		}
		if !op.Local {
			if _, isLeader := kv.rf.GetState(); isLeader {
				go kv.sendDeleteShard(kv.curConfig.Groups[op.GID], kv.curConfig.Num, sortedKeys(op.ShardData))
			}
		}
	} else {
		panic("error install shard")
	}

	allReceive := true
	for shard, gid := range op.NewConfig.Shards {
		if gid == kv.gid {
			kvShard, ok := kv.kvMap[shard]
			if !ok || kvShard.CfgNum != op.NewConfig.Num {
				allReceive = false
				break
			}
		}
	}
	if allReceive {
		kv.curConfig = op.NewConfig
		kv.logger.Infof("idx=%d reconfiguration %d success", msg.CommandIndex, op.NewConfig.Num)
	}
}

func (kv *ShardKV) applyKV(msg raft.ApplyMsg) {
	op := msg.Command.(OpKV)
	res := KVResult{Shard: op.Shard, RequestID: op.RequestID, ClientID: op.ClientID}

	switch op.OpType {
	case OpGet:
		if err := kv.isShardValid(op.Shard, op.CfgNum); err != nil {
			kv.logger.Infof("request_id=%s idx=%d failed: %v", op.RequestID, msg.CommandIndex, err)
			res.Err = ErrWrongGroup
			break
		}
		val, ok := kv.kvMap[op.Shard].Get(op.Key)
		if !ok {
			res.Err = ErrNoKey
			kv.logger.Warnf("request_id=%s idx=%d shard=%d apply GET of %s failed, key=%s",
				op.RequestID, msg.CommandIndex, op.Shard, op.ClientID, op.Key)
			break
		}
		res.Err = ErrOk
		res.Value = val
		kv.logger.Infof("request_id=%s idx=%d shard=%d apply GET of %s, key=%s val=%s",
			op.RequestID, msg.CommandIndex, op.Shard, op.ClientID, op.Key, res.Value)
	case OpPut:
		if _, ok := kv.opCaches.Get(op.Shard, op.ClientID, op.RequestID); ok {
			res.Err = ErrOk
			kv.logger.Infof("request_id=%s idx=%d duplicated", op.RequestID, msg.CommandIndex)
			break
		}
		if err := kv.isShardValid(op.Shard, op.CfgNum); err != nil {
			kv.logger.Infof("request_id=%s idx=%d failed: %v", op.RequestID, msg.CommandIndex, err)
			res.Err = ErrWrongGroup
			break
		}
		kv.kvMap[op.Shard].Put(op.Key, op.Value)
		res.Err = ErrOk
		kv.opCaches.Add(res)
		kv.logger.Infof("request_id=%s idx=%d shard=%d apply PUT of %s, key=%s, val=%s",
			op.RequestID, msg.CommandIndex, op.Shard, op.ClientID, op.Key, op.Value)
	case OpAppend:
		if _, ok := kv.opCaches.Get(op.Shard, op.ClientID, op.RequestID); ok {
			res.Err = ErrOk
			kv.logger.Infof("request_id=%s idx=%d duplicated", op.RequestID, msg.CommandIndex)
			break
		}
		if err := kv.isShardValid(op.Shard, op.CfgNum); err != nil {
			kv.logger.Infof("request_id=%s idx=%d failed: %v", op.RequestID, msg.CommandIndex, err)
			res.Err = ErrWrongGroup
			break
		}
		kv.kvMap[op.Shard].Append(op.Key, op.Value)
		res.Err = ErrOk
		kv.opCaches.Add(res)
		after, _ := kv.kvMap[op.Shard].Get(op.Key)
		kv.logger.Infof("request_id=%s, idx=%d shard=%d apply APPEND of %s, key=%s, val=%s after=%s",
			op.RequestID, msg.CommandIndex, op.Shard, op.ClientID, op.Key, op.Value, after)
	default:
		kv.logger.Errorf("unrecognized op type, request_id=%s", op.RequestID)
	}

	kv.readyCh.SendIfExist(msg.CommandIndex, res)
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) {
	if msg.SnapshotIndex <= kv.committedIdx {
		kv.logger.Warnf("discard outdated snapshot: snapIdx=%d commitIdx=%d",
			msg.SnapshotIndex, kv.committedIdx)
		return
	}
	kv.readSnapshot(msg.Snapshot)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	// bootstrap on start
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kvMap := map[int]*KvShard{}
	opCaches := map[int]*Cache{}
	committedIdx := 0
	curCfg := shardctrler.Config{}
	if d.Decode(&kvMap) != nil || d.Decode(&opCaches) != nil ||
		d.Decode(&committedIdx) != nil || d.Decode(&curCfg) != nil {
		kv.logger.Errorf("decode snapshot failed")
	} else {
		kv.kvMap = kvMap
		kv.committedIdx = committedIdx
		kv.cfgMu.Lock()
		kv.curConfig = curCfg
		kv.cfgMu.Unlock()
		kv.opCaches.LoadMap(opCaches)
		kv.logger.Infof("install snapshot index=%d", committedIdx)
	}
}

func (kv *ShardKV) snapshot() {
	var state []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.kvMap); err != nil {
		kv.logger.Errorf("encode kvMap fail")
	}
	if err := e.Encode(kv.opCaches.ToMap()); err != nil {
		kv.logger.Errorf("encode opCaches fail")
	}
	if err := e.Encode(kv.committedIdx); err != nil {
		kv.logger.Errorf("encode committedIdx fail")
	}
	if err := e.Encode(kv.curConfig); err != nil {
		kv.logger.Errorf("encode curConfig fail")
	}
	state = w.Bytes()
	kv.rf.Snapshot(kv.committedIdx, state)
	kv.lastSnapIdx = kv.committedIdx
	kv.logger.Infof("save snapshot index=%d", kv.committedIdx)
}

func (kv *ShardKV) isShardValid(shard, cfgNum int) error {
	if _, ok := kv.kvMap[shard]; !ok {
		return fmt.Errorf("shard %d not exist", shard)
	}
	if cfgNum < kv.curConfig.Num {
		return fmt.Errorf("shard %d request outdated num=%d", shard, cfgNum)
	}
	if kv.kvMap[shard].CfgNum != cfgNum {
		return fmt.Errorf("shard %d cfgNum not match cur=%d arg=%d", shard, kv.kvMap[shard].CfgNum, cfgNum)
	}
	if kv.kvMap[shard].Stop {
		return fmt.Errorf("shard %d stop", shard)
	}
	return nil
}

func (kv *ShardKV) Run() {
	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.fetchConfig()
	go kv.applier()
	kv.logger.Infof("start")
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.dead.Store(true)
	close(kv.close)
	kv.rf.Kill()
	kv.logger.Infof("shutdown")
}

func (kv *ShardKV) killed() bool {
	return kv.dead.Load()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpKV{})
	labgob.Register(OpReCfgStart{})
	labgob.Register(OpDeleteShard{})
	labgob.Register(OpUpdateShard{})
	labgob.Register(OpMoveShard{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		me:           me,
		make_end:     make_end,
		gid:          gid,
		ctrlers:      ctrlers,
		mck:          shardctrler.MakeClerk(ctrlers),
		maxraftstate: maxraftstate,
		dead:         atomic.NewBool(false),
		close:        make(chan struct{}),
		persister:    persister,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),
		cfgMu:        sync.RWMutex{},
		kvMap:        map[int]*KvShard{},
		readyCh:      &ReadyCh{chs: map[int]chan any{}},
		opCaches:     &OpKVCache{m: map[int]*Cache{}},
		curConfig:    shardctrler.Config{Num: 0, Shards: [10]int{}, Groups: nil},
		logger:       NewServerLogger(Server, strconv.Itoa(gid), strconv.Itoa(me)),
	}
	kv.Run()

	return kv
}
