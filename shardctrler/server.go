package shardctrler

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	close   chan struct{}

	maxraftstate int // snapshot if log grows this big

	readyCh      *ReadyCh
	opCaches     Caches
	curTerm      int
	committedIdx int
	lastSave     time.Time
	persister    *raft.Persister
	snapTicker   *time.Ticker

	configs []Config // indexed by config num

	logger *Logger
}

type Op struct {
	Args
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.RequestID = args.RequestID()
	opRes := sc.requestHandler(*args)
	reply.Err = opRes.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.RequestID = args.RequestID()
	opRes := sc.requestHandler(*args)
	reply.Err = opRes.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.RequestID = args.RequestID()
	opRes := sc.requestHandler(*args)
	reply.Err = opRes.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	reply.RequestID = args.RequestID()
	opRes := sc.requestHandler(*args)
	reply.Err = opRes.Err
	if reply.Err == ErrOk {
		reply.Config = opRes.Config
	}
}

func (sc *ShardCtrler) requestHandler(args Args) OpResult {
	// Add Raft log
	index, _, isLeader := sc.rf.Start(Op{Args: args})
	if !isLeader {
		return OpResult{Err: ErrWrongLeader, RequestID: args.RequestID()}
	}
	select {
	case opRes := <-sc.readyCh.GetOrCreate(index):
		if opRes.RequestID == args.RequestID() &&
			opRes.ClientID == args.ClientID() {
			sc.readyCh.Delete(index)
			return opRes
		}
		sc.logger.Infof("request_id=%s, lose leadership", args.RequestID())
		return OpResult{Err: ErrWrongLeader, RequestID: args.RequestID()}
	case <-time.After(500 * time.Millisecond):
		return OpResult{Err: ErrTimeout, RequestID: args.RequestID()}
	case <-sc.close:
		return OpResult{Err: ErrKill, RequestID: args.RequestID()}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.snapTicker.Stop()
	close(sc.close)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case <-sc.close:
			return
		case <-sc.snapTicker.C:
			if sc.maxraftstate > 0 &&
				sc.persister.RaftStateSize() > sc.maxraftstate {
				sc.snapshot()
			}
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.applyMsg(msg)
			} else if msg.SnapshotValid {
				sc.applySnapshot(msg)
			} else {
				sc.logger.Errorf("invalid message %+v", msg)
			}
		}
	}
}

func (sc *ShardCtrler) applyMsg(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	res := OpResult{ClientID: op.ClientID(), RequestID: op.RequestID()}

	switch arg := op.Args.(type) {
	case JoinArgs:
		if cache, ok := sc.opCaches.Get(op.ClientID(), op.RequestID()); ok {
			res.Err = cache.Err
			break
		}
		cfg := sc.configs[len(sc.configs)-1].Join(arg.Servers)
		sc.configs = append(sc.configs, cfg)
		res.Err = ErrOk
		sc.opCaches.Add(res)
		sc.logger.Infof("request_id=%s apply Join success: %v", op.RequestID(), cfg)
	case LeaveArgs:
		if cache, ok := sc.opCaches.Get(op.ClientID(), op.RequestID()); ok {
			res.Err = cache.Err
			break
		}
		cfg := sc.configs[len(sc.configs)-1].Leave(arg.GIDs)
		sc.configs = append(sc.configs, cfg)
		res.Err = ErrOk
		sc.opCaches.Add(res)
		sc.logger.Infof("request_id=%s apply Leave success: %v", op.RequestID(), cfg)
	case MoveArgs:
		if cache, ok := sc.opCaches.Get(op.ClientID(), op.RequestID()); ok {
			res.Err = cache.Err
			break
		}
		cfg := sc.configs[len(sc.configs)-1].Move(arg.GID, arg.Shard)
		sc.configs = append(sc.configs, cfg)
		res.Err = ErrOk
		sc.opCaches.Add(res)
		sc.logger.Infof("request_id=%s apply Move success: %v", op.RequestID(), cfg)
	case QueryArgs:
		if arg.Num == -1 {
			res.Config = sc.configs[len(sc.configs)-1]
			res.Err = ErrOk
		} else if arg.Num < len(sc.configs) {
			res.Config = sc.configs[arg.Num]
			res.Err = ErrOk
		} else {
			res.Err = ErrNoConfig
			sc.logger.Warnf("request_id=%s apply Query failed num=%d: %v",
				op.RequestID(), arg.Num, res.Err)
		}
		if res.Err == ErrOk {
			sc.logger.Infof("request_id=%s apply Query success, num=%d config=%+v",
				op.RequestID(), arg.Num, res.Config)
		}
	default:
		sc.logger.Errorf("request_id=%s unrecognized op type", op.RequestID())
	}

	if readyCh, ok := sc.readyCh.Get(msg.CommandIndex); ok {
		readyCh <- res
	}

	if msg.CommandIndex > sc.committedIdx {
		sc.committedIdx = msg.CommandIndex
	}
}

func (sc *ShardCtrler) applySnapshot(msg raft.ApplyMsg) {
	if msg.SnapshotIndex <= sc.committedIdx {
		sc.logger.Warnf("discard outdated snapshot: SnapshotIndex=%d maxCommittedIdx=%d",
			msg.SnapshotIndex, sc.committedIdx)
		return
	}
	sc.readSnapshot(msg.Snapshot)
}

func (sc *ShardCtrler) readSnapshot(snapshot []byte) {
	// bootstrap on start
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var configs []Config
	opCaches := Caches{}
	committedIdx := 0
	if d.Decode(&configs) != nil || d.Decode(&opCaches) != nil || d.Decode(&committedIdx) != nil {
		sc.logger.Errorf("decode snapshot failed")
	} else {
		sc.configs = configs
		sc.opCaches = opCaches
		sc.committedIdx = committedIdx
		sc.logger.Infof("install snapshot index=%d", committedIdx)
	}
}

func (sc *ShardCtrler) snapshot() {
	var state []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(sc.configs); err != nil {
		sc.logger.Errorf("encode configs fail")
	}
	if err := e.Encode(sc.opCaches); err != nil {
		sc.logger.Errorf("encode opCaches fail")
	}
	if err := e.Encode(sc.committedIdx); err != nil {
		sc.logger.Errorf("encode committedIdx fail")
	}
	state = w.Bytes()
	sc.rf.Snapshot(sc.committedIdx, state)
	sc.logger.Infof("save snapshot index=%d", sc.committedIdx)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Run() {
	sc.readSnapshot(sc.persister.ReadSnapshot())
	go sc.applier()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(Config{})

	sc := &ShardCtrler{
		me:           me,
		rf:           nil,
		applyCh:      make(chan raft.ApplyMsg),
		dead:         0,
		close:        make(chan struct{}),
		maxraftstate: 1000,
		readyCh:      &ReadyCh{chs: map[int]chan OpResult{}},
		opCaches:     Caches{Caches: map[string]*Cache{}},
		curTerm:      0,
		committedIdx: 0,
		lastSave:     time.Now(),
		persister:    persister,
		snapTicker:   time.NewTicker(50 * time.Millisecond),
		configs:      make([]Config, 1),
		logger:       NewLogger(Server, strconv.Itoa(me)),
	}
	sc.configs[0].Groups = map[int][]string{}
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.Run()
	return sc
}
