package raft

import (
	"bytes"
	"math/rand"
	"strconv"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

const (
	RoleLeader    = "Leader"
	RoleFollower  = "Follower"
	RoleCandidate = "Candidate"

	VoteNull = -1
)

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	role      string              // Server Role

	currentTerm int
	votedFor    int
	snapshot    []byte
	log         *Logs
	preState    PreState // Previous hard states

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimeout  time.Time
	heartbeatTimeout time.Time
	voteCount        map[int]bool
	appendInFlight   []*Inflights // AppendEntries request rate controller

	tick func(t time.Time) // Periodical actions such as heartbeat and election
	step func(msg Message) // Command processing for current role

	recvc     chan Message    // Command and request receive channel
	applyBuf  chan []ApplyMsg // apply command buffer
	applyChan chan ApplyMsg

	close  chan struct{}
	logger *Logger
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) becomeFollower(term int) {
	rf.logger.Infof("Role: %s => %s", rf.role, RoleFollower)
	rf.role = RoleFollower
	rf.reset(term)
	rf.step = rf.stepFollower
	rf.tick = rf.tickElection
}

func (rf *Raft) becomeCandidate() {
	rf.logger.Infof("Role: %s => %s", rf.role, RoleCandidate)
	rf.role = RoleCandidate
	rf.reset(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.step = rf.stepCandidate
	rf.tick = rf.tickElection
}

func (rf *Raft) becomeLeader() {
	rf.logger.Infof("Role: %s => %s", rf.role, RoleLeader)
	rf.role = RoleLeader
	rf.step = rf.stepLeader
	rf.tick = rf.tickHeartBeat
	rf.reset(rf.currentTerm)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.EndIdx + 1
		rf.matchIndex[i] = 0
		// Higher InFlight size may fail in RPCCount and RPCBytes test.
		rf.appendInFlight[i] = NewInflights(3)
	}
}

func (rf *Raft) Step(msg Message) {
	defer rf.maybePersist() // persist state such as term

	switch {
	case msg.Term == 0:
		// local message
	case msg.Term > rf.currentTerm:
		rf.becomeFollower(msg.Term)
	case msg.Term < rf.currentTerm:
		switch msg.Type {
		case MsgVote, MsgInstallSnap, MsgHeartbeat, MsgAppend:
			msg.reply <- Message{Term: rf.currentTerm, Success: false}
		}
		rf.logger.Tracef("ignore message response from lower term %d", msg.Term)
		return
	}

	switch msg.Type {
	case MsgHup:
		rf.election()
	case MsgVote:
		reply := Message{Term: rf.currentTerm, Success: false}
		if rf.canVote(msg) {
			rf.votedFor = msg.From
			rf.resetElectionTimeout()
			reply.Success = true
			rf.logger.Infof("vote for %d", msg.From)
		}
		rf.maybePersist() // persist state before replying to peers
		msg.reply <- reply
	case MsgSnap:
		if msg.Snapshot.Index < rf.log.StartIdx {
			rf.logger.Infof("drop outdated snap cur_idx=%d snap_idx=%d", rf.log.StartIdx, msg.Snapshot.Index)
		} else {
			rf.snapshot = copySlice(msg.Snapshot.Data)
			rf.log.TruncateFront(msg.Snapshot.Index)
			rf.logger.Infof("receive snapshot index=%d", msg.Snapshot.Index)
		}
	case MsgState:
		msg.reply <- Message{Status: Status{Term: rf.currentTerm, IsLeader: rf.role == RoleLeader}}
	default:
		rf.step(msg)
	}
}

func (rf *Raft) stepFollower(msg Message) {
	switch msg.Type {
	case MsgStart:
		msg.reply <- Message{Status: Status{IsLeader: false}}
	case MsgAppend, MsgHeartbeat:
		rf.resetElectionTimeout()
		msg.reply <- rf.handleAppendEntries(msg)
	case MsgInstallSnap:
		rf.resetElectionTimeout()
		msg.reply <- rf.handleInstallSnap(msg)
	default:
		rf.logger.Tracef("ignore message %+v", msg)
	}
}

func (rf *Raft) stepCandidate(msg Message) {
	switch msg.Type {
	case MsgStart:
		msg.reply <- Message{Status: Status{IsLeader: false}}
	case MsgAppend, MsgHeartbeat:
		if msg.Term == rf.currentTerm {
			rf.becomeFollower(msg.Term)
			rf.logger.Infof("found new leader term=%d", msg.Term)
		}
		rf.resetElectionTimeout()
		msg.reply <- rf.handleAppendEntries(msg)
	case MsgInstallSnap:
		if msg.Term == rf.currentTerm {
			rf.becomeFollower(msg.Term)
			rf.logger.Infof("found new leader term=%d", msg.Term)
		}
		rf.resetElectionTimeout()
		msg.reply <- rf.handleInstallSnap(msg)
	case MsgVoteResp:
		// always msg.Term == rf.currentTerm
		if msg.Success {
			rf.logger.Infof("receive vote from %d", msg.From)
		}
		rf.voteCount[msg.From] = msg.Success
		count := 1
		for _, voteGrant := range rf.voteCount {
			if voteGrant {
				count++
			}
		}
		if count >= len(rf.peers)/2+1 {
			rf.logger.Infof("win election")
			rf.becomeLeader()
		}
	default:
		rf.logger.Tracef("ignore message %+v", msg)
	}

}

func (rf *Raft) stepLeader(msg Message) {
	switch msg.Type {
	case MsgBeat:
		rf.broadcastHeartbeat()
	case MsgStart:
		log := Log{
			Content: msg.Command,
			Term:    rf.currentTerm,
			Index:   rf.log.EndIdx + 1,
		}
		rf.log.Append(log)
		rf.maybeCommit()
		rf.maybePersist() // persist state before replying to users
		rf.broadcastAppend(true)
		msg.reply <- Message{Status: Status{Term: rf.currentTerm, Index: log.Index, IsLeader: true}}
		rf.logger.Infof("add log %+v", log)
	case MsgAppendResp, MsgHeartBeatResp:
		// If failed: start log backtracking
		if !msg.Success && msg.LogInconsistent {
			if msg.LogIndex <= rf.matchIndex[msg.From] || msg.LogIndex != rf.nextIndex[msg.From]-1 {
				// Drop stale response
				rf.logger.Warnf("drop stale from %d append msg.LogIndex=%d matchIndex=%v nextIdx=%v",
					msg.From, msg.LogIndex, rf.matchIndex, rf.nextIndex)
				return
			}
			nextIdx := rf.log.Backtracking(msg.ConflictIndex, msg.ConflictTerm)
			rf.logger.Warnf("AppendEntries/Heartbeat to S%d fails because of log inconsistency", msg.From)
			rf.logger.Warnf("update nextIndex[%d] %d => %d", msg.From, rf.nextIndex[msg.From], nextIdx)
			rf.nextIndex[msg.From] = nextIdx
			rf.appendInFlight[msg.From].FreeFirstOne() // Create a slot to prevent blocking following append
			rf.sendAppendOrSnap(msg.From, true, false)
		}
		// If successful: update nextIndex and matchIndex for follower
		if msg.Success {
			oldPaused := rf.appendInFlight[msg.From].Full()
			if len(msg.Entries) > 0 {
				lastLog := msg.Entries[len(msg.Entries)-1]
				if lastLog.Index > rf.matchIndex[msg.From] {
					rf.logger.Infof("app update %d matchIndex %d => %d nextIndex %d => %d",
						msg.From, rf.matchIndex[msg.From], lastLog.Index, rf.nextIndex[msg.From], lastLog.Index+1)
					rf.matchIndex[msg.From] = lastLog.Index
					rf.nextIndex[msg.From] = lastLog.Index + 1
				}
				rf.appendInFlight[msg.From].FreeLE(lastLog.Index)
			} else {
				if msg.LogIndex > rf.matchIndex[msg.From] {
					rf.matchIndex[msg.From] = msg.LogIndex
					rf.logger.Infof("app update %d matchIndex %d => %d",
						msg.From, msg.LogIndex, rf.matchIndex[msg.From])
				}
				rf.appendInFlight[msg.From].FreeLE(msg.LogIndex)
			}
			if rf.maybeCommit() {
				rf.applyCmd()
				// Tell follower the new leader commit
				rf.broadcastAppend(true)
			} else if oldPaused {
				// Immediately Append log to paused follower
				rf.sendAppendOrSnap(msg.From, true, false)
			}
		}
	case MsgInstallSnapResp:
		if !msg.Success || msg.Snapshot.Index <= rf.matchIndex[msg.From] {
			// Drop failed and stale response
			return
		}
		rf.logger.Infof("snap update %d matchIndex %d => %d nextIndex %d => %d", msg.From,
			rf.matchIndex[msg.From], msg.Snapshot.Index,
			rf.nextIndex[msg.From], msg.Snapshot.Index+1)
		rf.matchIndex[msg.From] = msg.Snapshot.Index
		rf.nextIndex[msg.From] = msg.Snapshot.Index + 1
	case MsgAppend, MsgHeartbeat, MsgInstallSnap:
		msg.reply <- Message{Term: rf.currentTerm, Success: false}
	default:
		rf.logger.Tracef("ignore message %+v", msg)
	}
}

func (rf *Raft) tickHeartBeat(t time.Time) {
	if t.After(rf.heartbeatTimeout) {
		rf.heartbeatTimeout = t.Add(150 * time.Millisecond)
		rf.Step(Message{From: rf.me, Type: MsgBeat})
	}
}

func (rf *Raft) tickElection(t time.Time) {
	if t.After(rf.electionTimeout) {
		rf.Step(Message{From: rf.me, Type: MsgHup})
	}
}

func (rf *Raft) election() {
	if rf.role == RoleLeader {
		rf.logger.Infof("ignore MsgHup because already leader")
		return
	}
	rf.resetElectionTimeout()
	rf.becomeCandidate()
	rf.voteCount = map[int]bool{}
	rf.logger.Infof("start election term=%d", rf.currentTerm)
	rf.broadcastRequestVote()
}

func (rf *Raft) handleAppendEntries(msg Message) (reply Message) {
	defer rf.maybePersist() // persist state before replying to peers
	rf.logger.Tracef("receive AppendEntries from %d term=%d", msg.From, msg.Term)
	reply.Term = rf.currentTerm

	// Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	if msg.LogIndex > rf.log.EndIdx {
		reply.Success = false
		reply.ConflictIndex = rf.log.EndIdx + 1
		return
	}
	if msg.LogIndex > rf.log.StartIdx && rf.log.Get(msg.LogIndex).Term != msg.LogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log.Get(msg.LogIndex).Term
		rf.log.TruncateBack(msg.LogIndex - 1)
		idx := rf.log.EndIdx
		for idx > rf.log.StartIdx && rf.log.Get(idx).Term == reply.ConflictTerm {
			idx -= 1
		}
		reply.ConflictIndex = idx + 1
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i := 0; i < len(msg.Entries); i++ {
		log := msg.Entries[i]
		if log.Index <= rf.log.EndIdx && log.Index > rf.log.StartIdx &&
			rf.log.Get(log.Index).Term != log.Term {
			rf.log.TruncateBack(log.Index - 1)
			break
		}
	}

	// Append any new entries not already in the log
	for i := 0; i < len(msg.Entries); i++ {
		log := msg.Entries[i]
		if log.Index > rf.log.EndIdx {
			rf.log.Append(log)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if msg.CommitIndex > rf.commitIndex {
		lastNewEntryIndex := msg.LogIndex + len(msg.Entries)
		commitIdx := msg.CommitIndex
		if lastNewEntryIndex < msg.CommitIndex {
			commitIdx = lastNewEntryIndex
		}
		if commitIdx > rf.commitIndex {
			rf.logger.Infof("commit log %d => %d", rf.commitIndex, commitIdx)
			rf.commitIndex = commitIdx
			rf.applyCmd()
		}
	}

	reply.Success = true
	return
}

func (rf *Raft) handleInstallSnap(msg Message) (reply Message) {
	defer rf.maybePersist() // persist state before replying to peers
	rf.logger.Infof("receive InstallSnap")
	reply.Term = rf.currentTerm

	// Drop outdated snapshots
	if msg.Snapshot.Index <= rf.log.StartIdx {
		rf.logger.Warnf("drop outdated snapshot curStart=%d snap %d", rf.log.StartIdx, msg.Snapshot.Index)
		return
	}

	if msg.Snapshot.Index > rf.log.EndIdx {
		// Snapshot contain new information not already in the recipient’s log
		rf.log.Reset(Log{Term: msg.Snapshot.Term, Index: msg.Snapshot.Index})
		rf.logger.Infof("install snapshot and discard all logs. log start at %d", msg.Snapshot.Index)
	} else {
		// Follower receives a snapshot that describes a preﬁx of its log
		rf.log.TruncateFront(msg.Snapshot.Index)
		rf.logger.Infof("install snapshot and discard prefix logs. log start at %d", msg.Snapshot.Index)
	}

	rf.snapshot = copySlice(msg.Snapshot.Data)
	if msg.Snapshot.Index > rf.commitIndex {
		rf.logger.Infof("snapshot commit %d => %d", rf.commitIndex, msg.Snapshot.Index)
		rf.commitIndex = msg.Snapshot.Index
	}
	if msg.Snapshot.Index > rf.lastApplied {
		rf.lastApplied = msg.Snapshot.Index
		rf.applyBuf <- []ApplyMsg{{
			SnapshotValid: true,
			Snapshot:      copySlice(msg.Snapshot.Data),
			SnapshotTerm:  msg.Snapshot.Term,
			SnapshotIndex: msg.Snapshot.Index},
		}
	}
	return
}

func (rf *Raft) applyCmd() {
	if rf.commitIndex <= rf.lastApplied || rf.commitIndex <= rf.log.StartIdx {
		return
	}
	oldLastApplied := rf.lastApplied
	if oldLastApplied < rf.log.StartIdx {
		oldLastApplied = rf.log.StartIdx
	}
	var applyMsgs []ApplyMsg
	for idx := oldLastApplied + 1; idx <= rf.commitIndex; idx++ {
		applyMsgs = append(applyMsgs, ApplyMsg{
			CommandValid: true,
			Command:      rf.log.Get(idx).Content,
			CommandIndex: idx})
		rf.lastApplied = idx
	}
	rf.applyBuf <- applyMsgs
}

func (rf *Raft) canVote(msg Message) bool {
	if !(rf.votedFor == VoteNull || rf.votedFor == msg.From) {
		return false
	}
	// Check if log is up-to-date
	lastLog := rf.log.Get(rf.log.EndIdx)
	return msg.LogTerm > lastLog.Term || (msg.LogTerm == lastLog.Term && msg.LogIndex >= lastLog.Index)
}

func (rf *Raft) broadcastRequestVote() {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendRequestVote(i)
		}
	}
}

func (rf *Raft) broadcastAppend(sendIfEmpty bool) {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendAppendOrSnap(i, sendIfEmpty, false)
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendAppendOrSnap(i, true, true)
		}
	}
}

func (rf *Raft) sendRequestVote(to int) {
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.log.EndIdx,
		LastLogTerm:  rf.log.Get(rf.log.EndIdx).Term,
	}
	go func() {
		reply := RequestVoteReply{}
		if ok := rf.send(to, "Raft.RequestVote", &req, &reply); !ok {
			// Server shutdown
			return
		}
		rf.recvc <- Message{
			Type:    MsgVoteResp,
			Term:    reply.Term,
			Success: reply.VoteGranted,
			To:      rf.me,
			From:    to,
		}
	}()
}

func (rf *Raft) sendAppendOrSnap(to int, sendIfEmpty, heartbeat bool) bool {
	if rf.nextIndex[to] < rf.log.StartIdx+1 {
		return rf.sendInstallSnapshot(to, heartbeat)
	}
	return rf.sendAppendEntries(to, sendIfEmpty, heartbeat)
}

func (rf *Raft) sendInstallSnapshot(to int, heartbeat bool) bool {
	if !heartbeat {
		if rf.appendInFlight[to].Full() {
			rf.logger.Infof("throttle install snapshot to %d", to)
			return false
		}
		rf.appendInFlight[to].Add(rf.log.StartIdx)
	}
	req := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.StartIdx,
		LastIncludedTerm:  rf.log.Get(rf.log.StartIdx).Term,
		Data:              copySlice(rf.snapshot),
	}
	go func() {
		reply := InstallSnapshotReply{}
		if ok := rf.send(to, "Raft.InstallSnapshot", &req, &reply); !ok {
			// Server shutdown or timeout
			return
		}
		rf.recvc <- Message{
			Term:     reply.Term,
			To:       rf.me,
			From:     to,
			Success:  req.Term == reply.Term,
			Type:     MsgInstallSnapResp,
			Snapshot: Snapshot{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm},
		}
	}()
	return true
}

func (rf *Raft) sendAppendEntries(to int, sendIfEmpty, heartbeat bool) bool {
	// Sending Append and Heartbeat are essentially the same except that heartbeat won't be throttle
	if rf.appendInFlight[to].Full() && !heartbeat {
		rf.logger.Infof("throttle append entries to %d", to)
		return false
	}

	var entries []Log
	if rf.log.EndIdx >= rf.nextIndex[to] {
		entries = copySlice(rf.log.MGet(rf.nextIndex[to], rf.log.EndIdx+1))
	} else if !sendIfEmpty {
		return false
	}
	req := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[to] - 1,
		PrevLogTerm:  rf.log.Get(rf.nextIndex[to] - 1).Term,
		Entries:      entries,
	}

	msgRespType := MsgHeartBeatResp
	if !heartbeat {
		if len(req.Entries) == 0 {
			rf.appendInFlight[to].Add(req.PrevLogIndex)
		} else {
			rf.appendInFlight[to].Add(rf.log.EndIdx)
		}
		msgRespType = MsgAppendResp
	}

	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.send(to, "Raft.AppendEntries", &req, &reply); !ok {
			// Server shutdown
			return
		}
		rf.logger.Tracef("send AppendEntries to %d success: %v", to, reply)
		rf.recvc <- Message{
			Type:            msgRespType,
			Term:            reply.Term,
			To:              rf.me,
			From:            to,
			Success:         reply.Success,
			LogInconsistent: req.Term == reply.Term,
			Entries:         req.Entries,
			LogIndex:        req.PrevLogIndex,
			LogTerm:         req.PrevLogTerm,
			ConflictIndex:   reply.ConflictIndex,
			ConflictTerm:    reply.ConflictTerm,
		}
	}()
	return true
}

func (rf *Raft) send(server int, method string, args interface{}, reply interface{}) bool {
	for {
		if ok := rf.peers[server].Call(method, args, reply); ok {
			return true
		}
		rf.logger.Tracef("send %s to %d network failed", method, server)
		select {
		case <-time.After(50 * time.Millisecond):
		case <-rf.close:
			rf.logger.Tracef("send %s to %d killed on closed", method, server)
			return false
		}
	}
}

func (rf *Raft) maybeCommit() bool {
	commitIdx, ok := rf.nextCommitIndex()
	if !ok {
		return false
	}
	rf.logger.Infof("commit log %d => %d", rf.commitIndex, commitIdx)
	rf.commitIndex = commitIdx
	return true
}

func (rf *Raft) nextCommitIndex() (int, bool) {
	var N int
	start := rf.commitIndex + 1
	if start < rf.log.StartIdx {
		start = rf.log.StartIdx + 1
	}
	for idx := start; idx <= rf.log.EndIdx; idx++ {
		if rf.log.Get(idx).Term != rf.currentTerm {
			continue
		}
		count := 1
		for i, matchIdx := range rf.matchIndex {
			if i != rf.me && matchIdx >= idx {
				count += 1
			}
		}
		if count >= len(rf.peers)/2+1 {
			N = idx
		}
	}
	if N == 0 {
		return 0, false
	}
	return N, true
}

func (rf *Raft) reset(term int) {
	if rf.currentTerm != term {
		rf.logger.Infof("update term %d => %d", rf.currentTerm, term)
		rf.currentTerm = term
		rf.votedFor = VoteNull
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(600+rand.Int63n(400)))
}

func (rf *Raft) sendMsg(msg Message) (Message, bool) {
	select {
	case rf.recvc <- msg:
	case <-rf.close:
		return Message{}, false
	}
	select {
	case reply := <-msg.reply:
		return reply, true
	case <-rf.close:
		return Message{}, false
	}
}

func (rf *Raft) resetPrestate() {
	rf.preState.Term = rf.currentTerm
	rf.preState.VoteFor = rf.votedFor
	rf.preState.Snapshot = rf.snapshot
	rf.log.Stabilize()
}

func (rf *Raft) shouldPersist() bool {
	return rf.currentTerm != rf.preState.Term || rf.votedFor != rf.preState.VoteFor ||
		rf.log.IsUnstable() || !bytes.Equal(rf.snapshot, rf.preState.Snapshot)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) maybePersist() bool {
	if !rf.shouldPersist() {
		return false
	}
	var state []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		rf.logger.Errorf("encode currentTerm fail")
	}
	if err := e.Encode(rf.votedFor); err != nil {
		rf.logger.Errorf("encode voteFor fail")
	}
	if err := e.Encode(*rf.log); err != nil {
		rf.logger.Errorf("encode Log fail")
	}
	state = w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	rf.resetPrestate()
	rf.logger.Infof("persist log start=%d end=%d", rf.log.StartIdx, rf.log.EndIdx)
	return true
}

// restore previously persisted state.
func (rf *Raft) readPersist(state, snapshot []byte) {
	rf.logger.Tracef("reader from Persist")
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log Logs
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		rf.logger.Errorf("decode persist state failed")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.snapshot = snapshot
		rf.log = &log
		rf.resetPrestate()
		rf.logger.Infof("read persist log start=%d end=%d", log.StartIdx, log.EndIdx)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	msg := Message{
		Term:     args.Term,
		To:       rf.me,
		From:     args.CandidateID,
		Type:     MsgVote,
		LogIndex: args.LastLogIndex,
		LogTerm:  args.LastLogTerm,
		reply:    make(chan Message, 1),
	}
	if msgRes, ok := rf.sendMsg(msg); ok {
		reply.Term = msgRes.Term
		reply.VoteGranted = msgRes.Success
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	msg := Message{
		Term:        args.Term,
		To:          rf.me,
		From:        args.LeaderID,
		Type:        MsgAppend,
		LogIndex:    args.PrevLogIndex,
		LogTerm:     args.PrevLogTerm,
		Entries:     copySlice(args.Entries),
		CommitIndex: args.LeaderCommit,
		reply:       make(chan Message, 1),
	}
	if msgRes, ok := rf.sendMsg(msg); ok {
		reply.Term = msgRes.Term
		reply.Success = msgRes.Success
		reply.ConflictIndex = msgRes.ConflictIndex
		reply.ConflictTerm = msgRes.ConflictTerm
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	msg := Message{
		Term: args.Term,
		To:   rf.me,
		From: args.LeaderId,
		Type: MsgInstallSnap,
		Snapshot: Snapshot{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
			Data:  copySlice(args.Data),
		},
		reply: make(chan Message, 1),
	}
	if msgRes, ok := rf.sendMsg(msg); ok {
		reply.Term = msgRes.Term
	}
}

// Start new command
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	msg := Message{
		From:    rf.me,
		Type:    MsgStart,
		Command: command,
		reply:   make(chan Message, 1),
	}
	if msgRes, ok := rf.sendMsg(msg); ok {
		result := msgRes.Status
		return result.Index, result.Term, result.IsLeader
	}
	return 0, 0, false
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	msg := Message{From: rf.me, Type: MsgState, reply: make(chan Message, 1)}
	if msgRes, ok := rf.sendMsg(msg); ok {
		return msgRes.Status.Term, msgRes.Status.IsLeader
	}
	return 0, false
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	select {
	case rf.recvc <- Message{
		From:     rf.me,
		Type:     MsgSnap,
		Snapshot: Snapshot{Data: copySlice(snapshot), Index: index}}:
	case <-rf.close:
		return
	}
}

// Kill Raft Node
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	close(rf.close)
	rf.logger.Warnf("server killed")
}

// Run Raft Node
func (rf *Raft) Run() {
	rf.tick = rf.tickElection
	rf.step = rf.stepFollower
	rf.readPersist(rf.persister.ReadRaftState(), rf.persister.ReadSnapshot())

	// Use a buffer to prevent blocking on applyChan
	go func() {
		for {
			select {
			case batch := <-rf.applyBuf:
				for _, cmd := range batch {
					rf.applyChan <- cmd
				}
				if len(batch) == 0 && batch[0].SnapshotValid {
					rf.logger.Infof("apply snapshot %d", batch[0].SnapshotIndex)
				} else {
					rf.logger.Infof("apply command %d => %d",
						batch[0].CommandIndex, batch[len(batch)-1].CommandIndex)
				}
			case <-rf.close:
				return
			}
		}
	}()
	// Raft command processing pipeline
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case msg := <-rf.recvc:
				rf.Step(msg)
			case t := <-ticker.C:
				rf.tick(t)
			case <-rf.close:
				return
			}
		}
	}()
	rf.logger.Infof("start server")
}

// Make Raft Node
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		role:             RoleFollower,
		votedFor:         VoteNull,
		log:              NewLogs(),
		electionTimeout:  time.Now().Add(time.Millisecond * time.Duration(rand.Int63n(100))),
		heartbeatTimeout: time.Now(),
		recvc:            make(chan Message, 1000),
		applyBuf:         make(chan []ApplyMsg, 1000),
		applyChan:        applyCh,
		appendInFlight:   make([]*Inflights, len(peers)),
		close:            make(chan struct{}),
		logger:           NewLogger("SER", strconv.Itoa(me)),
	}
	rf.Run()

	return rf
}
