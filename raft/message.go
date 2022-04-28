package raft

const (
	MsgVote = iota + 1
	MsgAppend
	MsgHeartbeat
	MsgInstallSnap

	MsgVoteResp
	MsgAppendResp
	MsgHeartBeatResp
	MsgInstallSnapResp

	MsgStart
	MsgState
	MsgBeat
	MsgHup
	MsgSnap
)

type Message struct {
	Term int
	To   int
	From int
	Type int

	LogIndex        int      // AppendEntries -> PrevLogIndex or RequestVote -> LastLogIndex
	LogTerm         int      // AppendEntries -> PrevLogTerm or RequestVote -> LastLogTerm
	Entries         []Log    // AppendEntries -> Entries
	CommitIndex     int      // AppendEntries -> LeaderCommit
	Snapshot        Snapshot // InstallSnapshot -> Snapshot
	Success         bool     // RequestVote -> VoteGranted or AppendEntries -> Success
	LogInconsistent bool     // AppendEntries log inconsistent
	ConflictIndex   int      // AppendEntries -> ConflictIndex
	ConflictTerm    int      // AppendEntries -> ConflictTerm
	Command         any      // Start command -> Command
	Status          Status   // Result of GetState or Start

	reply chan Message
}

type Snapshot struct {
	Index int
	Term  int
	Data  []byte
}

type Status struct {
	Term     int
	Index    int
	IsLeader bool
}
