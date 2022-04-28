package kvraft

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const Debug = false

var (
	ErrOk          = Err("OK")
	ErrWrongLeader = Err("Wrong Leader")
	ErrNoKey       = Err("No Key")
	ErrTimeout     = Err("Timeout")
	ErrKill        = Err("Server Close")
	ErrNetwork     = Err("Network Failed")
)

type Err string

type BaseArg struct {
	RequestID string
	ClientID  string
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
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	BaseReply
}

type GetArgs struct {
	BaseArg
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	BaseReply
	Value string
}

var DebugStart = time.Now()

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
