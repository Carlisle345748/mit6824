package raft

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Debug flag
const Debug = false

var (
	DebugStart = time.Now()
	LogLevel   = log.InfoLevel
)

type Logger struct {
	Type string
	ID   string
}

func NewLogger(_type, id string) *Logger {
	log.SetLevel(LogLevel)
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

type Log struct {
	Content interface{}
	Term    int
	Index   int
}

type Logs struct {
	Log      []Log
	StartIdx int
	EndIdx   int
	Unstable bool
}

func NewLogs() *Logs {
	_log := &Logs{Log: make([]Log, 1), StartIdx: 0, EndIdx: 0, Unstable: true}
	_log.Log[0] = Log{}
	return _log
}

func (l *Logs) Get(index int) Log {
	if index < l.StartIdx || index > l.EndIdx {
		panic(fmt.Sprintf("index %d out of range", index))
	}
	return l.Log[index-l.StartIdx]
}

func (l *Logs) MGet(start, end int) []Log {
	if end < start {
		panic(fmt.Sprintf("slice bounds out of range [%d:%d]", start, end))
	}
	if start < l.StartIdx || start > l.EndIdx {
		panic(fmt.Sprintf("index %d out of range", start))
	}
	if end < l.StartIdx || end > l.EndIdx+1 {
		panic(fmt.Sprintf("index %d out of range", end))
	}
	return l.Log[start-l.StartIdx : end-l.StartIdx]
}

func (l *Logs) TruncateFront(newStart int) {
	l.Log = l.Log[newStart-l.StartIdx:]
	l.StartIdx = newStart
	l.Unstable = true
}

func (l *Logs) TruncateBack(newEnd int) {
	if newEnd < l.StartIdx {
		panic(fmt.Sprintf("new end %d < start index %d", newEnd, l.StartIdx))
	}
	l.Log = l.Log[:newEnd-l.StartIdx+1]
	l.EndIdx = newEnd
	l.Unstable = true
}

func (l *Logs) Set(log Log) {
	if log.Index < l.StartIdx || log.Index > l.EndIdx {
		panic(fmt.Sprintf("index %d out of range", log.Index))
	}
	l.Log[log.Index-l.StartIdx] = log
	l.Unstable = true
}

func (l *Logs) Reset(sentinel Log) {
	l.Log = make([]Log, 1)
	l.StartIdx = sentinel.Index
	l.EndIdx = sentinel.Index
	l.Log[0] = sentinel
	l.Unstable = true
}

func (l *Logs) Append(logs ...Log) {
	for _, log2 := range logs {
		if log2.Index != l.EndIdx+1 {
			panic(fmt.Sprintf("index %d out of order", log2.Index))
		}
		l.Log = append(l.Log, log2)
		l.EndIdx += 1
	}
	l.Unstable = true
}

func (l *Logs) IsUnstable() bool {
	return l.Unstable
}

func (l *Logs) Stabilize() {
	l.Unstable = false
}

func (l *Logs) Backtracking(conflictIndex, conflictTerm int) (nextIndex int) {
	if conflictTerm == 0 {
		return conflictIndex
	}
	for idx := l.EndIdx; idx >= l.StartIdx; idx-- {
		if l.Get(idx).Term == conflictTerm {
			return idx + 1
		}
	}
	return conflictIndex
}

func (l *Logs) String() string {
	return fmt.Sprintf("%+v", l.Log)
}

type Inflights struct {
	start  int   // the starting index in the buffer
	count  int   // number of inflights in the buffer
	size   int   // the size of the buffer
	buffer []int // buffer contains the index of the last entry inside one message.
}

func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

func (in *Inflights) Add(inflight int) {
	if in.Full() {
		panic("cannot add into a Full inflights")
	}
	next := in.start + in.count
	size := in.size
	if next >= size {
		next -= size
	}
	if next >= len(in.buffer) {
		in.grow()
	}
	in.buffer[next] = inflight
	in.count++
}

func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]int, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

func (in *Inflights) FreeLE(to int) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

func (in *Inflights) FreeFirstOne() {
	if in.count > 0 {
		in.FreeLE(in.buffer[in.start])
	}
}

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.count == in.size
}

type PreState struct {
	Term     int
	VoteFor  int
	Snapshot []byte
}

func copySlice[V int | byte | Log](arr []V) []V {
	a := make([]V, len(arr))
	copy(a, arr)
	return a
}
