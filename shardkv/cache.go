package shardkv

import (
	"encoding/json"
	"sync"
)

type Cache struct {
	Ops  map[string][]KvOpCache // clientID -> queue
	Size int
}

func NewCache(size int) *Cache {
	return &Cache{Ops: map[string][]KvOpCache{}, Size: size}
}

func (q *Cache) Add(op KVResult) {
	q.Ops[op.ClientID] = append(q.Ops[op.ClientID], op.ToCache())
	if len(q.Ops[op.ClientID]) > q.Size {
		q.Ops[op.ClientID] = q.Ops[op.ClientID][1:]
	}
}

func (q *Cache) Get(clientID, requestID string) (KvOpCache, bool) {
	for i := 0; i < len(q.Ops[clientID]); i++ {
		if q.Ops[clientID][i].RequestID == requestID {
			return q.Ops[clientID][i], true
		}
	}
	return KvOpCache{}, false
}

func (q *Cache) Clone() *Cache {
	_copy := &Cache{
		Ops:  map[string][]KvOpCache{},
		Size: q.Size,
	}
	for clientID := range q.Ops {
		_copy.Ops[clientID] = make([]KvOpCache, len(q.Ops[clientID]))
		copy(_copy.Ops[clientID], q.Ops[clientID])
	}
	return _copy
}

func (q *Cache) String() string {
	m := map[string][]string{}
	for cid, caches := range q.Ops {
		for _, c := range caches {
			m[cid] = append(m[cid], c.RequestID)
		}
	}
	marshal, _ := json.Marshal(m)
	return string(marshal)
}

// OpKVCache shard -> Cache
type OpKVCache struct {
	m map[int]*Cache
}

func (oc *OpKVCache) Get(shard int, clientID, RequestID string) (KvOpCache, bool) {
	if _, ok := oc.m[shard]; !ok {
		oc.m[shard] = NewCache(1)
	}
	return oc.m[shard].Get(clientID, RequestID)
}

func (oc *OpKVCache) Add(result KVResult) {
	if _, ok := oc.m[result.Shard]; !ok {
		oc.m[result.Shard] = NewCache(1)
	}
	oc.m[result.Shard].Add(result)
}

func (oc *OpKVCache) DeleteShard(shard int) {
	delete(oc.m, shard)
}

func (oc *OpKVCache) AddShard(shard int, cache *Cache) {
	oc.m[shard] = cache
}

func (oc *OpKVCache) ToMap(shards ...int) map[int]*Cache {
	m := map[int]*Cache{}
	if len(shards) > 0 {
		for _, shard := range shards {
			if cache, ok := oc.m[shard]; ok {
				m[shard] = cache.Clone()
			} else {
				m[shard] = NewCache(1)
			}
		}
	} else {
		for shard, cache := range oc.m {
			m[shard] = cache.Clone()
		}
	}
	return m
}

func (oc *OpKVCache) LoadMap(m map[int]*Cache) {
	//oc.mu.Lock()
	//defer oc.mu.Unlock()
	oc.m = map[int]*Cache{}
	for shard, cache := range m {
		oc.m[shard] = cache.Clone()
	}
}

type ReadyCh struct {
	mu  sync.RWMutex
	chs map[int]chan any // log index -> result
}

func (ch *ReadyCh) GetOrCreate(index int) chan any {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, ok := ch.chs[index]; !ok {
		ch.chs[index] = make(chan any, 1)
	}
	return ch.chs[index]
}

func (ch *ReadyCh) SendIfExist(index int, result any) bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	if readyCh, ok := ch.chs[index]; ok {
		readyCh <- result
		return true
	}
	return false
}

func (ch *ReadyCh) Delete(index int) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	delete(ch.chs, index)
}
