/*
 * Copyright 2018 The CovenantSQL Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package log

import (
	"io"
	"sort"
	"sync"
	"sync/atomic"

	kt "github.com/CovenantSQL/CovenantSQL/kayak/types"
)

type MemPool struct {
	sync.RWMutex
	logs     []*kt.Log
	revIndex map[uint64]int

	offset       uint64
	base         uint64
	nextToCommit uint64
	pendingLock  sync.Mutex
	pending      []uint64
	closed       uint32
}

func NewMemPool() (p *MemPool) {
	p = &MemPool{
		revIndex: make(map[uint64]int),
	}

	return
}

func (p *MemPool) Write(l *kt.Log) (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	if l == nil {
		err = ErrInvalidLog
		return
	}

	if l.Index < atomic.LoadUint64(&p.nextToCommit) {
		err = ErrAlreadyExists
		return
	}

	p.Lock()
	offset := atomic.AddUint64(&p.offset, 1) - 1
	p.logs = append(p.logs, nil)
	copy(p.logs[offset+1:], p.logs[offset:])
	p.logs[offset] = l
	p.revIndex[l.Index] = int(offset)
	p.Unlock()

	// advance nextToCommit
	if atomic.CompareAndSwapUint64(&p.nextToCommit, l.Index, l.Index+1) {
		// process pending
		p.pendingLock.Lock()
		defer p.pendingLock.Unlock()

		for len(p.pending) > 0 {
			if !atomic.CompareAndSwapUint64(&p.nextToCommit, p.pending[0], p.pending[0]+1) {
				break
			}
			p.pending = p.pending[1:]
		}
	} else {
		p.pendingLock.Lock()
		defer p.pendingLock.Unlock()

		i := sort.Search(len(p.pending), func(i int) bool {
			return p.pending[i] >= l.Index
		})

		if len(p.pending) == i || p.pending[i] != l.Index {
			p.pending = append(p.pending, 0)
			copy(p.pending[i+1:], p.pending[i:])
			p.pending[i] = l.Index
		}
	}

	return
}

func (p *MemPool) Read() (l *kt.Log, err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	p.RLock()
	defer p.RUnlock()

	index := atomic.AddUint64(&p.offset, 1)
	if index >= uint64(len(p.logs)) {
		// error
		err = io.EOF
		return
	}

	l = p.logs[index]

	return
}

func (p *MemPool) Seek(offset uint64) (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	atomic.StoreUint64(&p.offset, offset)
	return
}

func (p *MemPool) Truncate() (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	p.Lock()
	defer p.Unlock()

	newBase := atomic.LoadUint64(&p.nextToCommit)

	removed := 0
	for i := range p.logs {
		j := i - removed
		if p.logs[j].Index < newBase {
			delete(p.revIndex, p.logs[j].Index)
			p.logs = p.logs[:j+copy(p.logs[j:], p.logs[j+1:])]
			removed++
		}
	}

	for _, v := range p.logs {
		p.revIndex[v.Index] -= removed
	}

	atomic.StoreUint64(&p.base, newBase)
	atomic.StoreUint64(&p.offset, uint64(len(p.logs)))

	return
}

func (p *MemPool) Get(index uint64) (l *kt.Log, err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	if index < atomic.LoadUint64(&p.base) {
		err = ErrTruncated
		return
	}

	p.RLock()
	defer p.RUnlock()

	var i int
	var exists bool
	if i, exists = p.revIndex[index]; !exists {
		err = ErrNotExists
		return
	}

	l = p.logs[i]

	return
}

func (p *MemPool) Close() {
	if !atomic.CompareAndSwapUint32(&p.closed, 0, 1) {
		return
	}
}
