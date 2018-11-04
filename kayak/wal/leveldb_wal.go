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

package wal

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"

	kt "github.com/CovenantSQL/CovenantSQL/kayak/types"
	"github.com/CovenantSQL/CovenantSQL/utils"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	lu "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	// logKeyPrefix defines the leveldb log file log key prefix.
	logKeyPrefix = []byte{'L', '_'}
	baseIndexKey = []byte{'B', 'I'}
)

// LevelDBWal defines a pool using leveldb as storage.
type LevelDBWal struct {
	db     *leveldb.DB
	closed uint32

	// index offsets
	baseLock    sync.RWMutex
	base        uint64
	offset      uint64
	pendingLock sync.Mutex
	pending     []uint64
}

// NewLevelDBWal returns new leveldb pool instance.
func NewLevelDBWal(filename string) (p *LevelDBWal, err error) {
	p = &LevelDBWal{}
	if p.db, err = leveldb.OpenFile(filename, nil); err != nil {
		err = errors.Wrap(err, "open database failed")
	}

	// load current base
	var baseValue []byte
	if baseValue, err = p.db.Get(baseIndexKey, nil); err != nil && err != leveldb.ErrNotFound {
		err = errors.Wrap(err, "load pool base index failed")
		return
	}

	if err == nil {
		// decode base
		p.base = p.bytesToUint64(baseValue)
	}

	// loading pending logs
	// TODO():

	return
}

// Write implements Wal.Write.
func (p *LevelDBWal) Write(l *kt.Log) (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	if l == nil {
		err = ErrInvalidLog
		return
	}

	p.baseLock.RLock()
	defer p.baseLock.RUnlock()

	if l.Index < p.base+atomic.LoadUint64(&p.offset) {
		// already exists
		err = ErrAlreadyExists
		return
	}

	// build key
	key := append(append([]byte(nil), logKeyPrefix...), p.uint64ToBytes(l.Index)...)

	// encode
	var enc *bytes.Buffer
	if enc, err = utils.EncodeMsgPack(l); err != nil {
		return
	}

	// save
	if err = p.db.Put(key, enc.Bytes(), nil); err != nil {
		return
	}

	// update offset
	if atomic.CompareAndSwapUint64(&p.offset, l.Index-p.base, l.Index-p.base+1) {
		// process pending
		p.pendingLock.Lock()
		defer p.pendingLock.Unlock()

		for len(p.pending) > 0 {
			offset := p.pending[0] - p.base
			if !atomic.CompareAndSwapUint64(&p.offset, offset, offset+1) {
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

		if len(p.pending) == 1 || p.pending[i] != l.Index {
			p.pending = append(p.pending, 0)
			copy(p.pending[i+1:], p.pending[i:])
			p.pending[i] = l.Index
		}
	}

	return
}

// Read implements Wal.Read.
func (p *LevelDBWal) Read() (l *kt.Log, err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	var enc []byte
	if enc, err = func() ([]byte, error) {
		p.baseLock.RLock()
		defer p.baseLock.RUnlock()

		offset := atomic.AddUint64(&p.offset, 1)

		// build key
		key := append(append([]byte(nil), logKeyPrefix...), p.uint64ToBytes(p.base+offset-1)...)

		// read
		return p.db.Get(key, nil)
	}(); err != nil {
		err = errors.Wrap(err, "read log from database failed")
		return
	}

	// decode
	err = utils.DecodeMsgPack(enc, &l)

	return
}

// Seek implements Wal.Seek.
func (p *LevelDBWal) Seek(offset uint64) (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	atomic.StoreUint64(&p.offset, offset)
	return
}

// Truncate implements Wal.Truncate.
func (p *LevelDBWal) Truncate() (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	p.baseLock.Lock()
	defer p.baseLock.Unlock()

	// delete range
	startKey := append(append([]byte(nil), logKeyPrefix...), p.uint64ToBytes(p.base)...)
	endKey := append(append([]byte(nil), logKeyPrefix...), p.uint64ToBytes(p.offset)...)

	it := p.db.NewIterator(&lu.Range{Start: startKey, Limit: endKey}, nil)
	defer it.Release()

	for it.Next() {
		if err = p.db.Delete(it.Key(), nil); err != nil {
			return
		}
	}

	if err = it.Error(); err != nil {
		return
	}

	// set pointer
	p.base = atomic.SwapUint64(&p.offset, 0)

	return
}

// Get implements Wal.Get.
func (p *LevelDBWal) Get(i uint64) (l *kt.Log, err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	var enc []byte
	if enc, err = func() (e []byte, err error) {
		p.baseLock.RLock()
		defer p.baseLock.RUnlock()

		if i < p.base {
			err = ErrTruncated
			return
		}

		key := append(append([]byte(nil), logKeyPrefix...), p.uint64ToBytes(i)...)

		return p.db.Get(key, nil)
	}(); err == ErrTruncated {
		return
	} else if err != nil {
		err = errors.Wrap(err, "read log from database failed")
		return
	}

	// decode
	err = utils.DecodeMsgPack(enc, &l)

	return
}

// Close implements Wal.Close.
func (p *LevelDBWal) Close() {
	if !atomic.CompareAndSwapUint32(&p.closed, 0, 1) {
		return
	}

	if p.db != nil {
		p.db.Close()
	}
}

func (p *LevelDBWal) uint64ToBytes(o uint64) (res []byte) {
	res = make([]byte, 8)
	binary.BigEndian.PutUint64(res, o)
	return
}

func (p *LevelDBWal) bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
