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
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"

	kt "github.com/CovenantSQL/CovenantSQL/kayak/types"
	"github.com/pkg/errors"
)

const (
	// LogObjectMagic defines the magic number for log object in file.
	LogObjectMagic = 0xfb709394
	// MinFileSize defines the minimum log file size.
	MinFileSize = 4 * 1024 * 1024
)

var (
	// ErrOpenFailed represents error opening log pool file.
	ErrOpenFailed = errors.New("open pool file failed")
)

type fileIndex struct {
	NextToCommit uint64
	LogIndex     map[uint64]int64
}

// FilePool defines pool using plain file as storage.
type FilePool struct {
	sync.RWMutex
	closed uint32

	fb *os.File
	fi fileIndex
}

// NewFilePool return new file pool for log.
func NewFilePool(filename string) (p *FilePool, err error) {
	p = &FilePool{}

	indexFileName := filename + ".index"

	var st os.FileInfo
	if st, err = os.Stat(filename); err == nil && (st.IsDir() || !st.Mode().IsRegular()) {
		err = errors.Wrapf(ErrOpenFailed, "%s is not a regular file", filename)
		return
	} else if err != nil && !os.IsNotExist(err) {
		return
	} else if err != nil {
		err = nil
	}

	if st, err = os.Stat(indexFileName); err == nil && (st.IsDir() || !st.Mode().IsRegular()) {
		err = errors.Wrapf(ErrOpenFailed, "%s is not a regular file", indexFileName)
		return
	} else if err != nil && !os.IsNotExist(err) {
		return
	} else if err == nil {
		// load index
		fileBytes, _ := ioutil.ReadFile(indexFileName)
		json.Unmarshal(fileBytes, &p.fi)
	} else {
		err = nil
	}

	if p.fi.LogIndex == nil {
		p.fi.LogIndex = make(map[uint64]int64)
	}

	if p.fb, err = os.OpenFile(filename, os.O_CREATE, 0600); err != nil {
		return
	}

	defer func() {
		if err != nil {
			p.fb.Close()
			p = nil
		}
	}()

	// truncate?
	if st, err = p.fb.Stat(); err != nil {
		return
	}

	if st.Size() < MinFileSize {
		p.fb.Truncate(MinFileSize)
	}

	// have file index? seek to next write position.
	maxPos := int64(0)

	for _, v := range p.fi.LogIndex {
		if v > maxPos {
			maxPos = v
		}
	}

	if _, err = p.fb.Seek(maxPos, io.SeekStart); err != nil {
		return
	}

	// trigger read, switch to new write pos
	_, err = p.Read()

	return
}

// Write implements Pool.Write.
func (p *FilePool) Write(l *kt.Log) (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	// TODO(): not implemented
	return
}

// Read implements Pool.Read.
func (p *FilePool) Read() (l *kt.Log, err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	// TODO(): not implemented
	return
}

// Seek implements Pool.Seek.
func (p *FilePool) Seek(offset uint64) (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	// TODO(): not implemented
	return
}

// Truncate implements Pool.Truncate.
func (p *FilePool) Truncate() (err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	// TODO(): not implemented
	return
}

// Get implements Pool.Get.
func (p *FilePool) Get(index uint64) (l *kt.Log, err error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		err = ErrPoolClosed
		return
	}

	// TODO(): not implemented
	return
}

// Close implements Pooo.Close.
func (p *FilePool) Close() {
	if atomic.CompareAndSwapUint32(&p.closed, 0, 1) {
		return
	}

	if p.fb != nil {
		p.fb.Close()
	}

	return
}
