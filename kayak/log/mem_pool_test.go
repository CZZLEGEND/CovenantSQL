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
	"sync"
	"testing"

	kt "github.com/CovenantSQL/CovenantSQL/kayak/types"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMemPool_Write(t *testing.T) {
	Convey("test mem pool write", t, func() {
		var p *MemPool
		p = NewMemPool()

		l1 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 0,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy1"),
		}

		var err error
		err = p.Write(l1)
		So(err, ShouldBeNil)
		So(p.logs, ShouldResemble, []*kt.Log{l1})
		err = p.Write(l1)
		So(err, ShouldNotBeNil)
		So(p.revIndex, ShouldHaveLength, 1)
		So(p.revIndex[l1.Index], ShouldEqual, 0)
		So(p.offset, ShouldEqual, 1)
		So(p.pending, ShouldBeEmpty)
		So(p.nextToCommit, ShouldEqual, 1)

		// test get
		var l *kt.Log
		l, err = p.Get(l1.Index)
		So(err, ShouldBeNil)
		So(l, ShouldResemble, l1)

		// test consecutive writes
		l2 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 1,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy2"),
		}
		err = p.Write(l2)
		So(err, ShouldBeNil)
		So(p.revIndex, ShouldHaveLength, 2)
		So(p.revIndex[l2.Index], ShouldEqual, 1)
		So(p.offset, ShouldEqual, 2)
		So(p.pending, ShouldBeEmpty)
		So(p.nextToCommit, ShouldEqual, 2)

		// test not consecutive writes
		l4 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 3,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy3"),
		}
		err = p.Write(l4)
		So(err, ShouldBeNil)
		So(p.revIndex, ShouldHaveLength, 3)
		So(p.revIndex[l4.Index], ShouldEqual, 2)
		So(p.offset, ShouldEqual, 3)
		So(p.pending, ShouldHaveLength, 1)
		So(p.pending[0], ShouldEqual, 3)
		So(p.nextToCommit, ShouldEqual, 2)

		l3 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 2,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy4"),
		}
		err = p.Write(l3)
		So(err, ShouldBeNil)
		So(p.revIndex, ShouldHaveLength, 4)
		So(p.revIndex[l3.Index], ShouldEqual, 3)
		So(p.offset, ShouldEqual, 4)
		So(p.pending, ShouldBeEmpty)
		So(p.nextToCommit, ShouldEqual, 4)

		// test truncate
		err = p.Truncate()
		So(err, ShouldBeNil)
		So(p.revIndex, ShouldBeEmpty)
		So(p.offset, ShouldEqual, 0)
		So(p.pending, ShouldBeEmpty)
		So(p.nextToCommit, ShouldEqual, 4)
		So(p.base, ShouldEqual, 4)

		// write after truncate
		err = p.Write(l1)
		So(err, ShouldNotBeNil)
		So(p.revIndex, ShouldBeEmpty)

		// write new log
		l5 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 4,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy5"),
		}
		err = p.Write(l5)
		So(err, ShouldBeNil)
		So(p.revIndex, ShouldHaveLength, 1)
		So(p.revIndex[l5.Index], ShouldEqual, 0)
		So(p.offset, ShouldEqual, 1)
		So(p.pending, ShouldBeEmpty)
		So(p.nextToCommit, ShouldEqual, 5)
		So(p.base, ShouldEqual, 4)
	})
}

func TestMemPool_Write2(t *testing.T) {
	Convey("test mem pool write", t, func() {
		l1 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 0,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy1"),
		}
		l2 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 1,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy2"),
		}
		l3 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 2,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy4"),
		}
		l4 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 3,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy3"),
		}
		l5 := &kt.Log{
			LogHeader: kt.LogHeader{
				Index: 4,
				Type:  kt.LogPrepare,
			},
			Data: []byte("happy5"),
		}

		var wg sync.WaitGroup
		var p *MemPool
		p = NewMemPool()

		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Write(l1)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Write(l2)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Write(l3)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Write(l4)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Write(l5)
		}()

		wg.Wait()

		So(p.revIndex, ShouldHaveLength, 5)
		So(p.offset, ShouldEqual, 5)
		So(p.pending, ShouldBeEmpty)
		So(p.nextToCommit, ShouldEqual, 5)
		So(p.base, ShouldEqual, 0)
	})
}
