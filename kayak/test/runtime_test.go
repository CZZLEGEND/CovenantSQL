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

package test

import (
	"testing"

	"github.com/CovenantSQL/CovenantSQL/kayak"
	"github.com/CovenantSQL/CovenantSQL/proto"
	. "github.com/smartystreets/goconvey/convey"
)

type sqliteStorage struct {
}

func newSQLiteStorage(dsn string) (s *sqliteStorage, err error) {
}

func (s *sqliteStorage) Convert(dec []byte) (data interface{}, err error) {
}

func (s *sqliteStorage) Check(data interface{}) (err error) {
}

func (s *sqliteStorage) Commit(data interface{}) (result interface{}, err error) {
}

func TestRuntime(t *testing.T) {
	Convey("runtime test", t, func() {
		db, err := newSQLiteStorage(":memory:")
		So(err, ShouldBeNil)
		peers := proto.Peers{
			PeersHeader: proto.PeersHeader{
				Leader: proto.NodeID("000005aa62048f85da4ae9698ed59c14ec0d48a88a07c15a32265634e7e64ade"),
				Servers: []proto.NodeID{
					proto.NodeID("000005aa62048f85da4ae9698ed59c14ec0d48a88a07c15a32265634e7e64ade"),
					proto.NodeID("000005f4f22c06f76c43c4f48d5a7ec1309cc94030cbf9ebae814172884ac8b5"),
				},
			},
		}
		cfg := &kayak.RuntimeConfig{
			Storage:          db,
			PrepareThreshold: 1,
			CommitThreshold:  1,
		}
	})
}
