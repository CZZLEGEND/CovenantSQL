/*
 * Copyright 2018 The ThunderDB Authors.
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

package client

import (
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gitlab.com/thunderdb/ThunderDB/route"
)

func TestInit(t *testing.T) {
	// test init
	Convey("test init", t, func() {
		var stopTestService func()
		var confDir string
		var err error
		stopTestService, confDir, err = startTestService()
		So(err, ShouldBeNil)
		defer stopTestService()
		err = Init(filepath.Join(confDir, "config.yaml"), []byte(""))
		So(err, ShouldBeNil)
		// test loaded block producer nodes
		bps := route.GetBPs()
		So(len(bps), ShouldBeGreaterThanOrEqualTo, 1)
		//So(bps[0].ToRawNodeID().ToNodeID(), ShouldResemble, (*conf.GConf.KnownNodes)[0].ID)
	})
}

func TestCreate(t *testing.T) {
	Convey("test create", t, func() {
		var stopTestService func()
		var err error
		stopTestService, _, err = startTestService()
		So(err, ShouldBeNil)
		defer stopTestService()
		var dsn string
		dsn, err = Create(ResourceMeta{})
		So(err, ShouldBeNil)
		So(dsn, ShouldEqual, "thunderdb://db")
	})
}

func TestDrop(t *testing.T) {
	Convey("test drop", t, func() {
		var stopTestService func()
		var err error
		stopTestService, _, err = startTestService()
		So(err, ShouldBeNil)
		defer stopTestService()
		err = Drop("thunderdb://db")
		So(err, ShouldBeNil)
	})
}
