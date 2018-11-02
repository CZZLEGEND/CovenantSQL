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

package types

import (
	"github.com/CovenantSQL/CovenantSQL/proto"
)

//go:generate hsp

// LogType defines the log type.
type LogType uint16

const (
	// LogPrepare defines the prepare phase of a commit.
	LogPrepare LogType = iota
	// LogPrepare defines the rollback phase of a commit.
	LogRollback
	// LogCommit defines the commit phase of a commit.
	LogCommit
	// LogBarrier defines barrier log, all open windows should be waiting this operations to complete.
	LogBarrier
	// LogNop defines noop log.
	LogNoop
)

func (t LogType) String() (s string) {
	switch t {
	case LogPrepare:
		return "LogPrepare"
	case LogRollback:
		return "LogRollback"
	case LogCommit:
		return "LogCommit"
	case LogBarrier:
		return "LogBarrier"
	case LogNoop:
		return "LogNoop"
	default:
		return
	}
}

// LogHeader defines the checksum header structure.
type LogHeader struct {
	Index    uint64       // log index
	Type     LogType      // log type
	Producer proto.NodeID // producer node

	// TODO(): consider checksum of the log body in case of corruption
}

// Log defines the log data structure.
type Log struct {
	LogHeader

	// TODO(): consider sign the header hash by producer to prove log is authentic

	Data []byte // log payload, encode by msgpack
}