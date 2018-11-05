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

package kayak

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	kt "github.com/CovenantSQL/CovenantSQL/kayak/types"
	"github.com/CovenantSQL/CovenantSQL/proto"
	"github.com/CovenantSQL/CovenantSQL/rpc"
	"github.com/CovenantSQL/CovenantSQL/utils/log"
	"github.com/pkg/errors"
)

const (
	// commit channel window size
	commitWindow = 1
	// prepare window
	trackerWindow = 10
)

// Runtime defines the main kayak Runtime.
type Runtime struct {
	/// Indexes
	// index for next log.
	nextIndex uint64
	// nextFinished = last finished prepared log + 1.
	// prepare log index before nextFinished is treated as finished.
	nextFinished uint64

	/// Runtime entities
	// current node id.
	nodeID proto.NodeID
	// instance identifies kayak in multi-instance environment
	// e.g. use database id for SQLChain scenario.
	instanceID string
	// wal defines the wal for kayak.
	wal kt.Wal
	// underlying handler
	sh kt.Handler

	/// Peers info
	// peers defines the server peers.
	peers *proto.Peers
	// cached role of current node in peers, calculated from peers info.
	role proto.ServerRole
	// cached followers in peers, calculated from peers info.
	followers []proto.NodeID
	// peers lock for peers update logic.
	peersLock sync.RWMutex
	// calculated min follower nodes for prepare.
	minPreparedFollowers int
	// calculated min follower nodes for commit.
	minCommitFollowers int

	/// RPC related
	// callerMap caches the caller for peering nodes.
	callerMap sync.Map // map[proto.NodeID]Caller
	// service name for mux service.
	serviceName string
	// rpc method for coordination requests.
	rpcMethod string
	// tracks the outgoing rpc requests.
	rpcTrackCh chan *rpcTracker

	//// Parameters
	// prepare threshold defines the minimum node count requirement for prepare operation.
	prepareThreshold float64
	// commit threshold defines the minimum node count requirement for commit operation.
	commitThreshold float64
	// prepare timeout defines the max allowed time for prepare operation.
	prepareTimeout time.Duration
	// commit timeout defines the max allowed time for commit operation.
	commitTimeout time.Duration
	// channel for awaiting commits.
	commitCh chan *commitReq

	/// Sub-routines management.
	started uint32
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// commitReq defines the commit operation input.
type commitReq struct {
	ctx    context.Context
	data   interface{}
	index  uint64
	log    *kt.Log
	result chan *commitResult
}

// commitResult defines the commit operation result.
type commitResult struct {
	result interface{}
	err    error
	rpc    *rpcTracker
}

// NewRuntime creates new kayak Runtime.
func NewRuntime(cfg *kt.RuntimeConfig) (rt *Runtime, err error) {
	peers := cfg.Peers
	followers := make([]proto.NodeID, 0, len(peers.Servers))
	exists := false
	var role proto.ServerRole

	for _, v := range peers.Servers {
		if !v.IsEqual(&peers.Leader) {
			followers = append(followers, v)
		}

		if v.IsEqual(&cfg.NodeID) {
			exists = true
			if v.IsEqual(&peers.Leader) {
				role = proto.Leader
			} else {
				role = proto.Follower
			}
		}
	}

	if !exists {
		err = errors.Wrapf(kt.ErrNotInPeer, "node %v not in peers %v", cfg.NodeID, peers)
		return
	}

	// calculate fan-out count according to threshold and peers info
	minPreparedFollowers := int(math.Max(math.Ceil(cfg.PrepareThreshold*float64(len(peers.Servers))), 1) - 1)
	minCommitFollowers := int(math.Max(math.Ceil(cfg.CommitThreshold*float64(len(peers.Servers))), 1) - 1)

	rt = &Runtime{
		// handler and logs
		sh:  cfg.Handler,
		wal: cfg.Wal,

		// peers
		peers:                cfg.Peers,
		nodeID:               cfg.NodeID,
		followers:            followers,
		role:                 role,
		minPreparedFollowers: minPreparedFollowers,
		minCommitFollowers:   minCommitFollowers,

		// rpc related
		serviceName: cfg.ServiceName,
		rpcMethod:   fmt.Sprintf("%v.%v", cfg.ServiceName, cfg.MethodName),
		rpcTrackCh:  make(chan *rpcTracker, trackerWindow),

		// commits related
		prepareThreshold: cfg.PrepareThreshold,
		prepareTimeout:   cfg.PrepareTimeout,
		commitThreshold:  cfg.CommitThreshold,
		commitTimeout:    cfg.CommitTimeout,
		commitCh:         make(chan *commitReq, commitWindow),

		// stop coordinator
		stopCh: make(chan struct{}),
	}

	return
}

// Start starts the Runtime.
func (r *Runtime) Start() {
	if !atomic.CompareAndSwapUint32(&r.started, 0, 1) {
		return
	}

	// read from pool to rebuild uncommitted log map
	// TODO():

	// start commit cycle
	r.goFunc(r.commitCycle)
	// start rpc tracker collector

}

// Shutdown waits for the Runtime to stop.
func (r *Runtime) Shutdown() (err error) {
	if !atomic.CompareAndSwapUint32(&r.started, 1, 2) {
		return
	}

	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	r.wg.Wait()

	return
}

// Apply defines entry for Leader node.
func (r *Runtime) Apply(ctx context.Context, data interface{}) (result interface{}, logIndex uint64, err error) {
	r.peersLock.RLock()
	defer r.peersLock.RUnlock()

	var tmStart, tmLeaderPrepare, tmFollowerPrepare, tmLeaderRollback, tmRollback, tmCommit time.Time

	defer func() {
		fields := log.Fields{
			"r": logIndex,
		}
		if !tmLeaderPrepare.Before(tmStart) {
			fields["lp"] = tmLeaderPrepare.Sub(tmStart)
		}
		if !tmFollowerPrepare.Before(tmLeaderPrepare) {
			fields["fp"] = tmFollowerPrepare.Sub(tmLeaderPrepare)
		}
		if !tmLeaderRollback.Before(tmFollowerPrepare) {
			fields["lr"] = tmLeaderRollback.Sub(tmFollowerPrepare)
		}
		if !tmRollback.Before(tmLeaderRollback) {
			fields["fr"] = tmRollback.Sub(tmLeaderRollback)
		}
		if !tmCommit.Before(tmFollowerPrepare) {
			fields["c"] = tmCommit.Sub(tmFollowerPrepare)
		}
		log.WithFields(fields).Debug("kayak leader apply")
	}()

	if r.role != proto.Leader {
		// not leader
		err = kt.ErrNotLeader
		return
	}

	tmStart = time.Now()

	// create prepare request
	var prepareLog *kt.Log
	if prepareLog, err = r.leaderLogPrepare(data); err != nil {
		// serve error, leader could not write logs, change leader in block producer
		// TODO(): CHANGE LEADER
		return
	}

	tmLeaderPrepare = time.Now()

	// send prepare to all nodes
	prepareTracker := r.rpc(prepareLog, r.minPreparedFollowers)
	prepareCtx, prepareCtxCancelFunc := context.WithTimeout(ctx, r.prepareTimeout)
	defer prepareCtxCancelFunc()
	prepareErrors, prepareDone, _ := prepareTracker.get(prepareCtx)
	if !prepareDone {
		// timeout, rollback
		err = kt.ErrPrepareTimeout
		goto ROLLBACK
	}

	// collect errors
	if err = r.errorSummary(prepareErrors); err != nil {
		goto ROLLBACK
	}

	tmFollowerPrepare = time.Now()

	select {
	case cResult := <-r.commitResult(ctx, nil, prepareLog):
		if cResult != nil {
			logIndex = prepareLog.Index
			result = cResult.result
			err = cResult.err

			// wait until context deadline or commit done
			if cResult.rpc != nil {
				cResult.rpc.get(ctx)
			}
		} else {
			log.Fatal("IMPOSSIBLE BRANCH")
			select {
			case <-ctx.Done():
				err = errors.Wrap(ctx.Err(), "process commit timeout")
				goto ROLLBACK
			default:
			}
		}
	case <-ctx.Done():
		// pipeline commit timeout
		logIndex = prepareLog.Index
		err = errors.Wrap(ctx.Err(), "enqueue commit timeout")
		goto ROLLBACK
	}

	tmCommit = time.Now()

	return

ROLLBACK:
	// rollback local
	var rollbackLog *kt.Log
	if rollbackLog, err = r.leaderLogRollback(prepareLog.Index); err != nil {
		// serve error, construct rollback log failed, internal error
		// TODO(): CHANGE LEADER
		return
	}

	tmLeaderRollback = time.Now()

	// async send rollback to all nodes
	r.rpc(rollbackLog, 0)

	tmRollback = time.Now()

	return
}

// FollowerApply defines entry for follower node.
func (r *Runtime) FollowerApply(l *kt.Log) (err error) {
	if l == nil {
		err = errors.Wrap(kt.ErrInvalidLog, "log is nil")
		return
	}

	r.peersLock.RLock()
	defer r.peersLock.RUnlock()

	if r.role == proto.Leader {
		// not follower
		err = kt.ErrNotFollower
		return
	}

	// verify log structure
	switch l.Type {
	case kt.LogPrepare:
		return r.followerPrepare(l)
	case kt.LogRollback:
		return r.followerRollback(l)
	case kt.LogCommit:
		return r.followerCommit(l)
	case kt.LogBarrier:
		// support barrier for log truncation and peer update
		fallthrough
	case kt.LogNoop:
		// do nothing
		return r.followerNoop(l)
	}

	return
}

// UpdatePeers defines entry for peers update logic.
func (r *Runtime) UpdatePeers(peers *proto.Peers) (err error) {
	r.peersLock.Lock()
	defer r.peersLock.Unlock()

	return
}

func (r *Runtime) leaderLogPrepare(data interface{}) (*kt.Log, error) {
	// just write new log
	return r.newLog(kt.LogPrepare, data)
}

func (r *Runtime) leaderLogRollback(i uint64) (*kt.Log, error) {
	// just write new log
	return r.newLog(kt.LogRollback, i)
}

func (r *Runtime) followerPrepare(l *kt.Log) (err error) {
	if err = r.sh.Check(l.Data); err != nil {
		err = errors.Wrap(err, "follower verify log")
		return
	}

	// write log
	if err = r.wal.Write(l); err != nil {
		err = errors.Wrap(err, "write follower prepare log failed")
		return
	}

	return
}

func (r *Runtime) followerRollback(l *kt.Log) (err error) {
	if _, err = r.getPrepareLog(l); err != nil {
		err = errors.Wrap(err, "get original request failed")
		return
	}

	// write wal
	if err = r.wal.Write(l); err != nil {
		err = errors.Wrap(err, "write follower rollback log failed")
	}

	return
}

func (r *Runtime) followerCommit(l *kt.Log) (err error) {
	var prepareLog *kt.Log
	if prepareLog, err = r.getPrepareLog(l); err != nil {
		err = errors.Wrap(err, "get original request failed")
		return
	}

	cResult := <-r.commitResult(context.Background(), l, prepareLog)
	if cResult != nil {
		err = cResult.err
	}

	return
}

func (r *Runtime) commitResult(ctx context.Context, commitLog *kt.Log, prepareLog *kt.Log) (res chan *commitResult) {
	// decode log and send to commit channel to process
	res = make(chan *commitResult, 1)

	if prepareLog == nil {
		res <- &commitResult{
			err: errors.Wrap(kt.ErrInvalidLog, "nil prepare log in commit"),
		}
		return
	}

	req := &commitReq{
		ctx:    ctx,
		data:   prepareLog.Data,
		index:  prepareLog.Index,
		result: res,
		log:    commitLog,
	}

	select {
	case <-ctx.Done():
	case r.commitCh <- req:
	}

	return
}

func (r *Runtime) commitCycle() {
	// TODO(): panic recovery
	for {
		var cReq *commitReq

		select {
		case <-r.stopCh:
			return
		case cReq = <-r.commitCh:
		}

		if cReq != nil {
			r.doCommit(cReq)
		}
	}
}

func (r *Runtime) doCommit(req *commitReq) {
	r.peersLock.RLock()
	defer r.peersLock.RUnlock()

	resp := &commitResult{}

	if r.role == proto.Leader {
		resp.rpc, resp.result, resp.err = r.leaderDoCommit(req)
	} else {
		resp.err = r.followerDoCommit(req)
	}

	req.result <- resp
}

func (r *Runtime) leaderDoCommit(req *commitReq) (tracker *rpcTracker, result interface{}, err error) {
	if req.log != nil {
		// mis-use follower commit for leader
		log.Fatal("INVALID EXISTING LOG FOR LEADER COMMIT")
		return
	}

	// create leader log
	var l *kt.Log

	if l, err = r.newLog(kt.LogCommit, req.index); err != nil {
		// TODO(): record last commit
		// serve error, leader could not write log
		return
	}

	// not wrapping underlying handler commit error
	result, err = r.sh.Commit(req.data)

	// send commit
	tracker = r.rpc(l, r.minCommitFollowers)

	// TODO(): text log for rpc errors

	// TODO(): mark uncommitted nodes and remove from peers

	return
}

func (r *Runtime) followerDoCommit(req *commitReq) (err error) {
	if req.log == nil {
		log.Fatal("NO LOG FOR FOLLOWER COMMIT")
		return
	}

	// write log first
	if err = r.wal.Write(req.log); err != nil {
		err = errors.Wrap(err, "write follower commit log failed")
		return
	}

	// do commit, not wrapping underlying handler commit error
	_, err = r.sh.Commit(req.data)

	return
}

func (r *Runtime) getPrepareLog(l *kt.Log) (pl *kt.Log, err error) {
	var prepareIndex uint64

	// convert number to uint64
	switch v := l.Data.(type) {
	case int:
		prepareIndex = uint64(v)
	case uint:
		prepareIndex = uint64(v)
	case int8:
		prepareIndex = uint64(v)
	case int16:
		prepareIndex = uint64(v)
	case int32:
		prepareIndex = uint64(v)
	case int64:
		prepareIndex = uint64(v)
	case uint8:
		prepareIndex = uint64(v)
	case uint16:
		prepareIndex = uint64(v)
	case uint32:
		prepareIndex = uint64(v)
	case uint64:
		prepareIndex = uint64(v)
	default:
		err = errors.Wrap(kt.ErrInvalidLog, "log does not contains a valid prepare log index")
		return
	}

	pl, err = r.wal.Get(prepareIndex)

	return
}

func (r *Runtime) newLog(logType kt.LogType, data interface{}) (l *kt.Log, err error) {
	// allocate index
	i := atomic.AddUint64(&r.nextIndex, 1) - 1
	l = &kt.Log{
		LogHeader: kt.LogHeader{
			Index:    i,
			Type:     logType,
			Producer: r.nodeID,
		},
		Data: data,
	}

	// error write will be a fatal error, cause to node to fail fast
	if err = r.wal.Write(l); err != nil {
		log.Fatal("WRITE LOG FAILED")
	}

	return
}

func (r *Runtime) errorSummary(errs map[proto.NodeID]error) error {
	failNodes := make([]proto.NodeID, 0, len(errs))

	for s, err := range errs {
		if err != nil {
			failNodes = append(failNodes, s)
		}
	}

	if len(failNodes) == 0 {
		return nil
	}

	return errors.Wrapf(kt.ErrPrepareFailed, "fail on nodes: %v", failNodes)
}

/// rpc related
func (r *Runtime) rpc(l *kt.Log, minCount int) (tracker *rpcTracker) {
	req := &kt.RPCRequest{
		Instance: r.instanceID,
		Log:      l,
	}

	tracker = newTracker(r, req, minCount)
	tracker.send()

	// TODO(): track this rpc

	// TODO(): log remote errors

	return
}

func (r *Runtime) getCaller(id proto.NodeID) Caller {
	var caller Caller = rpc.NewPersistentCaller(id)
	rawCaller, _ := r.callerMap.LoadOrStore(id, caller)
	return rawCaller.(Caller)
}

// SetCaller injects caller for test purpose.
func (r *Runtime) SetCaller(id proto.NodeID, c Caller) {
	r.callerMap.Store(id, c)
}

// RemoveCaller removes cached caller.
func (r *Runtime) RemoveCaller(id proto.NodeID) {
	r.callerMap.Delete(id)
}

func (r *Runtime) goFunc(f func()) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f()
	}()
}

//// future extensions, barrier, noop log placeholder etc.
func (r *Runtime) followerNoop(l *kt.Log) (err error) {
	return r.wal.Write(l)
}
