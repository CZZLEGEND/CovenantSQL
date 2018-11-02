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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	kl "github.com/CovenantSQL/CovenantSQL/kayak/log"
	kt "github.com/CovenantSQL/CovenantSQL/kayak/types"
	"github.com/CovenantSQL/CovenantSQL/proto"
	"github.com/CovenantSQL/CovenantSQL/rpc"
	"github.com/CovenantSQL/CovenantSQL/utils/log"
	"github.com/pkg/errors"
)

var (
	ErrNotLeader      = errors.New("not leader")
	ErrNotFollower    = errors.New("not follower")
	ErrPrepareTimeout = errors.New("prepare timeout")
	ErrPrepareFailed  = errors.New("prepare failed")
	ErrInvalidLog     = errors.New("invalid log")
	ErrNotInPeer      = errors.New("node not in peer")
)

type Caller interface {
	Call(method string, req interface{}, resp interface{}) error
}

// RuntimeConfig defines the runtime config of kayak.
type RuntimeConfig struct {
	Storage          Storage
	PrepareThreshold int
	CommitThreshold  int
	PrepareTimeout   time.Duration
	CommitTimeout    time.Duration
	Peers            *proto.Peers
	Pool             kl.Pool
	NodeID           proto.NodeID
	ServiceName      string
	MethodName       string
}

// Storage defines the main underlying fsm of kayak.
type Storage interface {
	Convert([]byte) (interface{}, error)
	Check(interface{}) error
	Commit(interface{}) (interface{}, error)
}

// Runtime defines the main kayak Runtime.
type Runtime struct {
	/// indexes
	// index for next log.
	// TODO():
	nextIndex uint64
	// TODO():
	// index for next committed log.
	nextCommitted uint64

	/// underlying Storage
	st Storage

	/// rpc
	sv *muxService

	/// Runtime entities
	// node
	nodeID           proto.NodeID
	dbID             proto.DatabaseID
	logPool          kl.Pool
	peers            *proto.Peers
	role             proto.ServerRole
	followers        []proto.NodeID
	peerLock         sync.RWMutex
	callerMap        sync.Map // map[proto.NodeID]*rpc.PersistentCaller
	serviceName      string
	rpcMethod        string // serviceName ".Apply"
	prepareThreshold int
	commitThreshold  int
	prepareTimeout   time.Duration
	commitTimeout    time.Duration
	pendingRPCLock   sync.Mutex
	pendingRPCs      []*rpcTracker

	// wait group for sub-routines.
	wg sync.WaitGroup

	/// Runtime channels
	// channels for request coordination.
	commitCh chan *commitReq
	stopCh   chan struct{}
	started  uint32

	/// time oracles
	// time offset
	timeMux sync.Mutex
	offset  time.Duration
}

type RPCRequest struct {
	DB  proto.DatabaseID
	Log *kt.Log
}

type commitReq struct {
	ctx    context.Context
	data   interface{}
	index  uint64
	log    *kt.Log
	result chan *commitResult
}

type commitResult struct {
	result interface{}
	err    error
	rpc    *rpcTracker
}

type rpcTracker struct {
	r        *Runtime
	nodes    []proto.NodeID
	method   string
	req      interface{}
	minCount int
	doneCh   chan struct{}

	wg       sync.WaitGroup
	errLock  sync.RWMutex
	errors   map[proto.NodeID]error
	complete uint32
	sent     uint32
	closed   uint32
}

func newTracker(r *Runtime, req interface{}, minCount int) (t *rpcTracker) {
	// copy nodes
	nodes := append([]proto.NodeID(nil), r.followers...)

	if minCount > len(nodes) {
		minCount = len(nodes)
	}
	if minCount < 0 {
		minCount = 0
	}

	t = &rpcTracker{
		r:        r,
		nodes:    nodes,
		method:   r.rpcMethod,
		req:      req,
		minCount: minCount,
		errors:   make(map[proto.NodeID]error, len(nodes)),
		doneCh:   make(chan struct{}),
	}

	return
}

func (t *rpcTracker) send() {
	if !atomic.CompareAndSwapUint32(&t.sent, 0, 1) {
		return
	}

	for i := range t.nodes {
		t.wg.Add(1)
		go t.callSingle(i)
	}

	if t.minCount == 0 {
		t.done()
	}
}

func (t *rpcTracker) callSingle(idx int) {
	err := t.r.getCaller(t.nodes[idx]).Call(t.method, t.req, nil)
	t.errLock.Lock()
	t.errors[t.nodes[idx]] = err
	t.errLock.Unlock()
	if atomic.AddUint32(&t.complete, 1) >= uint32(t.minCount) {
		t.done()
	}
}

func (t *rpcTracker) done() {
	select {
	case <-t.doneCh:
	default:
		close(t.doneCh)
	}
}

func (t *rpcTracker) get(ctx context.Context) (errors map[proto.NodeID]error, meets bool, finished bool) {
	for {
		select {
		case <-t.doneCh:
			meets = true
		default:
		}

		select {
		case <-ctx.Done():
		case <-t.doneCh:
			meets = true
		}

		break
	}

	t.errLock.RLock()
	defer t.errLock.RUnlock()

	errors = make(map[proto.NodeID]error)

	for s, e := range t.errors {
		errors[s] = e
	}

	if !meets && len(errors) >= t.minCount {
		meets = true
	}

	if len(errors) == len(t.nodes) {
		finished = true
	}

	return
}

func (t *rpcTracker) close() {
	if !atomic.CompareAndSwapUint32(&t.closed, 0, 1) {
		return
	}

	t.wg.Wait()
}

// NewRuntime creates new kayak Runtime.
func NewRuntime(cfg *RuntimeConfig) (rt *Runtime, err error) {
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
		err = ErrNotInPeer
		return
	}

	rt = &Runtime{
		st:               cfg.Storage,
		logPool:          cfg.Pool,
		peers:            cfg.Peers,
		nodeID:           cfg.NodeID,
		followers:        followers,
		role:             role,
		serviceName:      cfg.ServiceName,
		rpcMethod:        fmt.Sprintf("%v.%v", cfg.ServiceName, cfg.MethodName),
		prepareThreshold: cfg.PrepareThreshold,
		prepareTimeout:   cfg.PrepareTimeout,
		commitThreshold:  cfg.CommitThreshold,
		commitTimeout:    cfg.CommitTimeout,
		commitCh:         make(chan *commitReq),
		stopCh:           make(chan struct{}),
	}
	return
}

func (r *Runtime) Start() {
	if !atomic.CompareAndSwapUint32(&r.started, 0, 1) {
		return
	}
	r.goFunc(r.commitCycle)
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

// updateTime updates the current coordinated time.
func (r *Runtime) updateTime(now time.Time) {
	r.timeMux.Lock()
	defer r.timeMux.Unlock()
	r.offset = time.Until(now)
}

// now returns the current coordinated time.
func (r *Runtime) now() time.Time {
	r.timeMux.Lock()
	defer r.timeMux.Unlock()
	return time.Now().Add(r.offset)
}

/// Leader logic
func (r *Runtime) newLog(logType kt.LogType, data []byte) (l *kt.Log, err error) {
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
	err = r.logPool.Write(l)

	return
}

func (r *Runtime) Apply(ctx context.Context, data []byte) (result interface{}, logIndex uint64, err error) {
	r.peerLock.RLock()
	defer r.peerLock.RUnlock()

	if r.role != proto.Leader {
		// not leader
		err = ErrNotLeader
		return
	}

	// create prepare request
	var prepareLog *kt.Log
	if prepareLog, err = r.leaderLogPrepare(data); err != nil {
		// serve error, leader could not write logs, change leader in block producer
		// TODO(): CHANGE LEADER
		log.Fatal("FATAL")
		return
	}

	// send prepare to all nodes
	prepareTracker := r.rpc(prepareLog, r.prepareThreshold-1)
	prepareCtx, prepareCtxCancelFunc := context.WithTimeout(ctx, r.prepareTimeout)
	defer prepareCtxCancelFunc()
	prepareErrors, prepareDone, _ := prepareTracker.get(prepareCtx)
	if !prepareDone {
		// timeout, rollback
		err = ErrPrepareTimeout
		goto ROLLBACK
	}

	// collect errors
	if err = r.errorSummary(prepareErrors); err != nil {
		goto ROLLBACK
	}

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
			log.Warningf("WEIRED")
			select {
			case <-ctx.Done():
				err = ctx.Err()
				goto ROLLBACK
			default:
			}
		}
	case <-ctx.Done():
		// pipeline commit timeout
		logIndex = prepareLog.Index
		err = ctx.Err()
		goto ROLLBACK
	}

	return

ROLLBACK:
	// rollback local
	var rollbackLog *kt.Log
	if rollbackLog, err = r.leaderLogRollback(prepareLog.Index); err != nil {
		// serve error, construct rollback log failed, internal error
		// TODO(): CHANGE LEADER
		log.Fatal("FATAL")
		return
	}

	// async send rollback to all nodes
	r.rpc(rollbackLog, 0)

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

	return errors.Wrapf(ErrPrepareFailed, "fail on nodes: %v", failNodes)
}

func (r *Runtime) commitResult(ctx context.Context, commitLog *kt.Log, prepareLog *kt.Log) (res chan *commitResult) {
	// decode log and send to commit channel to process
	res = make(chan *commitResult, 1)

	var d interface{}
	var err error
	if d, err = r.st.Convert(prepareLog.Data); err != nil {
		res <- &commitResult{
			err: err,
		}
		return
	}

	req := &commitReq{
		ctx:    ctx,
		data:   d,
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
	for {
		var cReq *commitReq

		select {
		case <-r.stopCh:
			return
		case cReq = <-r.commitCh:
		}

		if cReq == nil {
			// next
			continue
		}

		r.doCommit(cReq)
	}
}

func (r *Runtime) doCommit(req *commitReq) {
	r.peerLock.RLock()
	defer r.peerLock.RUnlock()

	resp := &commitResult{}

	if r.role == proto.Leader {
		resp.rpc, resp.result, resp.err = r.leaderDoCommit(req)
	} else {
		resp.err = r.followerDoCommit(req)
	}

	req.result <- resp
}

func (r *Runtime) leaderDoCommit(req *commitReq) (tracker *rpcTracker, result interface{}, err error) {
	// assert log is empty
	if req.log != nil {
		// fatal
		log.Fatal("FATAL")
		return
	}

	// create leader log
	var l *kt.Log

	if l, err = r.newLog(kt.LogCommit, r.uint64ToBytes(req.index)); err != nil {
		// serve error, leader could not write log
		// TODO(): CHANGE LEADER
		log.Fatal("FATAL")
		return
	}

	result, err = r.st.Commit(req.data)

	// send commit
	tracker = r.rpc(l, r.commitThreshold-1)

	return
}

func (r *Runtime) followerDoCommit(req *commitReq) (err error) {
	if req.log == nil {
		log.Fatal("FATAL")
		return
	}

	// write log first
	if err = r.logPool.Write(req.log); err != nil {
		// TODO(): suicide node
		log.Fatal("FATAL")
		return
	}

	// do commit
	_, err = r.st.Commit(req.data)

	return
}

func (r *Runtime) leaderLogPrepare(data []byte) (*kt.Log, error) {
	// write log
	return r.newLog(kt.LogPrepare, data)
}

func (r *Runtime) leaderLogRollback(i uint64) (*kt.Log, error) {
	// write log
	return r.newLog(kt.LogRollback, r.uint64ToBytes(i))
}

/// Follower logic
func (r *Runtime) FollowerApply(l *kt.Log) (err error) {
	r.peerLock.RLock()
	defer r.peerLock.RUnlock()

	if r.role == proto.Leader {
		// not follower
		err = ErrNotFollower
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
		// TODO():
		fallthrough
	case kt.LogNoop:
		// do nothing
		return r.followerNoop(l)
	}

	return
}

func (r *Runtime) followerNoop(l *kt.Log) (err error) {
	return r.logPool.Write(l)
}

func (r *Runtime) followerPrepare(l *kt.Log) (err error) {
	// verify
	var d interface{}
	if d, err = r.st.Convert(l.Data); err != nil {
		return
	}

	if err = r.st.Check(d); err != nil {
		return
	}

	// write log
	err = r.logPool.Write(l)

	return
}

func (r *Runtime) followerRollback(l *kt.Log) (err error) {
	if _, err = r.getPrepareLog(l); err == kl.ErrTruncated {
		err = nil
	} else if err != nil {
		return
	}

	// write log
	err = r.logPool.Write(l)

	return
}

func (r *Runtime) followerCommit(l *kt.Log) (err error) {
	var prepareLog *kt.Log
	if prepareLog, err = r.getPrepareLog(l); err != nil {
		return
	}

	cResult := <-r.commitResult(context.Background(), l, prepareLog)
	if cResult != nil {
		err = cResult.err
	}

	return
}

func (r *Runtime) getPrepareLog(l *kt.Log) (pl *kt.Log, err error) {
	var prepareIndex uint64
	if prepareIndex, err = r.bytesToUint64(l.Data); err != nil {
		return
	}

	pl, err = r.logPool.Get(prepareIndex)

	return
}

// common logic
func (r *Runtime) rpc(l *kt.Log, minCount int) (tracker *rpcTracker) {
	req := &RPCRequest{
		DB:  r.dbID,
		Log: l,
	}

	tracker = newTracker(r, req, minCount)
	tracker.send()

	// TODO(): pending rpc
	r.pendingRPCLock.Lock()
	defer r.pendingRPCLock.Unlock()
	r.pendingRPCs = append(r.pendingRPCs, tracker)

	return
}

func (r *Runtime) getCaller(id proto.NodeID) Caller {
	rawCaller, _ := r.callerMap.LoadOrStore(id, rpc.NewPersistentCaller(id))
	return rawCaller.(Caller)
}

func (r *Runtime) SetCaller(id proto.NodeID, c Caller) {
	r.callerMap.Store(id, c)
}

func (r *Runtime) goFunc(f func()) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f()
	}()
}

func (r *Runtime) uint64ToBytes(i uint64) (res []byte) {
	res = make([]byte, 8)
	binary.BigEndian.PutUint64(res, i)
	return
}

func (r *Runtime) bytesToUint64(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, ErrInvalidLog
	}
	return binary.BigEndian.Uint64(b), nil
}
