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

package sqlchain

import (
	"fmt"
	"sync"
	"time"

	"github.com/CovenantSQL/CovenantSQL/crypto/hash"
	"github.com/CovenantSQL/CovenantSQL/kayak"
	"github.com/CovenantSQL/CovenantSQL/proto"
	ct "github.com/CovenantSQL/CovenantSQL/sqlchain/types"
	wt "github.com/CovenantSQL/CovenantSQL/worker/types"
)

// runtime represents a chain runtime state.
type runtime struct {
	wg     sync.WaitGroup
	stopCh chan struct{}

	// chainInitTime is the initial cycle time, when the Genesis blcok is produced.
	chainInitTime time.Time
	// genesisHash is the hash of genesis block.
	genesisHash hash.Hash

	// The following fields are copied from config, and should be constant during whole runtime.

	// databaseID is the current runtime database ID.
	databaseID proto.DatabaseID
	// period is the block producing cycle.
	period time.Duration
	// tick defines the maximum duration between each cycle.
	tick time.Duration
	// queryTTL sets the unacknowledged query TTL in block periods.
	queryTTL int32
	// muxServer is the multiplexing service of sql-chain PRC.
	muxService *MuxService
	// price sets query price in gases.
	price           map[wt.QueryType]uint64
	producingReward uint64
	billingPeriods  int32

	// peersMutex protects following peers-relative fields.
	peersMutex sync.Mutex
	// peers is the peer list of the sql-chain.
	peers *kayak.Peers
	// server is the local peer service instance.
	server *kayak.Server
	// index is the index of the current server in the peer list.
	index int32
	// total is the total peer number of the sql-chain.
	total int32

	// stateMutex protects following turn-relative fields.
	stateMutex sync.Mutex
	// nextTurn is the height of the next block.
	nextTurn int32
	// head is the current head of the best chain.
	head *state
	// forks is the alternative head of the sql-chain.
	forks []*state

	// timeMutex protects following time-relative fields.
	timeMutex sync.Mutex
	// offset is the time difference calculated by: coodinatedChainTime - time.Now().
	//
	// TODO(leventeliu): update offset in ping cycle.
	offset time.Duration
}

// newRunTime returns a new sql-chain runtime instance with the specified config.
func newRunTime(c *Config) (r *runtime) {
	r = &runtime{
		stopCh:          make(chan struct{}),
		databaseID:      c.DatabaseID,
		period:          c.Period,
		tick:            c.Tick,
		queryTTL:        c.QueryTTL,
		muxService:      c.MuxService,
		price:           c.Price,
		producingReward: c.ProducingReward,
		billingPeriods:  c.BillingPeriods,
		peers:           c.Peers,
		server:          c.Server,
		index: func() int32 {
			if index, found := c.Peers.Find(c.Server.ID); found {
				return index
			}

			return -1
		}(),
		total:    int32(len(c.Peers.Servers)),
		nextTurn: 1,
		head:     &state{},
		offset:   time.Duration(0),
	}

	if c.Genesis != nil {
		r.setGenesis(c.Genesis)
	}

	return
}

func (r *runtime) setGenesis(b *ct.Block) {
	r.chainInitTime = b.Timestamp()
	r.genesisHash = *b.BlockHash()
	r.head = &state{
		node:   nil,
		Head:   *b.GenesisHash(),
		Height: -1,
	}
}

func (r *runtime) getMinValidHeight() int32 {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()
	return r.nextTurn - r.queryTTL
}

func (r *runtime) queryTimeIsExpired(t time.Time) bool {
	// Checking query expiration for the pending block, whose height is c.rt.NextHeight:
	//
	//     TimeLived = r.NextTurn - r.GetHeightFromTime(t)
	//
	// Return true if:  QueryTTL < TimeLived.
	//
	// NOTE(leventeliu): as a result, a TTL=1 requires any query to be acknowledged and received
	// immediately.
	// Consider the case that a query has happened right before period h, which has height h.
	// If its ACK+Roundtrip time>0, it will be seemed as acknowledged in period h+1, or even later.
	// So, period h+1 has NextHeight h+2, and TimeLived of this query will be 2 at that time - it
	// has expired.
	//
	return r.getHeightFromTime(t) < r.getMinValidHeight()
}

// updateTime updates the current coodinated chain time.
func (r *runtime) updateTime(now time.Time) {
	r.timeMutex.Lock()
	defer r.timeMutex.Unlock()
	r.offset = time.Until(now)
}

// now returns the current coodinated chain time.
func (r *runtime) now() time.Time {
	r.timeMutex.Lock()
	defer r.timeMutex.Unlock()
	return time.Now().Add(r.offset)
}

func (r *runtime) getChainTimeString() string {
	diff := r.now().Sub(r.chainInitTime)
	height := diff / r.period
	offset := diff % r.period
	return fmt.Sprintf("[@%d+%.9f]", int32(height), offset.Seconds())
}

func (r *runtime) getNextTurn() int32 {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()
	return r.nextTurn
}

// setNextTurn prepares the runtime state for the next turn.
func (r *runtime) setNextTurn() {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()
	r.nextTurn++
}

// getQueryGas gets the consumption of gas for a specified query type.
func (r *runtime) getQueryGas(t wt.QueryType) uint64 {
	return r.price[t]
}

// stop sends a signal to the Runtime stop channel by closing it.
func (r *runtime) stop() {
	r.stopService()
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	r.wg.Wait()
}

// getHeightFromTime calculates the height with this sql-chain config of a given time reading.
func (r *runtime) getHeightFromTime(t time.Time) int32 {
	return int32(t.Sub(r.chainInitTime) / r.period)
}

// nextTick returns the current clock reading and the duration till the next turn. If duration
// is less or equal to 0, use the clock reading to run the next cycle - this avoids some problem
// caused by concurrently time synchronization.
func (r *runtime) nextTick() (t time.Time, d time.Duration) {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()
	t = r.now()
	d = r.chainInitTime.Add(time.Duration(r.nextTurn) * r.period).Sub(t)

	if d > r.tick {
		d = r.tick
	}

	return
}

func (r *runtime) updatePeers(peers *kayak.Peers) (err error) {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	index, found := peers.Find(r.server.ID)

	if found {
		r.index = index
		r.total = int32(len(peers.Servers))
		r.peers = peers
		r.server = peers.Servers[index]
	} else {
		// Just clear the server list, and the database instance should call chain.Stop() later
		r.index = -1
		r.total = 0
		r.peers.Servers = r.peers.Servers[:0]
	}

	return
}

func (r *runtime) getIndex() int32 {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	return r.index
}

func (r *runtime) getTotal() int32 {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	return r.total
}

func (r *runtime) getIndexTotal() (int32, int32) {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	return r.index, r.total
}

func (r *runtime) getIndexTotalServer() (int32, int32, *kayak.Server) {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	return r.index, r.total, r.server
}

func (r *runtime) getPeerInfoString() string {
	index, total, server := r.getIndexTotalServer()
	return fmt.Sprintf("[%d/%d] %s", index, total, server.ID)
}

func (r *runtime) getServer() *kayak.Server {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	return r.server
}

func (r *runtime) startService(chain *Chain) {
	r.muxService.register(r.databaseID, &ChainRPCService{chain: chain})
}

func (r *runtime) stopService() {
	r.muxService.unregister(r.databaseID)
}

func (r *runtime) isMyTurn() (ret bool) {
	index, total := r.getIndexTotal()
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	if r.total <= 0 {
		ret = false
	} else {
		ret = (r.nextTurn%total == index)
	}

	return
}

func (r *runtime) getPeers() *kayak.Peers {
	r.peersMutex.Lock()
	defer r.peersMutex.Unlock()
	peers := r.peers.Clone()
	return &peers
}

func (r *runtime) getHead() *state {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()
	return r.head
}

func (r *runtime) setHead(head *state) {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()
	r.head = head
}
