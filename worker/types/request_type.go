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
	"database/sql"
	"time"

	"github.com/CovenantSQL/CovenantSQL/crypto/asymmetric"
	"github.com/CovenantSQL/CovenantSQL/crypto/hash"
	"github.com/CovenantSQL/CovenantSQL/proto"
	hsp "github.com/CovenantSQL/HashStablePack/marshalhash"
)

//go:generate hsp
//hsp:ignore Query

// QueryType enumerates available query type, currently read/write.
type QueryType int32

const (
	// ReadQuery defines a read query type.
	ReadQuery QueryType = iota
	// WriteQuery defines a write query type.
	WriteQuery
)

// Query defines single query.
type Query struct {
	Pattern string
	Args    []sql.NamedArg
}

// RequestPayload defines a queries payload.
type RequestPayload struct {
	Queries []Query `json:"qs"`
}

// RequestHeader defines a query request header.
type RequestHeader struct {
	QueryType    QueryType        `json:"qt"`
	NodeID       proto.NodeID     `json:"id"`   // request node id
	DatabaseID   proto.DatabaseID `json:"dbid"` // request database id
	ConnectionID uint64           `json:"cid"`
	SeqNo        uint64           `json:"seq"`
	Timestamp    time.Time        `json:"t"`  // time in UTC zone
	BatchCount   uint64           `json:"bc"` // query count in this request
	QueriesHash  hash.Hash        `json:"qh"` // hash of query payload
}

// QueryKey defines an unique query key of a request.
type QueryKey struct {
	NodeID       proto.NodeID `json:"id"`
	ConnectionID uint64       `json:"cid"`
	SeqNo        uint64       `json:"seq"`
}

// SignedRequestHeader defines a signed query request header.
type SignedRequestHeader struct {
	RequestHeader
	Hash      hash.Hash             `json:"hh"`
	Signee    *asymmetric.PublicKey `json:"e"`
	Signature *asymmetric.Signature `json:"s"`
}

// Request defines a complete query request.
type Request struct {
	proto.Envelope
	Header  SignedRequestHeader `json:"h"`
	Payload RequestPayload      `json:"p"`
}

func (t QueryType) String() string {
	switch t {
	case ReadQuery:
		return "read"
	case WriteQuery:
		return "write"
	default:
		return "unknown"
	}
}

// MarshalHash defines marshal hash for Query type.
func (q *Query) MarshalHash() (o []byte, err error) {
	// Refactored from original hsp generated code
	var b []byte
	o = hsp.Require(b, q.Msgsize())
	// map header, size 2
	o = append(o, 0x82, 0x82)
	o = hsp.AppendArrayHeader(o, uint32(len(q.Args)))
	for i := range q.Args {
		if o, err = hsp.AppendIntf(o, q.Args[i]); err != nil {
			return nil, err
		}
	}
	o = append(o, 0x82)
	o = hsp.AppendString(o, q.Pattern)
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message.
func (q *Query) Msgsize() (s int) {
	// Refactored from original hsp generated code
	s = 1 + 5 + hsp.ArrayHeaderSize
	for i := range q.Args {
		s += hsp.GuessSize(q.Args[i])
	}
	s += 8 + hsp.StringPrefixSize + len(q.Pattern)
	return
}

// Verify checks hash and signature in request header.
func (sh *SignedRequestHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.RequestHeader, &sh.Hash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.Hash[:], sh.Signee) {
		return ErrSignVerification
	}
	return nil
}

// Sign the request.
func (sh *SignedRequestHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// compute hash
	if err = buildHash(&sh.RequestHeader, &sh.Hash); err != nil {
		return
	}

	if signer == nil {
		return ErrSignRequest
	}

	// sign
	sh.Signature, err = signer.Sign(sh.Hash[:])
	sh.Signee = signer.PubKey()

	return
}

// Verify checks hash and signature in whole request.
func (r *Request) Verify() (err error) {
	// verify payload hash in signed header
	if err = verifyHash(&r.Payload, &r.Header.QueriesHash); err != nil {
		return
	}
	// verify header sign
	return r.Header.Verify()
}

// Sign the request.
func (r *Request) Sign(signer *asymmetric.PrivateKey) (err error) {
	// set query count
	r.Header.BatchCount = uint64(len(r.Payload.Queries))

	// compute payload hash
	if err = buildHash(&r.Payload, &r.Header.QueriesHash); err != nil {
		return
	}

	return r.Header.Sign(signer)
}

// GetQueryKey returns a unique query key of this request.
func (sh *SignedRequestHeader) GetQueryKey() QueryKey {
	return QueryKey{
		NodeID:       sh.NodeID,
		ConnectionID: sh.ConnectionID,
		SeqNo:        sh.SeqNo,
	}
}
