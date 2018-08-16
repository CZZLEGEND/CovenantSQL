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

package types

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"time"

	"gitlab.com/thunderdb/ThunderDB/crypto/asymmetric"
	"gitlab.com/thunderdb/ThunderDB/crypto/hash"
	"gitlab.com/thunderdb/ThunderDB/proto"
	"gitlab.com/thunderdb/ThunderDB/utils"
)

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
	Queries []Query
}

// RequestHeader defines a query request header.
type RequestHeader struct {
	QueryType    QueryType
	NodeID       proto.NodeID     // request node id
	DatabaseID   proto.DatabaseID // request database id
	ConnectionID uint64
	SeqNo        uint64
	Timestamp    time.Time // time in UTC zone
	BatchCount   uint64    // query count in this request
	QueriesHash  hash.Hash // hash of query payload
}

// QueryKey defines an unique query key of a request.
type QueryKey struct {
	NodeID       proto.NodeID
	ConnectionID uint64
	SeqNo        uint64
}

// SignedRequestHeader defines a signed query request header.
type SignedRequestHeader struct {
	RequestHeader
	HeaderHash hash.Hash
	Signee     *asymmetric.PublicKey
	Signature  *asymmetric.Signature
}

// Request defines a complete query request.
type Request struct {
	proto.Envelope
	Header  SignedRequestHeader
	Payload RequestPayload
}

// Serialize returns byte based binary form of struct.
func (p *RequestPayload) Serialize() []byte {
	// HACK(xq262144): currently use idiomatic serialization for hash generation
	buf, _ := utils.EncodeMsgPack(p)

	return buf.Bytes()
}

// Serialize returns bytes based binary form of struct.
func (h *RequestHeader) Serialize() []byte {
	if h == nil {
		return []byte{'\000'}
	}

	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, h.QueryType)
	binary.Write(buf, binary.LittleEndian, uint64(len(h.NodeID)))
	buf.WriteString(string(h.NodeID))
	buf.WriteString(string(h.DatabaseID))
	binary.Write(buf, binary.LittleEndian, h.ConnectionID)
	binary.Write(buf, binary.LittleEndian, h.SeqNo)
	binary.Write(buf, binary.LittleEndian, int64(h.Timestamp.UnixNano())) // use nanoseconds unix epoch
	binary.Write(buf, binary.LittleEndian, h.BatchCount)
	buf.Write(h.QueriesHash[:])

	return buf.Bytes()
}

// Serialize returns bytes based binary form of struct.
func (sh *SignedRequestHeader) Serialize() []byte {
	if sh == nil {
		return []byte{'\000'}
	}

	buf := new(bytes.Buffer)

	buf.Write(sh.RequestHeader.Serialize())
	buf.Write(sh.HeaderHash[:])
	if sh.Signee != nil {
		buf.Write(sh.Signee.Serialize())
	} else {
		buf.WriteRune('\000')
	}
	if sh.Signature != nil {
		buf.Write(sh.Signature.Serialize())
	} else {
		buf.WriteRune('\000')
	}

	return buf.Bytes()
}

// Verify checks hash and signature in request header.
func (sh *SignedRequestHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.RequestHeader, &sh.HeaderHash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.HeaderHash[:], sh.Signee) {
		return ErrSignVerification
	}
	return nil
}

// Sign the request.
func (sh *SignedRequestHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// compute hash
	buildHash(&sh.RequestHeader, &sh.HeaderHash)

	if sh.Signee == nil || signer == nil {
		return ErrSignRequest
	}

	// sign
	sh.Signature, err = signer.Sign(sh.HeaderHash[:])

	return
}

// Serialize returns bytes based binary form of struct.
func (r *Request) Serialize() []byte {
	if r == nil {
		return []byte{'\000'}
	}

	buf := new(bytes.Buffer)

	buf.Write(r.Header.Serialize())
	buf.Write(r.Payload.Serialize())

	return buf.Bytes()
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
	buildHash(&r.Payload, &r.Header.QueriesHash)

	return r.Header.Sign(signer)
}

// MarshalHash marshals for hash
func (sh *SignedRequestHeader) MarshalHash() ([]byte, error) {
	buffer := bytes.NewBuffer(nil)

	if err := utils.WriteElements(buffer, binary.BigEndian,
		int32(sh.QueryType),
		&sh.NodeID,
		&sh.DatabaseID,
		sh.ConnectionID,
		sh.SeqNo,
		sh.Timestamp,
		sh.BatchCount,
		&sh.QueriesHash,
		&sh.HeaderHash,
		sh.Signee,
		sh.Signature,
	); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// GetQueryKey returns a unique query key of this request.
func (sh *SignedRequestHeader) GetQueryKey() QueryKey {
	return QueryKey{
		NodeID:       sh.NodeID,
		ConnectionID: sh.ConnectionID,
		SeqNo:        sh.SeqNo,
	}
}
