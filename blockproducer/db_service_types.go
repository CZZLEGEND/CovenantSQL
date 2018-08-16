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

package blockproducer

import (
	"gitlab.com/thunderdb/ThunderDB/crypto/asymmetric"
	"gitlab.com/thunderdb/ThunderDB/crypto/hash"
	"gitlab.com/thunderdb/ThunderDB/proto"
	wt "gitlab.com/thunderdb/ThunderDB/worker/types"
)

// CreateDatabaseRequestHeader defines client create database rpc header.
type CreateDatabaseRequestHeader struct {
	ResourceMeta wt.ResourceMeta
}

// Serialize structure to bytes.
func (h *CreateDatabaseRequestHeader) Serialize() []byte {
	if h == nil {
		return []byte{'\000'}
	}

	return h.ResourceMeta.Serialize()
}

// SignedCreateDatabaseRequestHeader defines signed client create database request header.
type SignedCreateDatabaseRequestHeader struct {
	CreateDatabaseRequestHeader
	HeaderHash hash.Hash
	Signee     *asymmetric.PublicKey
	Signature  *asymmetric.Signature
}

// Verify checks hash and signature in create database request header.
func (sh *SignedCreateDatabaseRequestHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.CreateDatabaseRequestHeader, &sh.HeaderHash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.HeaderHash[:], sh.Signee) {
		return wt.ErrSignVerification
	}
	return
}

// Sign the request.
func (sh *SignedCreateDatabaseRequestHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// build hash
	buildHash(&sh.CreateDatabaseRequestHeader, &sh.HeaderHash)

	// sign
	sh.Signature, err = signer.Sign(sh.HeaderHash[:])

	return
}

// CreateDatabaseRequest defines client create database rpc request entity.
type CreateDatabaseRequest struct {
	proto.Envelope
	Header SignedCreateDatabaseRequestHeader
}

// Verify checks hash and signature in request header.
func (r *CreateDatabaseRequest) Verify() error {
	return r.Header.Verify()
}

// Sign the request.
func (r *CreateDatabaseRequest) Sign(signer *asymmetric.PrivateKey) (err error) {
	// sign
	return r.Header.Sign(signer)
}

// CreateDatabaseResponseHeader defines client create database rpc response header.
type CreateDatabaseResponseHeader struct {
	InstanceMeta wt.ServiceInstance
}

// Serialize structure to bytes.
func (h *CreateDatabaseResponseHeader) Serialize() []byte {
	if h == nil {
		return []byte{'\000'}
	}

	return h.InstanceMeta.Serialize()
}

// SignedCreateDatabaseResponseHeader defines signed client create database response header.
type SignedCreateDatabaseResponseHeader struct {
	CreateDatabaseResponseHeader
	HeaderHash hash.Hash
	Signee     *asymmetric.PublicKey
	Signature  *asymmetric.Signature
}

// Verify checks hash and signature in create database response header.
func (sh *SignedCreateDatabaseResponseHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.CreateDatabaseResponseHeader, &sh.HeaderHash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.HeaderHash[:], sh.Signee) {
		return wt.ErrSignVerification
	}
	return
}

// Sign the response.
func (sh *SignedCreateDatabaseResponseHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// build hash
	buildHash(&sh.CreateDatabaseResponseHeader, &sh.HeaderHash)

	// sign
	sh.Signature, err = signer.Sign(sh.HeaderHash[:])

	return
}

// CreateDatabaseResponse defines client create database rpc response entity.
type CreateDatabaseResponse struct {
	proto.Envelope
	Header SignedCreateDatabaseResponseHeader
}

// Verify checks hash and signature in response header.
func (r *CreateDatabaseResponse) Verify() error {
	return r.Header.Verify()
}

// Sign the response.
func (r *CreateDatabaseResponse) Sign(signer *asymmetric.PrivateKey) (err error) {
	// sign
	return r.Header.Sign(signer)
}

// DropDatabaseRequestHeader defines client drop database rpc request header.
type DropDatabaseRequestHeader struct {
	DatabaseID proto.DatabaseID
}

// Serialize structure to bytes.
func (h *DropDatabaseRequestHeader) Serialize() []byte {
	if h == nil {
		return []byte{'\000'}
	}

	return []byte(h.DatabaseID)
}

// SignedDropDatabaseRequestHeader defines signed client drop database rpc request header.
type SignedDropDatabaseRequestHeader struct {
	DropDatabaseRequestHeader
	HeaderHash hash.Hash
	Signee     *asymmetric.PublicKey
	Signature  *asymmetric.Signature
}

// Verify checks hash and signature in request header.
func (sh *SignedDropDatabaseRequestHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.DropDatabaseRequestHeader, &sh.HeaderHash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.HeaderHash[:], sh.Signee) {
		return wt.ErrSignVerification
	}
	return
}

// Sign the request.
func (sh *SignedDropDatabaseRequestHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// build hash
	buildHash(&sh.DropDatabaseRequestHeader, &sh.HeaderHash)

	// sign
	sh.Signature, err = signer.Sign(sh.HeaderHash[:])

	return
}

// DropDatabaseRequest defines client drop database rpc request entity.
type DropDatabaseRequest struct {
	proto.Envelope
	Header SignedDropDatabaseRequestHeader
}

// Verify checks hash and signature in request header.
func (r *DropDatabaseRequest) Verify() error {
	return r.Header.Verify()
}

// Sign the request.
func (r *DropDatabaseRequest) Sign(signer *asymmetric.PrivateKey) error {
	return r.Header.Sign(signer)
}

// DropDatabaseResponse defines client drop database rpc response entity.
type DropDatabaseResponse struct{}

// GetDatabaseRequestHeader defines client get database rpc request header entity.
type GetDatabaseRequestHeader struct {
	DatabaseID proto.DatabaseID
}

// Serialize structure to bytes.
func (h *GetDatabaseRequestHeader) Serialize() []byte {
	if h == nil {
		return []byte{'\000'}
	}

	return []byte(h.DatabaseID)
}

// SignedGetDatabaseRequestHeader defines signed client get database rpc request header entity.
type SignedGetDatabaseRequestHeader struct {
	GetDatabaseRequestHeader
	HeaderHash hash.Hash
	Signee     *asymmetric.PublicKey
	Signature  *asymmetric.Signature
}

// Verify checks hash and signature in request header.
func (sh *SignedGetDatabaseRequestHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.GetDatabaseRequestHeader, &sh.HeaderHash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.HeaderHash[:], sh.Signee) {
		return wt.ErrSignVerification
	}
	return
}

// Sign the request.
func (sh *SignedGetDatabaseRequestHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// build hash
	buildHash(&sh.GetDatabaseRequestHeader, &sh.HeaderHash)

	// sign
	sh.Signature, err = signer.Sign(sh.HeaderHash[:])

	return
}

// GetDatabaseRequest defines client get database rpc request entity.
type GetDatabaseRequest struct {
	proto.Envelope
	Header SignedGetDatabaseRequestHeader
}

// Verify checks hash and signature in request header.
func (r *GetDatabaseRequest) Verify() error {
	return r.Header.Verify()
}

// Sign the request.
func (r *GetDatabaseRequest) Sign(signer *asymmetric.PrivateKey) error {
	return r.Header.Sign(signer)
}

// GetDatabaseResponseHeader defines client get database rpc response header entity.
type GetDatabaseResponseHeader struct {
	InstanceMeta wt.ServiceInstance
}

// Serialize structure to bytes.
func (h *GetDatabaseResponseHeader) Serialize() []byte {
	if h == nil {
		return []byte{'\000'}
	}

	return h.InstanceMeta.Serialize()
}

// SignedGetDatabaseResponseHeader defines client get database rpc response header entity.
type SignedGetDatabaseResponseHeader struct {
	GetDatabaseResponseHeader
	HeaderHash hash.Hash
	Signee     *asymmetric.PublicKey
	Signature  *asymmetric.Signature
}

// Verify checks hash and signature in response header.
func (sh *SignedGetDatabaseResponseHeader) Verify() (err error) {
	// verify hash
	if err = verifyHash(&sh.GetDatabaseResponseHeader, &sh.HeaderHash); err != nil {
		return
	}
	// verify sign
	if sh.Signee == nil || sh.Signature == nil || !sh.Signature.Verify(sh.HeaderHash[:], sh.Signee) {
		return wt.ErrSignVerification
	}
	return
}

// Sign the request.
func (sh *SignedGetDatabaseResponseHeader) Sign(signer *asymmetric.PrivateKey) (err error) {
	// build hash
	buildHash(&sh.GetDatabaseResponseHeader, &sh.HeaderHash)

	// sign
	sh.Signature, err = signer.Sign(sh.HeaderHash[:])

	return
}

// GetDatabaseResponse defines client get database rpc response entity.
type GetDatabaseResponse struct {
	proto.Envelope
	Header SignedGetDatabaseResponseHeader
}

// Verify checks hash and signature in response header.
func (r *GetDatabaseResponse) Verify() (err error) {
	return r.Header.Verify()
}

// Sign the request.
func (r *GetDatabaseResponse) Sign(signer *asymmetric.PrivateKey) (err error) {
	return r.Header.Sign(signer)
}

// FIXIT(xq262144) remove duplicated interface in utils package
type canSerialize interface {
	Serialize() []byte
}

func verifyHash(data canSerialize, h *hash.Hash) (err error) {
	var newHash hash.Hash
	buildHash(data, &newHash)
	if !newHash.IsEqual(h) {
		return wt.ErrHashVerification
	}
	return
}

func buildHash(data canSerialize, h *hash.Hash) {
	newHash := hash.THashH(data.Serialize())
	copy(h[:], newHash[:])
}
