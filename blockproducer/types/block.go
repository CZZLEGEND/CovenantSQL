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
	"time"

	"gitlab.com/thunderdb/ThunderDB/crypto/asymmetric"
	"gitlab.com/thunderdb/ThunderDB/crypto/hash"
	"gitlab.com/thunderdb/ThunderDB/merkle"
	"gitlab.com/thunderdb/ThunderDB/proto"
	"gitlab.com/thunderdb/ThunderDB/utils"
)

//go:generate HashStablePack

// Header defines the main chain block header
type Header struct {
	Version    int32
	Producer   proto.AccountAddress
	MerkleRoot hash.Hash
	ParentHash hash.Hash
	Timestamp  time.Time
}

// SignedHeader defines the main chain header with the signature
type SignedHeader struct {
	Header
	BlockHash hash.Hash
	Signee    *asymmetric.PublicKey
	Signature *asymmetric.Signature
}

// Verify verifies the signature
func (s *SignedHeader) Verify() error {
	if !s.Signature.Verify(s.BlockHash[:], s.Signee) {
		return ErrSignVerification
	}

	return nil
}

// Block defines the main chain block
type Block struct {
	SignedHeader SignedHeader
	TxBillings   []*TxBilling
}

// GetTxHashes returns all hashes of tx in block.{TxBillings, ...}
func (b *Block) GetTxHashes() []*hash.Hash {
	// TODO(lambda): when you add new tx type, you need to put new tx's hash in the slice
	// get hashes in block.TxBillings
	hs := make([]*hash.Hash, len(b.TxBillings))
	for i := range hs {
		hs[i] = b.TxBillings[i].TxHash
	}
	return hs
}

// PackAndSignBlock computes block's hash and sign it
func (b *Block) PackAndSignBlock(signer *asymmetric.PrivateKey) error {
	hs := b.GetTxHashes()

	b.SignedHeader.MerkleRoot = *merkle.NewMerkle(hs).GetRoot()
	enc, err := b.SignedHeader.Header.MarshalHash()

	if err != nil {
		return err
	}

	b.SignedHeader.BlockHash = hash.THashH(enc)
	b.SignedHeader.Signature, err = signer.Sign(b.SignedHeader.BlockHash[:])
	b.SignedHeader.Signee = signer.PubKey()

	if err != nil {
		return err
	}

	return nil
}

// Serialize converts block to bytes
func (b *Block) Serialize() ([]byte, error) {
	buf, err := utils.EncodeMsgPack(b)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize converts bytes to block
func (b *Block) Deserialize(buf []byte) error {
	return utils.DecodeMsgPack(buf, b)
}

// PushTx pushes txes into block
func (b *Block) PushTx(tx *TxBilling) {
	if b.TxBillings != nil {
		// TODO(lambda): set appropriate capacity.
		b.TxBillings = make([]*TxBilling, 0, 100)
	}

	b.TxBillings = append(b.TxBillings, tx)
}

// Verify verifies whether the block is valid
func (b *Block) Verify() error {
	hs := b.GetTxHashes()
	merkleRoot := *merkle.NewMerkle(hs).GetRoot()
	if !merkleRoot.IsEqual(&b.SignedHeader.MerkleRoot) {
		return ErrMerkleRootVerification
	}

	enc, err := b.SignedHeader.Header.MarshalHash()
	if err != nil {
		return err
	}

	h := hash.THashH(enc)
	if !h.IsEqual(&b.SignedHeader.BlockHash) {
		return ErrHashVerification
	}

	return b.SignedHeader.Verify()
}

// Timestamp returns timestamp of block
func (b *Block) Timestamp() time.Time {
	return b.SignedHeader.Timestamp
}

// Producer returns the producer of block
func (b *Block) Producer() proto.AccountAddress {
	return b.SignedHeader.Producer
}
