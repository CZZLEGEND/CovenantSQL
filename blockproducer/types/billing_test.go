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
	"reflect"
	"testing"

	"github.com/CovenantSQL/CovenantSQL/crypto/asymmetric"
	"github.com/CovenantSQL/CovenantSQL/crypto/hash"
	"github.com/CovenantSQL/CovenantSQL/utils"
)

func TestBillingHeader_MarshalUnmarshalBinary(t *testing.T) {
	tc, err := generateRandomBillingHeader()
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	enc, err := utils.EncodeMsgPack(tc)
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	dec := &BillingHeader{}
	err = utils.DecodeMsgPack(enc.Bytes(), dec)
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	if tc.Nonce != dec.Nonce {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", tc.Nonce, tc.Nonce)
	}
	if tc.BillingRequest.RequestHash != dec.BillingRequest.RequestHash {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", tc.BillingRequest.RequestHash, tc.BillingRequest.RequestHash)
	}
	if !tc.BillingRequest.Signatures[0].IsEqual(dec.BillingRequest.Signatures[0]) {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", tc.BillingRequest.Signatures[0], dec.BillingRequest.Signatures[0])
	}
	for i := range tc.Receivers {
		if !reflect.DeepEqual(tc.Receivers[i], dec.Receivers[i]) {
			t.Fatalf("Value not match: \n\ttc.Receivers[%d]=%v\n\tReceive[%d]=%v", i, i, tc.Receivers[i], tc.Receivers[0])
		}
		if tc.Rewards[i] != dec.Rewards[i] {
			t.Fatalf("Value not match: \n\ttc.Rewards[%d]=%v\n\tRewards[%d]=%v", i, i, tc.Rewards[i], tc.Rewards[0])
		}
		if tc.Fees[i] != dec.Fees[i] {
			t.Fatalf("Value not match: \n\ttc.Fees[%d]=%v\n\tFees[%d]=%v", i, i, tc.Fees[i], tc.Fees[0])
		}
	}
}

func TestBilling_SerializeDeserialize(t *testing.T) {
	tb, err := generateRandomBilling()
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	enc, err := utils.EncodeMsgPack(tb)
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	dec := Billing{}
	err = utils.DecodeMsgPack(enc.Bytes(), &dec)
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	if !tb.Signature.IsEqual(dec.Signature) {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", tb.Signature, tb.Signature)
	}
	if !tb.Signee.IsEqual(dec.Signee) {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", tb.Signee, tb.Signee)
	}
	if !tb.Hash.IsEqual(&dec.Hash) {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", tb.Hash, tb.Hash)
	}
}

func TestBilling_PackAndSignTx(t *testing.T) {
	tb, err := generateRandomBilling()
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}

	priv, _, err := asymmetric.GenSecp256k1KeyPair()
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}
	tb.Sign(priv)
	enc, err := tb.BillingHeader.MarshalHash()
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}
	h := hash.THashH(enc[:])
	sign, err := priv.Sign(h[:])
	if err != nil {
		t.Fatalf("Unexpeted error: %v", err)
	}
	if !sign.IsEqual(tb.Signature) {
		t.Fatalf("Value not match: \n\tv1=%v\n\tv2=%v", sign, tb.Signature)
	}

	err = tb.Verify()
	if err != nil {
		t.Fatalf("Verify signature failed: %v", err)
	}
}
