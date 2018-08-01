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
	"gitlab.com/thunderdb/ThunderDB/crypto/hash"
	"gitlab.com/thunderdb/ThunderDB/utils"
)

type state struct {
	node   *blockNode
	Head   hash.Hash
	Height uint64
}

func (s *state) serialize() ([]byte, error) {
	buffer, err := utils.EncodeMsgPack(s)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (s *state) deserialize(b []byte) error {
	err := utils.DecodeMsgPack(b, s)
	return err
}
