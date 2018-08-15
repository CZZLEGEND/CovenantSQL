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

package conf

import (
	"io/ioutil"
	"time"

	"gitlab.com/thunderdb/ThunderDB/crypto/hash"

	"gitlab.com/thunderdb/ThunderDB/crypto/asymmetric"
	"gitlab.com/thunderdb/ThunderDB/pow/cpuminer"
	"gitlab.com/thunderdb/ThunderDB/proto"
	"gitlab.com/thunderdb/ThunderDB/utils/log"
	"gopkg.in/yaml.v2"
)

// these const specify the role of this app, which can be "miner", "blockProducer"
const (
	MinerBuildTag         = "M"
	BlockProducerBuildTag = "B"
	ClientBuildTag        = "C"
	UnknownBuildTag       = "U"
)

// StartSucceedMessage is printed when thunderDB started successfully
const StartSucceedMessage = "ThunderDB Started Successfully"

// RoleTag indicate which role the daemon is playing
var RoleTag = UnknownBuildTag

// BPGenesisInfo hold all genesis info fields
type BPGenesisInfo struct {
	// Version defines the block version
	Version int32 `yaml:"Version"`
	// Producer defines the block producer
	Producer hash.Hash `yaml:"Producer"`
	// MerkleRoot defines the transaction merkle tree's root
	MerkleRoot hash.Hash `yaml:"MerkleRoot"`
	// ParentHash defines the parent block's hash
	ParentHash hash.Hash `yaml:"ParentHash"`
	// Timestamp defines the initial time of chain
	Timestamp time.Time `yaml:"Timestamp"`
	// BlockHash defines the the block hash of genesis block
	BlockHash hash.Hash `yaml:"BlockHash"`
}

// BPInfo hold all BP info fields
type BPInfo struct {
	// PublicKey point to BlockProducer public key
	PublicKey *asymmetric.PublicKey `yaml:"PublicKey"`
	// NodeID is the node id of Block Producer
	NodeID proto.NodeID `yaml:"NodeID"`
	// RawNodeID
	RawNodeID proto.RawNodeID `yaml:"-"`
	// Nonce is the nonce, SEE: cmd/idminer for more
	Nonce cpuminer.Uint256 `yaml:"Nonce"`
	// ChainFileName is the chain db's name
	ChainFileName string `yaml:"ChainFileName"`
	// BPGenesisInfo is the genesis block filed
	BPGenesis BPGenesisInfo `yaml:"BPGenesisInfo"`
}

// MinerDatabaseFixture config.
type MinerDatabaseFixture struct {
	DatabaseID               proto.DatabaseID `yaml:"DatabaseID"`
	Term                     uint64           `yaml:"Term"`
	Leader                   proto.NodeID     `yaml:"Leader"`
	Servers                  []proto.NodeID   `yaml:"Servers"`
	GenesisBlockFile         string           `yaml:"GenesisBlockFile"`
	AutoGenerateGenesisBlock bool             `yaml:"AutoGenerateGenesisBlock,omitempty"`
}

// MinerInfo for miner config.
type MinerInfo struct {
	// node basic config.
	RootDir               string        `yaml:"RootDir"`
	MaxReqTimeGap         time.Duration `yaml:"MaxReqTimeGap,omitempty"`
	MetricCollectInterval time.Duration `yaml:"MetricCollectInterval,omitempty"`

	// when test mode, fixture database config is used.
	IsTestMode   bool                    `yaml:"IsTestMode,omitempty"`
	TestFixtures []*MinerDatabaseFixture `yaml:"TestFixtures,omitempty"`
}

// DNSSeed stuff
type DNSSeed struct {
	EnforcedDNSSEC bool     `yaml:"EnforcedDNSSEC"`
	DNSServers     []string `yaml:"DNSServers"`
}

// Config holds all the config read from yaml config file
type Config struct {
	IsTestMode      bool `yaml:"IsTestMode,omitempty"` // when testMode use default empty masterKey and test DNS domain
	GenerateKeyPair bool `yaml:"-"`
	//TODO(auxten): set yaml key for config
	WorkingRoot     string            `yaml:"WorkingRoot"`
	PubKeyStoreFile string            `yaml:"PubKeyStoreFile"`
	PrivateKeyFile  string            `yaml:"PrivateKeyFile"`
	DHTFileName     string            `yaml:"DHTFileName"`
	ListenAddr      string            `yaml:"ListenAddr"`
	ThisNodeID      proto.NodeID      `yaml:"ThisNodeID"`
	ValidDNSKeys    map[string]string `yaml:"ValidDNSKeys"` // map[DNSKEY]domain
	// Check By BP DHT.Ping
	MinNodeIDDifficulty int `yaml:"MinNodeIDDifficulty"`

	DNSSeed DNSSeed `yaml:"DNSSeed"`

	BP    *BPInfo    `yaml:"BlockProducer"`
	Miner *MinerInfo `yaml:"Miner,omitempty"`

	KnownNodes  []proto.Node `yaml:"KnownNodes"`
	SeedBPNodes []proto.Node `yaml:"-"`
}

// GConf is the global config pointer
var GConf *Config

// LoadConfig loads config from configPath
func LoadConfig(configPath string) (config *Config, err error) {
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Errorf("read config file failed: %s", err)
		return
	}
	config = &Config{}
	err = yaml.Unmarshal(configBytes, config)
	if err != nil {
		log.Errorf("unmarshal config file failed: %s", err)
		return
	}

	return
}
