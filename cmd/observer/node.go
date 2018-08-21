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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"gitlab.com/thunderdb/ThunderDB/conf"
	"gitlab.com/thunderdb/ThunderDB/crypto/kms"
	"gitlab.com/thunderdb/ThunderDB/route"
	"gitlab.com/thunderdb/ThunderDB/rpc"
	"gitlab.com/thunderdb/ThunderDB/utils/log"
	"golang.org/x/crypto/ssh/terminal"
)

func initNode() (server *rpc.Server, err error) {
	keyPairRootPath := conf.GConf.WorkingRoot
	pubKeyPath := filepath.Join(keyPairRootPath, conf.GConf.PubKeyStoreFile)
	privKeyPath := filepath.Join(keyPairRootPath, conf.GConf.PrivateKeyFile)

	var masterKey []byte
	if !conf.GConf.IsTestMode {
		fmt.Print("Type in Master key to continue:")
		masterKey, err = terminal.ReadPassword(syscall.Stdin)
		if err != nil {
			fmt.Printf("Failed to read Master key: %v", err)
		}
		fmt.Println("")
	}

	if err = kms.InitLocalKeyPair(privKeyPath, masterKey); err != nil {
		log.Error("init local key pair failed: %v", err)
		return
	}

	log.Infof("init routes")

	// init kms routing
	route.InitKMS(pubKeyPath)

	// init server
	if server, err = createServer(privKeyPath, pubKeyPath, masterKey, conf.GConf.ListenAddr); err != nil {
		log.Errorf("create server failed: %v", err)
		return
	}

	return
}

func createServer(privateKeyPath, pubKeyStorePath string, masterKey []byte, listenAddr string) (server *rpc.Server, err error) {
	os.Remove(pubKeyStorePath)

	server = rpc.NewServer()
	if err != nil {
		return
	}

	err = server.InitRPCServer(listenAddr, privateKeyPath, masterKey)

	return
}
