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

package rpc

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"strings"

	. "github.com/smartystreets/goconvey/convey"
	"gitlab.com/thunderdb/ThunderDB/conf"
	"gitlab.com/thunderdb/ThunderDB/consistent"
	"gitlab.com/thunderdb/ThunderDB/crypto/kms"
	"gitlab.com/thunderdb/ThunderDB/proto"
	"gitlab.com/thunderdb/ThunderDB/route"
	"gitlab.com/thunderdb/ThunderDB/utils"
	"gitlab.com/thunderdb/ThunderDB/utils/log"
)

const (
	DHTStorePath  = "./DHTStore"
	RPCConcurrent = 10
	RPCCount      = 10
)

func TestCaller_CallNode(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	os.Remove(PubKeyStorePath)
	defer os.Remove(PubKeyStorePath)
	os.Remove(publicKeyStore)
	defer os.Remove(publicKeyStore)

	_, testFile, _, _ := runtime.Caller(0)
	confFile := filepath.Join(filepath.Dir(testFile), "../test/node_standalone/config.yaml")
	privateKeyPath := filepath.Join(filepath.Dir(testFile), "../test/node_standalone/private.key")

	conf.GConf, _ = conf.LoadConfig(confFile)
	log.Debugf("GConf: %#v", conf.GConf)
	// reset the once
	route.Once = sync.Once{}
	route.InitKMS(publicKeyStore)

	addr := conf.GConf.ListenAddr // see ../test/node_standalone/config.yaml
	masterKey := []byte("")
	dht, err := route.NewDHTService(PubKeyStorePath, new(consistent.KMSStorage), true)

	server, err := NewServerWithService(ServiceMap{"DHT": dht})
	if err != nil {
		t.Fatal(err)
	}

	server.InitRPCServer(addr, privateKeyPath, masterKey)
	go server.Serve()

	//publicKey, err := kms.GetLocalPublicKey()
	//nonce := asymmetric.GetPubKeyNonce(publicKey, 10, 100*time.Millisecond, nil)
	//serverNodeID := proto.NodeID(nonce.Hash.String())
	//kms.SetPublicKey(serverNodeID, nonce.Nonce, publicKey)
	//
	//kms.SetLocalNodeIDNonce(nonce.Hash.CloneBytes(), &nonce.Nonce)
	//route.SetNodeAddrCache(&proto.RawNodeID{Hash: nonce.Hash}, server.Listener.Addr().String())

	client := NewCaller()
	node1 := proto.NewNode()
	node1.InitNodeCryptoInfo(100 * time.Millisecond)
	node1.Addr = "1.1.1.1:1"

	reqA := &proto.PingReq{
		Node: *node1,
	}

	respA := new(proto.PingResp)
	err = client.CallNode(conf.GConf.BP.NodeID, "DHT.Ping", reqA, respA)
	if err != nil {
		t.Fatal(err)
	}
	log.Debugf("respA: %v", respA)

	node1addr, err := GetNodeAddr(node1.ID.ToRawNodeID())
	Convey("test GetNodeAddr", t, func() {
		So(err, ShouldBeNil)
		So(node1addr, ShouldEqual, node1.Addr)
	})

	node2, err := GetNodeInfo(node1.ID.ToRawNodeID())
	Convey("test GetNodeInfo", t, func() {
		So(err, ShouldBeNil)
		So(node2.PublicKey.IsEqual(node1.PublicKey), ShouldBeTrue)
		log.Debugf("\nnode1 %##v \nnode2 %##v", node1, node2)
	})

	kms.DelNode(node2.ID)
	node2, err = GetNodeInfo(node1.ID.ToRawNodeID())
	Convey("test GetNodeInfo", t, func() {
		So(err, ShouldBeNil)
		So(node2.PublicKey.IsEqual(node1.PublicKey), ShouldBeTrue)
		log.Debugf("\nnode1 %##v \nnode2 %##v", node1, node2)
	})

	err = PingBP(node1, conf.GConf.BP.NodeID)
	//err = client.CallNode(conf.GConf.BP.NodeID, "DHT.Ping", reqA, respA)
	if err != nil {
		t.Fatal(err)
	}
	//log.Debugf("respA2: %v", respA)

	// call with canceled context
	ctx, contextCancel := context.WithCancel(context.Background())
	contextCancel()
	err = client.CallNodeWithContext(ctx, conf.GConf.BP.NodeID, "DHT.Ping", reqA, respA)
	if err == nil {
		t.Fatal("this call should failed, but actually not")
	} else {
		log.Debugf("err: %v", err)
	}

	// call with empty context
	err = client.CallNodeWithContext(context.Background(), conf.GConf.BP.NodeID, "DHT.Ping", reqA, respA)
	if err != nil {
		t.Fatal(err)
	}
	log.Debugf("respA2: %v", respA)

	// test get current bp, should only be myself
	chiefBPNodeID, err := GetCurrentBP()
	if err != nil {
		t.Fatal(err)
	}
	log.Debugf("current chief bp is: %v", chiefBPNodeID)

	// set another random node as block producer
	randomNode := proto.NodeID("00000000011a34cb8142780f692a4097d883aa2ac8a534a070a134f11bcca573")
	SetCurrentBP(randomNode)
	chiefBPNodeID, err = GetCurrentBP()
	if err != nil {
		t.Fatal(err)
	}
	if chiefBPNodeID != randomNode {
		t.Fatalf("SetCurrentBP does not works, set: %v, current: %v", randomNode, chiefBPNodeID)
	}

	server.Stop()
}

func TestNewPersistentCaller(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	os.Remove(PubKeyStorePath)
	defer os.Remove(PubKeyStorePath)
	os.Remove(publicKeyStore)
	defer os.Remove(publicKeyStore)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	err := utils.WaitForPorts(ctx, "127.0.0.1", []int{
		2230,
	}, time.Millisecond*200)

	if err != nil {
		log.Fatalf("wait for port ready timeout: %v", err)
	}

	_, testFile, _, _ := runtime.Caller(0)
	confFile := filepath.Join(filepath.Dir(testFile), "../test/node_standalone/config.yaml")
	privateKeyPath := filepath.Join(filepath.Dir(testFile), "../test/node_standalone/private.key")

	conf.GConf, _ = conf.LoadConfig(confFile)
	log.Debugf("GConf: %#v", conf.GConf)
	// reset the once
	route.Once = sync.Once{}
	route.InitKMS(publicKeyStore)

	addr := conf.GConf.ListenAddr // see ../test/node_standalone/config.yaml
	masterKey := []byte("")
	dht, err := route.NewDHTService(PubKeyStorePath, new(consistent.KMSStorage), true)

	server, err := NewServerWithService(ServiceMap{"DHT": dht})
	if err != nil {
		t.Fatal(err)
	}

	server.InitRPCServer(addr, privateKeyPath, masterKey)
	go server.Serve()

	client := NewPersistentCaller(conf.GConf.BP.NodeID)
	node1 := proto.NewNode()
	node1.InitNodeCryptoInfo(100 * time.Millisecond)
	node1.Addr = "1.1.1.1:1"

	reqA := &proto.PingReq{
		Node: *node1,
	}

	respA := new(proto.PingResp)
	err = client.Call("DHT.Ping", reqA, respA)
	if err != nil {
		t.Fatal(err)
	}
	log.Debugf("respA: %v", respA)

	req := &proto.FindNeighborReq{
		NodeID: "1234",
		Count:  10,
	}
	resp := new(proto.FindNeighborResp)

	err = client.Call("DHT.FindNeighbor", req, resp)
	if err == nil || !strings.Contains(err.Error(), "not permitted") {
		t.Fatal("anonymous ETLS connection used by " +
			"RPC other than DHTPing shuold not permitted")
	}

	// close anonymous ETLS connection, and create new one
	client.Close()

	wg := sync.WaitGroup{}
	client = NewPersistentCaller(conf.GConf.BP.NodeID)
	for i := 0; i < RPCConcurrent; i++ {
		wg.Add(1)
		go func(tt *testing.T, wg *sync.WaitGroup) {
			for j := 0; j < RPCCount; j++ {
				reqF := &proto.FindNeighborReq{
					NodeID: "1234",
					Count:  10,
				}
				respF := new(proto.FindNeighborResp)
				err := client.Call("DHT.FindNeighbor", reqF, respF)
				if err != nil {
					tt.Error(err)
				}
				log.Debugf("resp: %v", respF)
			}
			wg.Done()
		}(t, &wg)
	}

	wg.Wait()
	server.Stop()
}

func BenchmarkPersistentCaller_Call(b *testing.B) {
	log.SetLevel(log.InfoLevel)
	os.Remove(PubKeyStorePath)
	defer os.Remove(PubKeyStorePath)
	os.Remove(publicKeyStore)
	defer os.Remove(publicKeyStore)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	err := utils.WaitForPorts(ctx, "127.0.0.1", []int{
		2230,
	}, time.Millisecond*200)

	if err != nil {
		log.Fatalf("wait for port ready timeout: %v", err)
	}

	_, testFile, _, _ := runtime.Caller(0)
	confFile := filepath.Join(filepath.Dir(testFile), "../test/node_standalone/config.yaml")
	privateKeyPath := filepath.Join(filepath.Dir(testFile), "../test/node_standalone/private.key")

	conf.GConf, _ = conf.LoadConfig(confFile)
	log.Debugf("GConf: %#v", conf.GConf)
	// reset the once
	route.Once = sync.Once{}
	route.InitKMS(publicKeyStore)

	addr := conf.GConf.ListenAddr // see ../test/node_standalone/config.yaml
	masterKey := []byte("")
	dht, err := route.NewDHTService(PubKeyStorePath, new(consistent.KMSStorage), true)

	server, err := NewServerWithService(ServiceMap{"DHT": dht})
	if err != nil {
		b.Fatal(err)
	}

	server.InitRPCServer(addr, privateKeyPath, masterKey)
	go server.Serve()

	client := NewPersistentCaller(conf.GConf.BP.NodeID)
	node1 := proto.NewNode()
	node1.InitNodeCryptoInfo(100 * time.Millisecond)
	node1.Addr = "1.1.1.1:1"

	reqA := &proto.PingReq{
		Node: *node1,
	}

	respA := new(proto.PingResp)
	err = client.Call("DHT.Ping", reqA, respA)
	if err != nil {
		b.Fatal(err)
	}
	log.Debugf("respA: %v", respA)

	req := &proto.FindNeighborReq{
		NodeID: "1234",
		Count:  10,
	}
	resp := new(proto.FindNeighborResp)

	b.Run("benchmark Persistent", func(b *testing.B) {
		client = NewPersistentCaller(conf.GConf.BP.NodeID)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = client.Call("DHT.FindNeighbor", req, resp)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("benchmark non-Persistent", func(b *testing.B) {
		oldClient := NewCaller()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = oldClient.CallNode(conf.GConf.BP.NodeID, "DHT.FindNeighbor", req, resp)
			if err != nil {
				b.Error(err)
			}
		}
	})
	server.Stop()
}
