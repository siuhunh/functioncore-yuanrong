/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package client implements StateClient for functionstate
package client

import (
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functionbus/servicediscovery"
)

var stateClient IStateClient

const (
	key   = "key"
	value = "value"
)

func TestMain(m *testing.M) {
	stateClient, _ = CreateStateClient(StateClientConfig{
		// It will initialize a Redis client when running gate build.
		// If you want to run this test function locally, please change this ServerAddr to your local Redis address.
		ServerAddr: "127.0.0.1:6379",
		ServerMode: "single",
		ExpiredCb:  func(s string) {},
	})
	m.Run()
}

func Test_SetKeyWithTTL(t *testing.T) {
	Convey("Test set key with TTL", t, func() {
		ShouldBeNil(stateClient.SetKeyWithTTL(key, value, 1))
		exit, err := stateClient.Exists(key)
		So(exit, ShouldBeTrue)
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		exit, _ = stateClient.Exists(key)
		So(exit, ShouldBeFalse)
	})
}

func Test_Scan(t *testing.T) {
	Convey("Test Scan key", t, func() {
		const keyScanTest = "keyScanTest"
		stateIDs := []string{keyScanTest + "1", keyScanTest + "2", keyScanTest + "3"}
		for _, item := range stateIDs {
			stateClient.SaveState(item, "test", &SaveOption{TTL: 2})
		}
		arr, err := stateClient.Scan(keyScanTest + "*")
		So(err, ShouldBeNil)
		So(len(arr), ShouldEqual, len(stateIDs))
	})
}

func Test_Transaction(t *testing.T) {
	Convey("Test transaction", t, func() {
		key := "transaction_test"
		err := stateClient.Transaction(DoArg{
			"set", key, "one", "ex", 60,
		}, DoArg{
			"set", key, "two", "ex", 60,
		})
		res, err := stateClient.RestoreState(key)
		So(err, ShouldBeNil)
		So(res, ShouldEqual, "two")
		// Test error command, "test" is not redis command
		err = stateClient.Transaction(DoArg{
			"test", key, "one", "test", 60,
		})
		So(err, ShouldNotBeNil)
	})
}

func Test_SaveAndRestoreState_Concurrency(t *testing.T) {
	var wg sync.WaitGroup
	const (
		num              = 200
		concurrencyKey   = "concurrencyKey"
		concurrencyValue = "concurrencyValue"
	)
	wg.Add(num * 2)
	for i := 0; i < num; i++ {
		go func() {
			defer wg.Done()
			err := stateClient.SaveState(concurrencyKey, concurrencyValue, &SaveOption{TTL: 30})
			if err != nil {
				t.Fatalf("stateClient failed to Save State Concurrency")
			}
		}()
	}
	for i := 0; i < num; i++ {
		go func() {
			defer wg.Done()
			v, err := stateClient.RestoreState(concurrencyKey)
			if err != nil {
				t.Fatalf("stateClient failed to get State Concurrency, error %s", err.Error())
			}
			if v != concurrencyValue {
				t.Fatalf("stateClient failed to get State Concurrency, value %s", v)
			}
		}()
	}
	wg.Wait()
	stateClient.DeleteState(concurrencyKey)
}

func Test_NoExpiredCb(t *testing.T) {
	const (
		noexpiredcbKey001 = "noexpiredcb_key001"
		noexpiredcbKey002 = "noexpiredcb_key002"
	)
	Convey("Test NoExpiredCb", t, func() {
		err := stateClient.SaveState(noexpiredcbKey001, noexpiredcbKey001, &SaveOption{TTL: 2})
		So(err, ShouldBeNil)
		err = stateClient.ResetExpiredTTL(noexpiredcbKey001, 3)
		So(err, ShouldBeNil)
		err = stateClient.DeleteState(noexpiredcbKey001)
		So(err, ShouldBeNil)
		_, err = stateClient.RestoreState(noexpiredcbKey001)
		So(err, ShouldBeError, errNotExists)
		So(IsNotExist(err), ShouldBeTrue)
		err = stateClient.ResetExpiredTTL(noexpiredcbKey002, 3)
		So(err, ShouldNotBeNil)
	})
}

func Test_WithoutExpiredCb(t *testing.T) {
	const (
		withoutexpiredcbKey001 = "withoutexpiredcb_Key001"
	)
	Client1, _ := CreateStateClient(StateClientConfig{
		ServerAddr: "0.0.0.0:6379",
		ServerMode: "single",
	})
	Convey("test ResetExpiredTTL", t, func() {
		err := Client1.SaveState(withoutexpiredcbKey001, withoutexpiredcbKey001, &SaveOption{TTL: 2})
		So(err, ShouldBeNil)
		err = Client1.ResetExpiredTTL(withoutexpiredcbKey001, 3)
		So(err, ShouldBeNil)
		err = Client1.DeleteState(withoutexpiredcbKey001)
		So(err, ShouldBeNil)
		_, err = Client1.RestoreState(withoutexpiredcbKey001)
		So(err, ShouldBeError, errNotExists)
		So(IsNotExist(err), ShouldBeTrue)
	})
}

func TestStateClient_SAdd_SRem_SMembers(t *testing.T) {
	const (
		key     = "key01"
		value01 = "value01"
		value02 = "value02"
	)
	Convey("test SAdd", t, func() {
		n, err := stateClient.SAdd(key, value01)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
		n, err = stateClient.SAdd(key, value02)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
	})
	Convey("test SMembers", t, func() {
		res, err := stateClient.SMembers(key)
		So(err, ShouldBeNil)
		So(len(res), ShouldEqual, 2)
	})
	Convey("test SIsMember", t, func() {
		res, err := stateClient.SIsMember(key, value01)
		So(err, ShouldBeNil)
		So(res, ShouldBeTrue)
	})
	Convey("test SCard", t, func() {
		n, err := stateClient.SCard(key)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 2)
	})
	Convey("test SRem", t, func() {
		n, err := stateClient.SRem(key, value01)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
		n, err = stateClient.SRem(key, value02)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
	})
	// Test error condition
	Convey("test Set error condition", t, func() {
		// Create a hash with the same name first, and the set operation will fail
		n, err := stateClient.HSetNX(key, value01, value02)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, true)
		t, _ := stateClient.SAdd(key, value01)
		So(t, ShouldEqual, 0)
		res, _ := stateClient.SMembers(key)
		So(res, ShouldBeNil)
		n, err = stateClient.SIsMember(key, value01)
		So(err, ShouldNotBeNil)
		So(n, ShouldBeFalse)
		t, err = stateClient.SCard(key)
		So(err, ShouldNotBeNil)
		So(t, ShouldEqual, 0)
		t, err = stateClient.SRem(key, value01)
		So(err, ShouldNotBeNil)
		So(t, ShouldEqual, 0)
		t, err = stateClient.HDel(key, value01)
		So(err, ShouldBeNil)
		So(t, ShouldEqual, 1)
	})
}

func TestStateClient_HSetNX_HDel(t *testing.T) {
	const (
		key   = "hash_key01"
		field = "hash_field01"
		value = "hash_value01"
	)
	Convey("test HSetNX", t, func() {
		n, err := stateClient.HSetNX(key, field, value)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, true)
	})
	Convey("test HExists", t, func() {
		n, err := stateClient.HExists(key, field)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, true)
	})
	Convey("test HLel", t, func() {
		n, err := stateClient.HLen(key)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
	})
	Convey("test HGetAll", t, func() {
		n, err := stateClient.HGetAll(key)
		So(err, ShouldBeNil)
		m, _ := n[field]
		So(m, ShouldEqual, value)
	})
	Convey("test HDel", t, func() {
		n, err := stateClient.HDel(key, field)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
	})
	Convey("test HExists", t, func() {
		n, err := stateClient.HExists(key, field)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, false)
	})

	// Test error condition
	Convey("test Hash error condition", t, func() {
		// Create a set with the same name first, and the hash operation will fail
		i, err := stateClient.SAdd(key, field)
		So(err, ShouldBeNil)
		So(i, ShouldEqual, 1)
		n, _ := stateClient.HSetNX(key, field, value)
		So(n, ShouldEqual, false)
		n, err = stateClient.HExists(key, field)
		So(err, ShouldNotBeNil)
		So(n, ShouldEqual, false)
		i, err = stateClient.HLen(key)
		So(err, ShouldNotBeNil)
		So(i, ShouldEqual, 0)
		m, _ := stateClient.HGetAll(key)
		So(m, ShouldBeNil)
		i, err = stateClient.HDel(key, field)
		So(err, ShouldNotBeNil)
		So(i, ShouldEqual, 0)
		i, err = stateClient.SRem(key, field)
		So(err, ShouldBeNil)
		So(i, ShouldEqual, 1)
	})
}

func TestCreate_StopStateClient(t *testing.T) {
	Convey("test create StateClient", t, func() {
		client1, err := CreateStateClient(StateClientConfig{
			ServerMode: "cluster",
			ServerAddr: "ServerAddr",
		})
		So(err, ShouldNotBeNil)
		DeleteStateClient(client1)
	})
	Convey("test create StateClient", t, func() {
		client2, err := CreateStateClient(StateClientConfig{
			ServerAddr: "ServerAddr",
		})
		So(err, ShouldNotBeNil)
		DeleteStateClient(client2)
	})
	Convey("test create StateClient", t, func() {
		client3, err := CreateStateClient(StateClientConfig{
			ServerAddr: "127.0.0.1:6379",
			ServerMode: "single",
		})
		So(err, ShouldBeNil)
		DeleteStateClient(client3)
	})
}
func Test_DeleteExpiredKey(t *testing.T) {
	count := 0
	client := &StateClient{
		config: StateClientConfig{
			// trigger expire cb
			ExpiredCb: func(s string) {
				count += 1
			},
		},
	}
	now := time.Now().Unix()
	Convey("test no expire", t, func() {
		count = 0
		subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_noexpire"] = now + defaultStateTTL
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*servicediscovery.Registry).IsStateInsExist, func(_ *servicediscovery.Registry, stateID string) bool {
				return false
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		client.DeleteExpiredKey()
		So(subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_noexpire"], ShouldEqual, now+defaultStateTTL)
		So(count, ShouldEqual, 0)
		delete(subExpiredKeys.m, "{tenantID:0@serviceID}:stateID:stateID_noexpire")
	})
	Convey("test expire but stateID exist", t, func() {
		count = 0
		subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_expire_exist"] = now - 1
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*servicediscovery.Registry).IsStateInsExist, func(_ *servicediscovery.Registry, stateID string) bool {
				return stateID == "stateID_expire_exist"
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		client.DeleteExpiredKey()
		So(subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_expire_exist"], ShouldNotBeEmpty)
		// refresh expire time
		So(subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_expire_exist"], ShouldNotEqual, now-1)
		So(count, ShouldEqual, 0)
		delete(subExpiredKeys.m, "{tenantID:0@serviceID}:stateID:stateID_expire_exist")
	})
	Convey("test expire and stateID not exist", t, func() {
		count = 0
		subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_expire_notexist"] = now - 1
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*servicediscovery.Registry).IsStateInsExist, func(_ *servicediscovery.Registry, stateID string) bool {
				return stateID == "stateID_expire_exist"
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		client.DeleteExpiredKey()
		So(subExpiredKeys.m["{tenantID:0@serviceID}:stateID:stateID_expire_notexist"], ShouldEqual, 0)
		time.Sleep(time.Millisecond * 100)
		So(count, ShouldEqual, 1)
	})
	Convey("test expire and stateID invalid", t, func() {
		count = 0
		subExpiredKeys.m["{tenantID:0@serviceID}:stateID_invalid"] = now - 1
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*servicediscovery.Registry).IsStateInsExist, func(_ *servicediscovery.Registry, stateID string) bool {
				return false
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		client.DeleteExpiredKey()
		So(subExpiredKeys.m["{tenantID:0@serviceID}:stateID_invalid"], ShouldEqual, 0)
		time.Sleep(time.Millisecond * 100)
		So(count, ShouldEqual, 1)
	})
}
