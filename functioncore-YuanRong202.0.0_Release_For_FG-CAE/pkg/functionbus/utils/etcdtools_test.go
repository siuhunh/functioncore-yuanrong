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

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"functioncore/pkg/functionbus/common"
	"testing"
	"unsafe"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"functioncore/pkg/common/etcd3"
)

func TestDeleteValueInEtcd(t *testing.T) {
	convey.Convey("TestDeleteValueInEtcd", t, func() {
		convey.Convey("test: timeout", func() {
			patch := ApplyFunc((*etcd3.EtcdClient).Delete, func(w *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return context.DeadlineExceeded
			})
			patch.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			defer patch.Reset()
			err := DeleteValueInEtcd("key")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("success", func() {
			patches := [...]*Patches{
				ApplyFunc((*etcd3.EtcdClient).Delete, func(w *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
					return nil
				}),
				ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{}
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := DeleteValueInEtcd("key")
			assert.Nil(t, err)
		})
		convey.Convey("test: cae error", func() {
			patch := ApplyFunc((*etcd3.EtcdClient).Delete, func(w *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return context.DeadlineExceeded
			})
			patch.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			patch.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			patch.ApplyFunc(common.IsCAERouterEtcdDefined, func() bool {
				return true
			})
			defer patch.Reset()
			err := DeleteValueInEtcd("key")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test: cae router nil", func() {
			patch := ApplyFunc((*etcd3.EtcdClient).Delete, func(w *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return context.DeadlineExceeded
			})
			patch.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			patch.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: nil}
			})
			patch.ApplyFunc(common.IsCAERouterEtcdDefined, func() bool {
				return true
			})
			defer patch.Reset()
			err := DeleteValueInEtcd("key")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestGetValueFromEtcd(t *testing.T) {
	convey.Convey("TestGetValueFromEtcd", t, func() {
		convey.Convey("test: value nil", func() {
			patches := [...]*Patches{
				ApplyFunc((*etcd3.EtcdClient).GetValues,
					func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return nil, nil
					}),
				ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				ApplyFunc(concurrency.NewSession,
					func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
						return nil, nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			value, err := GetValueFromEtcd("key", etcd3.Meta)
			assert.Equal(t, "the value got from etcd is empty", err.Error())
			assert.Nil(t, value)
		})
		convey.Convey("test: timeout", func() {
			patch := NewPatches()
			patch.ApplyFunc((*etcd3.EtcdClient).GetValues,
				func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return nil, context.DeadlineExceeded
				})
			patch.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			defer patch.Reset()
			value, err := GetValueFromEtcd("key", 0)
			convey.So(value, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test: success", func() {
			patches := [...]*Patches{
				ApplyFunc((*etcd3.EtcdClient).GetValues,
					func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return []string{"value"}, nil
					}),
				ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				ApplyFunc(concurrency.NewSession,
					func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
						return nil, nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			value, err := GetValueFromEtcd("key", 0)
			assert.Nil(t, err)
			assert.Equal(t, []byte("value"), value)
		})
		convey.Convey("etcdClient = nil", func() {
			patches := [...]*Patches{
				ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
					return nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			_, err := GetValueFromEtcd("key", 0)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("etcdClient.Client = nil", func() {
			patches := [...]*Patches{
				ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: nil}
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			_, err := GetValueFromEtcd("key", 0)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestGetPodValue(t *testing.T) {
	convey.Convey("TestGetPodValue", t, func() {
		convey.Convey("get pod error", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, fmt.Errorf("get empty etcd client")
			})
			defer func() {
				patch.Reset()
			}()
			nodeIP, nodePort, err := GetPodValue("key")
			assert.Equal(t, "get empty etcd client", err.Error())
			assert.Equal(t, "", nodeIP)
			assert.Equal(t, "", nodePort)
		})
		convey.Convey("json error", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := nodeInfo{
					NodeIP:   "127.0.0.1",
					NodePort: "8080",
				}
				bytes := *(*[]byte)(unsafe.Pointer(&info))
				return bytes, nil
			})
			defer func() {
				patch.Reset()
			}()
			nodeIP, nodePort, err := GetPodValue("key")
			assert.NotNil(t, err)
			assert.Equal(t, "", nodeIP)
			assert.Equal(t, "", nodePort)
		})
		convey.Convey("ip or port invalid", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := nodeInfo{
					NodeIP:   "127.0.0.1",
					NodePort: "99999",
				}
				return json.Marshal(info)
			})
			defer func() {
				patch.Reset()
			}()
			nodeIP, nodePort, err := GetPodValue("key")
			assert.Contains(t, err.Error(), "error value of pod, ip")
			assert.Equal(t, "", nodeIP)
			assert.Equal(t, "", nodePort)
		})
		convey.Convey("success", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := nodeInfo{
					NodeIP:   "127.0.0.1",
					NodePort: "8080",
				}
				return json.Marshal(info)
			})
			defer func() {
				patch.Reset()
			}()
			nodeIP, nodePort, err := GetPodValue("key")
			assert.Nil(t, err)
			assert.Equal(t, "127.0.0.1", nodeIP)
			assert.Equal(t, "8080", nodePort)
		})
	})
}

func TestGetStateValue(t *testing.T) {
	convey.Convey("TestGetStateValue", t, func() {
		convey.Convey("get pod error", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, fmt.Errorf("get empty etcd client")
			})
			defer func() {
				patch.Reset()
			}()
			state, err := GetStateValue("key", "id")
			assert.Equal(t, "get empty etcd client", err.Error())
			assert.Equal(t, "", state)
		})
		convey.Convey("json error", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				curStates := make(map[string]string, 1)
				curStates["id"] = "state"
				bytes := *(*[]byte)(unsafe.Pointer(&curStates))
				return bytes, nil
			})
			defer func() {
				patch.Reset()
			}()
			state, err := GetStateValue("key", "id")
			assert.NotNil(t, err)
			assert.Equal(t, "", state)
		})
		convey.Convey("success", func() {
			patch := ApplyFunc(GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				curStates := make(map[string]string, 1)
				curStates["id"] = "state"
				return json.Marshal(curStates)
			})
			defer func() {
				patch.Reset()
			}()
			state, err := GetStateValue("key", "id")
			assert.Nil(t, err)
			assert.Equal(t, "state", state)
		})
	})
}

func TestInitEtcdClient(t *testing.T) {
	stopCh := make(chan struct{})
	err := InitEtcdClient(stopCh)
	assert.Nil(t, err)
}
