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

package etcd3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/signals"
	commonTLS "functioncore/pkg/common/tls"
	mockutils "functioncore/test/utils"
)

func TestInitEtcdClient(t *testing.T) {
	convey.Convey("TestInitEtcdClient", t, func() {
		convey.Convey("InitClientOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
					func(a *alarm.Client, almType alarm.Type) {
						return
					}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(clientv3.New, func(cfg clientv3.Config) (*clientv3.Client, error) {
					return &clientv3.Client{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopCh := make(<-chan struct{})
			err := InitParam().
				WithStopCh(stopCh).
				WithAlarmSwitch().
				WithMetaEtcdConfig(&ClientConfig{}).
				WithRouterEtcdConfig(&ClientConfig{}).
				WithCAEMetaEtcdConfig(&ClientConfig{}).
				WithCaeRouterEtcdConfig(&ClientConfig{}).
				WithMetaEtcdProxyConfig(&ClientConfig{}).
				WithCAEMetaEtcdProxyConfig(&ClientConfig{}).
				InitClient()
			convey.So(err, convey.ShouldBeNil)
			convey.So(metaEtcdClient, convey.ShouldNotBeNil)
			convey.So(routerEtcdClient, convey.ShouldNotBeNil)
			convey.So(GetCAEMetaEtcdClient(), convey.ShouldNotBeNil)
			convey.So(GetCaeRouterEtcdClient(), convey.ShouldNotBeNil)
			convey.So(GetMetaEtcdProxyEtcdClient(), convey.ShouldNotBeNil)
			convey.So(GetCAEMetaEtcdProxyEtcdClient(), convey.ShouldNotBeNil)
		})
	})
}

func TestNewClientError(t *testing.T) {
	convey.Convey("Test New Client Error 1", t, func() {
		routerEtcdClient, err := newClient(&ClientConfig{}, nil, false, Router)
		convey.So(routerEtcdClient, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Client Error 2", t, func() {
		stopCh := signals.WaitForSignal()
		routerEtcdClient, err := newClient(&ClientConfig{}, stopCh, false, Router)
		convey.So(routerEtcdClient, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Client Error 3", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
				return &EtcdClient{}, errors.New("fail")
			}),
		}
		stopCh := signals.WaitForSignal()
		metaEtcdClient, err := newClient(&ClientConfig{}, stopCh, false, Meta)
		convey.So(metaEtcdClient, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		for idx := range patches {
			patches[idx].Reset()
		}
	})
	convey.Convey("Test New Client Error 4", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
				return &EtcdClient{}, errors.New("fail")
			}),
		}
		stopCh := signals.WaitForSignal()
		caeRouterEtcd, err := newClient(&ClientConfig{}, stopCh, false, CAERouter)
		convey.So(caeRouterEtcd, convey.ShouldNotBeNil)
		convey.So(GetETCDClient(CAERouter), convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		for idx := range patches {
			patches[idx].Reset()
		}
	})
	convey.Convey("Test New Meta ETCD Proxy Client success", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
				return &EtcdClient{}, errors.New("fail")
			}),
		}
		metaEtcdClient = &EtcdClient{}
		stopCh := signals.WaitForSignal()
		metaEtcdProxyClient, err := newClient(&ClientConfig{}, stopCh, false, MetaProxy)
		convey.So(metaEtcdProxyClient, convey.ShouldNotBeNil)
		convey.So(GetETCDClient(MetaProxy), convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(metaEtcdProxyClient.DowngradesEtcdClient, convey.ShouldNotBeNil)
		for idx := range patches {
			patches[idx].Reset()
		}
	})
	convey.Convey("Test New CAE Meta ETCD Proxy Client success", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
				return &EtcdClient{}, errors.New("fail")
			}),
		}
		caeMetaEtcdClient = &EtcdClient{}
		stopCh := signals.WaitForSignal()
		caeMetaEtcdProxyClient, err := newClient(&ClientConfig{}, stopCh, false, CAEMetaProxy)
		convey.So(caeMetaEtcdProxyClient, convey.ShouldNotBeNil)
		convey.So(GetETCDClient(CAEMetaProxy), convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(caeMetaEtcdProxyClient.DowngradesEtcdClient, convey.ShouldNotBeNil)
		for idx := range patches {
			patches[idx].Reset()
		}
	})
}

func TestBuildClient(t *testing.T) {
	convey.Convey("Test Build Client Error 1", t, func() {
		conf := &ClientConfig{SslEnable: true}
		client, err := buildClient(conf)
		convey.So(client, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Build Client Error 2", t, func() {
		conf := &ClientConfig{SslEnable: false}
		client, err := buildClient(conf)
		convey.So(client, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Build Client Password", t, func() {
		patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			return []byte{}, nil
		})
		defer patch.Reset()
		conf := &ClientConfig{SslEnable: false, User: "a", Password: "a"}
		client, err := buildClient(conf)
		convey.So(client, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Build Client Error 4", t, func() {

	})
}

type KV2 struct {
}

func (kv *KV2) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return nil, errors.New("err")
}

func (kv *KV2) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	panic("implement me")
}

func (kv *KV2) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	panic("implement me")
}

func (kv *KV2) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	panic("implement me")
}

func (kv *KV2) Txn(ctx context.Context) clientv3.Txn {
	panic("implement me")
}

func (kv *KV2) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption,
) (*clientv3.PutResponse, error) {
	return nil, errors.New("err")
}

func TestGetRouterEtcdClient(t *testing.T) {
	client := GetRouterEtcdClient()
	assert.NotEmpty(t, client)
}

func TestGetMetadataEtcdClient(t *testing.T) {
	client := GetRouterEtcdClient()
	assert.NotEmpty(t, client)
}

func TestRestart(t *testing.T) {
	patch := mockutils.InitPatchSlice()
	client := &EtcdClient{config: &ClientConfig{SslEnable: false},
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}
	patch.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
		return &EtcdClient{
			Client:       &clientv3.Client{},
			clientExitCh: make(chan struct{}),
		}, nil
	})})
	patch.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&clientv3.Client{}), "Close",
		func(c *clientv3.Client) error {
			return errors.New("err")
		})})
	defer patch.ResetAll()
	client.RegisterRedoFunc(nil, RedoFailedOperations)
	err := client.restart()
	assert.Empty(t, err)
}

func TestGetBuildCfg(t *testing.T) {
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(commonTLS.GetX509CACertPool,
		func(caCertFilePath string) (caCertPool *x509.CertPool, err error) {
			return nil, nil
		})})
	defer patches.ResetAll()
	convey.Convey("Test build cfg error", t, func() {
		tlsConfig, err := buildCfg("DefaultCAFile", "DefaultCertFile", "DefaultKeyFile")
		convey.So(tlsConfig, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test build cfg", t, func() {
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(commonTLS.LoadServerTLSCertificate,
			func(certFile, keyFile string, passPhase string, isHttps bool) (tlsCert []tls.Certificate,
				err error) {
				return nil, nil
			})})
		tlsConfig, err := buildCfg("DefaultCAFile", "DefaultCertFile", "DefaultKeyFile")
		convey.So(tlsConfig, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestInitClientError(t *testing.T) {
	convey.Convey("Test init cfg error", t, func() {
		convey.Convey("Test init metaEtcd cfg error", func() {
			Param := &etcdInitParam{metaEtcdConfig: &ClientConfig{}}
			err := Param.InitClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("Test init routerEtcd cfg error", func() {
			Param := &etcdInitParam{routerEtcdConfig: &ClientConfig{}}
			err := Param.InitClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("Test init caeMetaEtcd cfg error", func() {
			Param := &etcdInitParam{caeMetaEtcdConfig: &ClientConfig{}}
			err := Param.InitClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("Test init caeRouterEtcd cfg error", func() {
			Param := &etcdInitParam{caeRouterEtcdConfig: &ClientConfig{}}
			err := Param.InitClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("Test init metaEtcdProxy cfg error", func() {
			Param := &etcdInitParam{metaEtcdProxyConfig: &ClientConfig{}}
			err := Param.InitClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("Test init caeMetaEtcdProxy cfg error", func() {
			Param := &etcdInitParam{caeMetaEtcdProxyConfig: &ClientConfig{}}
			err := Param.InitClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

// Test_checkConnState -
func Test_checkConnState(t *testing.T) {
	// An alarm should be reported when the query is abnormal.
	convey.Convey("test: check conn state", t, func() {
		convey.Convey("test: report and clear an alarm", func() {
			etcdClient := &clientv3.Client{
				KV: &KV2{},
			}
			client := &EtcdClient{
				etcdType:      Router,
				config:        &ClientConfig{},
				Client:        etcdClient,
				isAlarmEnable: true,
				operateETCD: func(ctx context.Context, path, value, operate string) error {
					return nil
				},
				redoFailedOperations: func(operateFunc func(ctx context.Context,
					path, value, operate string) error, stopCh <-chan struct{}) {
					return
				},
			}
			isReported := false
			isCleared := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
					func(a *alarm.Client, almType alarm.Type, specAddition string) {
						isReported = true
					}),
				gomonkey.ApplyFunc((*alarm.Client).ClearAlarm,
					func(a *alarm.Client, almType alarm.Type) {
						isCleared = true
					}),
				gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			client.checkConnState()
			convey.So(isReported, convey.ShouldBeFalse)
			client.checkConnState()
			client.checkConnState()
			convey.So(isReported, convey.ShouldBeTrue)
			convey.So(isCleared, convey.ShouldBeFalse)
			convey.Convey("test: clear an alarm", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(client.Client.KV), "Get", func(_ *KV2,
					ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{}, nil
				})
				client.checkConnState()
				convey.So(isCleared, convey.ShouldBeFalse)
			})
		})
	})
}

// Test_checkConnState -
func Test_doRestartCallback(t *testing.T) {
	// An alarm should be reported when the query is abnormal.
	convey.Convey("test: do restart callback", t, func() {
		convey.Convey("test: report and clear an alarm", func() {
			isRestartCallBack := false
			etcdClient := &clientv3.Client{
				KV: &KV2{},
			}
			client := &EtcdClient{
				etcdType: CAERouter,
				config:   &ClientConfig{},
				Client:   etcdClient,
				operateETCD: func(ctx context.Context, path, value, operate string) error {
					return nil
				},
				restartCallback: func(etcdClient *EtcdClient) {
					isRestartCallBack = true
				},
			}
			time := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
					func(a *alarm.Client, almType alarm.Type, specAddition string) {}),
				gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
					return nil, errors.New("test err")
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(client.Client.KV), "Get", func(_ *KV2,
					ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					time++
					if time > 1 {
						return &clientv3.GetResponse{}, nil
					} else {
						return &clientv3.GetResponse{}, errors.New("get err")
					}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			client.checkConnState()
			convey.So(isRestartCallBack, convey.ShouldBeFalse)
			client.checkConnState()
			convey.So(isRestartCallBack, convey.ShouldBeTrue)
		})
	})
}

// Test_checkMetaConnState -
func Test_checkMetaConnState(t *testing.T) {
	// An alarm should be reported when the query is abnormal.
	convey.Convey("test: check meta conn state", t, func() {
		convey.Convey("test: report and clear an alarm", func() {
			etcdClient := &clientv3.Client{
				KV: &KV2{},
			}
			client := &EtcdClient{
				etcdType:      Router,
				config:        &ClientConfig{},
				Client:        etcdClient,
				isAlarmEnable: true,
				operateETCD: func(ctx context.Context, path, value, operate string) error {
					return nil
				},
				redoFailedOperations: func(operateFunc func(ctx context.Context,
					path, value, operate string) error, stopCh <-chan struct{}) {
					return
				},
			}
			ErrorClient := &EtcdClient{
				etcdType:      Router,
				config:        &ClientConfig{},
				Client:        nil,
				isAlarmEnable: true,
				operateETCD: func(ctx context.Context, path, value, operate string) error {
					return nil
				},
				redoFailedOperations: func(operateFunc func(ctx context.Context,
					path, value, operate string) error, stopCh <-chan struct{}) {
					return
				},
			}
			isReported := false
			isCleared := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
					func(a *alarm.Client, almType alarm.Type, specAddition string) {
						isReported = true
					}),
				gomonkey.ApplyFunc((*alarm.Client).ClearAlarm,
					func(a *alarm.Client, almType alarm.Type) {
						isCleared = true
					}),
				gomonkey.ApplyFunc(buildClient, func(config *ClientConfig) (*EtcdClient, error) {
					return nil, errors.New("test err")
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(client.Client.KV), "Get", func(_ *KV2,
					ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{}, errors.New("b")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			client.checkMetaConnState(errors.New("a"))
			convey.So(isReported, convey.ShouldBeTrue)
			convey.So(isCleared, convey.ShouldBeFalse)
			ErrorClient.checkMetaConnState(errors.New("a"))
			convey.So(isReported, convey.ShouldBeTrue)
			convey.So(isCleared, convey.ShouldBeFalse)
			convey.Convey("test: clear an alarm", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(client.Client.KV), "Get", func(_ *KV2,
					ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{}, nil
				})
				client.checkMetaConnState(errors.New("a"))
				convey.So(isCleared, convey.ShouldBeFalse)
			})
		})
	})
}

// Test_restart -
func Test_restart(t *testing.T) {
	convey.Convey("test: etcd client restart", t, func() {
		convey.Convey("test: user is nil", func() {
			etcdClient := &clientv3.Client{
				Username: "test1",
			}
			client := &EtcdClient{
				config:       &ClientConfig{},
				Client:       etcdClient,
				clientExitCh: make(chan struct{}),
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(client.Client), "Close", func(_ *clientv3.Client) error {
					return nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("test"), nil
				}),
				gomonkey.ApplyFunc(clientv3.New, func(cfg clientv3.Config) (*clientv3.Client, error) {
					return &clientv3.Client{Username: client.config.User}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := client.restart()
			convey.So(err, convey.ShouldBeNil)
			convey.So(client.Client.Username, convey.ShouldNotEqual, etcdClient.Username)
		})
		convey.Convey("test: user name exists", func() {
			etcdClient := &clientv3.Client{
				Username: "test1",
			}
			client := &EtcdClient{
				config: &ClientConfig{
					User:       "test",
					ServerList: []string{"10.10.10.10:10"},
				},
				Client:       etcdClient,
				clientExitCh: make(chan struct{}),
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(client.Client), "Close", func(_ *clientv3.Client) error {
					return nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("test"), nil
				}),
				gomonkey.ApplyFunc(clientv3.New, func(cfg clientv3.Config) (*clientv3.Client, error) {
					return &clientv3.Client{Username: client.config.User}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := client.restart()
			convey.So(err, convey.ShouldBeNil)
			convey.So(client.Client.Username, convey.ShouldNotEqual, etcdClient.Username)
		})
	})
}

// TestEtcdClient_checkConnState_criticalAlarm -
func TestEtcdClient_checkConnState_criticalAlarm(t *testing.T) {
	convey.Convey("test: test criticalAlarm", t, func() {
		reportedNums := 0
		clearedNums := 0
		patch := gomonkey.ApplyFunc(alarm.GetClient, func() *alarm.Client {
			return new(alarm.Client)
		})
		patch.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
			if almType == alarm.MetaETCDConnectionAbnormalContinuously ||
				almType == alarm.RouterETCDConnectionAbnormalContinuously {
				reportedNums++
			}
		})
		patch.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			if almType == alarm.MetaETCDConnectionAbnormalContinuously ||
				almType == alarm.RouterETCDConnectionAbnormalContinuously {
				clearedNums++
			}
		})
		defer patch.Reset()
		convey.Convey("test: test report", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*KV2).Get, func(_ *KV2, ctx context.Context,
				key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return nil, errors.New("test err")
			})
			defer patch.Reset()
			client := &EtcdClient{
				Client:        &clientv3.Client{KV: &KV2{}},
				config:        &ClientConfig{},
				etcdType:      Router,
				isAlarmEnable: true,
			}
			for i := 0; i < 10; i++ {
				client.checkConnState()
			}
			convey.So(reportedNums, convey.ShouldEqual, 1)
		})
		convey.Convey("test: test clear", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*KV2).Get, func(_ *KV2, ctx context.Context,
				key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{}, nil
			})
			defer patch.Reset()
			client := &EtcdClient{
				Client:        &clientv3.Client{KV: &KV2{}},
				config:        &ClientConfig{},
				etcdType:      Router,
				isAlarmEnable: true,
				operateETCD: func(ctx context.Context, path, value, operate string) error {
					return nil
				},
				redoFailedOperations: func(operateFunc func(ctx context.Context, path string, value string, operate string) error, stopCh <-chan struct{}) {
					return
				},
			}
			client.checkConnState()
			convey.So(clearedNums, convey.ShouldEqual, 1)
		})
	})
}

// Test_CheckEtcdNodeStatus -
func Test_CheckEtcdNodeStatus(t *testing.T) {
	convey.Convey("test check etcd node status", t, func() {
		convey.Convey("check etcd node status success", func() {
			serverList := []string{"https://127.0.0.1:6379", "https://127.0.0.2:6379", "https://127.0.0.3:6379"}
			etcdClient := clientv3.Client{
				Maintenance: &mockutils.FakeMaintenance{},
			}
			client := &EtcdClient{
				etcdType: Router,
				config: &ClientConfig{
					ServerList: serverList,
				},
				Client: &etcdClient,
			}
			defer gomonkey.ApplyMethod(reflect.TypeOf(&clientv3.Client{}), "Endpoints", func(_ *clientv3.Client) []string {
				return []string{"https://127.0.0.1:6379", "https://127.0.0.2:6379", "https://127.0.0.3:6379"}
			}).Reset()
			defer gomonkey.ApplyMethod(
				reflect.TypeOf(&mockutils.FakeMaintenance{}), "Status",
				func(_ *mockutils.FakeMaintenance, ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
					return nil, nil
				}).Reset()
			status := client.CheckEtcdNodeStatus()
			convey.So(status, convey.ShouldBeTrue)
		})
		convey.Convey("check etcd node status fail", func() {
			serverList := []string{"https://127.0.0.1:6379", "https://127.0.0.2:6379", "https://127.0.0.3:6379"}
			etcdClient := clientv3.Client{
				Maintenance: &mockutils.FakeMaintenance{},
			}
			client := &EtcdClient{
				etcdType: Router,
				config: &ClientConfig{
					ServerList: serverList,
				},
				Client: &etcdClient,
			}
			defer gomonkey.ApplyMethod(reflect.TypeOf(&clientv3.Client{}), "Endpoints", func(_ *clientv3.Client) []string {
				return []string{"https://127.0.0.1:6379", "https://127.0.0.2:6379", "https://127.0.0.3:6379"}
			}).Reset()
			defer gomonkey.ApplyMethod(
				reflect.TypeOf(&mockutils.FakeMaintenance{}), "Status",
				func(_ *mockutils.FakeMaintenance, ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
					return nil, errors.New("mock error")
				}).Reset()
			status := client.CheckEtcdNodeStatus()
			convey.So(status, convey.ShouldBeFalse)
		})
	})
}

func TestEtcdClient_GetEtcdStatusLostContact(t *testing.T) {
	type fields struct {
		Client                     *clientv3.Client
		config                     *ClientConfig
		isAlarmEnable              bool
		etcdStatusAfterLostContact bool
		etcdStatusNow              uint32
		stopChan                   chan struct{}
		stopCh                     <-chan struct{}
		clientExitCh               chan struct{}
		operateETCD                operateETCDFunc
		redoFailedOperations       redoOperationsFunc
		etcdType                   EtcdType
		abnormalContinuouslyTimes  int
	}
	var f fields
	f.etcdStatusAfterLostContact = false
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"case1", f, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &EtcdClient{
				Client:                     tt.fields.Client,
				config:                     tt.fields.config,
				isAlarmEnable:              tt.fields.isAlarmEnable,
				etcdStatusAfterLostContact: tt.fields.etcdStatusAfterLostContact,
				etcdStatusNow:              tt.fields.etcdStatusNow,
				stopChan:                   tt.fields.stopChan,
				stopCh:                     tt.fields.stopCh,
				clientExitCh:               tt.fields.clientExitCh,
				operateETCD:                tt.fields.operateETCD,
				redoFailedOperations:       tt.fields.redoFailedOperations,
				etcdType:                   tt.fields.etcdType,
				abnormalContinuouslyTimes:  tt.fields.abnormalContinuouslyTimes,
			}
			if got := w.GetEtcdStatusLostContact(); got != tt.want {
				t.Errorf("GetEtcdStatusLostContact() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEtcdClient_GetEtcdStatusNow(t *testing.T) {
	type fields struct {
		Client                     *clientv3.Client
		config                     *ClientConfig
		isAlarmEnable              bool
		etcdStatusAfterLostContact bool
		etcdStatusNow              uint32
		stopChan                   chan struct{}
		stopCh                     <-chan struct{}
		clientExitCh               chan struct{}
		operateETCD                operateETCDFunc
		redoFailedOperations       redoOperationsFunc
		etcdType                   EtcdType
		abnormalContinuouslyTimes  int
	}
	var f fields
	f.etcdStatusNow = 0
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"case1", f, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &EtcdClient{
				Client:                     tt.fields.Client,
				config:                     tt.fields.config,
				isAlarmEnable:              tt.fields.isAlarmEnable,
				etcdStatusAfterLostContact: tt.fields.etcdStatusAfterLostContact,
				etcdStatusNow:              tt.fields.etcdStatusNow,
				stopChan:                   tt.fields.stopChan,
				stopCh:                     tt.fields.stopCh,
				clientExitCh:               tt.fields.clientExitCh,
				operateETCD:                tt.fields.operateETCD,
				redoFailedOperations:       tt.fields.redoFailedOperations,
				etcdType:                   tt.fields.etcdType,
				abnormalContinuouslyTimes:  tt.fields.abnormalContinuouslyTimes,
			}
			if got := w.GetEtcdStatusNow(); got != tt.want {
				t.Errorf("GetEtcdStatusNow() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestEtcdType(t *testing.T) {
	tests := []struct {
		etcdType EtcdType

		expectedName                        string
		connectionAlarm                     alarm.Type
		connectionAbnormalContinuouslyAlarm alarm.Type
	}{
		{
			etcdType:                            Meta,
			expectedName:                        "meta",
			connectionAlarm:                     alarm.MetadataEtcdConnection,
			connectionAbnormalContinuouslyAlarm: alarm.MetaETCDConnectionAbnormalContinuously,
		},
		{
			etcdType:                            Router,
			expectedName:                        "router",
			connectionAlarm:                     alarm.RouterEtcdConnection,
			connectionAbnormalContinuouslyAlarm: alarm.RouterETCDConnectionAbnormalContinuously,
		},
		{
			etcdType:                            CAEMeta,
			expectedName:                        "caeMeta",
			connectionAlarm:                     alarm.CAEMetaEtcdConnection,
			connectionAbnormalContinuouslyAlarm: alarm.CAEMetaETCDConnectionAbnormalContinuously,
		},
		{
			etcdType:                            MetaProxy,
			expectedName:                        "metaProxy",
			connectionAlarm:                     alarm.MetaEtcdProxyConnection,
			connectionAbnormalContinuouslyAlarm: alarm.MetaEtcdProxyConnectionAbnormalContinuously,
		},
		{
			etcdType:                            CAEMetaProxy,
			expectedName:                        "caeMetaProxy",
			connectionAlarm:                     alarm.CAEMetaEtcdProxyConnection,
			connectionAbnormalContinuouslyAlarm: alarm.CAEMetaEtcdProxyConnectionAbnormalContinuously,
		},
		{
			etcdType:                            -1,
			expectedName:                        "invalid etcd type -1",
			connectionAlarm:                     alarm.MetadataEtcdConnection,
			connectionAbnormalContinuouslyAlarm: alarm.MetaETCDConnectionAbnormalContinuously,
		},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(int(tt.etcdType)), func(t *testing.T) {
			assert.Equal(t, tt.expectedName, tt.etcdType.String())
			assert.Equal(t, tt.connectionAlarm, tt.etcdType.getEtcdConnectionAlarm())
			assert.Equal(t, tt.connectionAbnormalContinuouslyAlarm, tt.etcdType.getEtcdConnectionAbnormalContinuouslyAlarm())
		})
	}
}

// Test_CheckEtcdNodeStatus -
func TestEtcdClient_GetEtcdType(t *testing.T) {
	convey.Convey("TestEtcdClient_GetEtcdType", t, func() {
		w := &EtcdClient{etcdType: Meta}
		convey.So(w.GetEtcdType(), convey.ShouldEqual, Meta)
	})
}

func TestGetETCDClient(t *testing.T) {
	tests := []struct {
		beforeFunc     func()
		etcdType       EtcdType
		expectedClient *EtcdClient
	}{
		{
			beforeFunc: func() {
				metaEtcdClient = &EtcdClient{etcdType: Meta}
			},
			etcdType:       Meta,
			expectedClient: &EtcdClient{etcdType: Meta},
		},
		{
			beforeFunc: func() {
				caeRouterEtcdClient = &EtcdClient{etcdType: CAERouter}
				microServiceClusterEnable = "true"
			},
			etcdType:       State,
			expectedClient: &EtcdClient{etcdType: CAERouter},
		},
		{
			beforeFunc: func() {
				metaEtcdClient = &EtcdClient{etcdType: Meta}
				microServiceClusterEnable = "false"
			},
			etcdType:       State,
			expectedClient: &EtcdClient{etcdType: Meta},
		},
		{
			beforeFunc: func() {
				routerEtcdClient = &EtcdClient{etcdType: Router}
			},
			etcdType:       Router,
			expectedClient: &EtcdClient{etcdType: Router},
		},
		{
			beforeFunc: func() {
				caeMetaEtcdClient = &EtcdClient{etcdType: CAEMeta}
			},
			etcdType:       CAEMeta,
			expectedClient: &EtcdClient{etcdType: CAEMeta},
		},
		{
			beforeFunc: func() {
				metaEtcdProxyClient = &EtcdClient{etcdType: MetaProxy}
			},
			etcdType:       MetaProxy,
			expectedClient: &EtcdClient{etcdType: MetaProxy},
		},
		{
			beforeFunc: func() {
				caeMetaEtcdProxyClient = &EtcdClient{etcdType: MetaProxy}
			},
			etcdType:       CAEMetaProxy,
			expectedClient: &EtcdClient{etcdType: MetaProxy},
		},
		{
			beforeFunc: func() {
				metaEtcdClient = &EtcdClient{etcdType: Meta}
				metaEtcdProxyClient = &EtcdClient{etcdType: MetaProxy}
			},
			etcdType:       Meta,
			expectedClient: &EtcdClient{etcdType: MetaProxy},
		},
		{
			beforeFunc: func() {
				caeMetaEtcdClient = &EtcdClient{etcdType: CAEMeta}
				caeMetaEtcdProxyClient = &EtcdClient{etcdType: CAEMetaProxy}
			},
			etcdType:       CAEMeta,
			expectedClient: &EtcdClient{etcdType: CAEMetaProxy},
		},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(int(tt.etcdType)), func(t *testing.T) {
			tt.beforeFunc()
			got := GetETCDClient(tt.etcdType)
			if got.etcdType != tt.expectedClient.etcdType {
				t.Errorf("GetGetETCDClient() = %v, want %v", got, tt.expectedClient)
			}
			got = nil
		})
	}
	t.Run("invaild type", func(t *testing.T) {
		assert.Nil(t, GetETCDClient(-1))
	})
}
