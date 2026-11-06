/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package options define server
package server

import (
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/etcd3"
	mockutils "functioncore/test/utils"
)

func TestIsBeingMaster(t *testing.T) {
	// 1. 测试 resp 中包含与 podName 匹配的值
	gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Value: []byte("testPodIP:cluster"),
				},
			},
		}, nil
	})
	podIP = "testPodIP"
	clusterID = "cluster"
	registerValue = podIP + ":" + clusterID
	result := isBeingMaser()
	assert.True(t, result)

	// 2. 测试 resp 为 nil 的情况
	gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
		return nil, nil
	})
	result = isBeingMaser()
	assert.False(t, result)

	// 3. 测试 resp.Kvs 为空的情况
	gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
	})
	result = isBeingMaser()
	assert.False(t, result)

	// 4. 测试 resp.Kvs[0].Value 与 podName 不匹配的情况
	gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Value: []byte("differentPodIP:cluster"),
				},
			},
		}, nil
	})
	podIP = "testPodIP"
	clusterID = "cluster"
	registerValue = podIP + ":" + clusterID
	result = isBeingMaser()
	assert.False(t, result)

	// 4. 测试error非空的场景
	gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Value: []byte("differentPodIP:cluster"),
				},
			},
		}, errors.New("test error")
	})
	podIP = "testPodIP"
	clusterID = "cluster"
	registerValue = podIP + ":" + clusterID
	result = isBeingMaser()
	assert.False(t, result)
}

func TestInitSelectMasterConfig(t *testing.T) {
	patches := mockutils.InitPatchSlice()

	// Test case 1: Valid configuration
	patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
		return &config.Config{
			SelectMasterLeaseTTL:        60, // 1 minute
			SelectMasterRefreshInterval: 30, // 30 seconds
		}, nil
	})})

	err := initSelectMasterConfig()
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}

	// Test case 2: Invalid configuration - Lease TTL is zero
	patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
		return &config.Config{
			SelectMasterLeaseTTL:        0,  // 0 minute
			SelectMasterRefreshInterval: 30, // 30 seconds
		}, nil
	})})
	err = initSelectMasterConfig()
	if err == nil {
		t.Error("Expected error due to zero Lease TTL")
	}

	// Test case 3: Invalid configuration - Refresh interval is too long
	patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
		return &config.Config{
			SelectMasterLeaseTTL:        60, // 1 minute
			SelectMasterRefreshInterval: 70, // 70 seconds
		}, nil
	})})
	err = initSelectMasterConfig()
	if err == nil {
		t.Error("Expected error due to Refresh interval being too long")
	}

	patches.ResetAll()
}

func TestStartSelecterMasterNode(t *testing.T) {
	// Test case 1: Valid stop channel
	t.Run("Valid stop channel", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(startRegiterMasterNodeCircle,
			func(stopCh <-chan struct{}, registerMasterNodeJobStopCh <-chan struct{}) {
				return
			})})
		stopCh := make(chan struct{})
		startSelecterMasterNode(stopCh)
		close(stopCh)
		patches.ResetAll()
	})

	// Test case 2: Nil stop channel
	t.Run("Nil stop channel", func(t *testing.T) {
		startSelecterMasterNode(nil)
	})

	// Test case 3: initSelectMasterConfig err
	t.Run("err initSelectMasterConfig", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(initSelectMasterConfig, func() error {
			return errors.New("test.errors")
		})})
		startSelecterMasterNode(nil)
		patches.ResetAll()
	})
}

func TestStartRegiterMasterNodeCircle(t *testing.T) {
	// Test case 1: Register master node and close
	t.Run("Stop register service on stop signal", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{}
			}),
		})

		stopCh := make(chan struct{})
		registerMasterNodeJobStopCh := make(chan struct{})

		go func() {
			time.Sleep(5 * time.Second)
			close(stopCh)
			close(registerMasterNodeJobStopCh)
		}()

		startRegiterMasterNodeCircle(stopCh, registerMasterNodeJobStopCh)

		patches.ResetAll()
	})
}

func TestRegisterMasterNode(t *testing.T) {
	// Test case 1: Empty response from getAutoScalerMasterLockKey
	t.Run("Empty response from getAutoScalerMasterLockKey", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
			return nil, nil
		})})
		stopCh := make(chan struct{})
		registerMasterNode(stopCh)
		close(stopCh)
		patches.ResetAll()
	})

	// Test case 2: error response from getAutoScalerMasterLockKey
	t.Run("error response from getAutoScalerMasterLockKey", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: []byte("testPodName"),
					},
				},
			}, errors.New("test error")
		})})
		stopCh := make(chan struct{})
		registerMasterNode(stopCh)
		close(stopCh)
		patches.ResetAll()
	})

	// Test case 3: zero key value from getAutoScalerMasterLockKey but putCurrentNodeAsMaster error
	t.Run("zero key value from getAutoScalerMasterLockKey", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
		}), gomonkey.ApplyFunc(putCurrentNodeAsMaster, func() error {
			return errors.New("test error")
		}), gomonkey.ApplyFunc(upgradeToMaterNode, func(stopCh <-chan struct{}) {
			return
		})})
		stopCh := make(chan struct{})
		registerMasterNode(stopCh)
		close(stopCh)
		patches.ResetAll()
	})

	// Test case 4: zero key valuefrom getAutoScalerMasterLockKey and putCurrentNodeAsMaster correct
	t.Run("zero key value from getAutoScalerMasterLockKey", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
		}), gomonkey.ApplyFunc(putCurrentNodeAsMaster, func() error {
			return nil
		}), gomonkey.ApplyFunc(upgradeToMaterNode, func(stopCh <-chan struct{}) {
			return
		})})
		stopCh := make(chan struct{})
		registerMasterNode(stopCh)
		close(stopCh)
		patches.ResetAll()
	})

	// Test case 5: zero key valuefrom getAutoScalerMasterLockKey and putCurrentNodeAsMaster correct
	t.Run("zero key value from getAutoScalerMasterLockKey", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{
						Value: []byte("testPodName"),
					},
				},
			}, nil
		}), gomonkey.ApplyFunc(putCurrentNodeAsMaster, func() error {
			return nil
		})})
		stopCh := make(chan struct{})
		registerMasterNode(stopCh)
		close(stopCh)
		patches.ResetAll()
	})
}

func TestGetAutoScalerMasterLockKey(t *testing.T) {
	// Test case 1: Successful response from routerClient
	t.Run("Successful response from routerClient", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})})
		getAutoScalerMasterLockKey()
		patches.ResetAll()
	})

	// Test case 2: RouterClient is not initialized
	t.Run("RouterClient is not initialized", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})})
		getAutoScalerMasterLockKey()
		patches.ResetAll()
	})

	// Test case 3: Error response from routerClient
	t.Run("Error response from routerClient", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, nil
				}),
		})
		getAutoScalerMasterLockKey()
		patches.ResetAll()
	})
}

func TestPutCurrentNodeAsMaster(t *testing.T) {
	// Test case 1: Successful execution
	t.Run("Successful execution", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Grant,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
					return 0, nil
				}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).PutOnlyKeyMissing,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
					opts ...clientv3.OpOption) error {
					return nil
				}),
		})
		putCurrentNodeAsMaster()
		patches.ResetAll()
	})

	// Test case 2: RouterClient's client is nil
	t.Run("RouterClient's client is nil", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})})
		putCurrentNodeAsMaster()
		patches.ResetAll()
	})

	// Test case 3: Error during Grant operation
	t.Run("Error during Grant operation", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Grant,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
					return 0, errors.New("test error")
				}),
		})
		putCurrentNodeAsMaster()
		patches.ResetAll()
	})

	// Test case 4: Error during PutOnlyKeyMissing operation
	t.Run("Error during PutOnlyKeyMissing operation", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Grant,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
					return 0, nil
				}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).PutOnlyKeyMissing,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
					opts ...clientv3.OpOption) error {
					return errors.New("test error")
				}),
		})
		putCurrentNodeAsMaster()
		patches.ResetAll()
	})
}

func TestStartRefreshLeaseJob(t *testing.T) {
	t.Run("NewEventDrive fail", func(t *testing.T) {
		stopMasterNodeCh := make(chan struct{})
		stopAutoscalerCh := make(chan struct{})

		// Mock the necessary functions and objects
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{}
			}),
			gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Revoke,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, leaseID clientv3.LeaseID) error {
					return nil
				}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete,
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
					return nil
				}),
		})

		go func() {
			time.Sleep(5 * time.Second)
			stopAutoscalerCh <- struct{}{} // Simulate autoscaler stop
			close(stopMasterNodeCh)
			close(stopAutoscalerCh)
		}()

		// Run the startRefreshLeaseJob function
		startRefreshLeaseJob(stopMasterNodeCh, stopAutoscalerCh)

		defer patches.ResetAll()
	})
}

func TestRefreshLease(t *testing.T) {
	// Mock the necessary functions and objects
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(isBeingMaser, func() bool {
			return true // Mocking isBeingMaser to return true
		}),
		gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
		gomonkey.ApplyFunc((*etcd3.EtcdClient).KeepAliveOnce,
			func(_ *etcd3.EtcdClient, ctx etcd3.EtcdCtxInfo, leaseID clientv3.LeaseID) error {
				return nil
			}),
	})

	registerMasterNodeJobStopCh := make(chan struct{})
	// Run the refreshLease function
	refreshLease(registerMasterNodeJobStopCh)

	close(registerMasterNodeJobStopCh)

	patches.ResetAll()
}

func TestUpgradeToMasterNode(t *testing.T) {
	stopCh2 := make(chan struct{})

	// Mock the necessary functions and objects
	patches2 := mockutils.InitPatchSlice()
	patches2.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
			return nil, errors.New("aaa")
		}),
	})

	// Run the upgradeToMaterNode function
	upgradeToMaterNode(stopCh2)

	// Close the channels to avoid goroutine leaks
	t.Cleanup(func() {
		close(stopCh2)
		patches2.ResetAll()
	})

	stopCh := make(chan struct{})

	// Mock the necessary functions and objects
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
			return &config.Config{}, nil
		}),
		gomonkey.ApplyFunc(startMasterNodeJob, func(stopCh <-chan struct{}, conf *config.Config) error {
			return nil
		}),
		gomonkey.ApplyFunc(startRefreshLeaseJob, func(stopMasterNodeCh <-chan struct{}, stopAutoscalerCh <-chan struct{}) {
			return
		}),
	})

	// Run the upgradeToMaterNode function
	upgradeToMaterNode(stopCh)

	// Close the channels to avoid goroutine leaks
	t.Cleanup(func() {
		close(stopCh)
		patches.ResetAll()
	})
}
