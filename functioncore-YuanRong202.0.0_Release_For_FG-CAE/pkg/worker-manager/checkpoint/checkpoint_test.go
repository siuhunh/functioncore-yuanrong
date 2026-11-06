/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	fhttp "github.com/valyala/fasthttp"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

type mockKV struct {
	sync.RWMutex
	kv map[string]string
}

var FakeEtcd mockKV
var snapshotKey = "/snapshot/business/yrk/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@test-java-snapshot-restore-coldstart-001-hello/version/latest/id/000"
var mgr = manager{
	workerClient:      &fhttp.Client{},
	VpcHandler:        &vpcmanager.VpcHandler{},
	wg:                &sync.WaitGroup{},
	start:             time.Now(),
	finishNum:         atomic.Int64{},
	totalNum:          atomic.Int64{},
	needCheckPointMap: make(map[string]*types.SnapshotDeploymentSpec),
	cfg: &config.FaasConfig{
		HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
		LocalAuth:   &localauth.AuthConfig{},
		PoolMgrConfig: config.PoolMgrConfig{
			PoolContainerTags: config.PoolContainerImageTags{
				WorkerImageTag:     "yuanrong:test",
				RuntimeImageTag:    "yuanrong:test",
				WorkerInitImageTag: "yuanrong:test",
			},
			MultiSnapshotConfig: config.MultiSnapshotConfig{
				Enable: true,
				SnapshotCheckInfo: types.SnapshotCheckInfo{
					JavaRuntimeSize:  123123,
					RunTimeSize:      123123,
					ApmJavaAgentSize: 123123,
					ImageOS:          "aaa",
				},
			},
			CheckpointPodConfig: config.CheckpointPodConfig{
				MaxConcurrency:       1,
				FailureRetryInterval: 1,
				FailureRetryTimes:    2,
			},
		},
	},
	ConcurrencyLimiter: CreateConcurrencyLimiter(defaultMaxConcurrency),
}

func TestInitManager(t *testing.T) {
	convey.Convey("TestInitManager", t, func() {
		convey.Convey("empty clusterID", func() {
			err := InitManager(nil, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("GetVPCController error", func() {
			clusterID = "aaa"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
					return nil, errors.New("test")
				}),
				gomonkey.ApplyFunc(alarm.GetClient, func() *alarm.Client {
					return new(alarm.Client)
				}),
				gomonkey.ApplyFunc((*manager).monitorCheckpointPods, func(_ *manager, stopCh <-chan struct{}) {
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitManager(nil, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("get config error", func() {
			clusterID = "aaa"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
					return &vpcmanager.VpcHandler{}, nil
				}),
				gomonkey.ApplyFunc((*manager).monitorCheckpointPods, func(_ *manager, stopCh <-chan struct{}) {
					return
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return nil, errors.New("mock error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitManager(nil, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("init normal success", func() {
			clusterID = "aaa"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
					return &vpcmanager.VpcHandler{}, nil
				}),
				gomonkey.ApplyFunc((*manager).monitorCheckpointPods, func(_ *manager, stopCh <-chan struct{}) {
					return
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1, FailureRetryInterval: 1}}}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitManager(nil, nil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestRunWatcher(t *testing.T) {
	convey.Convey("TestRunWatcher", t, func() {
		assertFlag := false
		stopCh := make(chan struct{})
		once := sync.Once{}
		resultChan := make(chan *etcd3.Event, 2)
		resultChan <- &etcd3.Event{}
		resultChan <- &etcd3.Event{}
		stopOnce := sync.Once{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(parseCheckpointEvent, func(event *etcd3.Event, etcdType etcd3.EtcdType) *SnapshotEvent {
				SnapshotEvent := &SnapshotEvent{}
				key := snapshotEtcdKey{}
				spec := &types.SnapshotDeploymentSpec{}
				once.Do(func() {
					spec = nil
					SnapshotEvent.snapshotEtcdKey = key
					SnapshotEvent.snapshotResult = spec
				})
				return SnapshotEvent
			}),
			gomonkey.ApplyFunc((*manager).startCheckpoint, func(_ *manager, checkpointEvent *SnapshotEvent,
				dur *duration, checkpointMode string) {
				close(resultChan)
			}),
			gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				assertFlag = true
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(_ *etcd3.EtcdClient,
				ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return &etcd3.EtcdWatchChan{ResultChan: resultChan}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdWatchChan{}), "Stop", func(_ *etcd3.EtcdWatchChan) {
				stopOnce.Do(func() {
					close(stopCh)
				})
				return
			}),
			gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
				ch := make(chan time.Time, 1)
				ch <- time.Time{}
				return &time.Ticker{C: ch}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		mgr := &manager{ConcurrencyLimiter: CreateConcurrencyLimiter(defaultMaxConcurrency)}
		mgr.runWatcher(stopCh)
		time.Sleep(100 * time.Millisecond)
		convey.So(assertFlag, convey.ShouldBeTrue)
	})
}

func Test_CAEConfigIsEmpty(t *testing.T) {
	convey.Convey("TestCAEConfigIsEmpty", t, func() {
		assertFlag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				return nil
			}),
			gomonkey.ApplyFunc(getWatchChan, func(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
				assertFlag = true
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		stopCh := make(chan struct{})
		mgr := &manager{}
		mgr.processCheckpointWatcher(&watchOption{
			ETCDSelection: etcd3.CAEMeta,
			Path:          watchPathPrefix + clusterID,
			StopCh:        stopCh,
		})
		convey.So(assertFlag, convey.ShouldBeFalse)
	})
}

func Test_parseCheckpointEvent(t *testing.T) {
	snapshotDeployment := `{"bucketId": "ak"}`
	multiSnapshotDeployment := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip\",\"bucketUrl\":\"\",\"sha256\":\"\",\"size\":0,\"snapshotVersions\":[{\"objectId\":\"khj.zip\",\"sha256\":\"\",\"size\":0}]}"
	multiVersionKey := "/snapshot/business/yrk/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@test-java-snapshot-restore-coldstart-001-hello/version/latest/id/000\n"
	key := "/snapshot/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}/id/{snapshotID}"
	convey.Convey("Test parseCheckpointEvent", t, func() {
		convey.Convey("put empty value", func() {
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(""),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeTrue)
		})
		convey.Convey("put empty key", func() {
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   "",
				Value: []byte(snapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeTrue)
		})
		convey.Convey("delete", func() {
			event := &etcd3.Event{
				Type:  etcd3.DELETE,
				Key:   "",
				Value: []byte(snapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeTrue)
		})
		convey.Convey("put valid key value", func() {
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(snapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeFalse)
		})
		convey.Convey("parse first multi snapshot version event", func() {
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   multiVersionKey,
				Value: []byte(multiSnapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeFalse)
			convey.So(snapshotEvent.snapshotResult.SnapshotVersions, convey.ShouldNotBeNil)
		})
		convey.Convey("parse update snapshot version event", func() {
			updateMultiSnapshotDeployment := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip\",\"bucketUrl\":\"\",\"sha256\":\"\",\"size\":0,\"snapshotVersions\":[{\"objectId\":\"khj.zip\",\"sha256\":\"testhash\",\"size\":12312}]}"
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   multiVersionKey,
				Value: []byte(updateMultiSnapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeTrue)
		})

		convey.Convey("parse checkpoint finished event", func() {
			snapshotDeploymentFinsied := `{"bucketId": ""}`
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(snapshotDeploymentFinsied),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(finishedCheckpointEvent(snapshotEvent), convey.ShouldBeTrue)
		})
	})
}

func Test_startCheckpoint(t *testing.T) {
	cfg := &config.FaasConfig{
		HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
		LocalAuth:   &localauth.AuthConfig{},
		PoolMgrConfig: config.PoolMgrConfig{
			PoolContainerTags: config.PoolContainerImageTags{
				WorkerImageTag:     "yuanrong:test",
				RuntimeImageTag:    "yuanrong:test",
				WorkerInitImageTag: "yuanrong:test",
			},
			MultiSnapshotConfig: config.MultiSnapshotConfig{
				Enable: true,
				SnapshotCheckInfo: types.SnapshotCheckInfo{
					JavaRuntimeSize:  123123,
					JavaRuntimeHash:  "123hash",
					RunTimeSize:      123123,
					ApmJavaAgentSize: 123123,
					ImageOS:          "aaa",
				},
			},
		},
	}

	initCheckpointPodConfig(cfg)
	mgr := manager{
		workerClient:       &fhttp.Client{},
		VpcHandler:         &vpcmanager.VpcHandler{},
		cfg:                cfg,
		ConcurrencyLimiter: CreateConcurrencyLimiter(defaultMaxConcurrency),
	}
	var result string
	convey.Convey("Test_startCheckpoint", t, func() {
		convey.Convey("failed to get function meta", func() {
			dur := &duration{receiveRequest: time.Now()}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient,
					func(t etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			convey.So(strings.Contains(result, "failed to get function meta"), convey.ShouldBeTrue)
		})
		convey.Convey("failed HandleVpcFunctionInfo", func() {
			dur := &duration{receiveRequest: time.Now()}
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					fetcher := &podfetcher.PodFetcher{}
					fetcher.InitContexts()
					fetcher.CreateNewContext("aaa")
					return fetcher
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).GeneralizePodInternal,
					func(_ *podfetcher.PodFetcher, ctx context.Context, reqInfo *config.RequestInfo,
						service *faasfunction.FunctionService) (*corev1.Pod, error) {
						return nil, snerror.New(1, "vpc")
					}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient,
					func(t etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(utils.CrName,
					func(business, tenant, funcName, version string) string {
						return "aaa"
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			convey.So(strings.Contains(result, "vpc"), convey.ShouldBeTrue)
		})
		convey.Convey("failed with context", func() {
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			fetcher := &podfetcher.PodFetcher{}
			fetcher.InitContexts()
			dur := &duration{receiveRequest: time.Now()}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
					func() *podfetcher.PodFetcher {
						return fetcher
					}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient,
					func(t etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(utils.CrName,
					func(business, tenant, funcName, version string) string {
						return "aaa"
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			convey.So(strings.Contains(result, "150424"), convey.ShouldBeTrue)
		})
		convey.Convey("success GeneralizePod but checkpoint failed", func() {
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			fetcher := &podfetcher.PodFetcher{}
			fetcher.InitContexts()
			fetcher.CreateNewContext("aaa")
			dur := &duration{receiveRequest: time.Now()}
			timePatches := gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
				return
			})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
					func() *podfetcher.PodFetcher {
						return fetcher
					}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).GeneralizePodInternal, func(_ *podfetcher.PodFetcher,
					ctx context.Context, reqInfo *config.RequestInfo,
					service *faasfunction.FunctionService) (*corev1.Pod, error) {
					return &corev1.Pod{}, nil
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S, func(_ *podfetcher.PodFetcher, podName, traceID string) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						resp.SetStatusCode(fhttp.StatusNotFound)
						return nil
					}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient,
					func(t etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(utils.CrName,
					func(business, tenant, funcName, version string) string {
						return "aaa"
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			fetcher.CreateNewContext("aaa")
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			timePatches.Reset()
			convey.So(strings.Contains(result, "unsupported"), convey.ShouldBeTrue)
			time.Sleep(100 * time.Millisecond)
		})
		convey.Convey("success GeneralizePod and checkpoint success", func() {
			dur := &duration{receiveRequest: time.Now()}
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			fetcher := &podfetcher.PodFetcher{}
			fetcher.InitContexts()
			fetcher.CreateNewContext("aaa")
			once := sync.Once{}
			timePatches := gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
				return
			})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
					func() *podfetcher.PodFetcher {
						return fetcher
					}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).GeneralizePodInternal, func(_ *podfetcher.PodFetcher,
					ctx context.Context, reqInfo *config.RequestInfo,
					service *faasfunction.FunctionService) (*corev1.Pod, error) {
					return &corev1.Pod{}, nil
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S, func(_ *podfetcher.PodFetcher, podName, traceID string) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						body, _ := json.Marshal(&types.SnapshotDeploymentSpec{BucketID: "BucketID", SnapshotSize: 11})
						once.Do(func() {
							body = []byte("not finished")
						})
						resp.SetBody(body)
						resp.SetStatusCode(fhttp.StatusOK)
						return nil
					}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return cfg, nil
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient,
					func(t etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(utils.CrName,
					func(business, tenant, funcName, version string) string {
						return "aaa"
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			convey.So(strings.Contains(result, "BucketID"), convey.ShouldBeTrue)
			timePatches.Reset()
			time.Sleep(100 * time.Millisecond)
		})
	})
}

func Test_manager_processMultiVersion(t *testing.T) {
	convey.Convey("Test_processMultiVersion", t, func() {
		convey.Convey("processMultiVersion error", func() {
			err := errors.New("processMultiVersion error")
			value, _ := mgr.processMultiVersion(err, &types.SnapshotDeploymentSpec{}, constants.CheckpointModeCreate)
			snapshotDeployment := &types.SnapshotDeploymentSpec{}
			json.Unmarshal(value, snapshotDeployment)
			convey.So(snapshotDeployment.SnapshotVersions, convey.ShouldNotBeNil)
			convey.So(len(snapshotDeployment.SnapshotVersions), convey.ShouldEqual, 1)
			convey.So(snapshotDeployment.SnapshotVersions[0].ErrCode, convey.ShouldEqual, utils.StatusInternalServerError)
			convey.So(snapshotDeployment.SnapshotVersions[0].Timestamp, convey.ShouldNotEqual, 0)
		})
		convey.Convey("processMultiVersion successfully with create mode", func() {
			multiSnapshotDeployment := &types.SnapshotDeploymentSpec{
				ObjectID:       "khj.zip",
				SnapshotSha256: "etsthash",
				SnapshotSize:   123,
				SnapshotVersions: []types.SnapshotVersion{
					{SnapshotSha256: "",
						SnapshotSize: 0},
				},
			}

			value, _ := mgr.processMultiVersion(nil, multiSnapshotDeployment, constants.CheckpointModeCreate)
			snapshotDeployment := &types.SnapshotDeploymentSpec{}
			json.Unmarshal(value, snapshotDeployment)
			convey.So(snapshotDeployment.SnapshotVersions, convey.ShouldNotBeNil)
			convey.So(len(snapshotDeployment.SnapshotVersions), convey.ShouldEqual, 1)
			convey.So(snapshotDeployment.SnapshotVersions[0].SnapshotSha256, convey.ShouldEqual, "etsthash")
			convey.So(snapshotDeployment.SnapshotVersions[0].SnapshotSize, convey.ShouldEqual, 123)
		})
		convey.Convey("processMultiVersion successfully with update mode", func() {
			multiSnapshotDeployment := &types.SnapshotDeploymentSpec{
				ObjectID:       "khj.zip",
				SnapshotSha256: "etsthash",
				SnapshotSize:   123,
			}

			value, _ := mgr.processMultiVersion(nil, multiSnapshotDeployment, constants.CheckpointModeUpdate)
			snapshotDeployment := &types.SnapshotDeploymentSpec{}
			json.Unmarshal(value, snapshotDeployment)
			convey.So(snapshotDeployment.SnapshotVersions, convey.ShouldNotBeNil)
			convey.So(len(snapshotDeployment.SnapshotVersions), convey.ShouldEqual, 1)
			convey.So(snapshotDeployment.SnapshotVersions[0].SnapshotSha256, convey.ShouldEqual, "etsthash")
			convey.So(snapshotDeployment.SnapshotVersions[0].SnapshotSize, convey.ShouldEqual, 123)
		})
	})
}

func Test_manager_updateSnapshotVersion(t *testing.T) {
	convey.Convey("Test_updateSnapshotVersion", t, func() {
		convey.Convey("succeed to updateSnapshotVersion ", func() {
			count := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotValue := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip\",\"bucketUrl\":\"\",\"sha256\":\"testhash\",\"size\":12312}"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
					count++
					switch count {
					case 1:
						return nil
					case 2:
						return &etcd3.EtcdClient{}
					default:
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}
				}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := mgr.updateSnapshotListFromEtcd()
			convey.So(err, convey.ShouldBeNil)
			mgr.updateSnapshotVersion()
		})
		convey.Convey("failed to updateSnapshotVersion ", func() {
			patch := gomonkey.ApplyFunc((*manager).updateSnapshotListFromEtcd, func(_ *manager) error {
				return errors.New("error")
			})
			defer patch.Reset()
			mgr.updateSnapshotVersion()
		})

		convey.Convey("failed to unmarshal the json ", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotValue := "test"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			mgr.updateSnapshotVersion()
		})

		convey.Convey("current snapshot version exists ", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotValue := "{\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip.201.0.0.B001.202304150440\",\"bucketUrl\":\"\",\"sha256\":\"52ace7cfb12e5c505a2cfe972e00d6f47a6d096304d1d9286032f01f1fba4ebf\",\"size\":18001571,\"snapshotVersions\":[{\"objectId\":\"khj.zip.201.0.0.B001.20230415053440\",\"sha256\":\"ae4d652a963fdbd22822dfa737132bd166dfea32fc765f33f47fb492b3413dcb\",\"size\":17954937,\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"imageTag\":\"201.0.0.B001.2023041505340\",\"timestamp\":1681735337}]}"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			mgr.updateSnapshotVersion()
		})

		convey.Convey("failed to marshal snapshotDeployment ", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotValue := "{\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip.201.0.0.B001.202304150440\",\"bucketUrl\":\"\",\"sha256\":\"52ace7cfb12e5c505a2cfe972e00d6f47a6d096304d1d9286032f01f1fba4ebf\",\"size\":18001571,\"snapshotVersions\":[{\"objectId\":\"khj.zip.201.0.0.B001.20230415053440\",\"sha256\":\"ae4d652a963fdbd22822dfa737132bd166dfea32fc765f33f47fb492b3413dcb\",\"size\":17954937,\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"imageTag\":\"201.0.0.B001.2023041505340\",\"timestamp\":1681735337}]}"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
				gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("json.Marshal error")
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			mgr.updateSnapshotVersion()
		})

		convey.Convey("failed to submit result ", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotValue := "{\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip.201.0.0.B001.202304150440\",\"bucketUrl\":\"\",\"sha256\":\"52ace7cfb12e5c505a2cfe972e00d6f47a6d096304d1d9286032f01f1fba4ebf\",\"size\":18001571,\"snapshotVersions\":[{\"objectId\":\"khj.zip.201.0.0.B001.20230415053440\",\"sha256\":\"ae4d652a963fdbd22822dfa737132bd166dfea32fc765f33f47fb492b3413dcb\",\"size\":17954937,\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"imageTag\":\"201.0.0.B001.2023041505340\",\"timestamp\":1681735337}]}"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
				gomonkey.ApplyFunc((*manager).submitResult, func(_ *manager, resultKey, traceID string, result []byte, etcdType etcd3.EtcdType) error {
					return errors.New("submitResult error")
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			mgr.updateSnapshotVersion()
		})
	})
}

func Test_manager_reCheckpointSnapshotList(t *testing.T) {
	convey.Convey("Test_reCheckpointSnapshotList", t, func() {
		FakeEtcd.kv = make(map[string]string)
		info := types.SnapshotCheckInfo{
			JavaRuntimeSize:  123123,
			RunTimeSize:      123123,
			ApmJavaAgentSize: 123123,
			ImageOS:          "aaa",
		}
		convey.Convey("failed to failed to unmarshal the json ", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotValue := "testdsdsds"
			FakeEtcd.kv[snapshotKey] = snapshotValue
			var kvs []*mvccpb.KeyValue
			for k, v := range FakeEtcd.kv {
				kvs = append(kvs, &mvccpb.KeyValue{
					Key:   []byte(k),
					Value: []byte(v),
				})
			}
			snapshotList := &clientv3.GetResponse{
				Kvs: kvs,
			}
			err := mgr.reCheckpointSnapshotList(snapshotList, "imageTagTest", info)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("succeed to reCheckpointSnapshotList ", func() {
			snapshotValue := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\"," +
				"\"objectId\":\"khj.zip.201.0.0.B001.20230415053440\"," +
				"\"bucketUrl\":\"\"," +
				"\"sha256\":\"14fdcfd43314271a3908082428fa532fef8cc3e039d86871e7c3e137dd581e7c\"," +
				"\"size\":17826023," +
				"\"snapshotVersions\":[{\"objectId\":\"khj.zip.201.0.0.B001.20230415053440\",\"sha256\":\"ae4d652a963fdbd22822dfa737132bd166dfea32fc765f33f47fb492b3413dcb\",\"size\":17954937,\"snapshotCheckInfo\":{\"javaRuntimeSize\":5734900,\"javaRuntimeHash\":\"d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2\"},\"imageTag\":\"201.0.0.B001.2023041502ddss0\",\"timestamp\":1681735337}]}"
			FakeEtcd.kv = make(map[string]string)
			FakeEtcd.kv[snapshotKey] = snapshotValue
			var kvs []*mvccpb.KeyValue
			for k, v := range FakeEtcd.kv {
				kvs = append(kvs, &mvccpb.KeyValue{
					Key:   []byte(k),
					Value: []byte(v),
				})
			}
			snapshotList := &clientv3.GetResponse{
				Kvs: kvs,
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := mgr.reCheckpointSnapshotList(snapshotList, "imageTagTest", info)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_manager_reCheckpointByVersion(t *testing.T) {
	convey.Convey("Test_reCheckpointByVersion", t, func() {
		imageTag := "test"
		snapshotValue := &types.SnapshotDeploymentSpec{
			BucketID:       "bucket-test-log",
			ObjectID:       "khj.zip",
			SnapshotSha256: "14fdcfd43314271a3908082428fa532fef8cc3e039d86871e7c3e137dd581e7c",
			SnapshotSize:   17826023,
			SnapshotVersions: []types.SnapshotVersion{
				{
					SnapshotImageTag: imageTag,
					ObjectID:         "khj.zip",
					SnapshotSize:     17826023,
					SnapshotSha256:   "14fdcfd43314271a3908082428fa532fef8cc3e039d86871e7c3e137dd581e7c",
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize: 5734900,
						ImageOS:         "d20dd6fe755f7d418f743ba3d9c450a47fa2594fa460e23311678d00229028c2",
					},
				},
			},
		}
		out, _ := json.Marshal(&snapshotValue)
		snapshotValueStr := string(out)

		FakeEtcd.kv = make(map[string]string)
		FakeEtcd.kv[snapshotKey] = snapshotValueStr
		var kvs []*mvccpb.KeyValue
		for k, v := range FakeEtcd.kv {
			kvs = append(kvs, &mvccpb.KeyValue{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
		snapshotList := &clientv3.GetResponse{
			Kvs: kvs,
		}

		info := types.SnapshotCheckInfo{
			JavaRuntimeSize:  123123,
			ApmJavaAgentSize: 123123,
			RunTimeSize:      123123,
			ImageOS:          "aaa",
		}

		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("reCheckpointByVersion imageTag is the same ", func() {
			keyObj := parseFromKey(snapshotKey)
			mgr.reCheckpointByVersion(snapshotValue, imageTag, info, keyObj, snapshotList.Kvs[0])

			snapshotVersion := snapshotValue.SnapshotVersions[0]
			convey.ShouldEqual(snapshotVersion.SnapshotImageTag, imageTag)
		})

		convey.Convey("reCheckpointByVersion imageTag is not the same ", func() {
			keyObj := parseFromKey(snapshotKey)
			info2 := types.SnapshotCheckInfo{
				JavaRuntimeSize:  123131,
				ApmJavaAgentSize: 123123,
				RunTimeSize:      123123,
				ImageOS:          "aaa",
			}
			mgr.reCheckpointByVersion(snapshotValue, "test2", info2, keyObj, snapshotList.Kvs[0])
			snapshotVersion := snapshotValue.SnapshotVersions[0]
			time.Sleep(100 * time.Millisecond)
			convey.ShouldEqual(snapshotVersion.SnapshotImageTag, "test2")
		})
	})
}

func Test_manager_runUpdating(t *testing.T) {
	convey.Convey("Test_runUpdating", t, func() {
		convey.Convey("runUpdating task", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotKey := "/snapshot/business/yrk/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@test-java-snapshot-restore-coldstart-001-hello/version/latest/id/000"
						snapshotValue := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip\",\"bucketUrl\":\"\",\"sha256\":\"testhash\",\"size\":12312}"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			mgr.runUpdating()
		})

	})
}
func Test_manager_logProgress(t *testing.T) {
	var mgr = manager{
		wg:                 &sync.WaitGroup{},
		start:              time.Now(),
		finishNum:          atomic.Int64{},
		totalNum:           atomic.Int64{},
		ConcurrencyLimiter: CreateConcurrencyLimiter(1),
		cfg: &config.FaasConfig{
			HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
			LocalAuth:   &localauth.AuthConfig{},
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					WorkerImageTag:     "yuanrong:test",
					RuntimeImageTag:    "yuanrong:test",
					WorkerInitImageTag: "yuanrong:test",
				},
				MultiSnapshotConfig: config.MultiSnapshotConfig{
					Enable: true,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize:  123123,
						ApmJavaAgentSize: 123123,
						RunTimeSize:      123123,
						ImageOS:          "aaa",
					},
				},
				CheckpointPodConfig: config.CheckpointPodConfig{
					MaxConcurrency: 1,
				},
			},
		},
	}
	convey.Convey("Test_logProgress", t, func() {
		convey.Convey("No need to update snapshot and exit", func() {
			mgr.wg.Add(1)
			mgr.logProgress()
		})
		convey.Convey("Update snapshot and exit", func() {
			mgr.wg.Add(1)
			mgr.totalNum.Add(1)
			mgr.finishNum.Add(1)
			mgr.logProgress()
			mgr.totalNum.Sub(1)
			mgr.finishNum.Sub(1)
		})
		convey.Convey("Update snapshot but cost too much time, exit", func() {
			duration := time.Duration(50 * 60 * 1000000000)
			patch := gomonkey.ApplyFunc(time.Since, func(t time.Time) time.Duration {
				return duration
			})
			defer patch.Reset()
			mgr.wg.Add(1)
			mgr.totalNum.Add(10)
			mgr.logProgress()
			mgr.totalNum.Sub(10)
			convey.ShouldEqual(logProgressLast, float64(10*5)/float64(mgr.ConcurrencyLimiter.limit))
		})
		convey.Convey("Task num is 1, update snapshot but cost too much time exit", func() {
			duration := time.Duration(20 * 60 * 1000000000)
			patch := gomonkey.ApplyFunc(time.Since, func(t time.Time) time.Duration {
				return duration
			})
			defer patch.Reset()
			mgr.wg.Add(1)
			mgr.totalNum.Add(1)
			mgr.logProgress()
			convey.ShouldEqual(logProgressLast, 10)
			mgr.totalNum.Sub(1)
		})
	})
}

func Test_manager_addFailNumHandleCheckpointFailed(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: atomic.Int64{},
		totalNum:  atomic.Int64{},
		cfg: &config.FaasConfig{
			HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
			LocalAuth:   &localauth.AuthConfig{},
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					WorkerImageTag:     "yuanrong:test",
					RuntimeImageTag:    "yuanrong:test",
					WorkerInitImageTag: "yuanrong:test",
				},
				MultiSnapshotConfig: config.MultiSnapshotConfig{
					Enable: true,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize:  123123,
						ApmJavaAgentSize: 123123,
						RunTimeSize:      123123,
						ImageOS:          "aaa",
					},
				},
				CheckpointPodConfig: config.CheckpointPodConfig{
					MaxConcurrency: 1,
				},
			},
		},
	}
	convey.Convey("Test_manager_addFailNumHandleCheckpointFailed", t, func() {
		convey.Convey("Handle checkpoint failed add failed num", func() {
			mgr.addFailNumHandleCheckpointFailed(constants.CheckpointModeUpdate, true)
			convey.ShouldEqual(mgr.failedNum, 1)
			mgr.failedNum.Dec()
			mgr.addFailNumHandleCheckpointFailed(constants.CheckpointModeUpdate, false)
			convey.ShouldEqual(mgr.failedNum, 0)
		})
	})
}

func Test_manager_addFailedrecordExecuteCheckPointFailed(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: atomic.Int64{},
		totalNum:  atomic.Int64{},
		cfg: &config.FaasConfig{
			HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
			LocalAuth:   &localauth.AuthConfig{},
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					WorkerImageTag:     "yuanrong:test",
					RuntimeImageTag:    "yuanrong:test",
					WorkerInitImageTag: "yuanrong:test",
				},
				MultiSnapshotConfig: config.MultiSnapshotConfig{
					Enable: true,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize:  123123,
						ApmJavaAgentSize: 123123,
						RunTimeSize:      123123,
						ImageOS:          "aaa",
					},
				},
				CheckpointPodConfig: config.CheckpointPodConfig{
					MaxConcurrency: 1,
				},
			},
		},
	}
	convey.Convey("Test_manager_addFailedrecordExecuteCheckPointFailed", t, func() {
		convey.Convey("Execute Checkpoint Failed add failed Num", func() {
			mgr.addFailNumExecuteCheckpointFailed(constants.CheckpointModeUpdate, false)
			convey.ShouldEqual(mgr.failedNum, 1)
		})
	})
}

func Test_manager_recordExecuteCheckPointFailed(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: atomic.Int64{},
		totalNum:  atomic.Int64{},
		cfg: &config.FaasConfig{
			HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
			LocalAuth:   &localauth.AuthConfig{},
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					WorkerImageTag:     "yuanrong:test",
					RuntimeImageTag:    "yuanrong:test",
					WorkerInitImageTag: "yuanrong:test",
				},
				MultiSnapshotConfig: config.MultiSnapshotConfig{
					Enable: true,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize:  123123,
						ApmJavaAgentSize: 123123,
						RunTimeSize:      123123,
						ImageOS:          "aaa",
					},
				},
				CheckpointPodConfig: config.CheckpointPodConfig{
					MaxConcurrency: 1,
				},
			},
		},
	}
	convey.Convey("Test_manager_recordExecuteCheckPointFailed", t, func() {
		convey.Convey("Execute Checkpoint Failed add failed Num", func() {
			err := errors.New("underlying error")
			mgr.recordExecuteCheckPointFailed(constants.CheckpointModeUpdate, "funcSvcID", "traceID", err)
		})
	})
}

func Test_manager_recordHandleCheckpointResultFailed(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: atomic.Int64{},
		totalNum:  atomic.Int64{},
		cfg: &config.FaasConfig{
			HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
			LocalAuth:   &localauth.AuthConfig{},
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					WorkerImageTag:     "yuanrong:test",
					RuntimeImageTag:    "yuanrong:test",
					WorkerInitImageTag: "yuanrong:test",
				},
				MultiSnapshotConfig: config.MultiSnapshotConfig{
					Enable: true,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize:  123123,
						ApmJavaAgentSize: 123123,
						RunTimeSize:      123123,
						ImageOS:          "aaa",
					},
				},
				CheckpointPodConfig: config.CheckpointPodConfig{
					MaxConcurrency: 1,
				},
			},
		},
	}

	convey.Convey("Test_manager_recordHandleCheckpointResultFailed", t, func() {
		convey.Convey("Execute Checkpoint Failed add failed Num", func() {
			err := errors.New("underlying error")
			mgr.recordHandleCheckpointResultFailed(constants.CheckpointModeUpdate, "funcSvcID", "traceID", err)
		})
	})
}

func Test_manager_printSummary(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: atomic.Int64{},
		totalNum:  atomic.Int64{},
		summary:   sync.Map{},
		cfg: &config.FaasConfig{
			HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
			LocalAuth:   &localauth.AuthConfig{},
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					WorkerImageTag:     "yuanrong:test",
					RuntimeImageTag:    "yuanrong:test",
					WorkerInitImageTag: "yuanrong:test",
				},
			},
		},
	}
	mgr.summary.Store("test1", succeedStatus)
	mgr.summary.Store("test2", failedStatus)
	convey.Convey("Test_manager_printSummary", t, func() {
		convey.Convey("printSummary", func() {
			mgr.printSummary()
		})
	})
}

func Test_manager_InitCheckpointPodConfig(t *testing.T) {
	newCfg := &config.FaasConfig{
		PoolMgrConfig: config.PoolMgrConfig{
			CheckpointPodConfig: config.CheckpointPodConfig{},
		},
	}
	convey.Convey("Test_manager_initCheckpointPodConfig", t, func() {
		convey.Convey("initCheckpointPodConfig", func() {
			initCheckpointPodConfig(newCfg)
		})
	})
}

func Test_manager_UpdateCheckpointPodConfig(t *testing.T) {
	newCfg := &config.FaasConfig{
		HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true},
		LocalAuth:   &localauth.AuthConfig{},
		PoolMgrConfig: config.PoolMgrConfig{
			CheckpointPodConfig: config.CheckpointPodConfig{
				WaitReadyTimeout:     10,
				FailureRetryInterval: 30,
				FailureRetryTimes:    30,
				MaxConcurrency:       2,
			},
		},
	}
	convey.Convey("Test_manager_UpdateCheckpointPodConfig", t, func() {
		convey.Convey("UpdateCheckpointPodConfig", func() {
			mgr.UpdateCheckpointPodConfig(newCfg)
		})
	})
}

func Test_manager_getSummaryContByValue(t *testing.T) {
	type fields struct {
		workerClient       *fhttp.Client
		VpcHandler         *vpcmanager.VpcHandler
		cfg                *config.FaasConfig
		wg                 *sync.WaitGroup
		finishNum          atomic.Int64
		successNum         atomic.Int64
		failedNum          atomic.Int64
		totalNum           atomic.Int64
		summary            sync.Map
		ConcurrencyLimiter ConcurrencyLimiter
		needCheckPointMap  map[string]*types.SnapshotDeploymentSpec
	}
	type args struct {
		targetValue string
	}
	var a args
	var f fields
	f.summary.Store("keys", "value")
	a.targetValue = "key"
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{"case1", f, a, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &manager{
				workerClient:       tt.fields.workerClient,
				VpcHandler:         tt.fields.VpcHandler,
				cfg:                tt.fields.cfg,
				wg:                 tt.fields.wg,
				finishNum:          tt.fields.finishNum,
				successNum:         tt.fields.successNum,
				failedNum:          tt.fields.failedNum,
				totalNum:           tt.fields.totalNum,
				summary:            tt.fields.summary,
				ConcurrencyLimiter: tt.fields.ConcurrencyLimiter,
				needCheckPointMap:  tt.fields.needCheckPointMap,
			}
			if got := m.getSummaryContByValue(tt.args.targetValue); got != tt.want {
				t.Errorf("getSummaryContByValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_contains(t *testing.T) {
	type args struct {
		arr    []string
		target string
	}
	var a args
	a.target = "target"
	a.arr = []string{"target"}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := contains(tt.args.arr, tt.args.target); got != tt.want {
				t.Errorf("contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_manager_getSnapshotListFromEtcd(t *testing.T) {
	convey.Convey("Test_manager_getSnapshotListFromEtcd", t, func() {
		convey.Convey("failed to get meta ETCD client", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						snapshotKey := "/snapshot/business/yrk/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@test-java-snapshot-restore-coldstart-001-hello/version/latest/id/000"
						snapshotValue := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip\",\"bucketUrl\":\"\",\"sha256\":\"testhash\",\"size\":12312}"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[snapshotKey] = snapshotValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := mgr.getSnapshotListFromEtcd()
			convey.ShouldNotBeNil(err)
		})

		convey.Convey("failed to get response", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return nil, errors.New("fail")
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := mgr.getSnapshotListFromEtcd()
			convey.ShouldNotBeNil(err)
		})
	})
}

func Test_splitByColon(t *testing.T) {
	type args struct {
		content string
	}
	var a args
	a.content = ""
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitByColon(tt.args.content); got != tt.want {
				t.Errorf("splitByColon() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkAndAppendSnapshotVersions(t *testing.T) {
	convey.Convey("DoAction case:  action.Type == cache.ActionStatefulInstanceRePull", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(checkAndUpdateVersionLength, func(_ *types.SnapshotDeploymentSpec) {}),
		})
		patches.ResetAll()
		var s []types.SnapshotVersion
		s = append(s, types.SnapshotVersion{})
		sd := &types.SnapshotDeploymentSpec{SnapshotVersions: s}
		version := types.SnapshotVersion{ErrCode: 1, ErrMessage: "err"}
		checkAndAppendSnapshotVersions(sd, version)

	})
}
func Test_ProcessMultiSnapshotEnableUpdate(t *testing.T) {
	m := &manager{cfg: &config.FaasConfig{PoolMgrConfig: config.PoolMgrConfig{MultiSnapshotConfig: config.MultiSnapshotConfig{Enable: true}}}}
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc((*manager).updateSnapshotVersion, func(_ *manager) {}),
		gomonkey.ApplyFunc(utils.GetMultiSnapshotConfig, func() config.MultiSnapshotConfig {
			return config.MultiSnapshotConfig{}
		})})
	defer patches.ResetAll()
	m.ProcessMultiSnapshotEnableUpdate()
}

func Test_getMonitorInterval(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want int
	}{
		{
			name: "test use default",
			env:  "",
			want: defaultCheckpointAlarmInterval,
		},
		{
			name: "test use env",
			env:  "1",
			want: 1,
		},
		{
			name: "test use env",
			env:  "0",
			want: defaultCheckpointAlarmInterval,
		},
	}
	for _, tt := range tests {
		os.Setenv(checkpointAlarmIntervalEnvKey, tt.env)
		t.Run(tt.name, func(t *testing.T) {
			if got := getMonitorInterval(); got != tt.want {
				t.Errorf("getMonitorInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}
