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
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

type fakeLogger struct {
	DebugMsg string
	InfoMsg  string
	WarnMsg  string
	ErrorMsg string
	FatalMsg string
}

func (f *fakeLogger) With(fields ...zapcore.Field) log.FormatLogger {
	return nil
}

func (f *fakeLogger) Infof(format string, paras ...interface{}) {
	f.InfoMsg += fmt.Sprintf(format, paras...) + "\n"
}

func (f *fakeLogger) Errorf(format string, paras ...interface{}) {
	f.ErrorMsg += fmt.Sprintf(format, paras...) + "\n"
}

func (f *fakeLogger) Warnf(format string, paras ...interface{}) {
	f.WarnMsg += fmt.Sprintf(format, paras...) + "\n"
}

func (f *fakeLogger) Debugf(format string, paras ...interface{}) {
	f.DebugMsg += fmt.Sprintf(format, paras...) + "\n"
}

func (f *fakeLogger) Fatalf(format string, paras ...interface{}) {
	f.FatalMsg += fmt.Sprintf(format, paras...) + "\n"
}

func (f *fakeLogger) ForDebugf(format string, paras ...interface{}) {
	f.DebugMsg += fmt.Sprintf(format, paras...) + "\n"
}

func (f *fakeLogger) Info(msg string, fields ...zap.Field) {
	f.InfoMsg += msg + "\n"
}

func (f *fakeLogger) Error(msg string, fields ...zap.Field) {
	f.ErrorMsg += msg + "\n"
}

func (f *fakeLogger) Warn(msg string, fields ...zap.Field) {
	f.WarnMsg += msg + "\n"
}

func (f *fakeLogger) Debug(msg string, fields ...zap.Field) {
	f.DebugMsg += msg + "\n"
}

func (f *fakeLogger) Fatal(msg string, fields ...zap.Field) {
	f.WarnMsg += msg + "\n"
}

func (f *fakeLogger) Sync() {
}

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
	finishNum:         0,
	totalNum:          0,
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
	redisCli:           monitoring.GetRedisClient(),
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
			gomonkey.ApplyFunc((*manager).getCheckPointRedisClientWithRetry, func(_ *manager) {
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		mgr := &manager{
			ConcurrencyLimiter: CreateConcurrencyLimiter(defaultMaxConcurrency),
			redisCli:           monitoring.GetRedisClient()}
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
		convey.Convey("delete by func name is empty", func() {
			event := &etcd3.Event{
				Type:  etcd3.DELETE,
				Key:   key,
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

		convey.Convey("parse cancel event", func() {
			cancelSnapshotDeployment := "{\"appId\":\"\",\"bucketId\":\"bucket-test-log1\",\"objectId\":\"khj.zip\",\"bucketUrl\":\"\",\"sha256\":\"\",\"size\":0,\"snapshotVersions\":[{\"objectId\":\"khj.zip\",\"sha256\":\"\",\"size\":0}],\"event\":\"cancel\",\"taskID\":\"task123\"}"
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(cancelSnapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(snapshotEvent.snapshotResult.Event, convey.ShouldEqual, eventCancel)
			convey.So(snapshotEvent.snapshotResult.TaskID, convey.ShouldEqual, "task123")
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
			convey.So(strings.HasPrefix(snapshotEvent.snapshotResult.ObjectID, "snapshot/"), convey.ShouldBeTrue)
			timePatches.Reset()
			time.Sleep(100 * time.Millisecond)
		})
		convey.Convey("disable snapshot", func() {
			dur := &duration{receiveRequest: time.Now()}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return nil
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{DisableSnapshot: true}, nil
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
			convey.So(strings.Contains(result, "snapshot disabled"), convey.ShouldBeTrue)
		})
		convey.Convey("enable faststart", func() {
			dur := &duration{receiveRequest: time.Now()}
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("funcName", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "funcName"}})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					result = value
					return nil
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{FastStartWhiteList: []string{"id1"}}, nil
				}),
				gomonkey.ApplyFunc(utils.CrName, func(business, tenant, funcName, version string) string {
					return "funcName"
				}),
				gomonkey.ApplyFunc((*manager).generalizePod, func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return &corev1.Pod{}, nil
				}),
				gomonkey.ApplyFunc((*manager).queryCheckpointResult, func(_ *manager, pod *corev1.Pod,
					reqInfo *config.RequestInfo) (*types.SnapshotDeploymentSpec, error) {
					return &types.SnapshotDeploymentSpec{FastStartEnable: true}, nil
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S, func(_ *podfetcher.PodFetcher, podName, traceID string) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{
				snapshotEtcdKey: snapshotEtcdKey{tenantID: "id1"},
			}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			convey.So(strings.Contains(result, "fastStartEnable"), convey.ShouldBeTrue)
			convey.So(strings.Contains(result, "true"), convey.ShouldBeTrue)
		})
		convey.Convey("cancel after start checkpoint", func() {
			dur := &duration{receiveRequest: time.Now()}
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			fetcher := &podfetcher.PodFetcher{}
			fetcher.InitContexts()
			fetcher.CreateNewContext("aaa")
			generalizeTimes := 0

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
					generalizeTimes++
					return &corev1.Pod{}, errors.New("checkpoint canceled")
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
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{
				SnapshotVersions: []types.SnapshotVersion{},
				Event:            eventCancel,
				TaskID:           "taskID",
			}
			updateTaskInfo(snapshotEvent.snapshotResult.TaskID)
			cancelCheckpoint("taskID")
			mgr.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeCreate)
			convey.So(generalizeTimes, convey.ShouldEqual, 1)
			convey.So(strings.Contains(result, "canceled"), convey.ShouldBeTrue)
			convey.So(strings.Contains(result, "done"), convey.ShouldBeTrue)
		})
	})
}

func Test_manager_processMultiVersion(t *testing.T) {
	var (
		patch = gomonkey.NewPatches()
	)

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

		convey.Convey("processMultiVersion successfully with different runtime tag", func() {
			multiSnapshotDeployment := &types.SnapshotDeploymentSpec{
				ObjectID:       "test.zip",
				SnapshotSha256: "etsthash",
				SnapshotSize:   123,
				SnapshotVersions: []types.SnapshotVersion{
					{
						ObjectID:         "test.zip",
						SnapshotImageTag: "runtime-v2",
					},
				},
			}

			mockGetRuntimeManagerImageTag(patch, "runtime-v1")
			value, _ := mgr.processMultiVersion(nil, multiSnapshotDeployment, constants.CheckpointModeUpdate)
			snapshotDeployment := &types.SnapshotDeploymentSpec{}
			json.Unmarshal(value, snapshotDeployment)
			convey.So(snapshotDeployment.SnapshotVersions, convey.ShouldNotBeNil)
			convey.So(len(snapshotDeployment.SnapshotVersions), convey.ShouldEqual, 2)
			convey.So(snapshotDeployment.SnapshotVersions[0].SnapshotImageTag, convey.ShouldEqual, "runtime-v1")
			convey.So(snapshotDeployment.SnapshotVersions[1].SnapshotImageTag, convey.ShouldEqual, "runtime-v2")
		})

		convey.Convey("processMultiVersion successfully with more than 1 snapshot version", func() {
			multiSnapshotDeployment := &types.SnapshotDeploymentSpec{
				ObjectID:       "test.zip",
				SnapshotSha256: "etsthash",
				SnapshotSize:   123,
				SnapshotVersions: []types.SnapshotVersion{
					{
						ObjectID:         "test.zip",
						SnapshotImageTag: "runtime-v2",
					},
					{
						ObjectID:         "test.zip",
						SnapshotImageTag: "runtime-v1",
					},
				},
			}

			mockGetRuntimeManagerImageTag(patch, "runtime-v3")
			value, _ := mgr.processMultiVersion(nil, multiSnapshotDeployment, constants.CheckpointModeUpdate)
			snapshotDeployment := &types.SnapshotDeploymentSpec{}
			json.Unmarshal(value, snapshotDeployment)
			convey.So(snapshotDeployment.SnapshotVersions, convey.ShouldNotBeNil)
			convey.So(len(snapshotDeployment.NeedDeleteObjectIDs), convey.ShouldEqual, 1)
			convey.So(len(snapshotDeployment.SnapshotVersions), convey.ShouldEqual, 2)
			convey.So(snapshotDeployment.SnapshotVersions[0].SnapshotImageTag, convey.ShouldEqual, "runtime-v3")
			convey.So(snapshotDeployment.SnapshotVersions[1].SnapshotImageTag, convey.ShouldEqual, "runtime-v2")
		})

		convey.Reset(func() {
			patch.Reset()
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
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
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*manager).updateSnapshotListFromEtcd, func(_ *manager) error {
					return errors.New("error")
				}),
				gomonkey.ApplyFunc((*manager).updateSnapshotListFromCAEEtcd, func(_ *manager) error {
					return errors.New("error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyPrivateMethod(reflect.TypeOf(&mgr), "generalizePod", func(m *manager,
					requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, nil
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyPrivateMethod(reflect.TypeOf(&mgr), "generalizePod", func(m *manager,
					requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, nil
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
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
		finishNum:          0,
		totalNum:           0,
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
			atomic.AddInt64(&mgr.totalNum, 1)
			atomic.AddInt64(&mgr.finishNum, 1)
			mgr.logProgress()
			atomic.AddInt64(&mgr.totalNum, -1)
			atomic.AddInt64(&mgr.finishNum, -1)
		})
		convey.Convey("Update snapshot but cost too much time, exit", func() {
			duration := time.Duration(50 * 60 * 1000000000)
			patch := gomonkey.ApplyFunc(time.Since, func(t time.Time) time.Duration {
				return duration
			})
			defer patch.Reset()
			mgr.wg.Add(1)
			atomic.AddInt64(&mgr.totalNum, 10)
			mgr.logProgress()
			atomic.AddInt64(&mgr.totalNum, -10)
			convey.ShouldEqual(logProgressLast, float64(10*5)/float64(mgr.ConcurrencyLimiter.limit))
		})
		convey.Convey("Task num is 1, update snapshot but cost too much time exit", func() {
			duration := time.Duration(20 * 60 * 1000000000)
			patch := gomonkey.ApplyFunc(time.Since, func(t time.Time) time.Duration {
				return duration
			})
			defer patch.Reset()
			mgr.wg.Add(1)
			atomic.AddInt64(&mgr.totalNum, 1)
			mgr.logProgress()
			convey.ShouldEqual(logProgressLast, 10)
			atomic.AddInt64(&mgr.totalNum, -1)
		})
	})
}

func Test_manager_addFailNumHandleCheckpointFailed(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: 0,
		totalNum:  0,
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
			atomic.AddInt64(&mgr.failedNum, -1)
			mgr.addFailNumHandleCheckpointFailed(constants.CheckpointModeUpdate, false)
			convey.ShouldEqual(mgr.failedNum, 0)
		})
	})
}

func Test_manager_addFailedrecordExecuteCheckPointFailed(t *testing.T) {
	var mgr = manager{
		wg:        &sync.WaitGroup{},
		start:     time.Now(),
		finishNum: 0,
		totalNum:  0,
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
		finishNum: 0,
		totalNum:  0,
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
		finishNum: 0,
		totalNum:  0,
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
		finishNum: 0,
		totalNum:  0,
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
		finishNum          int64
		successNum         int64
		failedNum          int64
		totalNum           int64
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
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
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
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

func Test_monitorCheckpointPods(t *testing.T) {
	tickerCh := make(chan time.Time)
	alarmed := int64(0)
	cleared := int64(0)
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			return &time.Ticker{C: tickerCh}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(alarm.GetClient()), "ReportAlarm", func(_ *alarm.Client, _ alarm.Type, _ string) {
			atomic.AddInt64(&alarmed, 1)
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(alarm.GetClient()), "ClearAlarm", func(_ *alarm.Client, _ alarm.Type) {
			atomic.AddInt64(&cleared, 1)
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	stopCh := make(chan struct{})

	m := &manager{}

	for i := 0; i < defaultAlarmThresholdNumber+1; i++ {
		m.summary.Store(strconv.Itoa(i), failedStatus)
	}

	exit := make(chan struct{})
	go func() {
		m.monitorCheckpointPods(stopCh)
		close(exit)
	}()

	tickerCh <- time.Time{}
	tickerCh <- time.Time{}

	stopCh <- struct{}{}

	assert.Equal(t, int64(1), atomic.LoadInt64(&alarmed))
	assert.Equal(t, int64(1), atomic.LoadInt64(&cleared))
	<-exit
}

func Test_needUploadSnapshotToDS(t *testing.T) {
	// case1 snapshotEvent := &SnapshotEvent{}
	snapshotEvent := &SnapshotEvent{}
	result := needUploadSnapshotToDS(snapshotEvent)
	assert.Equal(t, false, result)

	// case2 snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{}}
	snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{}}
	result = needUploadSnapshotToDS(snapshotEvent)
	assert.Equal(t, false, result)

	// case3 snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{SnapshotVersions: []types.SnapshotVersion{}}}
	snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{SnapshotVersions: []types.SnapshotVersion{}}}
	result = needUploadSnapshotToDS(snapshotEvent)
	assert.Equal(t, false, result)

	// case4 snapshotEvent = &&SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{SnapshotVersions: []types.SnapshotVersion{{SnapshotSize: 0, SnapshotSha256: ""}}}}
	snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{SnapshotVersions: []types.SnapshotVersion{{SnapshotSize: 0, SnapshotSha256: ""}}}}
	result = needUploadSnapshotToDS(snapshotEvent)
	assert.Equal(t, false, result)

	// case5 config.GetConfig get error
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					PoolMgrConfig: config.PoolMgrConfig{
						PoolContainerTags: config.PoolContainerImageTags{
							WorkerImageTag:     "aaa",
							RuntimeImageTag:    "bbb",
							WorkerInitImageTag: "ccc",
						},
					},
				}, errors.New("error")
			}),
		gomonkey.ApplyFunc(os.Getenv,
			func(key string) string {
				return string("")
			}),
	}

	defer func() {
		for _, patch := range patches {
			patch.Reset()
		}
	}()
	snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{SnapshotVersions: []types.SnapshotVersion{{SnapshotSize: 111, SnapshotSha256: ""}}}}
	result = needUploadSnapshotToDS(snapshotEvent)
	assert.Equal(t, false, result)

	// case6 config.GetConfig get no error
	patches = []*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					PoolMgrConfig: config.PoolMgrConfig{
						PoolContainerTags: config.PoolContainerImageTags{
							WorkerImageTag:     "aaa",
							RuntimeImageTag:    "bbb",
							WorkerInitImageTag: "ccc",
						},
						CheckpointPodConfig: config.CheckpointPodConfig{SnapshotStorageType: "data-system"},
					},
				}, nil
			}),
		gomonkey.ApplyFunc(os.Getenv,
			func(key string) string {
				return string("cluster001")
			}),
	}

	defer func() {
		for _, patch := range patches {
			patch.Reset()
		}
	}()
	snapshotEvent = &SnapshotEvent{snapshotResult: &types.SnapshotDeploymentSpec{
		SnapshotVersions:   []types.SnapshotVersion{{SnapshotSize: 111, SnapshotSha256: ""}},
		UploadedDataSystem: map[string]bool{"cluster002": true}},
		snapshotEtcdKey: snapshotEtcdKey{clusterID: "cluster002"}}
	result = needUploadSnapshotToDS(snapshotEvent)
	assert.Equal(t, true, result)
}

func Test_needCheckpointEvent(t *testing.T) {
	snapshotDeployment := `{"bucketId": "ak"}`
	key := "/snapshot/business/yrk/cluster/cluster001/tenant/{tenantID}/function/{functionName}/version/{version}/id/{snapshotID}"
	snapshotDeploy := &types.SnapshotDeploymentSpec{}
	value, err := json.Marshal(snapshotDeploy)
	if err != nil {
		return
	}
	mgr := manager{
		cfg: &config.FaasConfig{
			PoolMgrConfig: config.PoolMgrConfig{
				PoolContainerTags: config.PoolContainerImageTags{
					RuntimeImageTag: "runtime:test",
				},
			},
		},
	}
	convey.Convey("Test parseCheckpointEvent", t, func() {
		convey.Convey("put empty value", func() {
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(""),
			}
			patch := gomonkey.ApplyFunc(os.Getenv,
				func(key string) string {
					return string(value)
				})
			defer patch.Reset()
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(mgr.needCheckpointEvent(snapshotEvent), convey.ShouldBeFalse)
		})
		convey.Convey("finishedCheckpointEvent return false", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return string("cluster001")
					}),
				gomonkey.ApplyFunc(finishedCheckpointEvent, func(snapshotEvent *SnapshotEvent) bool {
					return true
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()

			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(snapshotDeployment),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.Meta)
			convey.So(mgr.needCheckpointEvent(snapshotEvent), convey.ShouldBeFalse)
		})
		convey.Convey("upgrade event return true", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.GetEnvClusterID, func() string {
					return "cluster001"
				}),
				gomonkey.ApplyFunc(finishedCheckpointEvent, func(snapshotEvent *SnapshotEvent) bool {
					return true
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()

			upgradeEvent := `{"bucketId": "ak", "event": "upgrade"}`
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(upgradeEvent),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.CAEMeta)
			convey.So(mgr.needCheckpointEvent(snapshotEvent), convey.ShouldBeTrue)
		})
		convey.Convey("upgrade event return true and getNeedRemoveObjectIDs", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.GetEnvClusterID, func() string {
					return "cluster001"
				}),
				gomonkey.ApplyFunc(finishedCheckpointEvent, func(snapshotEvent *SnapshotEvent) bool {
					return true
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()

			upgradeEvent := `{"bucketId": "ak", "event": "upgrade"}`
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(upgradeEvent),
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.CAEMeta)
			snapshotEvent.snapshotResult.SnapshotVersions = []types.SnapshotVersion{
				{
					ObjectID:       "newObjectID",
					SnapshotSha256: "newSnapshotSha256",
					SnapshotSize:   30,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize: 30,
						JavaRuntimeHash: "newJavaRuntimeHash",
					},
					SnapshotImageTag: "new",
				},
				{
					ObjectID:       "oldObjectID2",
					SnapshotSha256: "oldSnapshotSha256",
					SnapshotSize:   20,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize: 20,
						JavaRuntimeHash: "newJavaRuntimeHash",
					},
					SnapshotImageTag: "new",
				},
				{
					ObjectID:       "oldObjectID1",
					SnapshotSha256: "oldSnapshotSha256",
					SnapshotSize:   10,
					SnapshotCheckInfo: types.SnapshotCheckInfo{
						JavaRuntimeSize: 10,
						JavaRuntimeHash: "oldJavaRuntimeHash",
					},
					SnapshotImageTag: "old",
				},
			}
			needCheckpoint := mgr.needCheckpointEvent(snapshotEvent)
			convey.So(needCheckpoint, convey.ShouldBeTrue)
			convey.So(len(snapshotEvent.snapshotResult.NeedDeleteObjectIDs), convey.ShouldBeGreaterThan, 1)
		})
	})
}

func Test_needCheckpointWhenUpgrade(t *testing.T) {
	key := "/snapshot/business/yrk/cluster/cluster001/tenant/{tenantID}/function/{functionName}/version/{version}/id/{snapshotID}"
	defaultInputSpec := &types.SnapshotDeploymentSpec{
		SnapshotCheckInfo: &types.SnapshotCheckInfo{
			JavaRuntimeSize: 20,
			JavaRuntimeHash: "newJavaRuntimeHash",
		},
		ObjectID:       "newObjectID",
		SnapshotSha256: "newSnapshotSha256",
		SnapshotSize:   20,
		SnapshotVersions: []types.SnapshotVersion{
			{
				ObjectID:       "newObjectID",
				SnapshotSha256: "newSnapshotSha256",
				SnapshotSize:   20,
				SnapshotCheckInfo: types.SnapshotCheckInfo{
					JavaRuntimeSize: 20,
					JavaRuntimeHash: "newJavaRuntimeHash",
				},
				SnapshotImageTag: "new",
			},
			{
				ObjectID:       "oldObjectID",
				SnapshotSha256: "oldSnapshotSha256",
				SnapshotSize:   10,
				SnapshotCheckInfo: types.SnapshotCheckInfo{
					JavaRuntimeSize: 10,
					JavaRuntimeHash: "oldJavaRuntimeHash",
				},
				SnapshotImageTag: "old",
			},
		},
		Event:  "upgrade",
		TaskID: "taskID-test",
	}
	tests := []struct {
		name       string
		mgr        manager
		inputSpec  *types.SnapshotDeploymentSpec
		expectSpec types.SnapshotDeploymentSpec
	}{
		{
			name: "exist old + new, upgrade in new version",
			mgr: manager{
				cfg: &config.FaasConfig{
					PoolMgrConfig: config.PoolMgrConfig{
						PoolContainerTags: config.PoolContainerImageTags{
							RuntimeImageTag: "runtime:new",
						},
					},
				},
			},
			inputSpec: defaultInputSpec,
			expectSpec: types.SnapshotDeploymentSpec{
				SnapshotCheckInfo: &types.SnapshotCheckInfo{
					JavaRuntimeSize: 20,
					JavaRuntimeHash: "newJavaRuntimeHash",
				},
				ObjectID:       "newObjectID",
				SnapshotSha256: "newSnapshotSha256",
				SnapshotSize:   20,
				SnapshotVersions: []types.SnapshotVersion{
					{
						ObjectID:       "newObjectID",
						SnapshotSha256: "newSnapshotSha256",
						SnapshotSize:   20,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize: 20,
							JavaRuntimeHash: "newJavaRuntimeHash",
						},
						SnapshotImageTag: "new",
					},
					{
						ObjectID:       "oldObjectID",
						SnapshotSha256: "oldSnapshotSha256",
						SnapshotSize:   10,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize: 10,
							JavaRuntimeHash: "oldJavaRuntimeHash",
						},
						SnapshotImageTag: "old",
					},
				},
				Event:  "done",
				TaskID: "taskID-test",
			},
		},
		{
			name: "exist old + new, upgrade in old version",
			mgr: manager{
				cfg: &config.FaasConfig{
					PoolMgrConfig: config.PoolMgrConfig{
						PoolContainerTags: config.PoolContainerImageTags{
							RuntimeImageTag: "runtime:old",
						},
					},
				},
			},
			inputSpec: defaultInputSpec,
			expectSpec: types.SnapshotDeploymentSpec{
				SnapshotCheckInfo: &types.SnapshotCheckInfo{
					JavaRuntimeSize: 10,
					JavaRuntimeHash: "oldJavaRuntimeHash",
				},
				ObjectID:       "oldObjectID",
				SnapshotSha256: "oldSnapshotSha256",
				SnapshotSize:   10,
				SnapshotVersions: []types.SnapshotVersion{
					{
						ObjectID:       "oldObjectID",
						SnapshotSha256: "oldSnapshotSha256",
						SnapshotSize:   10,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize: 10,
							JavaRuntimeHash: "oldJavaRuntimeHash",
						},
						SnapshotImageTag: "old",
					},
					{
						ObjectID:       "newObjectID",
						SnapshotSha256: "newSnapshotSha256",
						SnapshotSize:   20,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize: 20,
							JavaRuntimeHash: "newJavaRuntimeHash",
						},
						SnapshotImageTag: "new",
					},
				},
				Event:  "done",
				TaskID: "taskID-test",
			},
		},
		{
			name: "exist old + new, upgrade in newer version, but java runtime stay the same",
			mgr: manager{
				cfg: &config.FaasConfig{
					PoolMgrConfig: config.PoolMgrConfig{
						PoolContainerTags: config.PoolContainerImageTags{
							RuntimeImageTag: "runtime:newer",
						},
						MultiSnapshotConfig: config.MultiSnapshotConfig{
							SnapshotCheckInfo: types.SnapshotCheckInfo{
								JavaRuntimeSize: 20,
								JavaRuntimeHash: "newJavaRuntimeHash",
							},
						},
					},
				},
			},
			inputSpec: defaultInputSpec,
			expectSpec: types.SnapshotDeploymentSpec{
				SnapshotCheckInfo: &types.SnapshotCheckInfo{
					JavaRuntimeSize: 20,
					JavaRuntimeHash: "newJavaRuntimeHash",
				},
				ObjectID:       "newObjectID",
				SnapshotSha256: "newSnapshotSha256",
				SnapshotSize:   20,
				SnapshotVersions: []types.SnapshotVersion{
					{
						ObjectID:       "newObjectID",
						SnapshotSha256: "newSnapshotSha256",
						SnapshotSize:   20,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize: 20,
							JavaRuntimeHash: "newJavaRuntimeHash",
						},
						SnapshotImageTag: "newer",
					},
					{
						ObjectID:       "oldObjectID",
						SnapshotSha256: "oldSnapshotSha256",
						SnapshotSize:   10,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize: 10,
							JavaRuntimeHash: "oldJavaRuntimeHash",
						},
						SnapshotImageTag: "old",
					},
				},
				Event:  "done",
				TaskID: "taskID-test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualSpec := types.SnapshotDeploymentSpec{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.GetEnvClusterID, func() string {
					return "cluster001"
				}),
				gomonkey.ApplyFunc((*manager).submitResult, func(_ *manager, resultKey, traceID string, result []byte, etcdType etcd3.EtcdType) error {
					err := json.Unmarshal(result, &actualSpec)
					if err != nil {
						t.Fail()
					}
					return nil
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			snapshotDeployByte, err := json.Marshal(tt.inputSpec)
			if err != nil {
				t.Fail()
			}
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: snapshotDeployByte,
			}
			snapshotEvent := parseCheckpointEvent(event, etcd3.CAEMeta)
			assert.Equal(t, false, tt.mgr.needCheckpointEvent(snapshotEvent))
			assert.Equal(t, tt.expectSpec.ObjectID, actualSpec.ObjectID)
			assert.Equal(t, tt.expectSpec.SnapshotSha256, actualSpec.SnapshotSha256)
			assert.Equal(t, tt.expectSpec.SnapshotSize, actualSpec.SnapshotSize)
			assert.Equal(t, tt.expectSpec.SnapshotCheckInfo, actualSpec.SnapshotCheckInfo)
			assert.Equal(t, tt.expectSpec.Event, actualSpec.Event)
			assert.Equal(t, tt.expectSpec.TaskID, actualSpec.TaskID)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[0].ObjectID, actualSpec.SnapshotVersions[0].ObjectID)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[0].SnapshotSha256, actualSpec.SnapshotVersions[0].SnapshotSha256)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[0].SnapshotSize, actualSpec.SnapshotVersions[0].SnapshotSize)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[0].SnapshotCheckInfo, actualSpec.SnapshotVersions[0].SnapshotCheckInfo)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[0].SnapshotImageTag, actualSpec.SnapshotVersions[0].SnapshotImageTag)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[1].ObjectID, actualSpec.SnapshotVersions[1].ObjectID)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[1].SnapshotSha256, actualSpec.SnapshotVersions[1].SnapshotSha256)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[1].SnapshotSize, actualSpec.SnapshotVersions[1].SnapshotSize)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[1].SnapshotCheckInfo, actualSpec.SnapshotVersions[1].SnapshotCheckInfo)
			assert.Equal(t, tt.expectSpec.SnapshotVersions[1].SnapshotImageTag, actualSpec.SnapshotVersions[1].SnapshotImageTag)
		})
	}
}

func Test_uploadSnapShotToDS(t *testing.T) {
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
	convey.Convey("Test_uploadSnapShotToDS", t, func() {
		convey.Convey("failed to get function meta", func() {
			dur := &duration{receiveRequest: time.Now()}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*manager).getFunctionMetaWithRetry, func(_ *manager, traceID string, funcSvcID string) *faasfunction.FunctionService {
					result = "failed to get function meta"
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.uploadSnapShotToDS(snapshotEvent, dur)
			convey.So(strings.Contains(result, "failed to get function meta"), convey.ShouldBeTrue)
		})
		convey.Convey("failed to generalize pod", func() {
			dur := &duration{receiveRequest: time.Now()}
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*manager).getFunctionMetaWithRetry, func(_ *manager, traceID string, funcSvcID string) *faasfunction.FunctionService {
					return &faasfunction.FunctionService{}
				}),
				gomonkey.ApplyFunc((*manager).generalizePodWithRetry, func(_ *manager, checkpointEvent *SnapshotEvent, dur *duration, traceID string,
					funcSvcID string, functionService *faasfunction.FunctionService) (*corev1.Pod, *config.RequestInfo, bool) {
					result = "failed to generalize pod"
					return nil, nil, true
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
			mgr.uploadSnapShotToDS(snapshotEvent, dur)
			convey.So(strings.Contains(result, "failed to generalize pod"), convey.ShouldBeTrue)
		})
		convey.Convey("failed to send upload ds request to worker", func() {
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			fetcher := &podfetcher.PodFetcher{}
			fetcher.InitContexts()
			dur := &duration{receiveRequest: time.Now()}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*manager).getFunctionMetaWithRetry, func(_ *manager, traceID string, funcSvcID string) *faasfunction.FunctionService {
					return &faasfunction.FunctionService{}
				}),
				gomonkey.ApplyFunc((*manager).generalizePodWithRetry, func(_ *manager, checkpointEvent *SnapshotEvent, dur *duration, traceID string,
					funcSvcID string, functionService *faasfunction.FunctionService) (*corev1.Pod, *config.RequestInfo, bool) {
					return &corev1.Pod{}, &config.RequestInfo{}, false
				}),
				gomonkey.ApplyFunc((*manager).sendSnapshotRequestToWorker, func(_ *manager, pod *corev1.Pod, reqInfo *config.RequestInfo, language string) error {
					result = "failed to send upload ds request to worker"
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(utils.CrName,
					func(business, tenant, funcName, version string) string {
						return "aaa"
					}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S, func(_ *podfetcher.PodFetcher, podName, traceID string) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.snapshotResult = &types.SnapshotDeploymentSpec{}
			mgr.uploadSnapShotToDS(snapshotEvent, dur)
			convey.So(strings.Contains(result, "failed to send upload ds request to worker"), convey.ShouldBeTrue)
		})
		convey.Convey("succeed to send upload ds request to worker", func() {
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("aaa", &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "aaa"}})
			fetcher := &podfetcher.PodFetcher{}
			fetcher.InitContexts()
			fetcher.CreateNewContext("aaa")
			dur := &duration{receiveRequest: time.Now()}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*manager).getFunctionMetaWithRetry, func(_ *manager, traceID string, funcSvcID string) *faasfunction.FunctionService {
					return &faasfunction.FunctionService{}
				}),
				gomonkey.ApplyFunc((*manager).generalizePodWithRetry, func(_ *manager, checkpointEvent *SnapshotEvent, dur *duration, traceID string,
					funcSvcID string, functionService *faasfunction.FunctionService) (*corev1.Pod, *config.RequestInfo, bool) {
					return &corev1.Pod{}, &config.RequestInfo{}, false
				}),
				gomonkey.ApplyFunc((*manager).sendSnapshotRequestToWorker, func(_ *manager, pod *corev1.Pod, reqInfo *config.RequestInfo, language string) error {
					result = "succeed to send upload ds request to worker"
					return nil
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S, func(_ *podfetcher.PodFetcher, podName, traceID string) error {
					return nil
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
			mgr.uploadSnapShotToDS(snapshotEvent, dur)
			convey.So(strings.Contains(result, "succeed to send upload ds request to worker"), convey.ShouldBeTrue)
		})
	})
}

func Test_generalizePodWithRetry(t *testing.T) {
	convey.Convey("TestGeneralizePodWithRetry with error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, snerror.New(1, "vpc")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		checkpointEvent := &SnapshotEvent{}
		dur := &duration{}
		traceID := "test"
		funcSvcID := "test"
		functionService := &faasfunction.FunctionService{}
		mgr := &manager{}
		_, _, result := mgr.generalizePodWithRetry(checkpointEvent, dur, traceID, funcSvcID, functionService)
		convey.So(result, convey.ShouldBeTrue)
	})

	convey.Convey("TestGeneralizePodWithRetry without error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		checkpointEvent := &SnapshotEvent{}
		dur := &duration{}
		traceID := "test"
		funcSvcID := "test"
		functionService := &faasfunction.FunctionService{}
		mgr := &manager{}
		_, _, result := mgr.generalizePodWithRetry(checkpointEvent, dur, traceID, funcSvcID, functionService)
		convey.So(result, convey.ShouldBeFalse)
	})
}

func Test_sendUploadDSRequestToWorker(t *testing.T) {
	convey.Convey("TestGeneralizePodWithRetry with error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).fillInRequest,
				func(_ *manager, req *fhttp.Request) {

				}),
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, snerror.New(1, "vpc")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					resp.SetStatusCode(fhttp.StatusNotFound)
					return nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		mgr := &manager{cfg: &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false},
			PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1, FailureRetryInterval: 1}}}, workerClient: &fhttp.Client{}}
		pod := &corev1.Pod{}
		reqInfo := &config.RequestInfo{SnapshotDeployment: &types.SnapshotDeploymentSpec{}}
		language := "java8"
		err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, constants.UploadSnapshot)
		assert.NotNil(t, err)
	})

	convey.Convey("TestGeneralizePodWithRetry without error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).fillInRequest,
				func(_ *manager, req *fhttp.Request) {

				}),
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, snerror.New(1, "vpc")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					resp.SetStatusCode(fhttp.StatusOK)
					return nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		mgr := &manager{cfg: &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false},
			PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1, FailureRetryInterval: 1}}}, workerClient: &fhttp.Client{}}
		pod := &corev1.Pod{}
		reqInfo := &config.RequestInfo{SnapshotDeployment: &types.SnapshotDeploymentSpec{}}
		language := "java8"
		err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, constants.UploadSnapshot)
		assert.Nil(t, err)
	})

	convey.Convey("TestGeneralizePodWithRetry json marshal error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).fillInRequest,
				func(_ *manager, req *fhttp.Request) {

				}),
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, snerror.New(1, "vpc")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					resp.SetStatusCode(fhttp.StatusNotFound)
					return nil
				}),
			gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("s") }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		mgr := &manager{cfg: &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false},
			PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1, FailureRetryInterval: 1}}}, workerClient: &fhttp.Client{}}
		pod := &corev1.Pod{}
		reqInfo := &config.RequestInfo{SnapshotDeployment: &types.SnapshotDeploymentSpec{}}
		language := "java8"
		err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, constants.UploadSnapshot)
		assert.NotNil(t, err)
	})

	convey.Convey("TestGeneralizePodWithRetry DoTimeout error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).fillInRequest,
				func(_ *manager, req *fhttp.Request) {

				}),
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, snerror.New(1, "vpc")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					resp.SetStatusCode(fhttp.StatusNotFound)
					return nil
				}),
			gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, nil }),
			gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					return errors.New("test")
				}),
			gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		mgr := &manager{cfg: &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false},
			PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1, FailureRetryInterval: 1}}}, workerClient: &fhttp.Client{}}
		pod := &corev1.Pod{}
		reqInfo := &config.RequestInfo{SnapshotDeployment: &types.SnapshotDeploymentSpec{}}
		language := "java8"
		err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, constants.UploadSnapshot)
		assert.NotNil(t, err)
	})

	convey.Convey("TestGeneralizePodWithRetry response error", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).fillInRequest,
				func(_ *manager, req *fhttp.Request) {

				}),
			gomonkey.ApplyFunc((*manager).generalizePod,
				func(_ *manager, requestInfo *config.RequestInfo,
					functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
					return nil, snerror.New(1, "vpc")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					resp.SetStatusCode(fhttp.StatusNotFound)
					return nil
				}),
			gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, nil }),
			gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
				func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
					resp.SetStatusCode(fhttp.StatusInternalServerError)
					resp.SetBody([]byte("test"))
					return nil
				}),
			gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		mgr := &manager{cfg: &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false},
			PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1, FailureRetryInterval: 1}}}, workerClient: &fhttp.Client{}}
		pod := &corev1.Pod{}
		reqInfo := &config.RequestInfo{SnapshotDeployment: &types.SnapshotDeploymentSpec{}}
		language := "java8"
		err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, constants.UploadSnapshot)
		assert.NotNil(t, err)
	})
}

func Test_DeleteSnapShotData(t *testing.T) {
	patch := gomonkey.NewPatches()
	reqInfo := &config.RequestInfo{}
	functionService := &faasfunction.FunctionService{
		ObjectMeta: faasfunction.ObjectMeta{},
		Spec: faasfunction.FunctionServiceSpec{
			SnapshotDeployment: types.SnapshotDeploymentSpec{
				BucketID: "test-bucket1",
				ObjectID: "test-object1",
			},
			Configuration: faasfunction.ConfigurationSpec{
				BusinessType: "CAE",
			},
		},
		Status: faasfunction.FunctionServiceStatus{},
	}
	mgr := manager{
		workerClient:       &fhttp.Client{},
		VpcHandler:         &vpcmanager.VpcHandler{},
		cfg:                &config.FaasConfig{},
		ConcurrencyLimiter: CreateConcurrencyLimiter(defaultMaxConcurrency),
	}
	snapshotMetaData := &types.SnapshotMeta{ClusterID: "cluster1"}

	convey.Convey("Test_DeleteSnapShotData", t, func() {
		convey.Convey("test in different cluster and data storage in obs", func() {
			var result string
			patch.ApplyFunc(utils.GetEnvClusterID, func() string {
				result = "cluster2"
				return result
			})
			mockGeneralizePodWithRetry(patch, nil, reqInfo, true, &result)
			mgr.DeleteSnapShotData(functionService, snapshotMetaData, "")
			convey.So(result, convey.ShouldEqual, "cluster2")
		})

		convey.Convey("set redis key error", func() {
			var flag bool
			mockGetEnvClusterID(patch, "cluster1")
			mgr.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, mgr.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetErr(errors.New("set key failed"))
			mockTimeSleep(patch)
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX", func(_ *redisclient.Client,
				ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
				if boolCmd.Err() != nil {
					flag = true
				}
				return boolCmd
			})

			mgr.DeleteSnapShotData(functionService, snapshotMetaData, "")
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("failed to generalize pod", func() {
			var result string
			mockGetEnvClusterID(patch, "cluster1")
			mgr.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, mgr.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			mockRedisSetNX(patch, boolCmd)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			mockGeneralizePodWithRetry(patch, nil, reqInfo, true, &result)
			mgr.DeleteSnapShotData(functionService, snapshotMetaData, "")
			convey.So(result, convey.ShouldEqual, "failed to generalize pod")
		})
		convey.Convey("failed to send request to worker", func() {
			var result string
			mockGetEnvClusterID(patch, "cluster1")
			mgr.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, mgr.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			mockRedisSetNX(patch, boolCmd)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			mockGeneralizePodWithRetry(patch, &corev1.Pod{}, reqInfo, false, &result)
			mockSendSnapshotRequestToWorker(patch, errors.New("send request to worker error"), &result)
			mockDeletePodByK8S(patch, nil)
			mgr.DeleteSnapShotData(functionService, snapshotMetaData, "")
			convey.So(result, convey.ShouldEqual, "failed to send snapshot request to worker")
		})

		convey.Convey("succeed to send delete ds request to worker", func() {
			var result string
			mockGetEnvClusterID(patch, "cluster1")
			mgr.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, mgr.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			mockRedisSetNX(patch, boolCmd)
			intCmd := &redis.IntCmd{}
			mockRedisDel(patch, intCmd)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			mockGeneralizePodWithRetry(patch, &corev1.Pod{}, reqInfo, false, &result)
			mockSendSnapshotRequestToWorker(patch, nil, &result)
			mockDeletePodByK8S(patch, nil)
			mgr.DeleteSnapShotData(functionService, snapshotMetaData, "")
			convey.So(result, convey.ShouldEqual, "")
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_manager_DeleteSnapShotDataByWatchEtcd(t *testing.T) {
	patch := gomonkey.NewPatches()
	reqInfo := &config.RequestInfo{}
	functionService := &faasfunction.FunctionService{
		ObjectMeta: faasfunction.ObjectMeta{},
		Spec: faasfunction.FunctionServiceSpec{
			SnapshotDeployment: types.SnapshotDeploymentSpec{
				BucketID: "test-bucket1",
				ObjectID: "test-object1",
			},
			Configuration: faasfunction.ConfigurationSpec{
				BusinessType: "CAE",
			},
		},
		Status: faasfunction.FunctionServiceStatus{},
	}
	mgr := manager{
		workerClient:       &fhttp.Client{},
		VpcHandler:         &vpcmanager.VpcHandler{},
		cfg:                &config.FaasConfig{},
		ConcurrencyLimiter: CreateConcurrencyLimiter(defaultMaxConcurrency),
	}
	snapshotEvent := &SnapshotEvent{
		snapshotEtcdKey: snapshotEtcdKey{
			clusterID: "cluster1",
		},
		snapshotResult:  nil,
		resultKey:       "",
		etcdType:        0,
		needDelSnapshot: false,
		action:          "",
	}

	convey.Convey("Test_DeleteSnapShotDataByWatchEtcd", t, func() {
		convey.Convey("test in different cluster and data storage in obs", func() {
			var result string
			patch.ApplyFunc(utils.GetEnvClusterID, func() string {
				result = "cluster2"
				return result
			})
			mockGeneralizePodWithRetry(patch, nil, reqInfo, true, &result)
			mgr.DeleteSnapShotDataByWatchEtcd(snapshotEvent)
			convey.So(result, convey.ShouldEqual, "cluster2")
		})

		convey.Convey("failed to get function meta", func() {
			var result string
			mockGetEnvClusterID(patch, "cluster1")
			patch.ApplyPrivateMethod(reflect.TypeOf(&manager{}), "getFunctionMetaWithRetry", func(_ *manager,
				traceID string, funcSvcID string) *faasfunction.FunctionService {
				result = "function meta is nil"
				return nil
			})

			mgr.DeleteSnapShotDataByWatchEtcd(snapshotEvent)
			convey.So(result, convey.ShouldEqual, "function meta is nil")
		})


		convey.Convey("get func snapshot failed", func() {
			snapshotEvent.needDelSnapshot = false
			mockGetEnvClusterID(patch, "cluster1")
			functionService.Spec.Configuration.BusinessType = "CAE"
			functionService.Spec.SnapshotDeployment = types.SnapshotDeploymentSpec{}
			mockGetFunctionMetaWithRetry(patch, functionService)
			mgr.DeleteSnapShotDataByWatchEtcd(snapshotEvent)
			convey.So(snapshotEvent.needDelSnapshot, convey.ShouldBeFalse)
		})

		convey.Convey("failed to send request to worker", func() {
			var result string
			mockGetEnvClusterID(patch, "cluster1")
			functionService.Spec.SnapshotDeployment = types.SnapshotDeploymentSpec{
				BucketID: "test-bucket1",
				ObjectID: "test-object1",
			}
			mockGetFunctionMetaWithRetry(patch, functionService)
			mockGeneralizePodWithRetry(patch, &corev1.Pod{}, reqInfo, false, &result)
			mgr.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, mgr.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			mockRedisSetNX(patch, boolCmd)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			mockSendSnapshotRequestToWorker(patch, errors.New("send request to worker error"), &result)
			mockDeletePodByK8S(patch, nil)
			mgr.DeleteSnapShotDataByWatchEtcd(snapshotEvent)
			convey.So(result, convey.ShouldEqual, "failed to send snapshot request to worker")
		})

		convey.Convey("succeed to send delete ds request to worker", func() {
			var result string
			mockGetEnvClusterID(patch, "cluster1")
			mockGetFunctionMetaWithRetry(patch, functionService)
			mockGeneralizePodWithRetry(patch, &corev1.Pod{}, reqInfo, false, &result)
			mgr.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, mgr.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			mockRedisSetNX(patch, boolCmd)
			intCmd := &redis.IntCmd{}
			mockRedisDel(patch, intCmd)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			mockSendSnapshotRequestToWorker(patch, nil, &result)
			mockDeletePodByK8S(patch, nil)
			mgr.DeleteSnapShotDataByWatchEtcd(snapshotEvent)
			convey.So(result, convey.ShouldEqual, "")
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_manager_sendDeleteDataRequestToWorker(t *testing.T) {
	patch := gomonkey.NewPatches()
	mgr := &manager{cfg: &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false},
		PoolMgrConfig: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{FailureRetryTimes: 1,
			FailureRetryInterval: 1}}}, workerClient: &fhttp.Client{}}
	pod := &corev1.Pod{}
	reqInfo := &config.RequestInfo{SnapshotDeployment: &types.SnapshotDeploymentSpec{}}
	language := "java8"

	convey.Convey("test sendDeleteDataRequestToWorker", t, func() {
		convey.Convey("test unsupported data type", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, nil, errors.New("json marshal error"))
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "create")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "unsupported snapshot event")
		})

		convey.Convey("test json marshal error", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, nil, errors.New("json marshal error"))
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "upload")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "json marshal error")
		})

		convey.Convey("test request to worker error", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, []byte("test"), nil)
			mockWorkerClientDoTimeout(patch, mgr, 0, nil, errors.New("invalid uri"))
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "delete")

			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "invalid uri")
		})

		convey.Convey("test request uri not found", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, []byte("test"), nil)
			mockWorkerClientDoTimeout(patch, mgr, fhttp.StatusNotFound, nil, nil)
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "delete")

			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "worker unsupported checkpoint request")
		})

		convey.Convey("test response inter error and resp body is nil", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, []byte("test"), nil)
			mockWorkerClientDoTimeout(patch, mgr, fhttp.StatusInternalServerError, nil, nil)
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "delete")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to delete snapshot to ds")
		})

		convey.Convey("test response internal error and resp body is not empty", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, []byte("test"), nil)
			mockWorkerClientDoTimeout(patch, mgr, fhttp.StatusInternalServerError, []byte("test"), nil)
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "delete")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to delete snapshot to ds, response: test")
		})

		convey.Convey("test response status ok", func() {
			mockFillInRequest(patch)
			mockGeneralizePod(patch, pod, nil)
			mockJsonMarshal(patch, []byte("test"), nil)
			mockWorkerClientDoTimeout(patch, mgr, fhttp.StatusOK, []byte("test"), nil)
			err := mgr.sendSnapshotRequestToWorker(pod, reqInfo, language, "delete")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_manager_isNoNeedRedoSnapshot(t *testing.T) {
	var (
		patch           = gomonkey.NewPatches()
		m               = &manager{}
		redisKey        string
		funcSvcID       string
		traceID         string
		checkpointEvent = &SnapshotEvent{}
		service         *faasfunction.FunctionService
	)

	convey.Convey("test needRedoCheckpointForSnapshotModule", t, func() {
		convey.Convey("test worker-manager do snapshot should return", func() {
			mockIsSnapShotModel(patch, false)
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeCreate, traceID)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Convey("test snapshot-manager do snapshot get funcService is nil", func() {
			mockIsSnapShotModel(patch, true)
			service = nil
			mockGetFunctionMetaWithRetry(patch, service)
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeCreate, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test snapshot-manager do snapshot get funcService is not cae type", func() {
			mockIsSnapShotModel(patch, true)
			service = &faasfunction.FunctionService{}
			mockGetFunctionMetaWithRetry(patch, service)
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeCreate, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test snapshot-manager do snapshot should skip", func() {
			mockIsSnapShotModel(patch, true)
			service = &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}},
			}
			mockGetFunctionMetaWithRetry(patch, service)
			m.redisCli = &redisclient.Client{}
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeCreate, traceID)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Convey("test snapshot-manager do snapshot by set key failed", func() {
			mockIsSnapShotModel(patch, true)
			service = &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}},
			}
			mockGetFunctionMetaWithRetry(patch, service)
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetErr(errors.New("set key failed"))
			mockRedisSetNX(patch, boolCmd)
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeUpdate, traceID)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Convey("test snapshot-manager do snapshot by redisKey existed", func() {
			mockIsSnapShotModel(patch, true)
			service = &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}},
			}
			mockGetFunctionMetaWithRetry(patch, service)
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetErr(errLockRedisKey)
			mockRedisSetNX(patch, boolCmd)
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeUpdate, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test snapshot-manager do snapshot success", func() {
			mockIsSnapShotModel(patch, true)
			service = &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}},
			}
			mockGetFunctionMetaWithRetry(patch, service)
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			mockRedisSetNX(patch, boolCmd)
			res := m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, constants.CheckpointModeUpdate, traceID)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_manager_waitKeyDeletedOrTimeout(t *testing.T) {
	var (
		patch           = gomonkey.NewPatches()
		m               = &manager{}
		redisKey        string
		checkpointEvent = &SnapshotEvent{}
		traceID         string
	)

	convey.Convey("test waitKeyDeletedOrTimeout", t, func() {
		convey.Convey("test waitKeyDeletedOrTimeout success", func() {
			var flag = false
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetVal(true)
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX", func(_ *redisclient.Client,
				ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
				if boolCmd.Err() == nil {
					flag = true
				}
				return boolCmd
			})

			m.waitKeyDeletedOrTimeout(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test waitKeyDeletedOrTimeout failed", func() {
			var flag = false
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetErr(errConnRedisClient)
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX", func(_ *redisclient.Client,
				ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
				if boolCmd.Err() != nil {
					flag = true
				}
				return boolCmd
			})
			m.waitKeyDeletedOrTimeout(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test waitKeyDeletedOrTimeout timeout", func() {
			var flag = false
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			boolCmd := &redis.BoolCmd{}
			boolCmd.SetErr(errLockRedisKey)
			mockRedisSetNX(patch, boolCmd)
			timer := time.NewTimer(2 * time.Second)
			patch.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				if boolCmd.Err() != nil {
					flag = true
				}
				return timer
			})
			m.waitKeyDeletedOrTimeout(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_manager_deleteCheckpointRedisKey(t *testing.T) {
	var (
		patch           = gomonkey.NewPatches()
		m               = &manager{}
		redisKey        string
		checkpointEvent = &SnapshotEvent{}
		traceID         string
		mockLogger      = &fakeLogger{}
	)

	convey.Convey("test deleteCheckpointRedisKey", t, func() {
		convey.Convey("test getCheckpointRedisKey failed", func() {
			var flag = false
			checkpointEvent.uuid = "test123"
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetErr(errors.New("set key failed"))
			mockLogGetLogger(patch, mockLogger)
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get", func(_ *redisclient.Client,
				ctx context.Context, keys string) *redis.StringCmd {
				if stringCmd.Err() != nil {
					flag = true
				}
				return stringCmd
			})

			m.deleteCheckpointRedisKey(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
			convey.So(mockLogger.ErrorMsg, convey.ShouldContainSubstring, "with error: set key failed")
		})

		convey.Convey("test getCheckpointRedisKey success but not equal", func() {
			var flag = false
			checkpointEvent.uuid = "test123"
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			mockLogGetLogger(patch, mockLogger)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test456")
			mockLogGetLogger(patch, mockLogger)
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get", func(_ *redisclient.Client,
				ctx context.Context, keys string) *redis.StringCmd {
				if stringCmd.Val() != "test123" {
					flag = true
				}
				return stringCmd
			})

			m.deleteCheckpointRedisKey(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
			convey.So(mockLogger.ErrorMsg, convey.ShouldContainSubstring, "is locked by other")
		})

		convey.Convey("test deleteCheckpointRedisKey failed", func() {
			var flag = false
			checkpointEvent.uuid = "test123"
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			mockLogGetLogger(patch, mockLogger)
			intCmd := &redis.IntCmd{}
			intCmd.SetErr(errors.New("set key failed"))
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del", func(_ *redisclient.Client,
				ctx context.Context, keys ...string) *redis.IntCmd {
				if intCmd.Err() != nil {
					flag = true
				}
				return intCmd
			})

			m.deleteCheckpointRedisKey(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
			convey.So(mockLogger.ErrorMsg, convey.ShouldContainSubstring, "failed to del key:")
		})

		convey.Convey("test deleteCheckpointRedisKey success", func() {
			var flag = false
			checkpointEvent.uuid = "test123"
			m.redisCli = &redisclient.Client{}
			mockGetRedisClient(patch, m.redisCli)
			stringCmd := &redis.StringCmd{}
			stringCmd.SetVal("test123")
			mockRedisGet(patch, stringCmd)
			intCmd := &redis.IntCmd{}
			patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del", func(_ *redisclient.Client,
				ctx context.Context, keys ...string) *redis.IntCmd {
				if intCmd.Err() == nil {
					flag = true
				}
				return intCmd
			})
			mockLogGetLogger(patch, mockLogger)
			m.deleteCheckpointRedisKey(checkpointEvent, redisKey, traceID)
			convey.So(flag, convey.ShouldBeTrue)
			convey.So(mockLogger.InfoMsg, convey.ShouldContainSubstring, "success to release redis key:")
		})
		convey.Reset(func() {
			mockLogger = &fakeLogger{}
			patch.Reset()
		})
	})
}

func Test_getNeedRemoveObjectIDs(t *testing.T) {
	tests := []struct {
		name            string
		checkpointEvent *SnapshotEvent
		want            []string
	}{
		// case1: test snapshot version num is one
		{
			name: "test snapshot version num is one",
			checkpointEvent: &SnapshotEvent{
				snapshotResult: &types.SnapshotDeploymentSpec{
					SnapshotVersions: []types.SnapshotVersion{
						{
							SnapshotImageTag: "1.101",
							ObjectID:         "test.VERSION1.101",
						},
					},
				},
			},
			want: nil,
		},
		// case2: test snapshot version num is one
		{
			name: "test snapshot version num more than one should return need delete objectIDS",
			checkpointEvent: &SnapshotEvent{
				snapshotResult: &types.SnapshotDeploymentSpec{
					SnapshotVersions: []types.SnapshotVersion{
						{
							SnapshotImageTag: "1.101",
							ObjectID:         "test.VERSION1.101",
						},
						{
							SnapshotImageTag: "1.102",
							ObjectID:         "test.VERSION1.102",
						},
					},
				},
			},
			want: []string{"test.VERSION1.102"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getNeedRemoveObjectIDs(tt.checkpointEvent)
			assert.Equal(t, tt.checkpointEvent.snapshotResult.NeedDeleteObjectIDs, tt.want)
		})
	}
}

func Test_manager_GetSnapshotCount(t *testing.T) {
	var (
		funcSvcID string
	)

	convey.Convey("test GetSnapshotCount", t, func() {
		convey.Convey("test func svc id is exist", func() {
			m := &manager{}
			m.needSnapshot.Store(funcSvcID, struct{}{})
			res := m.GetSnapshotCount(funcSvcID)
			convey.So(res, convey.ShouldEqual, 1)
		})

		convey.Convey("test func svc id is not exist", func() {
			m := &manager{}
			res := m.GetSnapshotCount(funcSvcID)
			convey.So(res, convey.ShouldEqual, 0)
		})

		convey.Convey("test manger is nil", func() {
			var m *manager
			res := m.GetSnapshotCount(funcSvcID)
			convey.So(res, convey.ShouldEqual, 0)
		})
	})
}

func Test_manager_fillResultAndReturn(t *testing.T) {
	tests := []struct {
		name         string
		patchesFunc  mockUtils.PatchesFunc
		cfg          *config.FaasConfig
		snapshotSpec *types.SnapshotDeploymentSpec
		wantErr      error
	}{
		{
			name: "test get config error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("get config error")
					}),
				})
				return patches
			},
			snapshotSpec: &types.SnapshotDeploymentSpec{},
			wantErr:      errors.New("failed to get config err: get config error"),
		},
		{
			name: "test get config is empty error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							PoolMgrConfig: config.PoolMgrConfig{
								MultiSnapshotConfig: config.MultiSnapshotConfig{
									SnapshotCheckInfo: types.SnapshotCheckInfo{},
								},
							},
						}, nil
					}),
				})
				return patches
			},
			snapshotSpec: &types.SnapshotDeploymentSpec{},
			wantErr:      errors.New("config value is wrong"),
		},
		{
			name: "test get snapshot check info success",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							PoolMgrConfig: config.PoolMgrConfig{
								MultiSnapshotConfig: config.MultiSnapshotConfig{
									SnapshotCheckInfo: types.SnapshotCheckInfo{
										JavaRuntimeSize:  123123,
										JavaRuntimeHash:  "123hash",
										RunTimeSize:      123123,
										ApmJavaAgentSize: 123123,
										ImageOS:          "aaa",
									},
								},
							},
						}, nil
					}),
				})
				return patches
			},
			snapshotSpec: &types.SnapshotDeploymentSpec{},
			wantErr:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			m := &manager{}
			_, err := m.fillResultAndReturn(tt.snapshotSpec)
			assert.Equal(t, tt.wantErr, err)
			patches.ResetAll()
		})
	}
}

func mockGeneralizePodWithRetry(patch *gomonkey.Patches, pod *corev1.Pod, reqInfo *config.RequestInfo, needReturn bool, res *string) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&manager{}), "generalizePodWithRetry", func(_ *manager,
		checkpointEvent *SnapshotEvent, dur *duration, traceID string, funcSvcID string, functionService *faasfunction.FunctionService) (*corev1.Pod,
		*config.RequestInfo, bool) {
		if needReturn {
			*res = "failed to generalize pod"
		}
		return pod, reqInfo, needReturn
	})
}

func mockSendSnapshotRequestToWorker(patch *gomonkey.Patches, err error, res *string) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&manager{}), "sendSnapshotRequestToWorker", func(_ *manager, pod *corev1.Pod,
		reqInfo *config.RequestInfo, language, action string) error {
		if err != nil {
			*res = "failed to send snapshot request to worker"
		}
		return err
	})
}

func mockDeletePodByK8S(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S, func(_ *podfetcher.PodFetcher, podName,
		traceID string) error {
		return err
	})
}

func mockFillInRequest(patch *gomonkey.Patches) {
	patch.ApplyFunc((*manager).fillInRequest,
		func(_ *manager, req *fhttp.Request) {
		})
}

func mockGeneralizePod(patch *gomonkey.Patches, pod *corev1.Pod, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&manager{}), "generalizePod", func(_ *manager, requestInfo *config.RequestInfo,
		functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
		return pod, err
	})
}

func mockGetRuntimeManagerImageTag(patch *gomonkey.Patches, res string) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&manager{}), "getRuntimeManagerImageTag", func(_ *manager) string {
		return res
	})
}

func mockWorkerClientDoTimeout(patch *gomonkey.Patches, mgr *manager, respCode int, body []byte, err error) {
	patch.ApplyMethod(reflect.TypeOf(mgr.workerClient), "DoTimeout",
		func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
			resp.SetStatusCode(respCode)
			resp.SetBody(body)
			return err
		})
}

func mockJsonMarshal(patch *gomonkey.Patches, body []byte, err error) {
	patch.ApplyFunc(json.Marshal, func(v any) ([]byte, error) { return body, err })
}

func mockIsSnapShotModel(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(utils.IsSnapShotModel, func() bool {
		return res
	})
}

func mockGetEnvClusterID(patch *gomonkey.Patches, key string) {
	patch.ApplyFunc(utils.GetEnvClusterID, func() string {
		return key
	})
}

func mockGetFunctionMetaWithRetry(patch *gomonkey.Patches, service *faasfunction.FunctionService) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&manager{}), "getFunctionMetaWithRetry", func(_ *manager,
		traceID string, funcSvcID string) *faasfunction.FunctionService {
		return service
	})
}

func mockGetRedisClient(patch *gomonkey.Patches, redisCli *redisclient.Client) {
	patch.ApplyFunc(monitoring.GetRedisClient,
		func() *redisclient.Client {
			return redisCli
		})
}

func mockRedisSetNX(patch *gomonkey.Patches, redisBoolCmd *redis.BoolCmd) {
	patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX", func(_ *redisclient.Client,
		ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
		return redisBoolCmd
	})
}

func mockRedisGet(patch *gomonkey.Patches, stringCmd *redis.StringCmd) {
	patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get", func(_ *redisclient.Client,
		ctx context.Context, keys string) *redis.StringCmd {
		return stringCmd
	})
}

func mockRedisDel(patch *gomonkey.Patches, intCmd *redis.IntCmd) {
	patch.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del", func(_ *redisclient.Client,
		ctx context.Context, keys ...string) *redis.IntCmd {
		return intCmd
	})
}

func mockTimeSleep(patch *gomonkey.Patches) {
	patch.ApplyFunc(time.Sleep, func(_ time.Duration) {})
}

func mockLogGetLogger(patch *gomonkey.Patches, mockLogger log.FormatLogger) {
	patch.ApplyFunc(log.GetLogger, func() log.FormatLogger {
		return mockLogger
	})
}
