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

package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	commonmetrics "functioncore/pkg/common/metrics"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/types"
	commonutil "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	faasv1alpha1 "functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/breaker"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	tools "functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func Test_workerCache_CheckReachedMaxInstance(t *testing.T) {
	functionService := &faasfunction.FunctionService{}
	functionService.ObjectMeta = faasfunction.ObjectMeta{ID: "testCheckReachedMaxInstance"}
	functionService.Spec = faasfunction.FunctionServiceSpec{
		Configuration: faasfunction.ConfigurationSpec{
			AutoScalerConfig: faasfunction.AutoScalerConfig{},
		},
	}
	requestInfo := &config.RequestInfo{TraceID: "test-traceID"}
	assert.Equal(t, false, WorkerCache.CheckReachedMaxInstance(functionService, requestInfo))
	WorkerCache.createFunctionCacheIfNotExist("testCheckReachedMaxInstance", "", commonmetrics.FunctionInfo{}).maxScale = 10
	defer WorkerCache.functions.Delete("testCheckReachedMaxInstance")
	assert.Equal(t, false, WorkerCache.CheckReachedMaxInstance(functionService, requestInfo))
}

func Test_workerCache_GetInstanceList(t *testing.T) {
	WorkerCache.createFunctionCacheIfNotExist("testGetInstanceList", "", commonmetrics.FunctionInfo{}).instances = map[string]faasv1alpha1.WorkerInstance{"test": {}}
	worker1, worker2 := WorkerCache.GetInstanceList("unknownFunc"), WorkerCache.GetInstanceList("testGetInstanceList")
	defer WorkerCache.functions.Delete("testGetInstanceList")
	assert.Nil(t, worker1)
	assert.Equal(t, 1, len(worker2))
}

func Test_pullFunctionCache(t *testing.T) {
	type args struct {
		pod          *corev1.Pod
		functionName string
	}
	// mock worker cache
	WorkerCache = &workerCache{}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test001",
			args: args{
				pod:          &corev1.Pod{},
				functionName: "mock-function-name",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetETCDFuncKeyByPod,
						func(pod *corev1.Pod) string {
							return ""
						}),
					gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd,
						func(functionKey string) (*faasfunction.FunctionService, types.ProfileMetaData, etcd3.EtcdType, error) {
							return &faasfunction.FunctionService{}, types.ProfileMetaData{}, etcd3.Meta, nil
						}),
					gomonkey.ApplyFunc(utils.GetCRSignature,
						func(functionMeta *faasfunction.FunctionService) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			assert.NotNil(t, pullFunctionCache(tt.args.pod, tt.args.functionName), "pullFunctionCache(%v, %v) not nil", tt.args.pod, tt.args.functionName)
			patches.ResetAll()
		})
	}
}

func Test_TakeOutPrewarmPod(t *testing.T) {
	type args struct {
		fc  *functionCache
		num int
	}

	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test enough pool pod",
			args: args{
				fc: &functionCache{
					prewarmPool: []*corev1.Pod{{}, {}, {}, {}},
				},
				num: 3,
			},
			want: 3,
		},
		{
			name: "test not enough pool pods",
			args: args{
				fc: &functionCache{
					prewarmPool: []*corev1.Pod{{}},
				},
				num: 3,
			},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := tt.args.fc
			if len(fc.takeOutPrewarmPod(tt.args.num)) != tt.want {
				t.Log("failed to take out prewarm pod")
			}

		})
	}
}

func Test_pullFunctionCache_Error(t *testing.T) {
	type args struct {
		pod          *corev1.Pod
		functionName string
	}
	WorkerCache = &workerCache{}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test001",
			args: args{
				pod:          &corev1.Pod{},
				functionName: "mock-function-name",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetETCDFuncKeyByPod,
						func(pod *corev1.Pod) string {
							return ""
						}),
					gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd,
						func(functionKey string) (*faasfunction.FunctionService, types.ProfileMetaData, etcd3.EtcdType, error) {
							return nil, types.ProfileMetaData{}, etcd3.Meta, errors.New("mock error")
						}),
					gomonkey.ApplyFunc(utils.GetCRSignature,
						func(functionMeta *faasfunction.FunctionService) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			assert.Nil(t, pullFunctionCache(tt.args.pod, tt.args.functionName), "pullFunctionCache(%v, %v) not nil", tt.args.pod, tt.args.functionName)
			patches.ResetAll()
		})
	}
}

func Test_workerCache_createSimpleFunctionCache(t *testing.T) {
	type fields struct {
		kubeClient kubernetes.Interface
		functions  sync.Map
	}
	type args struct {
		funcSvcID   string
		minScale    int64
		maxScale    int64
		idleMode    bool
		profileMeta types.ProfileMetaData
	}
	mockFuncSvcID := "mockFuncSvcID"
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "test001",
			fields: fields{},
			args: args{
				funcSvcID:   mockFuncSvcID,
				minScale:    1,
				maxScale:    10,
				idleMode:    false,
				profileMeta: types.ProfileMetaData{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{}
			assert.NotNil(t, w.createSimpleFunctionCache(tt.args.funcSvcID, config.InstanceMetaData{
				MinInstance: tt.args.minScale, MaxInstance: tt.args.maxScale, IdleMode: tt.args.idleMode,
			}, tt.args.profileMeta, tools.FunctionInfo{}), "createSimpleFunctionCache(%v, %v, %v, %v)", tt.args.funcSvcID, tt.args.minScale, tt.args.maxScale, tt.args.idleMode)
		})
	}
}

// TestInitWorkerCache -
func TestInitWorkerCache(t *testing.T) {
	convey.Convey("TestInitWorkerCache", t, func() {
		kubeClient := &kubernetes.Clientset{}
		corev1Client := &typecorev1.CoreV1Client{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.FunctionNameSpace, func() string { return "default" }),
			gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return corev1Client
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
				func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
					return mockutils.NewK8sPodImp(namespace)
				}),
			gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return kubeClient
			}),
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
					}, nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("getInstanceListFromEtcdErr", func() {
			patch := gomonkey.ApplyFunc(getInstanceListFromEtcd, func() ([]syncCompare, error) {
				return nil, errors.New("test err")
			})
			err := InitWorkerCache(nil)
			convey.So(err, convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("ChangeOldWorkerOwnerAndHandleDifferenceErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(getInstanceListFromEtcd, func() ([]syncCompare, error) {
					return []syncCompare{}, nil
				}),
				gomonkey.ApplyFunc(initWorkerCacheWithPod, func(pod *corev1.Pod) *commonmetrics.FunctionInfo {
					return &commonmetrics.FunctionInfo{}
				}),
				gomonkey.ApplyFunc(utils.MakeWorkerInsInfo, func(pod *corev1.Pod) v1alpha1.WorkerInstance {
					return v1alpha1.WorkerInstance{}
				}),
				gomonkey.ApplyFunc(generateEtcdKey, func(instance v1alpha1.WorkerInstance,
					info commonmetrics.FunctionInfo) string {
					return ""
				}),
				gomonkey.ApplyFunc(utils.ChangeOldWorkerOwner, func(pod *corev1.Pod,
					requestInfo *config.RequestInfo) error {
					return errors.New("test err")
				}),
				gomonkey.ApplyFunc(handleDifference, func(expect, actual []syncCompare) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitWorkerCache([]corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "mockPodName"}}})
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("InitWorkerCacheOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(getInstanceListFromEtcd, func() ([]syncCompare, error) {
					return []syncCompare{}, nil
				}),
				gomonkey.ApplyFunc(initWorkerCacheWithPod, func(pod *corev1.Pod) *commonmetrics.FunctionInfo {
					return nil
				}),
				gomonkey.ApplyFunc(deletePod, func(pod *corev1.Pod) { return }),
				gomonkey.ApplyFunc(handleDifference, func(expect, actual []syncCompare) error { return nil }),
				gomonkey.ApplyFunc(checkPodImageTag, func(pod *corev1.Pod) error { return nil }),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitWorkerCache([]corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "mockPodName"}}})
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

// TestWorkerCache_GetOrWaitReservedIns -
func TestWorkerCache_GetOrWaitReservedIns(t *testing.T) {
	convey.Convey("TestWorkerCache_GetOrWaitReservedIns", t, func() {
		wc := &workerCache{
			functions: sync.Map{},
		}
		convey.Convey("getFunctionCacheErr", func() {
			workerIns, err := wc.GetOrWaitReservedIns(context.Background(), "", &config.RequestInfo{})
			convey.So(workerIns, convey.ShouldBeNil)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("selectInsIsNotNil", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetRadicalRedirectTimeout, func() time.Duration {
					return time.Second
				}),
				gomonkey.ApplyFunc(config.EnableRadicalAutoscaler, func() bool { return true }),
				gomonkey.ApplyFunc(timeutil.NowUnixMillisecond, func() int64 {
					return 2000
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			wc.functions.Store("test1", &functionCache{
				instances: map[string]v1alpha1.WorkerInstance{
					"test1": {},
				},
				autoScaleTimestamp: map[string]int64{
					"testNode1": 1500,
					"test1":     500,
				},
			})
			workerIns, err := wc.GetOrWaitReservedIns(context.Background(), "test1", &config.RequestInfo{
				DeployNode: "testNode1",
			})
			convey.So(workerIns, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("scaleNormalIns", func() {
			wc.functions.Store("test2", &functionCache{scalingNum: 0})
			patch := gomonkey.ApplyFunc(config.EnableRadicalAutoscaler, func() bool { return false })
			workerIns, err := wc.GetOrWaitReservedIns(context.Background(), "test2", &config.RequestInfo{
				OwnedWorkerView: types.WorkersView{OwnedNum: 1},
			})
			convey.So(workerIns, convey.ShouldBeNil)
			convey.So(err, convey.ShouldBeNil)
			patch.Reset()
		})
		convey.Convey("scalingReservedIsNotZero", func() {
			wc.functions.Store("test3", &functionCache{scalingReversed: 1})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.EnableRadicalAutoscaler, func() bool { return false }),
				gomonkey.ApplyFunc(waitScalingReservedIns, func(ctx context.Context, reservedInsChan chan v1alpha1.WorkerInstance,
					funcSvcID, traceID string) (*v1alpha1.WorkerInstance, error) {
					return &v1alpha1.WorkerInstance{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			workerIns, err := wc.GetOrWaitReservedIns(context.Background(), "test3", &config.RequestInfo{})
			convey.So(workerIns, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("selectReservedInsAndReturn", func() {
			wc.functions.Store("test4", &functionCache{
				instances: map[string]faasv1alpha1.WorkerInstance{
					"test4": {Applier: constants.WorkerManagerApplier},
				},
			})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.EnableRadicalAutoscaler, func() bool { return false }),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			workerIns, err := wc.GetOrWaitReservedIns(context.Background(), "test4", &config.RequestInfo{})
			convey.So(workerIns, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

// TestHandleDifference -
func TestHandleDifference(t *testing.T) {
	convey.Convey("TestHandleDifference", t, func() {
		convey.Convey("PutEtcdErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(putInstanceToEtcd, func(instance v1alpha1.WorkerInstance, path string) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.Convey("ActualInsIsNotEqualExpect", func() {
				convey.So(handleDifference(
					[]syncCompare{
						{instance: v1alpha1.WorkerInstance{InstanceID: "test1", IsRunning: true}},
					},
					[]syncCompare{
						{instance: v1alpha1.WorkerInstance{InstanceID: "test1", Signature: "test1", IsRunning: true}},
					}), convey.ShouldBeError)
			})
			convey.Convey("ActualInsIsNotExist", func() {
				convey.So(handleDifference(
					[]syncCompare{
						{instance: v1alpha1.WorkerInstance{InstanceID: "test1", IsRunning: true}},
					},
					[]syncCompare{}), convey.ShouldBeError)
			})
		})
		convey.Convey("delFromEtcd", func() {
			convey.Convey("delFromEtcdErr", func() {
				patch := gomonkey.ApplyFunc(deleteFromEtcd, func(path string) error {
					return errors.New("test err")
				})
				convey.So(handleDifference(
					[]syncCompare{},
					[]syncCompare{
						{instance: v1alpha1.WorkerInstance{InstanceID: "test1", IsRunning: true}},
					}), convey.ShouldBeError)
				patch.Reset()
			})
			convey.Convey("handleDifference", func() {
				convey.So(handleDifference(
					[]syncCompare{},
					[]syncCompare{}), convey.ShouldBeNil)
			})
		})
	})
}

// TestGetInstanceListFromEtcd -
func TestGetInstanceListFromEtcd(t *testing.T) {
	convey.Convey("TestGetInstanceListFromEtcd", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("GetResponseErr", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return nil, errors.New("test err")
			})
			syncCompares, err := getInstanceListFromEtcd()
			convey.So(syncCompares, convey.ShouldBeNil)
			convey.So(err, convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("GetListOk", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{
					Kvs: []*mvccpb.KeyValue{
						{Key: []byte{}},
						{Key: []byte("/sn/workers/business/yrk/tenant/0/function/test/version/test/defaultaz/test")},
						{
							Key:   []byte("/sn/workers/business/yrk/tenant/test/function/test/version/test/defaultaz/test"),
							Value: []byte("test"),
						},
						{
							Key:   []byte("/sn/workers/business/yrk/tenant/test/function/test/version/test/defaultaz/test"),
							Value: []byte(`{"instanceID":"test","signature":"test"}`),
						},
					},
				}, nil
			})
			syncCompares, err := getInstanceListFromEtcd()
			convey.So(syncCompares, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
			patch.Reset()
		})
	})
}

// TestInitWorkerCacheWithPod -
func TestInitWorkerCacheWithPod(t *testing.T) {
	convey.Convey("TestInitWorkerCacheWithPod", t, func() {
		convey.Convey("OwnedByFunctionServiceNotOk", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "", false
				}),
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
						}, nil
					}),
				gomonkey.ApplyFunc(checkPodImageTag, func(pod *corev1.Pod) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(initWorkerCacheWithPod(&corev1.Pod{}), convey.ShouldBeNil)
		})
		convey.Convey("pullFunctionCacheIsNil", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "initWorkerCacheWithPodTest1", true
				}),
				gomonkey.ApplyFunc(pullFunctionCache, func(pod *corev1.Pod, functionName string) *functionCache {
					return nil
				}),
				gomonkey.ApplyFunc(checkPodImageTag, func(pod *corev1.Pod) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(initWorkerCacheWithPod(&corev1.Pod{}), convey.ShouldBeNil)
		})
		convey.Convey("signatureIsNotEqual", func() {
			WorkerCache.functions.Store("initWorkerCacheWithPodTest2", &functionCache{
				crSignature: "",
				funcMeta:    &faasfunction.FunctionService{},
			})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "initWorkerCacheWithPodTest2", true
				}),
				gomonkey.ApplyFunc(checkPodImageTag, func(pod *corev1.Pod) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(initWorkerCacheWithPod(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.CRSignatureKey: "test2",
					},
				},
			}), convey.ShouldBeNil)
		})
		convey.Convey("initWorkerCacheWithPodOK", func() {
			fc := &functionCache{
				crSignature:   "test3",
				funcMeta:      &faasfunction.FunctionService{},
				initInstances: map[string]struct{}{},
			}
			WorkerCache.functions.Store("initWorkerCacheWithPodTest3", fc)
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "initWorkerCacheWithPodTest3", true
				}),
				gomonkey.ApplyFunc(utils.IsPodReady, func(pod *corev1.Pod) bool {
					return true
				}),
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
						}, nil
					}),
				gomonkey.ApplyFunc(checkPodImageTag, func(pod *corev1.Pod) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(initWorkerCacheWithPod(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.CRSignatureKey: "test3",
					},
					Labels: map[string]string{
						reserveInstanceSymbol: constants.WorkerManagerApplier,
					},
				},
			}), convey.ShouldNotBeNil)
			convey.So(len(fc.initInstances), convey.ShouldEqual, 1)
		})
		convey.Convey("signature not equal, but CAE func", func() {
			fc := &functionCache{
				crSignature:      "test44",
				funcMeta:         &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}}},
				initInstances:    map[string]struct{}{},
				rollingInstances: map[string]faasv1alpha1.WorkerInstance{},
			}
			WorkerCache.functions.Store("initWorkerCacheWithPodTest4", fc)
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "initWorkerCacheWithPodTest4", true
				}),
				gomonkey.ApplyFunc(utils.IsPodReady, func(pod *corev1.Pod) bool {
					return true
				}),
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
						}, nil
					}),
				gomonkey.ApplyFunc(checkPodImageTag, func(pod *corev1.Pod) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(initWorkerCacheWithPod(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.CRSignatureKey: "test4",
					},
					Labels: map[string]string{
						reserveInstanceSymbol: constants.WorkerManagerApplier,
					},
				},
			}), convey.ShouldNotBeNil)
			convey.So(len(fc.initInstances), convey.ShouldEqual, 0)
			convey.So(len(fc.rollingInstances), convey.ShouldEqual, 1)
		})
	})
}

// TestPutInstanceToEtcd -
func TestPutInstanceToEtcd(t *testing.T) {
	convey.Convey("TestPutInstanceToEtcd", t, func() {
		convey.Convey("jsonMarshalErr", func() {
			patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("test err")
			})
			convey.So(putInstanceToEtcd(v1alpha1.WorkerInstance{}, ""), convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("putEtcdErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(putInstanceToEtcd(v1alpha1.WorkerInstance{}, ""), convey.ShouldBeError)
		})
		convey.Convey("putEtcdOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(putInstanceToEtcd(v1alpha1.WorkerInstance{}, ""), convey.ShouldBeNil)
		})
		convey.Convey("putCAEEtcdOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(putInstanceToEtcd(v1alpha1.WorkerInstance{
				BusinessType: constants.BusinessTypeCAE,
			}, ""), convey.ShouldBeNil)
		})
		convey.Convey("putCAEEtcdErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(putInstanceToEtcd(v1alpha1.WorkerInstance{
				BusinessType: constants.BusinessTypeCAE,
			}, ""), convey.ShouldBeError)
		})
		convey.Convey("put CAEEtcd nil", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: nil}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(putInstanceToEtcd(v1alpha1.WorkerInstance{BusinessType: constants.BusinessTypeCAE}, ""),
				convey.ShouldBeNil)
		})
	})
}

// TestWorkerCache_GetInstanceMap -
func TestWorkerCache_GetInstanceMap(t *testing.T) {
	convey.Convey("TestWorkerCache_GetInstanceMap", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("funcSvcNameIsNotExist", func() {
			convey.So(WorkerCache.GetInstanceMap("test1"), convey.ShouldBeNil)
		})
		convey.Convey("GetInstanceMapOK", func() {
			WorkerCache.functions.Store("test2", &functionCache{
				instances: map[string]faasv1alpha1.WorkerInstance{
					"test1": {InstanceID: "test1"},
					"test2": {InstanceID: "test2"},
				},
			})
			convey.So(WorkerCache.GetInstanceMap("test2"), convey.ShouldNotBeNil)
		})
	})
}

// TestWorkerCache_GetFuncMeta -
func TestWorkerCache_GetFuncMeta(t *testing.T) {
	convey.Convey("TestWorkerCache_GetFuncMeta", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("funcSvcNameIsNotExist", func() {
			convey.So(WorkerCache.GetFuncMeta("test1"), convey.ShouldBeNil)
		})
		convey.Convey("GetInstanceMapOK", func() {
			WorkerCache.functions.Store("test2", &functionCache{funcMeta: &faasfunction.FunctionService{}})
			convey.So(WorkerCache.GetFuncMeta("test2"), convey.ShouldNotBeNil)
			WorkerCache.functions.Delete("test2")
		})
	})
}

// TestDeletePod -
func TestDeletePod(t *testing.T) {
	convey.Convey("TestDeletePod", t, func() {
		kubeClient := &kubernetes.Clientset{}
		corev1Client := &typecorev1.CoreV1Client{}
		checkFlag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.FunctionNameSpace, func() string { return "default" }),
			gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return corev1Client
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
				func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
					return mockutils.NewK8sPodImp(namespace)
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
				ctx context.Context, name string, opts metav1.DeleteOptions) error {
				checkFlag = true
				return errors.New("test err")
			}),
			gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return kubeClient
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		deletePod(&corev1.Pod{})
		convey.So(checkFlag, convey.ShouldBeTrue)
	})
	convey.Convey("TestDeletePodSuccess", t, func() {
		kubeClient := &kubernetes.Clientset{}
		corev1Client := &typecorev1.CoreV1Client{}
		checkFlag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.FunctionNameSpace, func() string { return "default" }),
			gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return corev1Client
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
				func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
					return mockutils.NewK8sPodImp(namespace)
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
				ctx context.Context, name string, opts metav1.DeleteOptions) error {
				checkFlag = true
				return nil
			}),
			gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return kubeClient
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		deletePod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "mock-pod-name",
				Labels: map[string]string{utils.LabelShortFuncName: "mock-func-name"},
			},
		})
		convey.So(checkFlag, convey.ShouldBeTrue)
	})
}

// TestWorkerCache_GetMaxScale -
func TestWorkerCache_GetMaxScale(t *testing.T) {
	convey.Convey("TestWorkerCache_GetMaxScale", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("funcSvcNameIsNotExist", func() {
			convey.So(WorkerCache.GetMaxScale("test1"), convey.ShouldEqual, 0)
		})
		convey.Convey("GetInstanceMapOK", func() {
			WorkerCache.functions.Store("test2", &functionCache{maxScale: 1})
			convey.So(WorkerCache.GetMaxScale("test2"), convey.ShouldEqual, 1)
		})
	})
}

// TestDeleteFromEtcd -
func TestDeleteFromCAERouterEtcd(t *testing.T) {
	convey.Convey("TestDeleteFromCAEEtcd", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer func() {
			patch.Reset()
		}()
		convey.Convey("deleteErr", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return errors.New("test err")
			})
			convey.So(deleteFromCAERouterEtcd("test"), convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("deleteOK", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return nil
			})
			convey.So(deleteFromCAERouterEtcd("test"), convey.ShouldBeNil)
			patch.Reset()
		})
	})
	convey.Convey("TestDeleteFromCAEEtcd", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: nil}
		})
		defer func() {
			patch.Reset()
		}()
		convey.So(deleteFromCAERouterEtcd("test"), convey.ShouldBeNil)
	})
}

// TestDeleteFromEtcd -
func TestDeleteFromEtcd(t *testing.T) {
	convey.Convey("TestDeleteFromEtcd", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		defer func() {
			patch.Reset()
		}()
		convey.Convey("deleteErr", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return errors.New("test err")
			})
			convey.So(deleteFromEtcd("test"), convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("deleteOK", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return nil
			})
			convey.So(deleteFromEtcd("test"), convey.ShouldBeNil)
			patch.Reset()
		})
	})
}

// TestGenerateEtcdKey -
func TestGenerateEtcdKey(t *testing.T) {
	convey.Convey("TestGenerateEtcdKey", t, func() {
		patch := gomonkey.ApplyFunc(commonutil.AzEnv, func() string {
			return "az"
		})
		convey.So(
			generateEtcdKey(v1alpha1.WorkerInstance{InstanceID: "test"}, commonmetrics.FunctionInfo{
				BusinessID:   "test",
				TenantID:     "test",
				FunctionName: "test",
				Version:      "test",
			}),
			convey.ShouldEqual, "/sn/workers/business/test/tenant/test/function/test/version/test/az/test")
		patch.Reset()
	})
}

// TestWorkerCache_GetIdleMode -
func TestWorkerCache_GetIdleMode(t *testing.T) {
	convey.Convey("TestWorkerCache_GetIdleMode", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("funcSvcNameIsNotExist", func() {
			convey.So(WorkerCache.GetIdleMode("test1"), convey.ShouldBeFalse)
		})
		convey.Convey("GetIdleModeOK", func() {
			WorkerCache.functions.Store("test2", &functionCache{idleMode: true})
			convey.So(WorkerCache.GetIdleMode("test2"), convey.ShouldBeTrue)
		})
	})
}

// TestWorkerCache_GetWorkerInstance -
func TestWorkerCache_GetWorkerInstance(t *testing.T) {
	convey.Convey("TestWorkerCache_GetWorkerInstance", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("funcSvcNameIsNotExist", func() {
			ins, ok := WorkerCache.GetWorkerInstance("test1", "test1")
			convey.So(ins.InstanceID, convey.ShouldEqual, "")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("GetWorkerInstanceOK", func() {
			WorkerCache.functions.Store("test2", &functionCache{instances: map[string]faasv1alpha1.WorkerInstance{
				"test2": {InstanceID: "test2"},
			}})
			ins, ok := WorkerCache.GetWorkerInstance("test2", "test2")
			convey.So(ins.InstanceID, convey.ShouldEqual, "test2")
			convey.So(ok, convey.ShouldBeTrue)
		})
	})
}

// TestWorkerCache_GetWorkerRecords -
func TestWorkerCache_GetWorkerRecords(t *testing.T) {
	convey.Convey("TestWorkerCache_GetWorkerRecords", t, func() {
		convey.Convey("assertTypeErr", func() {
			WorkerCache.functions = sync.Map{}
			WorkerCache.functions.Store("test1", "")
			convey.So(WorkerCache.GetWorkerRecords(), convey.ShouldEqual, 0)
			WorkerCache.functions.Delete("test1")
		})
		convey.SkipConvey("GetWorkerRecordsOK", func() {
			WorkerCache.functions = sync.Map{}
			WorkerCache.functions.Store("test1", &functionCache{
				instances: map[string]faasv1alpha1.WorkerInstance{
					"1": {},
					"2": {},
				},
			})
			WorkerCache.functions.Store("test2", &functionCache{
				instances: map[string]faasv1alpha1.WorkerInstance{
					"1": {},
					"2": {},
				},
			})
			convey.So(WorkerCache.GetWorkerRecords(), convey.ShouldEqual, 4)
		})
	})
}

// TestWorkerCache_CheckFunctionExist -
func TestWorkerCache_CheckFunctionExist(t *testing.T) {
	convey.Convey("TestWorkerCache_CheckFunctionExist", t, func() {
		cache := &workerCache{
			functions: sync.Map{},
		}
		cache.functions.Store("test", &functionCache{})
		convey.So(cache.CheckFunctionExist("test"), convey.ShouldBeTrue)
	})
}

func TestUpdateAllFuncDNSDomain(t *testing.T) {
	convey.Convey("TestUpdateAllFuncDNSDomainError", t, func() {
		hasMocked := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				hasMocked = true
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		stopCh := make(chan struct{})
		UpdateAllFuncDNSDomain(stopCh, 1*time.Second)
		convey.So(hasMocked, convey.ShouldBeTrue)
	})
	convey.Convey("TestUpdateAllFuncDNSDomain", t, func() {
		hasMocked := uint32(0)
		mockWorkerCache := &workerCache{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockWorkerCache), "UpdateDNSDomainNames",
				func(_ *workerCache, cfg *config.FaasConfig, interval time.Duration) {
					atomic.AddUint32(&hasMocked, 1)
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
		UpdateAllFuncDNSDomain(nil, 1*time.Second)
		convey.So(hasMocked, convey.ShouldEqual, 0)
		stopCh := make(chan struct{})
		go UpdateAllFuncDNSDomain(stopCh, 2*time.Second)
		time.Sleep(50 * time.Millisecond)
		close(stopCh)
		time.Sleep(50 * time.Millisecond)
		convey.So(atomic.LoadUint32(&hasMocked), convey.ShouldEqual, 1)
	})
}

func Test_workerCache_UpdateDNSDomainNames(t *testing.T) {
	convey.Convey("domainKeyMap is nil", t, func() {
		hasMocked := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cache := &workerCache{
			functions: sync.Map{},
		}
		cache.functions.Store("mockFuncSvcID", "mockFunctionCache")
		cache.UpdateDNSDomainNames(&config.FaasConfig{}, time.Second)
		convey.So(hasMocked, convey.ShouldBeFalse)
	})
	convey.Convey("function cache is nil", t, func() {
		hasMocked := false
		dnsCache := &utils.DNSCache{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetDNSCache, func() *utils.DNSCache {
				return dnsCache
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cache := &workerCache{
			functions: sync.Map{},
		}
		dnsDomainMap := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-001.com":  {"192.168.100.1"},
		}
		dnsCache.StoreDNSCache("mockFuncSvcID01", "vpc001", "domain001", dnsDomainMap)
		cache.functions.Store("mockFuncSvcID", "mockFunctionCache")
		cache.UpdateDNSDomainNames(&config.FaasConfig{}, time.Second)
		convey.So(hasMocked, convey.ShouldBeFalse)
	})
	convey.Convey("dnsDomainCfg is nil", t, func() {
		hasMocked := false
		dnsCache := &utils.DNSCache{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetDNSCache, func() *utils.DNSCache {
				return dnsCache
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cache := &workerCache{
			functions: sync.Map{},
		}
		dnsDomainMap := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-001.com":  {"192.168.100.1"},
		}
		dnsCache.StoreDNSCache("mockFuncSvcID01", "vpc001", "domain001", dnsDomainMap)
		cache.functions.Store("mockFuncSvcID01", "mockFunctionCache")
		cache.UpdateDNSDomainNames(&config.FaasConfig{}, time.Second)
		convey.So(hasMocked, convey.ShouldBeFalse)
	})
	convey.Convey("failed to get dns userAgency ", t, func() {
		hasMocked := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetUserAgencyByVpcConfig, func(vpcConfig faasfunction.Vpc) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cache := &workerCache{
			functions: sync.Map{},
		}
		cache.functions.Store("mockFuncSvcID01", &functionCache{
			funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						EndpointTenantVpc: faasfunction.Vpc{},
					},
				},
			},
			dnsDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "111",
					DomainName: "www.example.com",
				},
				{
					ID:         "222",
					DomainName: "www.sample.com",
				},
			},
			endpointDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "333",
					DomainName: "www.example3.com",
				},
				{
					ID:         "444",
					DomainName: "www.sample4.com",
				},
			},
			vpcID:               "vpc002",
			endpointTenantVpcID: "vpc002",
		})
		dnsDomainMap := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-001.com":  {"192.168.100.1"},
		}
		dnsCache := &utils.DNSCache{}
		dnsCache.StoreDNSCache("mockFuncSvcID01", "vpc001", "domain001", dnsDomainMap)
		cache.UpdateDNSDomainNames(&config.FaasConfig{LocalAuth: &localauth.AuthConfig{}}, time.Second)
		convey.So(hasMocked, convey.ShouldBeFalse)
	})
	convey.Convey("dnsDomainCfg is not nil", t, func() {
		hasMocked := false
		dnsCache := &utils.DNSCache{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetDNSCache, func() *utils.DNSCache {
				return dnsCache
			}),
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetUserAgencyByVpcConfig, func(vpcConfig faasfunction.Vpc) utils.UserAgency {
				hasMocked = true
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetIPsByDNS, func(omsvcAddress, ak, sk string,
				param utils.DNSProxyRequestParam) (map[string][]string, error) {
				hasMocked = true
				info := param.Info
				if info.DomainName == "www.example.com" {
					return map[string][]string{"www.example.com": {"192.168.0.1"}}, nil
				}
				if info.DomainName == "www.sample.com" {
					return map[string][]string{"www.sample.com": {"192.168.0.2", "192.168.0.3"}}, nil
				}
				if info.DomainName == "www.example3.com" {
					return map[string][]string{"www.example3.com": {"192.168.0.4"}}, nil
				}
				if info.DomainName == "www.sample4.com" {
					return map[string][]string{"www.sample4.com": {"192.168.0.6", "192.168.0.5"}}, nil
				}
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cache := &workerCache{
			functions: sync.Map{},
		}
		cache.functions.Store("mockFuncSvcID02", &functionCache{
			funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						EndpointTenantVpc: faasfunction.Vpc{},
					},
				},
			},
			dnsDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "111",
					DomainName: "www.example.com",
				},
				{
					ID:         "222",
					DomainName: "www.sample.com",
				},
			},
			endpointDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "333",
					DomainName: "www.example3.com",
				},
				{
					ID:         "444",
					DomainName: "www.sample4.com",
				},
			},
			vpcID:               "vpc001",
			endpointTenantVpcID: "vpc002",
		})
		dnsDomainMap := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-001.com":  {"192.168.100.1"},
		}
		dnsDomainMap2 := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-002.com":  {"192.168.100.1"},
		}
		dnsCache.StoreDNSCache("mockFuncSvcID02", "vpc001", "www.example.com", dnsDomainMap)
		dnsCache.StoreDNSCache("mockFuncSvcID02", "vpc001", "www.sample.com", dnsDomainMap)
		dnsCache.StoreDNSCache("mockFuncSvcID02", "vpc002", "www.example3.com", dnsDomainMap2)
		dnsCache.StoreDNSCache("mockFuncSvcID02", "vpc002", "www.sample4.com", dnsDomainMap2)
		domainNames, err := dnsCache.GetDNSDomainNames("mockFuncSvcID02")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 3)
		convey.So(len(domainNames["test.autotest-001.com"]), convey.ShouldEqual, 1)
		convey.So(len(domainNames["multi.autotest-001.com"]), convey.ShouldEqual, 3)
		convey.So(len(domainNames["www.example.com"]), convey.ShouldEqual, 0)
		convey.So(len(domainNames["www.sample.com"]), convey.ShouldEqual, 0)
		convey.So(len(domainNames["www.example3.com"]), convey.ShouldEqual, 0)
		convey.So(len(domainNames["www.sample4.com"]), convey.ShouldEqual, 0)
		cache.UpdateDNSDomainNames(&config.FaasConfig{LocalAuth: &localauth.AuthConfig{}}, time.Second)
		convey.So(hasMocked, convey.ShouldBeTrue)
		domainNames, err = dnsCache.GetDNSDomainNames("mockFuncSvcID02")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 4)
		convey.So(len(domainNames["test.autotest-001.com"]), convey.ShouldEqual, 0)
		convey.So(len(domainNames["multi.autotest-001.com"]), convey.ShouldEqual, 0)
		convey.So(len(domainNames["www.example.com"]), convey.ShouldEqual, 1)
		convey.So(len(domainNames["www.sample.com"]), convey.ShouldEqual, 2)
		convey.So(len(domainNames["www.example3.com"]), convey.ShouldEqual, 1)
		convey.So(len(domainNames["www.sample4.com"]), convey.ShouldEqual, 2)
	})
}

func Test_workerCache_GetResInsNodeDis(t *testing.T) {
	functionCache1 := sync.Map{}
	instasnces1 := make(map[string]v1alpha1.WorkerInstance, initCacheSize)
	instasnces1["abc"] = v1alpha1.WorkerInstance{Applier: constants.WorkerManagerApplier,
		NodeName: "192.168.77.88"}
	functionCache1.LoadOrStore("test1", &functionCache{
		instances:             instasnces1,
		funcSvcID:             "test1",
		reservedInsNodePreDis: make(map[string]int, initCacheSize),
	})

	functionCache2 := sync.Map{}
	instasnces2 := make(map[string]v1alpha1.WorkerInstance, initCacheSize)
	instasnces2["abc"] = v1alpha1.WorkerInstance{Applier: constants.WorkerManagerApplier,
		NodeName: "192.168.77.88"}
	instasnces2["abcd"] = v1alpha1.WorkerInstance{Applier: constants.WorkerManagerApplier,
		NodeName: "192.168.77.88"}
	instasnces2["abcde"] = v1alpha1.WorkerInstance{Applier: constants.WorkerManagerApplier,
		NodeName: "192.168.77.89"}
	functionCache2.LoadOrStore("test2", &functionCache{
		instances:             instasnces2,
		funcSvcID:             "test2",
		reservedInsNodePreDis: make(map[string]int, initCacheSize),
	})
	type fields struct {
		kubeClient kubernetes.Interface
		functions  sync.Map
	}
	type args struct {
		funcSvcID string
		nodeList  []string
		traceID   string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{"case1 fc.instances has reserved-instance, calculate",
			fields{functions: functionCache1}, args{
				funcSvcID: "test1",
				nodeList:  []string{"192.168.77.88", "192.168.77.89"},
				traceID:   "reserved-6aceb507-52fe-4e8f-ada5-5ebd76e129c1",
			}, []string{"192.168.77.89"}},
		{"case2 nodeList length is 0", fields{functions: functionCache1}, args{
			funcSvcID: "test1",
			nodeList:  []string{},
			traceID:   "reserved-6aceb507-52fe-4e8f-ada5-5ebd76e129c1",
		}, []string{}},
		{"case3 fcCache not exists", fields{functions: functionCache1}, args{
			funcSvcID: "test2",
			nodeList:  []string{},
			traceID:   "reserved-6aceb507-52fe-4e8f-ada5-5ebd76e129c1",
		}, nil},
		{"case4 fc.instances has reserved-instance, calculate min node",
			fields{functions: functionCache2}, args{
				funcSvcID: "test2",
				nodeList:  []string{"192.168.77.88", "192.168.77.89"},
				traceID:   "reserved-6aceb507-52fe-4e8f-ada5-5ebd76e129c1",
			}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
			}
			if got := w.GetReservedInsNodeDis(tt.args.funcSvcID,
				tt.args.nodeList, tt.args.traceID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetReservedInsNodeDis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_functionCache_resetScalingNum(t *testing.T) {
	type fields struct {
		scalingNum         int64
		scalingOnDemandNum int64
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				scalingNum:         2,
				scalingOnDemandNum: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := &functionCache{
				scalingNum:         tt.fields.scalingNum,
				scalingOnDemandNum: tt.fields.scalingOnDemandNum,
			}
			fc.resetScalingNum()
		})
	}
}

func Test_functionCache_decreaseScalingNum(t *testing.T) {
	type fields struct {
		scalingNum         int64
		scalingOnDemandNum int64
	}
	type args struct {
		isReversed bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				scalingNum:         2,
				scalingOnDemandNum: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := &functionCache{
				scalingNum:         tt.fields.scalingNum,
				scalingOnDemandNum: tt.fields.scalingOnDemandNum,
			}
			fc.decreaseScalingNum(tt.args.isReversed)
		})
	}
}

func Test_functionCache_addInstance(t *testing.T) {
	type fields struct {
		instances map[string]v1alpha1.WorkerInstance
	}
	type args struct {
		podName  string
		instance faasv1alpha1.WorkerInstance
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
			args: args{
				podName:  "test_pod",
				instance: faasv1alpha1.WorkerInstance{},
			},
		},
		{
			name: "case_02 migrated scenario",
			fields: fields{
				instances: map[string]v1alpha1.WorkerInstance{"1": {PodName: "test_pod", MigratedFrom: "aaa"}},
			},
			args: args{
				podName:  "test_pod",
				instance: faasv1alpha1.WorkerInstance{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := &functionCache{
				instances: tt.fields.instances,
			}
			fc.addInstance(tt.args.podName, tt.args.instance)
		})
	}
}

func Test_functionCache_deleteInstance(t *testing.T) {
	type fields struct {
		instances map[string]v1alpha1.WorkerInstance
	}
	type args struct {
		podName    string
		isReversed bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
			args: args{
				podName:    "test_pod",
				isReversed: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := &functionCache{
				instances: tt.fields.instances,
			}
			fc.instances["test_pod"] = v1alpha1.WorkerInstance{}
			fc.deleteInstance(tt.args.podName, tt.args.isReversed)
		})
	}
}

func Test_functionCache_deleteInstanceAndUpdateTime(t *testing.T) {
	convey.Convey("Test delete instance and update minScaleUpdatedTime", t, func() {
		fc := &functionCache{
			instances: make(map[string]v1alpha1.WorkerInstance, 1),
			minScale:  1,
		}
		fc.instances["test_pod"] = v1alpha1.WorkerInstance{}
		fc.deleteInstance("test_pod", true)
		convey.So(fc.minScaleUpdatedTime, convey.ShouldNotEqual, time.Time{})
	})
}

func Test_functionCache_resetInstance(t *testing.T) {
	type fields struct {
		instances map[string]v1alpha1.WorkerInstance
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := &functionCache{
				instances: tt.fields.instances,
			}
			fc.resetInstance()
		})
	}
}

func Test_UpdatePrewarmInstancesIfNotExist(t *testing.T) {
	funcServiceID := "mockFuncSvcID1"
	WorkerCache.createFunctionCache(funcServiceID)
	WorkerCache.UpdatePrewarmInstancesIfNotExist(funcServiceID, &corev1.Pod{})
	pod := WorkerCache.GetPodFromPrewarmPool(funcServiceID)
	assert.Equal(t, pod != nil, true)
}

func Test_workerCache_GetProfile(t *testing.T) {
	functions := sync.Map{}
	functions.Store("test_funcSvcID", &functionCache{profile: types.ProfileMetaData{}})
	type fields struct {
		functions sync.Map
	}
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   types.ProfileMetaData
	}{
		{
			name:   "test01 normal scenario",
			fields: fields{functions: functions},
			args:   args{funcSvcID: "test_funcSvcID"},
			want:   types.ProfileMetaData{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
			}
			if got := w.GetProfile(tt.args.funcSvcID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetProfile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_workerCache_GetWorkerInsOrPreWarmPod(t *testing.T) {
	type fields struct {
		functions sync.Map
	}
	type args struct {
		funcSvcID string
		podName   string
	}
	hasWorkerInsFunctions := sync.Map{}
	hasWorkerInsFunctions.Store("mockFuncSvcID", &functionCache{
		instances: map[string]v1alpha1.WorkerInstance{
			"mockPodName": {
				PodName: "mockPodName",
			},
		},
	})
	hasPreWarmPodFunctions := sync.Map{}
	hasPreWarmPodFunctions.Store("mockFuncSvcID", &functionCache{
		prewarmPool: []*corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mockPodName",
				},
			},
		},
	})
	noInstanceFunctions := sync.Map{}
	noInstanceFunctions.Store("mockFuncSvcID", &functionCache{})
	tests := []struct {
		name   string
		fields fields
		args   args
		want1  bool
	}{
		{
			name: "test with get worker instance",
			fields: fields{
				functions: hasWorkerInsFunctions,
			},
			args: args{
				funcSvcID: "mockFuncSvcID",
				podName:   "mockPodName",
			},
			want1: true,
		}, // test with get worker instance
		{
			name: "test with get pre warm pod",
			fields: fields{
				functions: hasPreWarmPodFunctions,
			},
			args: args{
				funcSvcID: "mockFuncSvcID",
				podName:   "mockPodName",
			},
			want1: true,
		}, // test with get pre warm pod
		{
			name: "test with get pre warm pod",
			fields: fields{
				functions: noInstanceFunctions,
			},
			args: args{
				funcSvcID: "mockFuncSvcID",
				podName:   "mockPodName",
			},
			want1: false,
		}, // test with no instance
		{
			name: "test with get pre warm pod",
			fields: fields{
				functions: sync.Map{},
			},
			args: args{
				funcSvcID: "mockFuncSvcID",
				podName:   "mockPodName",
			},
			want1: false,
		}, // test with no function cache
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
			}
			_, got1 := w.GetWorkerInsOrPrewarmPod(tt.args.funcSvcID, tt.args.podName)
			assert.Equalf(t, tt.want1, got1, "GetWorkerInsOrPrewarmPod(%v, %v)", tt.args.funcSvcID, tt.args.podName)
		})
	}
}

func Test_checkPodTag(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	workerMap[utils.WorkerContainerName] = "aaa"
	workerMap[utils.RuntimeContainerName] = "bbb"
	initMap[utils.WorkerInitContainerName] = "ccc"
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "case1_pass",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbb",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "ccc",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "case2_wrong_init_image",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbb",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "cccx",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "case3_wrong_length",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "ccc",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "case4_wrong_worker_name",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaax",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbb",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "ccc",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "case4_wrong_runtime_name",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbbx",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "ccc",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "case5_wrong_runtime_name",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbbx",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "ccc",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "case5_wrong_runtime_name",
			args: args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbbx",
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  utils.WorkerInitContainerName,
								Image: "ccc",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "case6_custom_pass",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "custom-aa",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaa",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbbx",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "case7_custom_fail",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "custom-aa",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  utils.WorkerContainerName,
								Image: "aaax",
							},
							{
								Name:  utils.RuntimeContainerName,
								Image: "bbbxx",
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkPodImageTag(tt.args.pod); (err != nil) != tt.wantErr {
				t.Errorf("checkPodImageTag() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFunctionCache_ProcessUpdateDomainConfig(t *testing.T) {
	convey.Convey("TestFunctionCache_ProcessUpdateDomainConfig", t, func() {
		cache := &utils.DNSCache{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetUserAgencyByVpcConfig, func(vpcConfig faasfunction.Vpc) utils.UserAgency {
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					LocalAuth: &localauth.AuthConfig{},
				}, nil
			}),
			gomonkey.ApplyFunc(utils.GetDNSCache, func() *utils.DNSCache {
				return cache
			}),
			gomonkey.ApplyFunc(utils.GetIPsByDNS, func(omsvcAddress, ak, sk string,
				param utils.DNSProxyRequestParam) (map[string][]string, error) {
				info := param.Info
				if info.DomainName == "www.test2.com" {
					return map[string][]string{"www.test2.com": {"192.168.0.1"}}, nil
				}
				if info.DomainName == "www.test3.com" {
					return map[string][]string{"www.test3.com": {"192.168.0.2", "192.168.0.3"}}, nil
				}
				if info.DomainName == "www.test.com" {
					return map[string][]string{"www.test.com": {"192.168.0.4"}}, nil
				}
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fc := &functionCache{
			funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						EndpointTenantVpc: faasfunction.Vpc{},
					},
				},
			},
			endpointTenantVpcID: "mockEndpointVpcID01",
			vpcID:               "mockVpcID01",
			dnsDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "111",
					DomainName: "www.test.com",
				},
			},
			endpointDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "333",
					DomainName: "www.test2.com",
				},
				{
					ID:         "444",
					DomainName: "www.test3.com",
				},
			},
		}
		fc.funcMeta.ID = "mockFuncSvcID10"
		dnsDomainMap := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-001.com":  {"192.168.100.1"},
		}
		cache.StoreDNSCache(
			"mockFuncSvcID10", "mockVpcID01", "www.test.com", dnsDomainMap)
		domainNames, err := cache.GetDNSDomainNames("mockFuncSvcID10")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 2)
		fc.ProcessUpdateDomainConfig("mockFuncSvcID10")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID10")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 4)

		fc.vpcID = "mockVpcID03"
		fc.endpointTenantVpcID = ""
		fc.ProcessUpdateDomainConfig("mockFuncSvcID10")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID10")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 1)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 1)

		fc.vpcID = ""
		fc.dnsDomainCfg = nil
		fc.ProcessUpdateDomainConfig("mockFuncSvcID10")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID10")
		convey.So(err, convey.ShouldEqual, utils.ErrDomainInfoInvalid)
		convey.So(len(domainNames), convey.ShouldEqual, 0)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 0)
	})
}

func TestFunctionCache_ProcessDeleteDomainConfig(t *testing.T) {
	convey.Convey("TestFunctionCache_ProcessDeleteDomainConfig", t, func() {
		cache := &utils.DNSCache{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) utils.UserAgency {
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetUserAgencyByVpcConfig, func(vpcConfig faasfunction.Vpc) utils.UserAgency {
				return utils.UserAgency{Token: "token"}
			}),
			gomonkey.ApplyFunc(utils.GetDNSCache, func() *utils.DNSCache {
				return cache
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					LocalAuth: &localauth.AuthConfig{},
				}, nil
			}),
			gomonkey.ApplyFunc(utils.GetIPsByDNS, func(omsvcAddress, ak, sk string,
				param utils.DNSProxyRequestParam) (map[string][]string, error) {
				info := param.Info
				if info.DomainName == "www.test2.com" {
					return map[string][]string{"www.test2.com": {"192.168.0.1"}}, nil
				}
				if info.DomainName == "www.test3.com" {
					return map[string][]string{"www.test3.com": {"192.168.0.2", "192.168.0.3"}}, nil
				}
				if info.DomainName == "www.test4.com" {
					return map[string][]string{"www.test4.com": {"192.168.0.2", "192.168.0.3"}}, nil
				}
				if info.DomainName == "www.test.com" {
					return map[string][]string{"www.test.com": {"192.168.0.4"}}, nil
				}
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fc := &functionCache{
			funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						EndpointTenantVpc: faasfunction.Vpc{},
					},
				},
			},
			endpointTenantVpcID: "mockEndpointVpcID01",
			vpcID:               "mockVpcID01",
			dnsDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "111",
					DomainName: "www.test.com",
				},
			},
			endpointDomainCfg: []types.DNSDomainInfo{
				{
					ID:         "333",
					DomainName: "www.test2.com",
				},
				{
					ID:         "444",
					DomainName: "www.test3.com",
				},
			},
		}
		fc.funcMeta.ID = "mockFuncSvcID11"
		domainNames, err := cache.GetDNSDomainNames("mockFuncSvcID11")
		convey.So(err, convey.ShouldEqual, utils.ErrDomainInfoInvalid)
		convey.So(len(domainNames), convey.ShouldEqual, 0)
		fc.ProcessUpdateDomainConfig("mockFuncSvcID11")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID11")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 3)

		fc.ProcessUpdateDomainConfig("mockFuncSvcID11")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID11")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 3)

		fc.vpcID = ""
		fc.dnsDomainCfg = nil
		fc.funcMeta.ID = "mockFuncSvcID12"
		fc.ProcessUpdateDomainConfig("mockFuncSvcID12")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID12")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 2)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 3)

		fc.funcMeta.ID = "mockFuncSvcID13"
		fc.endpointTenantVpcID = ""
		fc.endpointDomainCfg = nil
		fc.ProcessUpdateDomainConfig("mockFuncSvcID13")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID13")
		convey.So(err, convey.ShouldEqual, utils.ErrDomainInfoInvalid)
		convey.So(len(domainNames), convey.ShouldEqual, 0)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 3)

		fc.funcMeta.ID = "mockFuncSvcID14"
		fc.endpointTenantVpcID = "mockEndpointVpcID02"
		fc.endpointDomainCfg = []types.DNSDomainInfo{
			{
				ID:         "666",
				DomainName: "www.test4.com",
			},
		}
		fc.ProcessUpdateDomainConfig("mockFuncSvcID14")
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID14")
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(domainNames), convey.ShouldEqual, 1)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 4)

		processDeleteDomainConfig("mockFuncSvcID14", "mockEndpointVpcID02", fc.endpointDomainCfg)
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID14")
		convey.So(err, convey.ShouldEqual, utils.ErrDomainInfoInvalid)
		convey.So(len(domainNames), convey.ShouldEqual, 0)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 3)

		processDeleteDomainConfig("mockFuncSvcID12", "mockEndpointVpcID02", fc.endpointDomainCfg)
		domainNames, err = cache.GetDNSDomainNames("mockFuncSvcID12")
		convey.So(err, convey.ShouldEqual, utils.ErrDomainInfoInvalid)
		convey.So(len(domainNames), convey.ShouldEqual, 0)
		convey.So(len(cache.GetDNSDomainKeyMap()), convey.ShouldEqual, 3)
	})
}

// TestWorkerCache_GetFunctionPreStopHandler -
func TestWorkerCache_GetFunctionPreStopHandler(t *testing.T) {
	convey.Convey("TestWorkerCache_GetFunctionPreStopHandler", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("funcSvcNameIsNotExist", func() {
			handler := WorkerCache.GetFunctionPreStopHandler("test1")
			convey.So(handler, convey.ShouldEqual, "")
		})
		convey.Convey("funcSvcIsNotExist", func() {
			WorkerCache.functions.Store("test1", nil)
			handler := WorkerCache.GetFunctionPreStopHandler("test1")
			convey.So(handler, convey.ShouldEqual, "")
		})
		convey.Convey("GetFunctionPreStopHandlerOK", func() {
			WorkerCache.functions.Store("test2", &functionCache{funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						PreStop: types.PreStop{
							Handler: "test",
							Timeout: 1,
						},
					},
				},
			}})
			handler := WorkerCache.GetFunctionPreStopHandler("test2")
			convey.So(handler, convey.ShouldEqual, "test")
		})
	})
}

func Test_functionCache_getAvailableCluster(t *testing.T) {
	clusterstatusutil.ClusterStatusCache.ClusterStatusMap = make(map[string]clusterstatusutil.ClusterFlowState)
	patch := gomonkey.ApplyFunc(utils.GetFuncClusterByFuncMeta, func(funcMeta *faasfunction.FunctionService) []string {
		return []string{"az1"}
	})
	defer patch.Reset()
	fc := &functionCache{funcMeta: &faasfunction.FunctionService{}}
	fc.funcMeta.Spec.Configuration.FunctionVersionURN = "test1"
	clusterstatusutil.ClusterStatusCache.ClusterStatusMap["az1"] = clusterstatusutil.ClusterFlowState{State: "running"}
	fc.updateFunctionClusters(clusterstatusutil.ClusterStatusCache.ClusterStatusMap)
	assert.Equal(t, fc.availableCluster, []string{"az1"})

}

func Test_updateClusterMaxScale(t *testing.T) {
	clusterID = "az1"
	patch := gomonkey.ApplyFunc(utils.GetUpdateFunctionCacheEnable, func() bool {
		return true
	})
	defer patch.Reset()
	clusterstatusutil.ClusterStatusCache.DefaultClusterAmount = 1
	fc := &functionCache{funcMeta: &faasfunction.FunctionService{}}
	fc.updateClusterMaxScale()
	fc.originalMaxScale = 1000
	fc.availableCluster = []string{"az1", "az2", "az3"}
	fc.updateClusterMaxScale()
	assert.Equal(t, fc.maxScale, int64(334))
	fc.originalMaxScale = 999
	fc.availableCluster = []string{"az1", "az2", "az3"}
	fc.updateClusterMaxScale()
	assert.Equal(t, fc.maxScale, int64(333))
	fc.originalMaxScale = 5
	fc.availableCluster = []string{"az1", "az2"}
	fc.updateClusterMaxScale()
	assert.Equal(t, fc.maxScale, int64(3))
}

func TestInitClusterStatusCache(t *testing.T) {
	type args struct{}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		want        int
	}{
		{
			name: "case 1 Client is nil",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
							{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
						}}, nil
					}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
			want: 0,
		},
		{
			name: "case 2 GetResponse failed",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return nil, errors.New("error")
					}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			want: 0,
		},
		{
			name: "case 3 unmarshal failed",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
							{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
						}}, nil
					}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("test")
					}),
				})
				return patches
			},
			want: 0,
		},
		{
			name: "case 4",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
							{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
						}}, nil
					}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterstatusutil.ClusterStatusCache.DefaultClusterAmount = 0
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			clusterstatusutil.InitClusterStatusCache()
			assert.NotNil(t, clusterstatusutil.ClusterStatusCache)
			assert.Equal(t, tt.want, clusterstatusutil.ClusterStatusCache.DefaultClusterAmount)
		})
	}
}

func TestRemoveStateIDFromFuncCache(t *testing.T) {
	wc := workerCache{
		functions: sync.Map{},
	}
	wc.RemoveStateIDFromFuncCache("test", "stateID")
	wc.functions.Store("test", &functionCache{})
	wc.CheckAndInsertStateIDMaxInstance("test", "stateID")
	wc.RemoveStateIDFromFuncCache("test", "stateID")
}

func TestRemovePrewarmInstances(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
		opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
		}}, nil
	})
	patch = gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{}}
	})
	defer patch.Reset()
	type args struct {
		funcSvcID string
		podName   string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case 1",
			args: args{
				funcSvcID: "test1",
				podName:   "state1",
			},
		},
	}
	wc := workerCache{
		functions: sync.Map{},
	}
	var pod []*corev1.Pod
	pod = append(pod, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "state1"}})
	wc.functions.Store("test1", &functionCache{
		prewarmPool: pod,
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !wc.RemovePrewarmInstances(tt.args.funcSvcID, tt.args.podName) {
				t.Errorf("this should be not ok")
			}
		})
	}
}

func TestCheckAndInsertStateIDMaxInstance(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
		opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
		}}, nil
	})
	patch = gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{}}
	})
	defer patch.Reset()
	type args struct {
		funcSvcID string
		stateID   string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case 1",
			args: args{
				funcSvcID: "test1",
				stateID:   "state1",
			},
		},
	}
	wc := workerCache{}
	wc.functions.Store("test1", &functionCache{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !wc.CheckAndInsertStateIDMaxInstance(tt.args.funcSvcID, tt.args.stateID) {
				t.Errorf("this should ok")
			}
			fc, ok := wc.getFunctionCache(tt.args.funcSvcID)
			if !ok {
				t.Errorf("this should ok")
			}
			r, ok := fc.stateMap.Load("state1")
			if !ok {
				t.Errorf("this should ok")
			}
			if r != 1 {
				t.Errorf("this should be 1")
			}
		})
	}
}

func TestResetStateIDMap(t *testing.T) {
	convey.Convey("ResetStateIDMap, reset success", t, func() {
		wc := workerCache{}
		wc.functions.Store("test1", &functionCache{})
		wc.ResetStateIDMap("test1", "ttt")
	})
}

func TestInsertStateIDMap(t *testing.T) {
	convey.Convey("insertStateIDMap, load stateMap err", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load", func(_ *sync.Map, key any) (value any, ok bool) {
			return "", false
		})
		defer patch.Reset()
		fc := &functionCache{}
		if !fc.insertStateIDMap("test") {
			t.Errorf("this should ne true")
		}
	})

	convey.Convey("insertStateIDMap, stateMap val err", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", "abc")
		if fc.insertStateIDMap("test") {
			t.Errorf("this should ne false")
		}
	})

	convey.Convey("insertStateIDMap, stateMap val 0", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", 0)
		if !fc.insertStateIDMap("test") {
			t.Errorf("this should ne false")
		}
	})

	convey.Convey("insertStateIDMap, stateMap val 1", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", 1)
		if fc.insertStateIDMap("test") {
			t.Errorf("this should ne false")
		}
	})
}

func TestResetStateIDMap2(t *testing.T) {
	convey.Convey("resetStateIDMap, load stateMap err", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load", func(_ *sync.Map, key any) (value any, ok bool) {
			return "", false
		})
		defer patch.Reset()
		fc := &functionCache{}
		fc.stateMap.Store("testtest", "123123")
		fc.resetStateIDMap("test")
		if !fc.insertStateIDMap("test") {
			t.Errorf("this should ne true")
		}
	})

	convey.Convey("resetStateIDMap, reset success", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("testtest", "123123")
		fc.resetStateIDMap("testtest")
		val, _ := fc.stateMap.Load("testtest")
		if val != 0 {
			t.Errorf("this should be 0")
		}
	})
}

func TestCheckStateIDKeyExist(t *testing.T) {
	convey.Convey("checkStateIDKeyExist, false", t, func() {
		fc := &functionCache{}
		if fc.checkStateIDKeyExist("test") {
			t.Errorf("this should be false")
		}
	})

	convey.Convey("checkStateIDKeyExist, true", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", "111")
		if !fc.checkStateIDKeyExist("test") {
			t.Errorf("this should be true")
		}
	})

}

func TestCheckStateIDMaxInstance(t *testing.T) {
	convey.Convey("checkStateIDMaxInstance, Load false", t, func() {
		fc := &functionCache{}
		if fc.checkStateIDMaxInstance("test") {
			t.Errorf("this should be false")
		}
	})

	convey.Convey("checkStateIDMaxInstance, not int false", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", "asd")
		if fc.checkStateIDMaxInstance("test") {
			t.Errorf("this should be false")
		}
	})

	convey.Convey("checkStateIDMaxInstance, intVal 0", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", 0)
		if fc.checkStateIDMaxInstance("test") {
			t.Errorf("this should be false")
		}
	})

	convey.Convey("checkStateIDMaxInstance, intVal 0", t, func() {
		fc := &functionCache{}
		fc.stateMap.Store("test", 1)
		if !fc.checkStateIDMaxInstance("test") {
			t.Errorf("this should be true")
		}
	})
}

func TestSortReserved(t *testing.T) {
	convey.Convey("SortReserved", t, func() {
		nodeCache.NodeList = map[string]*NodeInfo{
			"NodeA": {Label: map[string]string{"node-type": "fixed"}},
			"NodeB": {Label: map[string]string{"node-type": "elastic"}},
			"NodeC": {Label: map[string]string{"node-type": "XX"}},
		}
		fc := &functionCache{
			funcMeta: &faasfunction.FunctionService{},
		}
		fc.funcMeta.Spec.Configuration.BusinessType = "CAE"
		reserved := []v1alpha1.WorkerInstance{
			{PodName: "Pod1", NodeName: "NodeA"},
			{PodName: "Pod2", NodeName: "NodeB"},
			{PodName: "Pod3", NodeName: "NodeC"},
			{PodName: "Pod4", NodeName: "NodeA"},
			{PodName: "Pod5", NodeName: "NodeB"},
			{PodName: "Pod6", NodeName: "NodeC"},
			{PodName: "Pod7", NodeName: "NodeD"},
		}
		sortedReserved := fc.sortWorkerInstance(reserved)
		convey.So("Pod2", convey.ShouldEqual, sortedReserved[0].PodName)
		convey.So("Pod5", convey.ShouldEqual, sortedReserved[1].PodName)
		convey.So("Pod3", convey.ShouldEqual, sortedReserved[2].PodName)
		convey.So("Pod6", convey.ShouldEqual, sortedReserved[3].PodName)
		convey.So("Pod7", convey.ShouldEqual, sortedReserved[4].PodName)
		convey.So("Pod1", convey.ShouldEqual, sortedReserved[5].PodName)
		convey.So("Pod4", convey.ShouldEqual, sortedReserved[6].PodName)
	})
	convey.Convey("SortReserved， meta nil", t, func() {
		fc := &functionCache{
			funcMeta: nil,
		}
		reserved := []v1alpha1.WorkerInstance{
			{PodName: "Pod1", NodeName: "NodeA"},
			{PodName: "Pod2", NodeName: "NodeB"},
			{PodName: "Pod3", NodeName: "NodeC"},
		}
		sortedReserved := fc.sortWorkerInstance(reserved)
		convey.So("Pod1", convey.ShouldEqual, sortedReserved[0].PodName)
		convey.So("Pod2", convey.ShouldEqual, sortedReserved[1].PodName)
		convey.So("Pod3", convey.ShouldEqual, sortedReserved[2].PodName)
	})
}

func TestPickWorkInstanceToBeReduced(t *testing.T) {
	convey.Convey("PickWorkInstanceToBeReduced, pick elastic", t, func() {
		nodeCache.NodeList = map[string]*NodeInfo{
			"NodeA": {Label: map[string]string{"node-type": "fixed"}},
			"NodeB": {Label: map[string]string{}},
			"NodeC": {Label: map[string]string{"node-type": "elastic"}},
		}
		fc := &functionCache{
			instances: map[string]v1alpha1.WorkerInstance{
				"Ins1": {Applier: "ASBRes", NodeName: "NodeA"},
				"Ins2": {Applier: "ASBRes", NodeName: "NodeB"},
				"Ins3": {Applier: "ASBRes", NodeName: "NodeC"},
			},
		}
		id := pickWorkInstanceToBeReduced(fc)
		convey.So(id, convey.ShouldEqual, "Ins3")
	})
	convey.Convey("PickWorkInstanceToBeReduced, pick empty", t, func() {
		nodeCache.NodeList = map[string]*NodeInfo{
			"NodeA": {Label: map[string]string{"node-type": "fixed"}},
			"NodeB": {Label: map[string]string{}},
		}
		fc := &functionCache{
			instances: map[string]v1alpha1.WorkerInstance{
				"Ins1": {Applier: "ASBRes", NodeName: "NodeA"},
				"Ins2": {Applier: "ASBRes", NodeName: "NodeB"},
			},
		}
		id := pickWorkInstanceToBeReduced(fc)
		convey.So(id, convey.ShouldEqual, "Ins2")
	})
	convey.Convey("PickWorkInstanceToBeReduced, pick none cache", t, func() {
		nodeCache.NodeList = map[string]*NodeInfo{
			"NodeA": {Label: map[string]string{"node-type": "fixed"}},
		}
		fc := &functionCache{
			instances: map[string]v1alpha1.WorkerInstance{
				"Ins1": {Applier: "ASBRes", NodeName: "NodeA"},
				"Ins2": {Applier: "ASBRes", NodeName: "NodeB"},
			},
		}
		id := pickWorkInstanceToBeReduced(fc)
		convey.So(id, convey.ShouldEqual, "Ins2")
	})
	convey.Convey("PickWorkInstanceToBeReduced, pick fixed", t, func() {
		nodeCache.NodeList = map[string]*NodeInfo{
			"NodeA": {Label: map[string]string{"node-type": "fixed"}},
		}
		fc := &functionCache{
			instances: map[string]v1alpha1.WorkerInstance{
				"Ins1": {Applier: "ASBRes", NodeName: "NodeA"},
			},
		}
		id := pickWorkInstanceToBeReduced(fc)
		convey.So(id, convey.ShouldEqual, "Ins1")
	})
}

func TestUpdateFuncCache(t *testing.T) {
	convey.Convey("UpdateFuncCache, etcd error", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService, types.ProfileMetaData,
				etcd3.EtcdType, error) {
				return nil, types.ProfileMetaData{}, 0, errors.New("error")
			}),
		})
		defer patches.ResetAll()
		UpdateFuncCache("funcName", "businessID", "tenantID", "version")
	})

}

func TestUpdateFuncCache2(t *testing.T) {
	convey.Convey("UpdateFuncCache, CheckEventSource false", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService, types.ProfileMetaData,
				etcd3.EtcdType, error) {
				return nil, types.ProfileMetaData{}, 0, nil
			}),
			gomonkey.ApplyFunc(CheckEventSource, func(event eventsource.Event, funcSvcID string) bool {
				return false
			}),
			gomonkey.ApplyFunc(loadOrStoreFuncCache, func(funcSvcID string, funcMeta *faasfunction.FunctionService,
				profileMeta types.ProfileMetaData) *functionCache {
				return nil
			}),
		})
		defer patches.ResetAll()
		UpdateFuncCache("funcName", "businessID", "tenantID", "version")
	})
}

func TestUpdateFuncCache3(t *testing.T) {
	convey.Convey("UpdateFuncCache, CheckEventSource true", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService, types.ProfileMetaData,
				etcd3.EtcdType, error) {
				return nil, types.ProfileMetaData{}, 0, nil
			}),
			gomonkey.ApplyFunc(CheckEventSource, func(event eventsource.Event, funcSvcID string) bool {
				return true
			}),
			gomonkey.ApplyFunc(loadOrStoreFuncCache, func(funcSvcID string, funcMeta *faasfunction.FunctionService,
				profileMeta types.ProfileMetaData) *functionCache {
				return nil
			}),
		})
		defer patches.ResetAll()
		UpdateFuncCache("funcName", "businessID", "tenantID", "version")
	})
}

func Test_workerCache_SetAndGetFuncExpectedNum(t *testing.T) {
	convey.Convey("Test SetAndGetFuncExpectedNum", t, func() {
		cache := &workerCache{
			functions: sync.Map{},
		}
		cache.functions.Store("mockFuncSvcID", &functionCache{expectedNum: 1})
		cache.SetFuncExpectedNum("mockFuncSvcID", 5)
		expectNum, err := cache.GetFuncExpectedNum("mockFuncSvcID")
		convey.So(err, convey.ShouldBeNil)
		convey.So(expectNum, convey.ShouldEqual, 5)

		cache.SetFuncExpectedNum("not_exist_funcSvcID", 5)
		expectNum, err = cache.GetFuncExpectedNum("not_exist_funcSvcID")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(expectNum, convey.ShouldEqual, 0)
	})
}

func Test_checkStateFulEventCheckRepeat_cache(t *testing.T) {
	wc := &workerCache{}
	wc.functions.Store("function1", &functionCache{})
	wc.InsertHashKeyInMap("function1", "podName", "stateID")
	fc, _ := wc.getFunctionCache("function1")
	_, ok := fc.stateFulEventCheckRepeat.Load("")
	if ok {
		t.Error("this should be not ok")
	}
	wc.InsertHashKeyInMap("function2", "podName", "stateID")

	wc.InsertHashKeyInMap("function1", "podName", "stateID")
	fc, _ = wc.getFunctionCache("function1")
	if !fc.CheckHashKeyInMap("stateID", "podName") {
		t.Error("this should ok")
	}
	fc.delHashKeyInMap("stateID")
	if fc.CheckHashKeyInMap("stateID", "podName") {
		t.Error("this should not ok")
	}
	fc.stateFulEventCheckRepeat.Store("stateID", 123123)
	if fc.CheckHashKeyInMap("stateID", "podName") {
		t.Error("this should not ok")
	}
	fc.delHashKeyInMap("stateID")
	fc.stateFulEventCheckRepeat.Store("stateID", "podName2")
	if fc.CheckHashKeyInMap("stateID", "podName") {
		t.Error("this should not ok")
	}
	fc.delHashKeyInMap("stateID")
}

func Test_workerCache_ReservedInstanceUpdateMultiTimes(t *testing.T) {
	w := &workerCache{}
	w.HandleFunctionRepo("test", &FunctionMeta{
		EventType: eventsource.Updated,
		FuncMeta:  &faasfunction.FunctionService{},
	})
	w.HandleInsMetadata("test", config.InstanceMetaData{
		MinInstance: 2, MaxInstance: 10, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})

	fc, _ := w.getFunctionCache("test")
	assert.Equal(t, int64(2), fc.scalingReversed)
	assert.Equal(t, 0, len(fc.scalingDone))

	// assert scalingReversed only decrease once for multi update events
	for i := 0; i < 10; i++ {
		w.HandleInstance("test", &InstanceMeta{
			Instance: faasv1alpha1.WorkerInstance{
				PodName: "pool22-300-128-funsion-test",
				Applier: constants.WorkerManagerApplier,
			},
			EventType: eventsource.Updated,
		})

		assert.Equal(t, int64(1), fc.scalingReversed)
		assert.Equal(t, 1, len(fc.scalingDone))
	}

	// try to delete multi times
	for i := 0; i < 10; i++ {
		w.HandleInstance("test", &InstanceMeta{
			Instance: faasv1alpha1.WorkerInstance{
				PodName: "pool22-300-128-funsion-test",
				Applier: constants.WorkerManagerApplier,
			},
			EventType: eventsource.Deleted,
		})

		assert.Equal(t, int64(2), fc.scalingReversed)
		assert.Equal(t, 0, len(fc.scalingDone))
	}
}

func TestGetWorkerInstanceMap(t *testing.T) {
	convey.Convey("TestGetWorkerInstanceMap", t, func() {
		workerInstances := make(map[string]v1alpha1.WorkerInstance)
		workerInstances["/sn/workers/business/yrk/tenant/7e1ad6a6cc5c44fabd/function/0@debugservice@helloworld/"+
			"version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888a"] =
			v1alpha1.WorkerInstance{
				InstanceID:   "defaultaz-#-pool46-600-512-fusion-7f8b95888a",
				IP:           "10.244.1.9",
				OwnerIP:      "10.28.88.224",
				Port:         "8080",
				BusinessType: "CAE",
				BusinessID:   "yrk",
				TenantID:     "7e1ad6a6cc5c44fabd",
				FuncName:     "0@debugservice@helloworld",
				Version:      "latest",
			}
		workerInstances["/sn/workers/business/yrk/tenant/7e1ad6a6cc5c44fabd/function/0@debugservice@helloworld/"+
			"version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888b"] =
			v1alpha1.WorkerInstance{
				InstanceID:   "defaultaz-#-pool46-600-512-fusion-7f8b95888b",
				IP:           "10.244.0.9",
				OwnerIP:      "10.28.88.225",
				Port:         "8080",
				BusinessType: "CAE",
				BusinessID:   "yrk",
				TenantID:     "7e1ad6a6cc5c44fabd",
				FuncName:     "0@debugservice@helloworld",
				Version:      "latest",
			}
		cache := &workerCache{}
		cache.functions.Store("funcSvcID", &functionCache{instances: workerInstances})
		workerInstanceGet := cache.GetWorkerInstanceMap()
		convey.So(len(workerInstanceGet), convey.ShouldEqual, 2)
		convey.So(workerInstanceGet["/sn/workers/business/yrk/tenant/7e1ad6a6cc5c44fabd/function"+
			"/0@debugservice@helloworld/version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888b"].OwnerIP,
			convey.ShouldEqual, "10.28.88.225")
		convey.So(workerInstanceGet["/sn/workers/business/yrk/tenant/7e1ad6a6cc5c44fabd/function"+
			"/0@debugservice@helloworld/version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888a"].OwnerIP,
			convey.ShouldEqual, "10.28.88.224")
	})
}

func Test_RouterEtcdRestartCallback(t *testing.T) {
	convey.Convey("Test_RouterEtcdRestartCallback", t, func() {
		convey.Convey("current worker-manager is not cluster leader", func() {
			etcdClient := &etcd3.EtcdClient{}
			routerEtcdRestartCallback(etcdClient)
		})
		convey.Convey("update route info to etcd", func() {
			HasSynced = true
			etcdClient := &etcd3.EtcdClient{}
			etcdMap := make(map[string]string)
			wc := &workerCache{}
			etcdMap["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/version/latest/"+
				"defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toKeep"] = "{ \"ip\": \"10.244.0.9\", \"" +
				"port\": \"8080\", \"businessType\": \"CAE\" }"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
					return etcdClient
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(wc), "GetWorkerInstanceMap",
					func(w *workerCache) map[string]*v1alpha1.WorkerInstance {
						workerInstances := make(map[string]*v1alpha1.WorkerInstance)
						workerInstances["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/"+
							"version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toKeep"] =
							&v1alpha1.WorkerInstance{
								IP:           "10.244.0.9",
								Port:         "8080",
								BusinessType: "CAE",
							}
						workerInstances["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/"+
							"version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toPut"] =
							&v1alpha1.WorkerInstance{
								IP:           "10.244.0.9",
								Port:         "8080",
								BusinessType: "CAE",
							}
						return workerInstances
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse", func(w *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					kvs := make([]*mvccpb.KeyValue, 0)
					for k, v := range etcdMap {
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
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "Txn", func(w *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, ops ...clientv3.Op) error {
					for _, op := range ops {
						if op.IsPut() {
							etcdMap[string(op.KeyBytes())] = string(op.ValueBytes())
						}
						if op.IsDelete() {
							delete(etcdMap, string(op.KeyBytes()))
						}
					}
					return nil
				}),
			}
			routerEtcdRestartCallback(etcdClient)
			convey.So(checkDataConsistence(etcdMap, WorkerCache.GetWorkerInstanceMap()), convey.ShouldBeTrue)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("delete route info from etcd", func() {
			HasSynced = true
			etcdClient := &etcd3.EtcdClient{}
			etcdMap := make(map[string]string)
			wc := &workerCache{}
			etcdMap["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/version/latest/"+
				"defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toKeep"] = "{ \"ip\": \"10.244.0.9\", " +
				"\"port\": \"8080\", \"businessType\": \"CAE\" }"
			etcdMap["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/version/latest/"+
				"defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toDelete"] = "{ \"ip\": \"10.244.0.9\", " +
				"\"port\": \"8080\", \"businessType\": \"CAE\" }"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
					return etcdClient
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(wc), "GetWorkerInstanceMap",
					func(w *workerCache) map[string]*v1alpha1.WorkerInstance {
						workerInstances := make(map[string]*v1alpha1.WorkerInstance)
						workerInstances["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/"+
							"version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toKeep"] =
							&v1alpha1.WorkerInstance{
								IP:           "10.244.0.9",
								Port:         "8080",
								BusinessType: "CAE",
							}
						workerInstances["/sn/workers/business/yrk/tenant/580943/function/0@debugservice@helloworld/"+
							"version/latest/defaultaz/defaultaz-#-pool46-600-512-fusion-7f8b95888c-toPut"] =
							&v1alpha1.WorkerInstance{
								IP:           "10.244.0.9",
								Port:         "8080",
								BusinessType: "CAE",
							}
						return workerInstances
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse", func(w *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					kvs := make([]*mvccpb.KeyValue, 0)
					for k, v := range etcdMap {
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
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "Txn", func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ops ...clientv3.Op) error {
					for _, op := range ops {
						if op.IsPut() {
							etcdMap[string(op.KeyBytes())] = string(op.ValueBytes())
						}
						if op.IsDelete() {
							delete(etcdMap, string(op.KeyBytes()))
						}
					}
					return nil
				}),
			}
			routerEtcdRestartCallback(etcdClient)
			convey.So(checkDataConsistence(etcdMap, WorkerCache.GetWorkerInstanceMap()), convey.ShouldBeTrue)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func checkDataConsistence(etcdMap map[string]string, instanceMap map[string]*v1alpha1.WorkerInstance) bool {
	if len(etcdMap) != len(instanceMap) {
		return false
	}
	for k, v := range instanceMap {
		val := etcdMap[k]
		if val == "" {
			return false
		}
		jsonBytes, _ := json.Marshal(v)
		if string(jsonBytes) != val {
			return false
		}
	}
	return true
}

func Test_InitStateWorkersFromETCD(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"test getStateMetaDataFromEtcd error", true, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getStateMetaDataFromEtcd, func() error {
						return errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"test getStateMetaDataFromEtcd error", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getStateMetaDataFromEtcd, func() error {
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := InitStateWorkersFromETCD(); (err != nil) != tt.wantErr {
				t.Errorf("executeSnapshotModeProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_InitStatefulCache(t *testing.T) {
	convey.Convey("test state worker components has null string", t, func() {
		StateWorkersList = sync.Map{}
		StateWorkersList.LoadOrStore("stateID", 1)
		InitStatefulCache()
	})

	convey.Convey("test state worker components has null string", t, func() {
		StateWorkersList = sync.Map{}
		tt := StateMetadata{}
		StateWorkersList.LoadOrStore("stateID", tt)
		InitStatefulCache()
	})
	ttt := StateMetadata{StateID: "stateID",
		FuncName:   "funcName",
		TenantID:   "tenantID",
		BusinessID: "businessID",
		Version:    "version",
		ServiceID:  "serviceID",
		NodeID:     "nodeID",
		Creator:    "creator",
		Cluster:    "cluster"}
	convey.Convey("test failed to get functionCache", t, func() {
		StateWorkersList = sync.Map{}
		StateWorkersList.LoadOrStore("stateID", ttt)
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
				return nil, false
			}),
			gomonkey.ApplyFunc(getUpdatedFuncCache, func(_ StateMetadata) (*functionCache, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(utils.DeleteStateWorkersFromMetaETCD, func(stateID string) {}),
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "CLUSTER_ID" {
					return "cluster"
				}
				return ""
			}),
		})
		defer patches.ResetAll()
		InitStatefulCache()
	})
	convey.Convey("test not in current cluster", t, func() {
		StateWorkersList = sync.Map{}
		StateWorkersList.LoadOrStore("stateID", ttt)
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
				return &functionCache{}, true
			}),
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "CLUSTER_ID" {
					return "cluster"
				}
				return ""
			}),
		})
		defer patches.ResetAll()
		InitStatefulCache()
	})
	convey.Convey("test exist but no stateful instance", t, func() {
		StateWorkersList = sync.Map{}
		StateWorkersList.LoadOrStore("stateID", ttt)
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
				return &functionCache{}, true
			}),
			gomonkey.ApplyFunc((*workerCache).statefulFunctionReload, func(_ *workerCache, _ Metadata, _ *InstanceMeta) ([]Action, Metadata) {
				return nil, Metadata{}
			}),
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "CLUSTER_ID" {
					return "cluster"
				}
				return ""
			}),
		})
		defer patches.ResetAll()
		nodeCache.NodeList["nodeID"] = &NodeInfo{}
		InitStatefulCache()
	})
	convey.Convey("test exist but no stateful instance check all elements include", t, func() {
		StateWorkersList = sync.Map{}
		StateWorkersList.LoadOrStore("stateID", ttt)
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
				return &functionCache{}, true
			}),
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "CLUSTER_ID" {
					return "cluster"
				}
				return ""
			}),
			gomonkey.ApplyFunc((*workerCache).statefulFunctionReload, func(_ *workerCache, meta Metadata, _ *InstanceMeta) ([]Action, Metadata) {
				var actions []Action
				actions = append(actions, Action{Type: ActionStatefulInstanceRePull, NeedPull: 1})
				return actions, meta
			}),
		})
		defer patches.ResetAll()
		nodeCache.NodeList["nodeID"] = &NodeInfo{}
		poa := InitStatefulCache()
		if len(poa) == 0 {
			t.Errorf("this should not be 0")
		}
		if len(poa[0].StatefulAction) == 0 {
			t.Errorf("this is not 0")
		}
		if poa[0].StatefulAction[0].Type != ActionStatefulInstanceRePull {
			t.Errorf("this is stateful reload")
		}
		if poa[0].StatefulMeta.Creator != ttt.Creator {
			t.Errorf("val error")
		}
	})
}

func Test_getUpdatedFuncCache(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"test1 state etcd client is nil", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			"test2  state client.Client is nil", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"test3 connect to state etcd error", true, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"test4 invalid state workers key error", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						var te []*mvccpb.KeyValue
						te = append(te, &mvccpb.KeyValue{})
						return &clientv3.GetResponse{Kvs: te}, nil
					}),
					gomonkey.ApplyFunc(parseStateETCDKey, func(stateKey string) (string, error) {
						return "", errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"test5 invalid state workers error", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						var te []*mvccpb.KeyValue
						te = append(te, &mvccpb.KeyValue{})
						return &clientv3.GetResponse{Kvs: te}, nil
					}),
					gomonkey.ApplyFunc(parseStateETCDKey, func(stateKey string) (string, error) {
						return "", nil
					}),
					gomonkey.ApplyFunc((*StateMetadata).parseStateWorkerValue, func(_ *StateMetadata, stateVal []byte, stateID string) error {
						return errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"test6 all clear", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						var te []*mvccpb.KeyValue
						te = append(te, &mvccpb.KeyValue{})
						return &clientv3.GetResponse{Kvs: te}, nil
					}),
					gomonkey.ApplyFunc(parseStateETCDKey, func(stateKey string) (string, error) {
						return "", nil
					}),
					gomonkey.ApplyFunc((*StateMetadata).parseStateWorkerValue, func(_ *StateMetadata, stateVal []byte, stateID string) error {
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := getStateMetaDataFromEtcd(); (err != nil) != tt.wantErr {
				t.Errorf("executeSnapshotModeProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_parseStateETCDKey(t *testing.T) {
	_, err := parseStateETCDKey("/sn/stateworkers/")
	if err == nil {
		t.Errorf("this is error")
	}
	_, err = parseStateETCDKey("/sn/stateworkers/business/testbusiness/tenant/testtenant/function/function-task/version/$latest/defaultaz/")
	if err == nil {
		t.Errorf("this is error")
	}
	res, err := parseStateETCDKey("/sn/stateworkers/business/testbusiness/tenant/testtenant/function/function-task/version/$latest/defaultaz/test-stateID")
	if err != nil {
		t.Errorf("this is error")
	}
	if res != "test-stateID" {
		t.Errorf("not equal")
	}
}

func Test_parseStateWorkerValue(t *testing.T) {
	s := &StateMetadata{}
	testString1 := `{\"stateID\"`
	testBytes1 := []byte(testString1)
	if err := s.parseStateWorkerValue(testBytes1, "stateID"); err == nil {
		t.Errorf("this is error")
	}
	testString2 := `{"stateID":"nodeID","businessIDstateID":"yrk","creatorstateID":"","funcNamestateID":"testfunc","serviceIDstateID":"testservice","tenantIDstateID":"testtenant","versionstateID":"latest"}`
	testBytes2 := []byte(testString2)
	if err := s.parseStateWorkerValue(testBytes2, "stateID"); err != nil {
		t.Errorf("this is error")
	}
}

func Test_checkPodStateIDExist(t *testing.T) {
	if !checkPodStateIDExist("") {
		t.Errorf("this should true")
	}
	StateWorkersList.Delete("stateID")
	if checkPodStateIDExist("stateID") {
		t.Errorf("this should false")
	}
}

func TestCheckClusterStatus(t *testing.T) {
	cacheVal := WorkerCache
	envValClusterID := clusterID
	defer func() {
		WorkerCache = cacheVal
		clusterID = envValClusterID
	}()
	convey.Convey("TestCheckClusterStatus", t, func() {
		convey.Convey("status is not flowouting or flowoutfinish", func() {
			clusterID = "testCluster"
			clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterRunningStatus}}}
			if pa := ClusterStatusStatefulFlowOut(); len(pa) != 0 {
				t.Errorf("this is error")
			}
		})
		convey.Convey("functionCache funcMeta is nil", func() {
			clusterID = "testCluster"
			clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterFlowOutingStatus}}}
			WorkerCache = &workerCache{}
			testCache := &functionCache{}
			testCache.stateMap.Store("testStateID", 1)
			WorkerCache.functions.Store("test", testCache)
			if pa := ClusterStatusStatefulFlowOut(); len(pa) != 0 {
				t.Errorf("this is error")
			}
		})
		convey.Convey("functions is not stateful function", func() {
			clusterID = "testCluster"
			clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterFlowOutingStatus}}}
			WorkerCache = &workerCache{}
			WorkerCache.functions.Store("test", &functionCache{funcMeta: &faasfunction.FunctionService{}})
			if pa := ClusterStatusStatefulFlowOut(); len(pa) != 0 {
				t.Errorf("this is error")
			}
		})
		convey.Convey("functions is stateful function, no instance", func() {
			clusterID = "testCluster"
			clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterFlowOutingStatus}}}
			WorkerCache = &workerCache{}
			testCache := &functionCache{funcMeta: &faasfunction.FunctionService{}}
			testCache.stateMap.Store("testStateID", 1)
			WorkerCache.functions.Store("test", testCache)
			if pa := ClusterStatusStatefulFlowOut(); len(pa) != 0 {
				t.Errorf("this is error")
			}
		})
		convey.Convey("functions is stateful function, podAction is not nil", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(handleStatefulInstanceDelete, func(metadata Metadata, event *InstanceMeta) ([]Action, Metadata) {
					var act []Action
					return act, Metadata{}
				}),
			})
			defer patches.ResetAll()
			clusterID = "testCluster"
			clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterFlowOutingStatus}}}
			WorkerCache = &workerCache{}
			testCache := &functionCache{funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{IsStatefulFunction: true}}}}
			testCache.stateMap.Store("testStateID", 1)
			testCache.instances = make(map[string]faasv1alpha1.WorkerInstance)
			testCache.instances["testPodName"] = v1alpha1.WorkerInstance{StateID: "testStateID"}
			WorkerCache.functions.Store("test", testCache)
			if pa := ClusterStatusStatefulFlowOut(); len(pa) == 0 {
				t.Errorf("this is error")
			}
		})
		convey.Convey("function is both a stateful function and a bridge function.", func() {
			clusterID = "testCluster"
			clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterFlowOutingStatus}}}
			WorkerCache = &workerCache{}
			testCache := &functionCache{funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{IsStatefulFunction: true, IsBridgeFunction: true}}}}
			testCache.stateMap.Store("testStateID", 1)
			testCache.instances = make(map[string]faasv1alpha1.WorkerInstance)
			testCache.instances["testPodName"] = v1alpha1.WorkerInstance{StateID: "testStateID"}
			WorkerCache.functions.Store("test", testCache)
			if pa := ClusterStatusStatefulFlowOut(); len(pa) != 0 {
				t.Errorf("this is error")
			}
		})
	})
}

func Test_checkStateMetaInfo(t *testing.T) {
	type args struct {
		value interface{}
	}
	var a args
	value := StateMetadata{
		StateID:    "stateID",
		FuncName:   "funcName",
		TenantID:   "tenantID",
		BusinessID: "businessID",
		Version:    "version",
		ServiceID:  "serviceID",
		NodeID:     "nodeID",
		Creator:    "creator",
		Cluster:    "cluster11111",
	}
	a.value = value
	tests := []struct {
		name string
		args args
		want StateMetadata
	}{
		{"case1", a, StateMetadata{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := checkStateMetaInfo(tt.args.value)
			assert.Equalf(t, tt.want, got, "checkStateMetaInfo(%v)", tt.args.value)
		})
	}
}

func Test_initStatefulMetadata(t *testing.T) {
	convey.Convey("Given a function cache and state metadata", t, func() {
		fc := &functionCache{
			funcMeta:    &faasfunction.FunctionService{},
			crSignature: "mockCrSignature",
		}
		stateMetaInfo := StateMetadata{
			Creator: "mockCreator",
			CPU:     2,
			Memory:  4096,
		}
		convey.Convey("When initStatefulMetadata is called", func() {
			metadata := initStatefulMetadata(fc, stateMetaInfo)
			convey.Convey("It should initialize metadata correctly", func() {
				convey.So(metadata.Status, convey.ShouldEqual, NotChanged)
				convey.So(metadata.FuncMeta, convey.ShouldResemble, &faasfunction.FunctionService{})
				convey.So(metadata.CRSignature, convey.ShouldEqual, "mockCrSignature")
				convey.So(metadata.Creator, convey.ShouldEqual, "mockCreator")
				convey.So(metadata.CreateOptions.CPU, convey.ShouldEqual, 2)
				convey.So(metadata.CreateOptions.Memory, convey.ShouldEqual, 4096)
			})
		})
		convey.Convey("When CPU and Memory are zero in state metadata", func() {
			stateMetaInfo.CPU = 0
			stateMetaInfo.Memory = 0
			metadata := initStatefulMetadata(fc, stateMetaInfo)

			convey.Convey("It should not set CreateOptions", func() {
				convey.So(metadata.CreateOptions, convey.ShouldBeNil)
			})
		})
	})
}

func Test_statefulReloadingCache(t *testing.T) {
	convey.Convey("Given a function cache", t, func() {
		wc := workerCache{
			functions: sync.Map{},
		}
		fc := &functionCache{
			stateReloading: sync.Map{},
		}
		convey.Convey("check reloading", func() {
			res := wc.CheckStatefulReloading("testfunc", "1")
			convey.So(res, convey.ShouldBeFalse)
			fc.storeReloadingState("1")
			wc.functions.Store("testfunc", fc)
			res = wc.CheckStatefulReloading("testfunc", "1")
			convey.So(res, convey.ShouldBeTrue)
			wc.DeleteReloadDoneState("testfunc", "1")
			res = wc.CheckStatefulReloading("testfunc", "1")
			convey.So(res, convey.ShouldBeFalse)
		})
	})
}

func Test_workerCache_GetWorkerInstanceToDeleteByASBRes(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
	}
	var f fields
	var a args
	funcMeta := &faasfunction.FunctionService{}
	instances := map[string]v1alpha1.WorkerInstance{}
	instance := v1alpha1.WorkerInstance{}
	instances["test"] = instance
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        faasv1alpha1.WorkerInstance
		want1       bool
		patchesFunc mockutils.PatchesFunc
	}{
		{"GetStatePodActionReduce get function cache fail",
			f, a, faasv1alpha1.WorkerInstance{}, false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{}, false
					}),
				})
				return patches
			}},
		{"GetStatePodActionReduce get function cache success but pick error",
			f, a, faasv1alpha1.WorkerInstance{}, false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{funcMeta: funcMeta, instances: instances}, true
					}),
				})
				return patches
			}},
		{"GetStatePodActionReduce get function cache success and pick success",
			f, a, faasv1alpha1.WorkerInstance{
				Applier:    constants.ASBResApplier,
				WillDelete: true,
			}, true, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{funcMeta: funcMeta, instances: map[string]v1alpha1.WorkerInstance{
							"testPod": {
								Applier: constants.ASBResApplier,
							},
						}}, true
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			got, got1 := w.GetWorkerInstanceToDeleteByASBRes(tt.args.funcSvcID)
			assert.Equalf(t, tt.want, got, "GetWorkerInstanceToDeleteByASBRes(%v)", tt.args.funcSvcID)
			assert.Equalf(t, tt.want1, got1, "GetWorkerInstanceToDeleteByASBRes(%v)", tt.args.funcSvcID)
		})
		patches.ResetAll()
	}
}

func Test_getUpdatedFuncCache1(t *testing.T) {
	type args struct {
		stateMetaInfo StateMetadata
	}
	var a args
	tests := []struct {
		name        string
		args        args
		want        *functionCache
		patchesFunc mockutils.PatchesFunc
	}{
		{"case1", a, nil, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService, types.ProfileMetaData,
					etcd3.EtcdType, error) {
					return nil, types.ProfileMetaData{}, 0, errors.New("aaa")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			got, _ := getUpdatedFuncCache(tt.args.stateMetaInfo)
			assert.Equalf(t, tt.want, got, "getUpdatedFuncCache(%v)", tt.args.stateMetaInfo)
		})
		patches.ResetAll()
	}
}

func Test_workerCache_SetAndGetAndDecreaseFuncExpectedNum(t *testing.T) {
	convey.Convey("Test SetAndGetFuncExpectedNum", t, func() {
		cache := &workerCache{
			functions: sync.Map{},
		}
		cache.functions.Store("mockFuncSvcID", &functionCache{expectedNum: 1})
		cache.SetFuncExpectedNum("mockFuncSvcID", 5)
		expectNum, err := cache.GetFuncExpectedNum("mockFuncSvcID")
		convey.So(err, convey.ShouldBeNil)
		convey.So(expectNum, convey.ShouldEqual, 5)
		cache.DecreaseFuncExpectedNum("mockFuncSvcID")
		expectNum, err = cache.GetFuncExpectedNum("mockFuncSvcID")
		convey.So(err, convey.ShouldBeNil)
		convey.So(expectNum, convey.ShouldEqual, 4)

		cache.SetFuncExpectedNum("not_exist_funcSvcID", 5)
		expectNum, err = cache.GetFuncExpectedNum("not_exist_funcSvcID")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(expectNum, convey.ShouldEqual, 0)
		cache.DecreaseFuncExpectedNum("not_exist_funcSvcID")
		expectNum, err = cache.GetFuncExpectedNum("not_exist_funcSvcID")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(expectNum, convey.ShouldEqual, 0)
	})
}

func Test_workerCache_reduceRollingWhenDisabled(t *testing.T) {
	tests := []struct {
		name string
		fc   *functionCache

		expected []v1alpha1.WorkerInstance
	}{
		{
			name: "rolling nil",
			fc:   &functionCache{},

			expected: []v1alpha1.WorkerInstance{},
		},
		{
			name: "minScale is 0",
			fc: &functionCache{
				rollingInstances: map[string]faasv1alpha1.WorkerInstance{
					"pod1": {PodName: "pod1", Applier: constants.WorkerManagerApplier},
					"pod2": {PodName: "pod2", Applier: constants.ASBResApplier},
				},
				maxScale: 100,
			},

			expected: []v1alpha1.WorkerInstance{{PodName: "pod1", Applier: constants.WorkerManagerApplier}},
		},
		{
			name: "maxScale is 0",
			fc: &functionCache{
				rollingInstances: map[string]faasv1alpha1.WorkerInstance{
					"pod1": {PodName: "pod1", Applier: constants.WorkerManagerApplier},
					"pod2": {PodName: "pod2", Applier: constants.ASBResApplier},
				},
				minScale: 1,
				maxScale: 0,
			},

			expected: []v1alpha1.WorkerInstance{{PodName: "pod2", Applier: constants.ASBResApplier}},
		},
		{
			name: "both 0",
			fc: &functionCache{
				rollingInstances: map[string]faasv1alpha1.WorkerInstance{
					"pod1": {PodName: "pod1", Applier: constants.WorkerManagerApplier},
					"pod2": {PodName: "pod2", Applier: constants.ASBResApplier},
				},
			},

			expected: []v1alpha1.WorkerInstance{{PodName: "pod2", Applier: constants.ASBResApplier}, {PodName: "pod1", Applier: constants.WorkerManagerApplier}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sort.Slice(tt.expected, func(i, j int) bool {
				return tt.expected[i].PodName < tt.expected[j].PodName
			})
			res := tt.fc.reduceRollingWhenDisabled()
			sort.Slice(res, func(i, j int) bool {
				return res[i].PodName < res[j].PodName
			})

			assert.Equal(t, tt.expected, res, tt.name)
		})
	}
}

func Test_functionCache_recoverCustomImageSize(t *testing.T) {
	type fields struct {
		instances map[string]v1alpha1.WorkerInstance
		fs        *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		fields      fields
		want        int64
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case_01 recover image size success",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 1000,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("1000")}}}, nil
					}),
					gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
						return 1000, nil
					}),
				})
				return patches
			},
		},
		{
			name: "case_02 get etcd client failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
		},
		{
			name: "case_03 get etcd response failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "case_04 parse value failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("abc")}}}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			fc := &functionCache{
				instances: tt.fields.instances,
				funcMeta:  tt.fields.fs,
			}
			fc.recoverCustomImageSize()
			assert.Equal(t, tt.want, fc.CustomImgSize)
		})
	}
}

func Test_functionCache_getAndSetImageSize(t *testing.T) {
	type fields struct {
		instances map[string]v1alpha1.WorkerInstance
		imageName string
		fs        *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		fields      fields
		want        int64
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case_01 get and set image size success",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
			want: 1000,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(client.NewClientWithOpts,
						func(ops ...func(*client.Client) error) (*client.Client, error) {
							return &client.Client{}, nil
						}),
					gomonkey.ApplyFunc((*client.Client).NegotiateAPIVersion,
						func(_ *client.Client, ctx context.Context) {
							return
						}),
					gomonkey.ApplyFunc(getSWRLoginAuth,
						func(ctx context.Context, s *faasfunction.FunctionService, funcSvcID string) (string, error) {
							return "str", nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).PullPrivateImage,
						func(_ *engine.DockerClient, ctx context.Context, imageName, authStr string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*functionCache).watchEvent,
						func(fc *functionCache, ctx context.Context, imageName string) {
							fc.CustomImgSize = 1000
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			name: "case_02 get client failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(client.NewClientWithOpts,
						func(ops ...func(*client.Client) error) (*client.Client, error) {
							return &client.Client{}, errors.New("error")
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			name: "case_03 getSWRLoginAuth failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(client.NewClientWithOpts,
						func(ops ...func(*client.Client) error) (*client.Client, error) {
							return &client.Client{}, nil
						}),
					gomonkey.ApplyFunc((*client.Client).NegotiateAPIVersion,
						func(_ *client.Client, ctx context.Context) {
							return
						}),
					gomonkey.ApplyFunc(getSWRLoginAuth,
						func(ctx context.Context, s *faasfunction.FunctionService, funcSvcID string) (string, error) {
							return "", errors.New("err")
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			name: "case_04 PullPrivateImage failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(client.NewClientWithOpts,
						func(ops ...func(*client.Client) error) (*client.Client, error) {
							return &client.Client{}, nil
						}),
					gomonkey.ApplyFunc((*client.Client).NegotiateAPIVersion,
						func(_ *client.Client, ctx context.Context) {
							return
						}),
					gomonkey.ApplyFunc(getSWRLoginAuth,
						func(ctx context.Context, s *faasfunction.FunctionService, funcSvcID string) (string, error) {
							return "", nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).PullPrivateImage,
						func(_ *engine.DockerClient, ctx context.Context, imageName, authStr string) error {
							return errors.New("err")
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			fc := &functionCache{
				instances: tt.fields.instances,
			}
			fc.getAndSetImageSize(tt.fields.imageName, tt.fields.fs)
			assert.Equal(t, tt.want, fc.CustomImgSize)
		})
	}
}

func Test_getSWRLoginAuth(t *testing.T) {
	type args struct {
		ctx       context.Context
		fs        *faasfunction.FunctionService
		funcSvcID string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
		want        string
	}{
		// test Decrypt error
		{
			"case1",
			args{
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							Role: faasfunction.Role{
								XRole: "test",
							},
						},
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), errors.New("decrypt error")
					}),
					gomonkey.ApplyFunc(commonutil.ClearByteMemory,
						func(b []byte) {
							return
						}),
				})
				return patches
			},
			true,
			"",
		},
		// test http error
		{
			"case2",
			args{
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							Role: faasfunction.Role{
								XRole: "test",
							},
						},
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{}, errors.New("error")
						}),
					gomonkey.ApplyFunc(commonutil.ClearByteMemory,
						func(b []byte) {
							return
						}),
				})
				return patches
			},
			true,
			"",
		},
		// test NewRequestWithContext failed
		{
			"case3",
			args{
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							Role: faasfunction.Role{
								XRole: "test",
							},
						},
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{
								Header: http.Header{},
							}, errors.New("error")
						}),
					gomonkey.ApplyFunc(commonutil.ClearByteMemory,
						func(b []byte) {
							return
						}),
				})
				return patches
			},
			true,
			"",
		},
		// test Read body failed
		{
			"case4",
			args{
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							Role: faasfunction.Role{
								XRole: "test",
							},
						},
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{
								Header: http.Header{},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do",
						func(_ *http.Client, req *http.Request) (*http.Response, error) {
							return &http.Response{StatusCode: http.StatusUnauthorized,
								Body: ioutil.NopCloser(bytes.NewReader([]byte("abc")))}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), errors.New("read all error")
					}),
					gomonkey.ApplyFunc(commonutil.ClearByteMemory,
						func(b []byte) {
							return
						}),
				})
				return patches
			},
			true,
			"",
		},
		// test success
		{
			name: "case5",
			args: args{
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							Role: faasfunction.Role{
								XRole: "test",
							},
						},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{
								Header: http.Header{},
							}, nil
						}),
					gomonkey.ApplyFunc((*http.Client).Do,
						func(_ *http.Client, req *http.Request) (*http.Response, error) {
							m := make(map[string][]string)
							m["X-Swr-Dockerlogin"] = []string{"docker login -u cn-north-6@7E7TKCCWJLPQ7SKMA1R8 " +
								"-p 998025c6e53180360f79fb9542961f8c825dfa989a29cb10535b68bbfb573334"}
							return &http.Response{
								StatusCode: http.StatusOK,
								Body:       mockutils.ReadCloserImp{},
								Header:     m,
							}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(commonutil.ClearByteMemory,
						func(b []byte) {
							return
						}),
				})
				return patches
			},
			wantErr: false,
			want: "eyJ1c2VybmFtZSI6ImNuLW5vcnRoLTZAN0U3VEtDQ1dKTFBRN1NLTUExUjgiLCJwYXNzd29" +
				"yZCI6Ijk5ODAyNWM2ZTUzMTgwMzYwZjc5ZmI5NTQyOTYxZjhjODI1ZGZhOTg5YTI5Y2IxMDUzNWI2OGJiZmI1NzMzMzQifQ==",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := getSWRLoginAuth(tt.args.ctx, tt.args.fs, tt.args.funcSvcID)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err != nil)
			patches.ResetAll()
		})
	}
}

func Test_watchEvent(t *testing.T) {
	type fields struct {
		instances map[string]v1alpha1.WorkerInstance
		ctx       context.Context
		image     string
		fs        *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		fields      fields
		want        int64
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case_01 watch event success",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				ctx:       context.Background(),
				image:     "imageName",
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*engine.DockerClient).Event,
						func(_ *engine.DockerClient, ctx context.Context) (<-chan events.Message, <-chan error) {
							msgChan := make(chan events.Message, 1)
							errChan := make(chan error, 1)
							msg := events.Message{
								Type:   "image",
								Action: "pull",
								Actor: events.Actor{
									ID: "image",
								},
							}
							msgChan <- msg
							return msgChan, errChan
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).ImageInspectWithRaw,
						func(_ *engine.DockerClient, ctx context.Context, imageName string) (
							dockertypes.ImageInspect, []byte, error) {
							return dockertypes.ImageInspect{Size: 1000}, []byte{}, nil
						}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).ImageRemove,
						func(_ *engine.DockerClient, ctx context.Context, imageName string,
							removeOp dockertypes.ImageRemoveOptions) ([]dockertypes.ImageDeleteResponseItem, error) {
							return []dockertypes.ImageDeleteResponseItem{}, nil
						}),
				})
				return patches
			},
		},
		{
			name: "case_02 create event failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				ctx:       context.Background(),
				image:     "imageName",
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*engine.DockerClient).Event,
						func(_ *engine.DockerClient, ctx context.Context) (<-chan events.Message, <-chan error) {
							return nil, nil
						}),
				})
				return patches
			},
		},
		{
			name: "case_03 ImageInspectWithRaw failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				ctx:       context.Background(),
				image:     "imageName",
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*engine.DockerClient).Event,
						func(_ *engine.DockerClient, ctx context.Context) (<-chan events.Message, <-chan error) {
							msgChan := make(chan events.Message, 1)
							errChan := make(chan error, 1)
							msg := events.Message{
								Type:   "image",
								Action: "pull",
								Actor: events.Actor{
									ID: "image",
								},
							}
							msgChan <- msg
							return msgChan, errChan
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).ImageInspectWithRaw,
						func(_ *engine.DockerClient, ctx context.Context, imageName string) (
							dockertypes.ImageInspect, []byte, error) {
							return dockertypes.ImageInspect{}, []byte{}, errors.New("err")
						}),
				})
				return patches
			},
		},
		{
			name: "case_04 put failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				ctx:       context.Background(),
				image:     "imageName",
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*engine.DockerClient).Event,
						func(_ *engine.DockerClient, ctx context.Context) (<-chan events.Message, <-chan error) {
							msgChan := make(chan events.Message, 1)
							errChan := make(chan error, 1)
							msg := events.Message{
								Type:   "image",
								Action: "pull",
								Actor: events.Actor{
									ID: "image",
								},
							}
							msgChan <- msg
							return msgChan, errChan
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).ImageInspectWithRaw,
						func(_ *engine.DockerClient, ctx context.Context, imageName string) (
							dockertypes.ImageInspect, []byte, error) {
							return dockertypes.ImageInspect{Size: 1000}, []byte{}, nil
						}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
							opts ...clientv3.OpOption) error {
							return errors.New("error")
						}),
				})
				return patches
			},
		},
		{
			name: "case_05 ImageRemove failed",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				ctx:       context.Background(),
				image:     "imageName",
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*engine.DockerClient).Event,
						func(_ *engine.DockerClient, ctx context.Context) (<-chan events.Message, <-chan error) {
							msgChan := make(chan events.Message, 1)
							errChan := make(chan error, 1)
							msg := events.Message{
								Type:   "image",
								Action: "pull",
								Actor: events.Actor{
									ID: "image",
								},
							}
							msgChan <- msg
							return msgChan, errChan
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).ImageInspectWithRaw,
						func(_ *engine.DockerClient, ctx context.Context, imageName string) (
							dockertypes.ImageInspect, []byte, error) {
							return dockertypes.ImageInspect{Size: 1000}, []byte{}, nil
						}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).ImageRemove,
						func(_ *engine.DockerClient, ctx context.Context, imageName string,
							removeOp dockertypes.ImageRemoveOptions) ([]dockertypes.ImageDeleteResponseItem, error) {
							return []dockertypes.ImageDeleteResponseItem{}, errors.New("error")
						}),
				})
				return patches
			},
		},
		{
			name: "case_06 errChan receive event",
			fields: fields{
				instances: make(map[string]v1alpha1.WorkerInstance, 1),
				ctx:       context.Background(),
				image:     "imageName",
				fs: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName:    "funcName",
							FunctionVersion: "funcVersion",
							TenantID:        "tenantID",
						},
					},
				},
			},
			want: 0,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*engine.DockerClient).Event,
						func(_ *engine.DockerClient, ctx context.Context) (<-chan events.Message, <-chan error) {
							msgChan := make(chan events.Message, 1)
							errChan := make(chan error, 1)
							errChan <- errors.New("error")
							return msgChan, errChan
						}),
					gomonkey.ApplyFunc((*engine.DockerClient).Close,
						func(_ *engine.DockerClient) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			fc := &functionCache{
				instances: tt.fields.instances,
			}
			go fc.watchEvent(tt.fields.ctx, tt.fields.image)
			time.Sleep(1 * time.Second)
			tt.fields.ctx.Done()
			assert.Equal(t, tt.want, fc.CustomImgSize)
		})
	}
}

func Test_workerCache_GetCustomImgSize(t *testing.T) {
	case2functions := sync.Map{}
	case2functions.Store("funcSvcID1", &functionCache{CustomImgSize: 100})

	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{
			"case1",
			fields{},
			args{},
			0,
		},
		{
			"case2",
			fields{functions: case2functions},
			args{
				funcSvcID: "funcSvcID1",
			},
			100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			assert.Equalf(t, tt.want, w.GetCustomImgSize(tt.args.funcSvcID), "GetCustomImgSize(%v)", tt.args.funcSvcID)
		})
	}
}

func Test_workerCache_GetAlreadyExistNodes(t *testing.T) {
	case2functions := sync.Map{}
	case2functions.Store("funcSvcID1", &functionCache{instances: map[string]v1alpha1.WorkerInstance{"": {NodeIP: "127.0.0.1"}}})

	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]struct{}
	}{
		{
			"case1",
			fields{},
			args{},
			map[string]struct{}{},
		},
		{
			"case2",
			fields{functions: case2functions},
			args{
				funcSvcID: "funcSvcID1",
			},
			map[string]struct{}{"127.0.0.1": {}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			assert.Equalf(t, tt.want, w.GetAlreadyExistNodes(tt.args.funcSvcID), "GetAlreadyExistNodes(%v)", tt.args.funcSvcID)
		})
	}
}

func Test_isStateIDExistInInstances(t *testing.T) {

	isStateIDExistInInstances(nil, "")

	if !isStateIDExistInInstances(map[string]v1alpha1.WorkerInstance{
		"": {StateID: "a"},
	}, "a") {
		t.Errorf("failed to Test_isStateIDExistInInstances ")
	}

}

func Test_handleStatefulInstanceDelete(t *testing.T) {
	_, metadata := handleStatefulInstanceDelete(Metadata{}, &InstanceMeta{
		Instance: v1alpha1.WorkerInstance{
			StateID: "111",
		},
	})
	if metadata.StateID != "111" {
		t.Errorf("failed to Test_handleStatefulInstanceDelete")
	}

}

func Test_functionCache_succeed(t *testing.T) {
	fc := &functionCache{}
	fc.succeed()

	fc.breaker = breaker.NewCircuitBreaker(breaker.GetRule())
	fc.succeed()
}

func Test_functionCache_fail(t *testing.T) {
	fc := &functionCache{}
	fc.fail()

	fc.breaker = breaker.NewCircuitBreaker(breaker.GetRule())
	fc.fail()
}

func Test_workerCache_GetWorkerAgencyMap(t *testing.T) {
	type fields struct {
		functions sync.Map
	}
	functions := sync.Map{}
	functions.Store("test_funcSvcID", &functionCache{
		funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
			Runtime: faasfunction.RuntimeSpec{Language: "java17"}}}},
		instances: map[string]faasv1alpha1.WorkerInstance{},
	})
	results := make(map[string]PodAgency)
	results[""] = PodAgency{
		FuncMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
			Runtime: faasfunction.RuntimeSpec{Language: "java17"}}}},
		WorkerIns: map[string]faasv1alpha1.WorkerInstance{},
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]PodAgency
	}{
		{
			name: "test get worker agency",
			fields: fields{
				functions: functions,
			},
			want: results,
		},
		{
			name: "test nil",
			fields: fields{
				functions: sync.Map{},
			},
			want: map[string]PodAgency{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
			}
			assert.Equalf(t, tt.want, w.GetWorkerAgencyMap(), "GetWorkerAgencyMap()")
		})
	}
}

func Test_GetCPUMemoryOfStateRouters(t *testing.T) {
	convey.Convey("Test checkStateMetaInfo error", t, func() {
		patch := gomonkey.ApplyFunc(checkStateMetaInfo, func(value interface{}) (StateMetadata, error) {
			return StateMetadata{}, errors.New("need errors")
		})
		defer patch.Reset()
		cpu, memory := GetCPUMemoryOfStateRouters("errorsStateValue")
		convey.So(cpu, convey.ShouldEqual, 0)
		convey.So(memory, convey.ShouldEqual, 0)
	})
	convey.Convey("Test checkStateMetaInfo ok", t, func() {
		stateValues := StateMetadata{StateID: "stateID",
			FuncName:   "funcName",
			TenantID:   "tenantID",
			BusinessID: "businessID",
			Version:    "version",
			ServiceID:  "serviceID",
			NodeID:     "nodeID",
			CPU:        128,
			Memory:     256,
			Creator:    "creator",
			Cluster:    "cluster"}
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			if key == "CLUSTER_ID" {
				return "cluster"
			}
			return ""
		})
		defer patch.Reset()
		cpu, memory := GetCPUMemoryOfStateRouters(stateValues)
		convey.So(cpu, convey.ShouldEqual, 128)
		convey.So(memory, convey.ShouldEqual, 256)
	})
}

func Test_UpdateStateWorkerList(t *testing.T) {
	tests := []struct {
		name        string
		stateID     string
		wantBool    bool
		wantErr     error
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"test1 state etcd client is nil",
			"stateID_UpdateStateWorkerList",
			false,
			errors.New("failed to init state ETCD client"),
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return nil
					}),
				})
				return patches
			},
		},
		{
			"test2  state client.Client is nil",
			"stateID_UpdateStateWorkerList",
			false,
			errors.New("failed to init state ETCD client"),
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
		},
		{
			"test3 get value from etcd error",
			"stateID_UpdateStateWorkerList",
			false,
			errors.New("test error"),
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return nil, errors.New("test error")
					}),
				})
				return patches
			},
		},
		{
			"test4 can not get value from etcd",
			"stateID_UpdateStateWorkerList",
			false,
			errors.New("stateID does not exist in state ETCD"),
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return []string{}, nil
					}),
				})
				return patches
			},
		},
		{
			"test5 invalid state workers error",
			"stateID_UpdateStateWorkerList",
			false,
			errors.New("parseStateWorkerValue error"),
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return []string{"invalid stateRouter"}, nil
					}),
					gomonkey.ApplyFunc((*StateMetadata).parseStateWorkerValue, func(_ *StateMetadata, stateVal []byte, stateID string) error {
						return errors.New("parseStateWorkerValue error")
					}),
				})
				return patches
			},
		},
		{
			"test6 stateID not local cluster",
			"stateID_UpdateStateWorkerList",
			false,
			nil,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc(utils.GetEnvClusterID, func() string {
						return "testCluster"
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						testString := `{"stateID":"nodeID","businessIDstateID":"yrk","creatorstateID":"","funcNamestateID":"testfunc","serviceIDstateID":"testservice","tenantIDstateID":"testtenant","versionstateID":"latest"}`
						return []string{testString}, nil
					}),
				})
				return patches
			},
		},
		{
			"test7 stateID in local cluster",
			"stateID_UpdateStateWorkerList",
			true,
			nil,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc(utils.GetEnvClusterID, func() string {
						return "testCluster"
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						testString := `{"stateID":"nodeID","businessIDstateID":"yrk","creatorstateID":"","funcNamestateID":"testfunc",
										"serviceIDstateID":"testservice","tenantIDstateID":"testtenant","versionstateID":"latest",
										"cluster":"testCluster"}`
						if key == "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/stateID_UpdateStateWorkerList" {
							return []string{testString}, nil
						}
						return []string{"invalid stateRouter"}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			res, err := UpdateStateWorkerList(tt.stateID)
			assert.Equal(t, tt.wantBool, res, tt.name)
			if tt.wantErr != nil {
				assert.Equalf(t, tt.wantErr.Error(), err.Error(), "Test %s failed, expectedErr: %+v, actualErr: %+v",
					tt.name, tt.wantErr.Error(), err.Error())
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestIsActiveFunction(t *testing.T) {
	type args struct {
		tenantID string
		funcName string
		version  string
	}
	var a args
	a.tenantID = "12345678901234561234567890123456"
	a.funcName = "0@serviceID0@funcName0"
	a.version = "version0"

	var a1 args
	a1.tenantID = "12345678901234561234567890123456"
	a1.funcName = "0@serviceID1@funcName1"
	a1.version = "version1"

	var a2 args
	a2.tenantID = "12345678901234561234567890123456"
	a2.funcName = "0@serviceID2@funcName2"
	a2.version = "version2"

	activenessCaches.InactiveFunctionList = make(map[string]string)
	activenessCaches.InactiveFunctionList["aaaa"] = "60"
	activenessCaches.InactiveFunctionList["123*456/0@default/latest/300-128"] = "60"
	activenessCaches.InactiveFunctionList["123*456/0@serviceID1@funcName1/version1/300-128"] = "aaa"
	activenessCaches.InactiveFunctionList["123*456/0@serviceID2@funcName2/version2/300-128"] = "60"

	tests := []struct {
		name string
		args args
		want bool
	}{
		{"not found func", a, true},
		{"found but not active", a1, false},
		{"found and active", a2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isActiveFunction(tt.args.tenantID, tt.args.funcName, tt.args.version), "IsActiveFunction(%v, %v)", tt.args.funcName, tt.args.version)
		})
	}
}

func Test_functionCache_handleCacheForImageAcceleration(t *testing.T) {
	type fields struct {
		funcMeta *faasfunction.FunctionService
	}
	type args struct {
		funcSvcID   string
		crSignature string
		funcMeta    *faasfunction.FunctionService
	}
	var f fields
	var a args
	a.funcMeta = &faasfunction.FunctionService{}
	a.funcMeta.Spec.Configuration.FunctionVersionURN = "sn:cn:yrk:12345678901234561234567890123456:function:0@default@shy3:latest"

	var a2 args
	a2.funcMeta = &faasfunction.FunctionService{}
	a2.funcMeta.Spec.Configuration.FunctionVersionURN = "sn:cn:yrk:12345678901234561234567890000000:function:0@default@shy3:latest"
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{"failed to get faaS config", f, a, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return nil, errors.New("failed to get faaS config")
				}),
			})
			return patches
		}},
		{"disable image affinity cache", f, a, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			imageAffinityCacheConfig := &config.ImageAffinityCacheConfig{DisableImageAffinityCache: 1}
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{ImageAffinityCacheConfig: imageAffinityCacheConfig}, nil
				}),
			})
			return patches
		}},
		{"in the whiteList and is active function", f, a, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			imageAffinityCacheConfig := &config.ImageAffinityCacheConfig{
				DisableImageAffinityCache: 0,
				TenantWhiteList:           []string{"12345678901234561234567890123456"},
			}
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{ImageAffinityCacheConfig: imageAffinityCacheConfig}, nil
				}),
			})
			return patches
		}},
		{"not in the whiteList or is  not active function", f, a2, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			imageAffinityCacheConfig := &config.ImageAffinityCacheConfig{
				DisableImageAffinityCache: 0,
				TenantWhiteList:           []string{"12345678901234561234567890123456"},
			}
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{ImageAffinityCacheConfig: imageAffinityCacheConfig}, nil
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			handleCacheForImageAcceleration(tt.args.funcSvcID, tt.args.crSignature, tt.args.funcMeta)
		})
		patches.ResetAll()
	}
}

func Test_functionCache_getCurOnDemandNum(t *testing.T) {
	convey.Convey("test get curOnDemandNum", t, func() {
		fc := WorkerCache.createSimpleFunctionCache("test_func", config.InstanceMetaData{}, types.ProfileMetaData{}, utils.FunctionInfo{})
		convey.Convey("test all elastic instance without migrating", func() {
			for i := 0; i < 10; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index}
			}
			convey.So(fc.getCurOnDemandNum(), convey.ShouldEqual, 10)
		})
		convey.Convey("test all elastic instance with migrating done", func() {
			for i := 0; i < 10; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "otherName" + index}
			}
			convey.So(fc.getCurOnDemandNum(), convey.ShouldEqual, 10)
		})
		convey.Convey("test all elastic instance with some migrating doing", func() {
			for i := 0; i < 5; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "podName" + index}
			}
			for i := 5; i < 10; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "otherName" + index}
			}
			convey.So(fc.getCurOnDemandNum(), convey.ShouldEqual, 5)
		})
		convey.Convey("test all elastic instance with some migrating doing but migratedFrom not exist", func() {
			for i := 0; i < 5; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "podName" + index}
			}
			for i := 5; i < 10; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true}
			}
			convey.So(fc.getCurOnDemandNum(), convey.ShouldEqual, 5)
		})
		convey.Convey("test reserved and elastic instance with some migrating doing", func() {
			for i := 0; i < 5; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "podName" + index}
			}
			for i := 5; i < 10; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "otherName" + index}
			}
			for i := 10; i < 15; i++ {
				index := strconv.Itoa(i)
				fc.instances[index] = v1alpha1.WorkerInstance{PodName: "podName" + index,
					IsMigrating: true, MigratedFrom: "otherName" + index, Applier: constants.WorkerManagerApplier}
			}
			convey.So(fc.getCurOnDemandNum(), convey.ShouldEqual, 5)
		})
	})
}

func Test_waitScalingReservedIns(t *testing.T) {
	type args struct {
		ctx             context.Context
		reservedInsChan chan v1alpha1.WorkerInstance
		funcSvcID       string
		traceID         string
	}
	ctx, cancel := context.WithCancel(context.Background())
	tests := []struct {
		name            string
		args            args
		cancelFunc      context.CancelFunc
		isCancelContext bool
		isCloseChan     bool
		actualWorkerIns v1alpha1.WorkerInstance
		patchesFunc     mockutils.PatchesFunc
		wantErr         assert.ErrorAssertionFunc
	}{
		{
			name: "test success",
			args: args{
				ctx:             ctx,
				reservedInsChan: make(chan faasv1alpha1.WorkerInstance, 1),
				funcSvcID:       "mockFuncSvcID",
				traceID:         "mockTraceID",
			},
			cancelFunc:      cancel,
			isCancelContext: false,
			isCloseChan:     false,
			actualWorkerIns: v1alpha1.WorkerInstance{PodName: "mockPodName001"},
			patchesFunc: func() mockutils.PatchSlice {
				return mockutils.InitPatchSlice()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		},
		{
			name: "test reservedInsChan is nil",
			args: args{
				ctx:             ctx,
				reservedInsChan: nil,
				funcSvcID:       "mockFuncSvcID",
				traceID:         "mockTraceID",
			},
			cancelFunc:      cancel,
			isCancelContext: false,
			isCloseChan:     false,
			actualWorkerIns: v1alpha1.WorkerInstance{PodName: "mockPodName001"},
			patchesFunc: func() mockutils.PatchSlice {
				return mockutils.InitPatchSlice()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
		{
			name: "test pod name is empty",
			args: args{
				ctx:             ctx,
				reservedInsChan: make(chan faasv1alpha1.WorkerInstance, 1),
				funcSvcID:       "mockFuncSvcID",
				traceID:         "mockTraceID",
			},
			cancelFunc:      cancel,
			isCancelContext: false,
			isCloseChan:     false,
			actualWorkerIns: v1alpha1.WorkerInstance{PodName: ""},
			patchesFunc: func() mockutils.PatchSlice {
				return mockutils.InitPatchSlice()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
		{
			name: "test channel is closed",
			args: args{
				ctx:             ctx,
				reservedInsChan: make(chan faasv1alpha1.WorkerInstance, 1),
				funcSvcID:       "mockFuncSvcID",
				traceID:         "mockTraceID",
			},
			cancelFunc:      cancel,
			isCancelContext: false,
			isCloseChan:     true,
			actualWorkerIns: v1alpha1.WorkerInstance{PodName: "mockPodName001"},
			patchesFunc: func() mockutils.PatchSlice {
				return mockutils.InitPatchSlice()
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
	}
	for _, tt := range tests {
		if tt.args.ctx != nil && tt.isCancelContext {
			tt.cancelFunc()
		}
		if tt.args.reservedInsChan != nil {
			if tt.isCloseChan {
				close(tt.args.reservedInsChan)
			} else {
				go func() {
					select {
					case tt.args.reservedInsChan <- tt.actualWorkerIns:
					default:
					}
				}()
			}
		}
		t.Run(tt.name, func(t *testing.T) {
			_, err := waitScalingReservedIns(tt.args.ctx, tt.args.reservedInsChan, tt.args.funcSvcID, tt.args.traceID)
			if !tt.wantErr(t, err, fmt.Sprintf("waitScalingReservedIns(%v, %v, %v, %v)", tt.args.ctx, tt.args.reservedInsChan, tt.args.funcSvcID, tt.args.traceID)) {
				t.Errorf("it is not expected with %v", err)
				return
			}
			fmt.Println(err)
		})
	}
}

func TestGetScheduleNodesByLabels(t *testing.T) {
	tests := []struct {
		name        string
		fgsTags     map[string]string
		want        PodScheduleInfo
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "GetScheduleNodesByLabels have tagged label",
			fgsTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				MatchedSSDPoolLabel:    false,
				AffinityNodes:          []string{"node1"},
				PreferredAffinityNodes: []string{"node3"},
				AntiAffinityNodes:      []string{"node2"},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							SSDPoolLabels: map[string]string{
								"daikin": "daikin"},
						}, nil
					}),
					gomonkey.ApplyGlobalVar(nodeCache, NodeCache{
						NodeList: map[string]*NodeInfo{
							"node1": {
								Label: map[string]string{
									"key2":   "value2-Schedule",
									"test-1": "test-1",
								},
							},
							"node2": {
								Label: map[string]string{
									"key1":    "value1-NoSchedule",
									"test-22": "test-22",
								},
							},
							"node3": {
								Label: map[string]string{
									"key1": "value1-PreferredSchedule",
								},
							},
							"node4": {
								Label: map[string]string{
									"default": "default-NoSchedule",
								},
							},
						},
					}),
				})
				return patches
			},
		},

		{
			name: "GetScheduleNodesByLabels have label match with ssd label",
			fgsTags: map[string]string{
				"key1":   "value1",
				"key2":   "value2",
				"daikin": "daikin",
			},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				MatchedSSDPoolLabel:    true,
				AffinityNodes:          []string{"node1"},
				PreferredAffinityNodes: []string{"node3"},
				AntiAffinityNodes:      []string{"node2"},
			},
			patchesFunc: func() mockutils.PatchSlice {
				config.SSDPoolLabelsMap.Store("daikin", "daikin")
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							SSDPoolLabels: map[string]string{
								"daikin": "daikin",
								"ab:c":   "test-v"},
						}, nil
					}),
					gomonkey.ApplyGlobalVar(nodeCache, NodeCache{
						NodeList: map[string]*NodeInfo{
							"node1": {
								Label: map[string]string{
									"key2":   "value2-Schedule",
									"test-1": "test-1",
								},
							},
							"node2": {
								Label: map[string]string{
									"key1":    "value1-NoSchedule",
									"test-22": "test-22",
								},
							},
							"node3": {
								Label: map[string]string{
									"key1": "value1-PreferredSchedule",
								},
							},
							"node4": {
								Label: map[string]string{
									"default": "default-NoSchedule",
								},
							},
						},
					}),
				})
				return patches
			},
		},

		{
			name: "GetScheduleNodesByLabels have tagged label not match",
			fgsTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				MatchedSSDPoolLabel:    false,
				AffinityNodes:          []string{},
				PreferredAffinityNodes: []string{},
				AntiAffinityNodes:      []string{},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							SSDPoolLabels: map[string]string{
								"daikin": "daikin"},
						}, nil
					}),
					gomonkey.ApplyGlobalVar(nodeCache, NodeCache{
						NodeList: map[string]*NodeInfo{
							"node1": {
								Label: map[string]string{
									"test-1":  "test-1",
									"test-11": "test-11",
								},
							},
							"node2": {
								Label: map[string]string{
									"test-2":  "test-2-NoSchedule",
									"test-22": "test-22",
								},
							},
							"node3": {
								Label: map[string]string{
									"test-3":  "test-3-Schedule",
									"test-33": "test-33-PreferredSchedule",
								},
							},
							"node4": {
								Label: map[string]string{
									"default": "default-NoSchedule",
								},
							},
						},
					}),
				})
				return patches
			},
		},
		{
			name:    "GetScheduleNodesByLabels have not tagged label",
			fgsTags: map[string]string{},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    false,
				MatchedSSDPoolLabel:    false,
				AntiAffinityNodes:      []string{"node4"},
				AffinityNodes:          nil,
				PreferredAffinityNodes: nil,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyGlobalVar(nodeCache, NodeCache{
						NodeList: map[string]*NodeInfo{
							"node1": {
								Label: map[string]string{
									"test-1":  "test-1",
									"test-11": "test-11",
								},
							},
							"node2": {
								Label: map[string]string{
									"test-2":  "test-2-NoSchedule",
									"test-22": "test-22",
								},
							},
							"node3": {
								Label: map[string]string{
									"test-3":  "test-3-Schedule",
									"test-33": "test-33-PreferredSchedule",
								},
							},
							"node4": {
								Label: map[string]string{
									"default": "default-NoSchedule",
								},
							},
						},
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			podScheduleInfo := getScheduleNodesByLabels(tt.fgsTags)
			assert.Equal(t, tt.want, podScheduleInfo)
			patches.ResetAll()
		})
	}
}

func TestWorkerCache_GetScheduleNodesByLabels(t *testing.T) {
	type args struct {
		funcSvcID string
	}

	tests := []struct {
		name         string
		args         args
		want         PodScheduleInfo
		storedArgs   args
		storedResult PodScheduleInfo
	}{
		{
			name: "WorkerCache_GetScheduleNodesByLabels have tagged label",
			args: args{
				funcSvcID: "test-function-svc-ID-1",
			},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				AffinityNodes:          []string{"node3"},
				PreferredAffinityNodes: []string{"node3"},
				AntiAffinityNodes:      []string{"node2"},
			},
			storedArgs: args{
				funcSvcID: "test-function-svc-ID-1",
			},
			storedResult: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				AffinityNodes:          []string{"node3"},
				PreferredAffinityNodes: []string{"node3"},
				AntiAffinityNodes:      []string{"node2"},
			},
		},

		{
			name: "WorkerCache_GetScheduleNodesByLabels have not tagged label",
			args: args{
				funcSvcID: "test-function-svc-ID-2",
			},
			want: PodScheduleInfo{},
			storedArgs: args{
				funcSvcID: "test-function-svc-ID-1",
			},
			storedResult: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				AffinityNodes:          []string{"node3"},
				PreferredAffinityNodes: []string{"node3"},
				AntiAffinityNodes:      []string{"node2"},
			},
		},
	}

	wc := workerCache{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wc.functions.Store(tt.storedArgs.funcSvcID, &functionCache{
				funcScheduleNodes: tt.storedResult,
			})
			t.Run(tt.name, func(t *testing.T) {
				assert.Equalf(t, tt.want, wc.GetScheduleLabelNodes(tt.args.funcSvcID), "getScheduleNodesByLabels(%v, %v)", tt.args.funcSvcID, tt.want)
			})

		})
	}
}

func TestWorkerCache_UpdateFuncScheduleNodes(t *testing.T) {
	type args struct {
		funcSvcID string
		node      *corev1.Node
	}

	tests := []struct {
		name           string
		args           args
		want           PodScheduleInfo
		storedArgs     args
		storedResult   PodScheduleInfo
		storedMetaData *faasfunction.FunctionService
	}{
		{
			name: "WorkerCache_UpdateFuncScheduleNodes have tagged label",
			args: args{
				funcSvcID: "test-function-svc-ID-1",
				node: &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"key1": "value1-Schedule",
							"xx":   "xxx-NoSchedule",
						},
					},
				},
			},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				AffinityNodes:          []string{"node3", "node1"},
				PreferredAffinityNodes: []string{},
				AntiAffinityNodes:      []string{},
			},
			storedArgs: args{
				funcSvcID: "test-function-svc-ID-1",
			},
			storedResult: PodScheduleInfo{
				TaggedScheduleLabel:    true,
				AffinityNodes:          []string{"node3"},
				PreferredAffinityNodes: nil,
				AntiAffinityNodes:      nil,
			},
			storedMetaData: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FgsTags: map[string]string{
							"key1": "value1",
							"key2": "value2",
						},
					},
				},
			},
		},

		{
			name: "WorkerCache_UpdateFuncScheduleNodes have not tagged label",
			args: args{
				funcSvcID: "test-function-svc-ID-1",
				node: &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node4",
						Labels: map[string]string{
							"default": "default-NoSchedule",
							"xx":      "xxx-NoSchedule",
						},
					},
				},
			},
			want: PodScheduleInfo{
				TaggedScheduleLabel:    false,
				AntiAffinityNodes:      []string{"node2", "node4"},
				PreferredAffinityNodes: []string{},
				AffinityNodes:          []string{},
			},
			storedArgs: args{
				funcSvcID: "test-function-svc-ID-1",
			},
			storedResult: PodScheduleInfo{
				TaggedScheduleLabel:    false,
				AntiAffinityNodes:      []string{"node2"},
				AffinityNodes:          nil,
				PreferredAffinityNodes: nil,
			},
			storedMetaData: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FgsTags: map[string]string{},
					},
				},
			},
		},
	}

	wc := workerCache{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wc.functions.Store(tt.storedArgs.funcSvcID, &functionCache{
				funcScheduleNodes: tt.storedResult,
				funcMeta:          tt.storedMetaData,
			})
			wc.UpdateFuncScheduleNodes(tt.args.node)
			t.Run(tt.name, func(t *testing.T) {
				assert.Equalf(t, tt.want, wc.GetScheduleLabelNodes(tt.args.funcSvcID), "getScheduleNodesByLabels(%v, %v)", tt.args.funcSvcID, tt.want)
			})
		})
	}
}

// TestUpdatePodScheduleInfoByNodeEvent -
func TestUpdatePodScheduleInfoByNodeEvent(t *testing.T) {
	convey.Convey("TestUpdatePodScheduleInfoByNodeEvent", t, func() {
		convey.Convey("add node with default schedule label", func() {
			fgsTags := map[string]string{}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{"node10"},
				PreferredAffinityNodes: []string{"node11"},
				AntiAffinityNodes:      []string{"node12"},
				TaggedScheduleLabel:    false,
			}
			node := &corev1.Node{}
			node.Name = "node1"
			node.Labels = map[string]string{
				"default": "default-NoSchedule",
				"xx":      "xxx-NoSchedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeFalse)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 2)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 1)
			convey.So(newSchInfo.AntiAffinityNodes[0], convey.ShouldEqual, "node12")
			convey.So(newSchInfo.AntiAffinityNodes[1], convey.ShouldEqual, "node1")
		})

		convey.Convey("exist node with default schedule label ", func() {
			fgsTags := map[string]string{}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{"node10"},
				PreferredAffinityNodes: []string{"node11"},
				AntiAffinityNodes:      []string{"node12"},
				TaggedScheduleLabel:    false,
			}
			node := &corev1.Node{}
			node.Name = "node12"
			node.Labels = map[string]string{
				"default": "default-NoSchedule",
				"xx":      "xxx-NoSchedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeFalse)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 1)
			convey.So(newSchInfo.AntiAffinityNodes[0], convey.ShouldEqual, "node12")
		})

		convey.Convey("with no match label ", func() {
			fgsTags := map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{"node10"},
				PreferredAffinityNodes: []string{"node11"},
				AntiAffinityNodes:      []string{"node12"},
				TaggedScheduleLabel:    true,
			}
			node := &corev1.Node{}
			node.Name = "node12"
			node.Labels = map[string]string{
				"default": "default-NoSchedule",
				"xx":      "xxx-NoSchedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeTrue)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 0)
			convey.So(newSchInfo.AffinityNodes[0], convey.ShouldEqual, "node10")
			convey.So(newSchInfo.PreferredAffinityNodes[0], convey.ShouldEqual, "node11")
		})

		convey.Convey("with matched schedule label and remove node from AntiAffinityNode", func() {
			fgsTags := map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{"node10"},
				PreferredAffinityNodes: []string{"node11"},
				AntiAffinityNodes:      []string{"node12"},
				TaggedScheduleLabel:    true,
			}
			node := &corev1.Node{}
			node.Name = "node12"
			node.Labels = map[string]string{
				"default": "default-NoSchedule",
				"key1":    "value1-Schedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeTrue)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 2)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 0)

			convey.So(newSchInfo.AffinityNodes[0], convey.ShouldEqual, "node10")
			convey.So(newSchInfo.AffinityNodes[1], convey.ShouldEqual, "node12")
			convey.So(newSchInfo.PreferredAffinityNodes[0], convey.ShouldEqual, "node11")
		})

		convey.Convey("with matched schedule label, is exist in AffinityNode, remove node from AntiAffinityNode", func() {
			fgsTags := map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{"node10"},
				PreferredAffinityNodes: []string{"node11"},
				AntiAffinityNodes:      []string{"node10", "node12"},
				TaggedScheduleLabel:    true,
			}
			node := &corev1.Node{}
			node.Name = "node10"
			node.Labels = map[string]string{
				"default": "default-NoSchedule",
				"key1":    "value1-Schedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeTrue)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 1)

			convey.So(newSchInfo.AffinityNodes[0], convey.ShouldEqual, "node10")
			convey.So(newSchInfo.PreferredAffinityNodes[0], convey.ShouldEqual, "node11")
			convey.So(newSchInfo.AntiAffinityNodes[0], convey.ShouldEqual, "node12")
		})

		convey.Convey("with matched schedule and no schedule label", func() {
			fgsTags := map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{"node10"},
				PreferredAffinityNodes: []string{"node11"},
				AntiAffinityNodes:      []string{"node12"},
				TaggedScheduleLabel:    true,
			}
			node := &corev1.Node{}
			node.Name = "node13"
			node.Labels = map[string]string{
				"default": "default-NoSchedule",
				"key1":    "value1-Schedule",
				"key2":    "value2-NoSchedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeTrue)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 2)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 2)

			convey.So(newSchInfo.AffinityNodes[0], convey.ShouldEqual, "node10")
			convey.So(newSchInfo.AffinityNodes[1], convey.ShouldEqual, "node13")
			convey.So(newSchInfo.PreferredAffinityNodes[0], convey.ShouldEqual, "node11")
			convey.So(newSchInfo.AntiAffinityNodes[0], convey.ShouldEqual, "node12")
			convey.So(newSchInfo.AntiAffinityNodes[1], convey.ShouldEqual, "node13")
		})
		convey.Convey("with matched schedule and no schedule label when AntiAffinityNodes is empty ", func() {
			fgsTags := map[string]string{
				"key1": "value1",
				"key2": "value2",
			}
			curSchInfo := PodScheduleInfo{
				AffinityNodes:          []string{},
				PreferredAffinityNodes: []string{},
				AntiAffinityNodes:      []string{},
				TaggedScheduleLabel:    true,
			}
			node := &corev1.Node{}
			node.Name = "node13"
			node.Labels = map[string]string{
				"key2": "value2-NoSchedule",
			}

			node2 := &corev1.Node{}
			node2.Name = "node14"
			node2.Labels = map[string]string{
				"key2": "value2-Schedule",
			}

			node3 := &corev1.Node{}
			node3.Name = "node15"
			node3.Labels = map[string]string{
				"key2": "value2-PreferredSchedule",
			}

			newSchInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
			newSchInfo = updatePodScheduleInfoByNodeEvent(fgsTags, node2, newSchInfo)
			newSchInfo = updatePodScheduleInfoByNodeEvent(fgsTags, node3, newSchInfo)
			convey.So(newSchInfo.TaggedScheduleLabel, convey.ShouldBeTrue)
			convey.So(len(newSchInfo.AffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.PreferredAffinityNodes), convey.ShouldEqual, 1)
			convey.So(len(newSchInfo.AntiAffinityNodes), convey.ShouldEqual, 1)

			convey.So(newSchInfo.AntiAffinityNodes[0], convey.ShouldEqual, "node13")
			convey.So(newSchInfo.AffinityNodes[0], convey.ShouldEqual, "node14")
			convey.So(newSchInfo.PreferredAffinityNodes[0], convey.ShouldEqual, "node15")
		})
	})
}
