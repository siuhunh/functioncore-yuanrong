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

// Package podfetcher -
package podfetcher

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

func TestPodFetcher_PullUpInstance_Reserved(t *testing.T) {
	type args struct {
		functionMeta *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to pull up reserved instance", &PodFetcher{},
			args{functionMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
			}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							NameSpace: "default",
						}, nil
					}),
				})
				return patches
			}},
		{"case2 succeed to pull up reserved instance", &PodFetcher{},
			args{functionMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
			}, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							NameSpace: "default",
						}, nil
					}),
					gomonkey.ApplyFunc((*PodFetcher).WithContext, func(_ *PodFetcher,
						f decorateFunc, requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "abc"}}, nil
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := tt.p.PullUpInstance(tt.args.functionMeta, Reserved); (err != nil) != tt.wantErr {
				t.Errorf("PullUpReservedInstance() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_provision(t *testing.T) {
	_, _ = cache.WorkerCache.HandleInsMetadata("test", config.InstanceMetaData{
		MinInstance: 0, MaxInstance: 1000, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
	type args struct {
		functionMeta *faasfunction.FunctionService
		needPullNums int
		crSignature  string
		delay        time.Duration
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 needPullNums is 0", &PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{}}},
			args{functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{ID: "test"},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
			}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case2 ctx not ok", &PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{}}},
			args{functionMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
			}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case3 failedNums = 0", &PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{}}},
			args{functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{ID: "test"},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
				needPullNums: 1,
				delay:        tools.BaseDelayForReservedIns}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).PullUpInstance, func(_ *PodFetcher,
						functionMeta *faasfunction.FunctionService, pullType PullType) error {
						return nil
					}),
				})

				return patches
			}},
		{"case4 failedNums > 0, delay timeout", &PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{ctx: context.TODO()}}},
			args{functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{ID: "test"},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
				needPullNums: 1,
				delay:        tools.BaseDelayForReservedIns}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).PullUpInstance, func(_ *PodFetcher,
						functionMeta *faasfunction.FunctionService, pullType PullType) error {
						return errors.New("test")
					}),
					gomonkey.ApplyFunc(cache.UpdateSilentFuncCache, func(funcName, businessID, tenantID, version string) {
						return
					}),
				})

				return patches
			}},
		{"case5 getting pod error", &PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{ctx: context.TODO()}}},
			args{functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{ID: "test"},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Resource:        faasfunction.ResourceSpec{CPU: 1, Mem: 2},
						BusinessID:      "test",
						TenantID:        "test",
						FunctionName:    "test",
						FunctionVersion: "latest",
					},
				}},
				needPullNums: 1,
				delay:        tools.BaseDelayForReservedIns}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).PullUpInstance, func(_ *PodFetcher,
						functionMeta *faasfunction.FunctionService, pullType PullType) error {
						return snerror.New(snerror.GettingPodErrorCode, "")
					}),
					gomonkey.ApplyFunc(cache.UpdateSilentFuncCache, func(funcName, businessID, tenantID, version string) {
						return
					}),
				})

				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.p.provision(tt.args.functionMeta, tt.args.needPullNums, tt.args.crSignature, tt.args.delay)
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_migrateInstance(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            v1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		metadata    cache.Metadata
		migratedIns v1alpha1.WorkerInstance
	}
	patch := gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {})
	defer func() {
		patch.Reset()
	}()
	isMocked := false
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		isMocked    bool
	}{
		{
			name:   "test with success",
			fields: fields{},
			args: args{
				metadata: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
				migratedIns: v1alpha1.WorkerInstance{
					PodName:     "mock-pod-name",
					IsMigrating: true,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).WithContext, func(_ *PodFetcher, f decorateFunc, requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						isMocked = true
						log.GetLogger().Infof("succeed to generalized pod for migrating")
						return &corev1.Pod{}, nil
					}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByWorkerInstance, func(_ *PodFetcher, funcSvcID, namespace, traceID string, instance v1alpha1.WorkerInstance) {
						return
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test with success
		{
			name:   "test with generalize pod error",
			fields: fields{},
			args: args{
				metadata: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
				migratedIns: v1alpha1.WorkerInstance{
					PodName:     "mock-pod-name",
					IsMigrating: true,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).WithContext, func(_ *PodFetcher, f decorateFunc, requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						isMocked = true
						log.GetLogger().Infof("failed to generalized pod for migrating")
						return &corev1.Pod{}, errors.New("mock generalize pod error")
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test with generalize pod error for reserved instance
		{
			name:   "test with generalize pod error",
			fields: fields{},
			args: args{
				metadata: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
				migratedIns: v1alpha1.WorkerInstance{
					PodName:     "mock-pod-name",
					Applier:     constants.WorkerManagerApplier,
					IsMigrating: true,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).WithContext, func(_ *PodFetcher, f decorateFunc, requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						isMocked = true
						log.GetLogger().Infof("failed to generalized pod for migrating")
						return &corev1.Pod{}, errors.New("mock generalize pod error")
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test with generalize pod error for reserved instance
		{
			name:   "test with migrating bridge function",
			fields: fields{},
			args: args{
				metadata: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
				migratedIns: v1alpha1.WorkerInstance{
					PodName:      "mock-pod-name",
					IsMigrating:  true,
					StateID:      "StateID",
					IsBridgeFunc: true,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).WithContext, func(_ *PodFetcher, f decorateFunc, requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						if requestInfo.StateID != "" {
							isMocked = true
						}
						return &corev1.Pod{}, errors.New("mock generalize pod error")
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test with migrating a stateful function instance, the request contains the stateID
		{
			name:   "test with migrating stateful function, delete first",
			fields: fields{},
			args: args{
				metadata: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
				migratedIns: v1alpha1.WorkerInstance{
					PodName:     "mock-pod-name",
					IsMigrating: true,
					StateID:     "StateID",
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S, func(_ *PodFetcher, _, _ string) error {
						isMocked = true
						return nil
					}),
					gomonkey.ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(_ context.Context, _ v1alpha1.WorkerInstance) {
						return
					}),
					gomonkey.ApplyFunc(cache.WorkerCache.HandleInstance, func(funcSvcID string, event *cache.InstanceMeta) ([]cache.Action, cache.Metadata) {
						return []cache.Action{}, cache.Metadata{}
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test with migrating a stateful function instance which is not a bridge,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PodFetcher{
				workerClient:         tt.fields.workerClient,
				VpcHandler:           tt.fields.VpcHandler,
				PoolMgr:              tt.fields.PoolMgr,
				KubeClient:           tt.fields.KubeClient,
				podLister:            tt.fields.podLister,
				Queue:                tt.fields.Queue,
				stopCh:               tt.fields.stopCh,
				contexts:             tt.fields.contexts,
				contextsMutex:        tt.fields.contextsMutex,
				podGeneralizeTimeout: tt.fields.podGeneralizeTimeout,
				httpsEnable:          tt.fields.httpsEnable,
				IdleRetryInfo:        tt.fields.IdleRetryInfo,
			}
			isMocked = false
			patches := tt.patchesFunc()
			p.migrateInstance(tt.args.metadata, tt.args.migratedIns, nil)
			if isMocked != tt.isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_ProvisionWorkerPod_PullTrigger_Retry(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            v1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		functionMeta      *faasfunction.FunctionService
		requiredMinInsNum int
		crSignature       string
	}
	mockFuncSvcID := "mockFuncSvcID001"
	tests := []struct {
		name          string
		fields        fields
		args          args
		patchesFunc   mockUtils.PatchesFunc
		wantFailNum   int
		wantExceedNum int
	}{
		{
			name:   "retry to pull up trigger deploy when get deploy error",
			fields: fields{},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: mockFuncSvcID,
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							VPCTriggerImage: "mockVPCTriggerImage001",
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*appsv1.Deployment, error) {
							return nil, errors.New("mock get deploy by k8s error")
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return mockUtils.K8sFakeClient
					}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-pat-container-ip",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc(vpcmanager.ParseFunctionMeta,
						func(meta *faasfunction.FunctionService) *vpcmanager.CreateVpcTriggerInstruct {
							return &vpcmanager.CreateVpcTriggerInstruct{
								PodName: "mockPullTriggerDeploy",
							}
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).CheckPullTriggerInfo,
						func(_ *vpcmanager.VpcHandler, funcSvcID, patServiceID, podName string) bool {
							return false
						}),
				})
				return patches
			},
			wantFailNum:   1,
			wantExceedNum: 0,
		}, // retry to pull up trigger deploy when get deploy error
		{
			name:   "retry to pull up trigger deploy when create deploy error",
			fields: fields{},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: mockFuncSvcID,
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							VPCTriggerImage: "mockVPCTriggerImage001",
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*appsv1.Deployment, error) {
							return nil, k8serrors.NewNotFound(schema.GroupResource{}, "mock-not-found-error")
						}),
					gomonkey.ApplyFunc(tools.CreateDeployByK8S,
						func(k8sClient kubernetes.Interface, deploy *appsv1.Deployment) error {
							return errors.New("mock create deploy by k8s error")
						}),
					gomonkey.ApplyFunc(vpcmanager.MakePullTriggerDeploy,
						func(triggerInfo *vpcmanager.CreateVpcTriggerInstruct, vpcNatConfByte []byte, triggerReplicas int32) *appsv1.Deployment {
							return &appsv1.Deployment{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "mock-trigger-deploy-name",
									Namespace: "default",
								},
							}
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return mockUtils.K8sFakeClient
					}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-pat-container-ip",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc(vpcmanager.ParseFunctionMeta,
						func(meta *faasfunction.FunctionService) *vpcmanager.CreateVpcTriggerInstruct {
							return &vpcmanager.CreateVpcTriggerInstruct{
								PodName: "mockPullTriggerDeploy",
							}
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).CheckPullTriggerInfo,
						func(_ *vpcmanager.VpcHandler, funcSvcID, patServiceID, podName string) bool {
							return false
						}),
				})
				return patches
			},
			wantFailNum:   1,
			wantExceedNum: 0,
		}, // retry to pull up trigger deploy when create deploy error
		{
			name:   "retry to pull up trigger deploy when create deploy error",
			fields: fields{},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: mockFuncSvcID,
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							VPCTriggerImage: "mockVPCTriggerImage001",
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*appsv1.Deployment, error) {
							return &appsv1.Deployment{}, nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
						return nil
					}),
					gomonkey.ApplyFunc(tools.UpdateDeployByK8S,
						func(k8sClient kubernetes.Interface, deploy *appsv1.Deployment) error {
							return errors.New("mock update deploy by k8s error")
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return mockUtils.K8sFakeClient
					}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-pat-container-ip",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc(vpcmanager.ParseFunctionMeta,
						func(meta *faasfunction.FunctionService) *vpcmanager.CreateVpcTriggerInstruct {
							return &vpcmanager.CreateVpcTriggerInstruct{
								PodName: "mockPullTriggerDeploy",
							}
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).CheckPullTriggerInfo,
						func(_ *vpcmanager.VpcHandler, funcSvcID, patServiceID, podName string) bool {
							return false
						}),
				})
				return patches
			},
			wantFailNum:   1,
			wantExceedNum: 0,
		}, // retry to pull up trigger deploy when update deploy error
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PodFetcher{
				workerClient:         tt.fields.workerClient,
				VpcHandler:           tt.fields.VpcHandler,
				PoolMgr:              tt.fields.PoolMgr,
				KubeClient:           tt.fields.KubeClient,
				podLister:            tt.fields.podLister,
				Queue:                tt.fields.Queue,
				stopCh:               tt.fields.stopCh,
				contexts:             tt.fields.contexts,
				contextsMutex:        tt.fields.contextsMutex,
				podGeneralizeTimeout: tt.fields.podGeneralizeTimeout,
				httpsEnable:          tt.fields.httpsEnable,
				IdleRetryInfo:        tt.fields.IdleRetryInfo,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			got, got1 := p.ProvisionWorkerPod(tt.args.functionMeta, tt.args.requiredMinInsNum, tt.args.crSignature, Reserved)
			assert.Equalf(t, tt.wantFailNum, got, "ProvisionWorkerPod(%v, %v, %v)", tt.args.functionMeta, tt.args.requiredMinInsNum, tt.args.crSignature)
			assert.Equalf(t, tt.wantExceedNum, got1, "ProvisionWorkerPod(%v, %v, %v)", tt.args.functionMeta, tt.args.requiredMinInsNum, tt.args.crSignature)
		})
	}
}

func Test_DoAction1(t *testing.T) {

	convey.Convey("DoAction case: metadata.Status == cache.IsChanged", t, func() {

		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*PodFetcher).CancelAndCreateNewContext, func(_ *PodFetcher, _ string) context.Context {
				ctx, _ := context.WithCancel(context.Background())
				return ctx
			}),
			gomonkey.ApplyFunc(tools.PackRequestInfo, func(functionSvc *faasfunction.FunctionService, applier string, traceID string) *config.RequestInfo {
				return &config.RequestInfo{}
			}),
		})
		defer patches.ResetAll()
		p := &PodFetcher{}
		funcSvcID := "123"
		var actions []cache.Action
		metadata := cache.Metadata{
			Status: cache.IsChanged,
		}
		p.DoActions(funcSvcID, actions, metadata)
	})
}

func Test_DoAction3(t *testing.T) {
	convey.Convey("DoAction case: action.Type == cache.ActionIdleModeChanged", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*PodFetcher).migrateInstance, func(_ *PodFetcher, _ cache.Metadata, _ v1alpha1.WorkerInstance,
				_ *corev1.Pod) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
				ctx, _ := context.WithCancel(context.Background())
				return ctx
			}),
			gomonkey.ApplyFunc((*IdleRetryList).updateCtxAndRetryNum, func(_ *IdleRetryList, podName string) (context.Context, int) {
				ctx, _ := context.WithCancel(context.Background())
				return ctx, 0
			}),
			gomonkey.ApplyFunc((*PodFetcher).SendIdleModeRequestByRetry, func(_ *PodFetcher, ctx context.Context, instance v1alpha1.WorkerInstance, idleMode bool,
				traceID string, retryNum int) {
			}),
			gomonkey.ApplyFunc(tools.PackRequestInfo, func(functionSvc *faasfunction.FunctionService, applier string, traceID string) *config.RequestInfo {
				return &config.RequestInfo{}
			}),
		})
		defer patches.ResetAll()
		p := &PodFetcher{IdleRetryInfo: &IdleRetryList{}}
		funcSvcID := "123"
		var actions []cache.Action
		var need []v1alpha1.WorkerInstance
		need = append(need, v1alpha1.WorkerInstance{PodName: "123"})
		actions = append(actions, cache.Action{Type: cache.ActionIdleModeChanged, NeedIdle: need})
		metadata := cache.Metadata{}
		p.DoActions(funcSvcID, actions, metadata)
	})
}

func Test_DoAction4(t *testing.T) {
	convey.Convey("DoAction case:  action.Type == cache.ActionPull, metadata.Delay == 0 ", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*PodFetcher).migrateInstance, func(_ *PodFetcher, _ cache.Metadata, _ v1alpha1.WorkerInstance,
				_ *corev1.Pod) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
				ctx, _ := context.WithCancel(context.Background())
				return ctx
			}),
			gomonkey.ApplyFunc((*IdleRetryList).updateCtxAndRetryNum, func(_ *IdleRetryList, podName string) (context.Context, int) {
				ctx, _ := context.WithCancel(context.Background())
				return ctx, 0
			}),
			gomonkey.ApplyFunc((*PodFetcher).SendIdleModeRequestByRetry, func(_ *PodFetcher, ctx context.Context, instance v1alpha1.WorkerInstance, idleMode bool,
				traceID string, retryNum int) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).provision, func(_ *PodFetcher, _ *faasfunction.FunctionService,
				_ int, _ string, _ time.Duration) {
			}),
			gomonkey.ApplyFunc(tools.PackRequestInfo, func(functionSvc *faasfunction.FunctionService, applier string, traceID string) *config.RequestInfo {
				return &config.RequestInfo{}
			}),
		})
		defer patches.ResetAll()
		p := &PodFetcher{IdleRetryInfo: &IdleRetryList{}}
		funcSvcID := "123"
		var actions []cache.Action
		var need []v1alpha1.WorkerInstance
		need = append(need, v1alpha1.WorkerInstance{PodName: "123"})
		actions = append(actions, cache.Action{Type: cache.ActionPull, NeedPull: 1})
		metadata := cache.Metadata{FuncMeta: &faasfunction.FunctionService{}, Delay: 0}
		p.DoActions(funcSvcID, actions, metadata)
	})
}

func Test_DoAction5(t *testing.T) {
	convey.Convey("DoAction case:  action.Type == cache.ActionStatefulInstanceRePull", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*PodFetcher).migrateInstance, func(_ *PodFetcher, _ cache.Metadata, _ v1alpha1.WorkerInstance,
				_ *corev1.Pod) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
				ctx, _ := context.WithCancel(context.Background())
				return ctx
			}),
			gomonkey.ApplyFunc((*IdleRetryList).updateCtxAndRetryNum, func(_ *IdleRetryList, podName string) (context.Context, int) {
				ctx, _ := context.WithCancel(context.Background())
				return ctx, 0
			}),
			gomonkey.ApplyFunc((*PodFetcher).SendIdleModeRequestByRetry, func(_ *PodFetcher, ctx context.Context, instance v1alpha1.WorkerInstance, idleMode bool,
				traceID string, retryNum int) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).provision, func(_ *PodFetcher, _ *faasfunction.FunctionService,
				_ int, _ string, _ time.Duration) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).statefulInstanceReload, func(_ *PodFetcher, _ string, _ cache.Action, _ cache.Metadata) {}),
			gomonkey.ApplyFunc((*PodFetcher).microServiceInstanceReload, func(_ *PodFetcher, _ string, _ cache.Action, _ cache.Metadata) {}),
			gomonkey.ApplyFunc(tools.PackRequestInfo, func(functionSvc *faasfunction.FunctionService, applier string, traceID string) *config.RequestInfo {
				return &config.RequestInfo{}
			}),
		})
		defer patches.ResetAll()
		p := &PodFetcher{IdleRetryInfo: &IdleRetryList{}}
		funcSvcID := "123"
		var actions []cache.Action
		var need []v1alpha1.WorkerInstance
		need = append(need, v1alpha1.WorkerInstance{PodName: "123"})
		actions = append(actions, cache.Action{Type: cache.ActionStatefulInstanceRePull, NeedPull: 1})
		metadata := cache.Metadata{FuncMeta: &faasfunction.FunctionService{}, Delay: 0}
		p.DoActions(funcSvcID, actions, metadata)
	})
}

func Test_statefulInstanceReload(t *testing.T) {
	convey.Convey("DoAction case: statefulInstanceReload action.NeedPull <= 0", t, func() {
		p := &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}}
		funcSvcID := "123"
		action := cache.Action{Type: cache.ActionStatefulInstanceRePull, NeedPull: 0}
		metadata := cache.Metadata{FuncMeta: &faasfunction.FunctionService{}, Delay: 0}
		p.statefulInstanceReload(funcSvcID, action, metadata)
	})

	convey.Convey("DoAction case: statefulInstanceReload action.NeedPull <= 0", t, func() {
		p := &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}}
		funcSvcID := "123"
		action := cache.Action{Type: cache.ActionStatefulInstanceRePull, NeedPull: 1}
		metadata := cache.Metadata{}
		p.statefulInstanceReload(funcSvcID, action, metadata)
	})
	convey.Convey("DoAction case: statefulInstanceReload action.NeedPull <= 0", t, func() {
		p := &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}}
		funcSvcID := "123"
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*PodFetcher).WithContext, func(p *PodFetcher, f decorateFunc, requestInfo *config.RequestInfo,
				functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
				return nil, errors.New("error")
			}),
		})
		defer patches.ResetAll()
		action := cache.Action{Type: cache.ActionStatefulInstanceRePull, NeedPull: 1}
		metadata := cache.Metadata{FuncMeta: &faasfunction.FunctionService{}, Delay: 0}
		p.statefulInstanceReload(funcSvcID, action, metadata)
	})
}

func Test_microServiceReload(t *testing.T) {
	convey.Convey("DoAction case: microServiceReload action.NeedPull <= 0", t, func() {
		p := &PodFetcher{}
		funcSvcID := "123"
		action := cache.Action{Type: cache.ActionMicroServiceInstanceRePull, NeedPull: 0}
		metadata := cache.Metadata{FuncMeta: &faasfunction.FunctionService{}, Delay: 0}
		p.microServiceInstanceReload(funcSvcID, action, metadata)
	})

	convey.Convey("DoAction case: microServiceReload, metadata is nil", t, func() {
		p := &PodFetcher{}
		funcSvcID := "123"
		action := cache.Action{Type: cache.ActionMicroServiceInstanceRePull, NeedPull: 1}
		metadata := cache.Metadata{}
		p.microServiceInstanceReload(funcSvcID, action, metadata)
	})

	convey.Convey("DoAction case: microServiceReload, mock GeneralizePod", t, func() {
		isMocked := false
		p := &PodFetcher{}
		funcSvcID := "123"
		patch := gomonkey.ApplyFunc((*PodFetcher).WithContext, func(p *PodFetcher, f decorateFunc, requestInfo *config.RequestInfo,
			functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
			// check request info is MicroServiceReloadRequestInfo
			if requestInfo.Applier == constants.ASBResApplier && requestInfo.IsReload {
				isMocked = true
			}
			return nil, errors.New("error")
		})
		patch.Reset()
		action := cache.Action{Type: cache.ActionMicroServiceInstanceRePull, NeedPull: 1}
		metadata := cache.Metadata{FuncMeta: &faasfunction.FunctionService{}, Delay: 0}
		p.microServiceInstanceReload(funcSvcID, action, metadata)
		convey.ShouldBeTrue(isMocked)
	})
}

func TestPodFetcher_ProvisionWorkerPod(t *testing.T) {
	tests := []struct {
		name           string
		requiredInsNum int
		failed         int
		exceed         int
		patchesFunc    mockUtils.PatchesFunc
	}{
		{
			name:           "failed and not snerror",
			requiredInsNum: 2,
			failed:         2,
			exceed:         0,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).PullUpInstance,
						func(_ *PodFetcher, service *faasfunction.FunctionService, pullType PullType) error {
							return poolmanager.ErrRequestExceedLimit
						}),
				})
				return patches
			},
		},
		{
			name:           "failed and snerror, but resource is not exceed",
			requiredInsNum: 2,
			failed:         2,
			exceed:         0,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).PullUpInstance,
						func(_ *PodFetcher, service *faasfunction.FunctionService, pullType PullType) error {
							return snerror.New(snerror.GettingPodErrorCode, "some create pod error")
						}),
				})
				return patches
			},
		},
		{
			name:           "failed and snerror, and resource is exceed",
			requiredInsNum: 2,
			failed:         2,
			exceed:         2,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).PullUpInstance,
						func(_ *PodFetcher, service *faasfunction.FunctionService, pullType PullType) error {
							return snerror.New(snerror.GettingPodErrorCode, poolmanager.ErrRequestExceedLimit.Error())
						}),
				})
				return patches
			},
		},
	}

	p := &PodFetcher{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			failed, exceed := p.ProvisionWorkerPod(&faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: tt.name}}, tt.requiredInsNum, "", Reserved)
			assert.Equal(t, tt.failed, failed, tt.name)
			assert.Equal(t, tt.exceed, exceed, tt.name)
			patches.ResetAll()
		})
	}
}

func TestPullType(t *testing.T) {
	tests := []struct {
		name     string
		pullType PullType

		expectedName    string
		expectedApplier string
	}{
		{
			name:            "reserved",
			pullType:        Reserved,
			expectedName:    "reserved",
			expectedApplier: constants.WorkerManagerApplier,
		},
		{
			name:            "cae rolling on-demand",
			pullType:        CAERollingOnDemand,
			expectedName:    "rolling-demand",
			expectedApplier: constants.ASBResApplier,
		},
		{
			name:            "invalid pull type",
			pullType:        -1,
			expectedName:    "invalid pullType(-1)",
			expectedApplier: "invalid pullType(-1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedName, tt.pullType.String(), tt.name)
			assert.Equal(t, tt.expectedApplier, tt.pullType.Applier(), tt.name)
		})
	}
}

func TestPullType_CheckPullErr(t *testing.T) {
	tests := []struct {
		name     string
		pullType PullType
		err      error

		expected bool
	}{
		{
			name:     "invalid pull type",
			pullType: -1,
			err:      nil,

			expected: true,
		},
		{
			name:     "reserved no error",
			pullType: Reserved,
			err:      nil,
			expected: false,
		},
		{
			name:     "reserved reach max instance",
			pullType: Reserved,
			err:      tools.ErrReachMaxInstanceNum,
			expected: true,
		},
		{
			name:     "reserved some error",
			pullType: Reserved,
			err:      errors.New("some error"),
			expected: true,
		},
		{
			name:     "cae rolling on demand reach max instance",
			pullType: CAERollingOnDemand,
			err:      tools.ErrReachMaxInstanceNum,
			expected: false,
		},
		{
			name:     "cae rolling on demand reach tenant max instance",
			pullType: CAERollingOnDemand,
			err:      tools.ErrReachMaxOnDemandInstanceNumPerTenant,
			expected: false,
		},
		{
			name:     "cae rolling on demand some error",
			pullType: CAERollingOnDemand,
			err:      errors.New("some error"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.pullType.CheckPullErr(tt.err), tt.name)
		})
	}
}

func TestPodFetcher_Rolling(t *testing.T) {
	p := &PodFetcher{}
	_, _ = cache.WorkerCache.HandleInsMetadata("test_rolling", config.InstanceMetaData{
		MinInstance: 9, MaxInstance: 1000, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	pullDone := make(chan int)
	rollingNum := make([]int, 0)
	defer close(pullDone)

	delayTime := make([]time.Duration, 0)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(p), "ProvisionWorkerPod", func(_ *PodFetcher, functionMeta *faasfunction.FunctionService, requiredMinInsNum int,
			crSignature string, pullType PullType) (int, int) {
			failed := <-pullDone
			rollingNum = append(rollingNum, requiredMinInsNum)
			return failed, 0
		}),
		gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
			ch := make(chan time.Time, 1)
			ch <- time.Time{}
			delayTime = append(delayTime, d)
			return &time.Timer{C: ch}
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	go func() {
		p.Rolling(ctx, 9, 50, Reserved, cache.Metadata{FuncMeta: &faasfunction.FunctionService{
			ObjectMeta: faasfunction.ObjectMeta{ID: "test_rolling"},
		}})
		wg.Done()
	}()

	// rolling step 4,4,1
	pullDone <- 1 // rolling 4 failed 1 delay
	pullDone <- 0 // retry rolling 1 failed 0
	pullDone <- 0 // rolling 4 failed 0
	pullDone <- 1 // rolling 1 failed 1 delay
	pullDone <- 1 // retry rolling 1 failed 1 delay*2
	pullDone <- 1 // retry rolling 1 failed 1 delay*4
	pullDone <- 0 // rolling 1 failed 0

	wg.Wait()
	assert.Equal(t, []int{4, 1, 4, 1, 1, 1, 1}, rollingNum)
	assert.Equal(t, []time.Duration{tools.BaseDelayForReservedIns, tools.BaseDelayForReservedIns, tools.BaseDelayForReservedIns * 2, tools.BaseDelayForReservedIns * 4}, delayTime)
}

func TestPodFetcher_Rolling_Disable(t *testing.T) {
	p := &PodFetcher{}
	_, _ = cache.WorkerCache.HandleInsMetadata("test_rolling_disable", config.InstanceMetaData{
		MinInstance: 0, MaxInstance: 1000, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	pullDone := make(chan int)
	rollingNum := make([]int, 0)
	defer close(pullDone)

	delayTime := make([]time.Duration, 0)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(p), "ProvisionWorkerPod", func(_ *PodFetcher, functionMeta *faasfunction.FunctionService, requiredMinInsNum int,
			crSignature string, pullType PullType) (int, int) {
			failed := <-pullDone
			rollingNum = append(rollingNum, requiredMinInsNum)
			return failed, 0
		}),
		gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
			ch := make(chan time.Time, 1)
			ch <- time.Time{}
			delayTime = append(delayTime, d)
			return &time.Timer{C: ch}
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	go func() {
		p.Rolling(ctx, 9, 50, Reserved, cache.Metadata{FuncMeta: &faasfunction.FunctionService{
			ObjectMeta: faasfunction.ObjectMeta{ID: "test_rolling_disable"},
		}})
		wg.Done()
	}()

	// rolling step 4,4,1
	pullDone <- 3 // rolling 4 failed 3 delay

	wg.Wait()
	assert.Equal(t, []int{4}, rollingNum)
	assert.Equal(t, []time.Duration{}, delayTime)
}

func TestPodFetcher_RollingAction(t *testing.T) {
	p := &PodFetcher{
		contexts: map[string]*Ctx{
			"func1": {ctx: context.Background()},
		},
	}
	rollingCount := uint64(0)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(p), "Rolling", func(_ *PodFetcher, ctx context.Context, totalNeed, percentage int, pullType PullType,
			metadata cache.Metadata) {
			atomic.AddUint64(&rollingCount, 1)
		}),
	}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	tests := []struct {
		name      string
		funcSvcID string
		action    cache.Action
		metadata  cache.Metadata

		expectedRollingCount uint64
	}{
		{
			name:                 "func not exits",
			funcSvcID:            "funcxxxx",
			action:               cache.Action{},
			metadata:             cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
			expectedRollingCount: 0,
		},
		{
			name:                 "normal case",
			funcSvcID:            "func1",
			action:               cache.Action{},
			metadata:             cache.Metadata{FuncMeta: &faasfunction.FunctionService{}},
			expectedRollingCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreUint64(&rollingCount, 0)
			p.RollingAction(tt.funcSvcID, tt.action, tt.metadata)
			assert.Equal(t, tt.expectedRollingCount, atomic.LoadUint64(&rollingCount), tt.name)
		})
	}
}

func Test_DoAction_Silent_Function(t *testing.T) {
	convey.Convey("DoAction case:  action.Type == cache.ActionPull, metadata.FuncMeta == nil ", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*PodFetcher).migrateInstance, func(_ *PodFetcher, _ cache.Metadata, _ v1alpha1.WorkerInstance,
				_ *corev1.Pod) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
				ctx, _ := context.WithCancel(context.Background())
				return ctx
			}),
			gomonkey.ApplyFunc((*IdleRetryList).updateCtxAndRetryNum, func(_ *IdleRetryList, podName string) (context.Context, int) {
				ctx, _ := context.WithCancel(context.Background())
				return ctx, 0
			}),
			gomonkey.ApplyFunc((*PodFetcher).SendIdleModeRequestByRetry, func(_ *PodFetcher, ctx context.Context, instance v1alpha1.WorkerInstance, idleMode bool,
				traceID string, retryNum int) {
			}),
			gomonkey.ApplyFunc((*PodFetcher).provision, func(_ *PodFetcher, _ *faasfunction.FunctionService,
				_ int, _ string, _ time.Duration) {
			}),
			gomonkey.ApplyFunc(tools.PackRequestInfo, func(functionSvc *faasfunction.FunctionService, applier string, traceID string) *config.RequestInfo {
				return &config.RequestInfo{}
			}),
			gomonkey.ApplyFunc(cache.UpdateSilentFuncCache, func(funcName, businessID, tenantID, version string) {
				return
			}),
		})
		defer patches.ResetAll()
		p := &PodFetcher{IdleRetryInfo: &IdleRetryList{}}
		funcSvcID := "123"
		var actions []cache.Action
		var need []v1alpha1.WorkerInstance
		need = append(need, v1alpha1.WorkerInstance{PodName: "123"})
		actions = append(actions, cache.Action{Type: cache.ActionPull, NeedPull: 1})
		metadata := cache.Metadata{FuncMeta: nil, Delay: 0}
		p.DoActions(funcSvcID, actions, metadata)
	})
}

func TestMarkWorkerInstanceWillDelete(t *testing.T) {
	podFetcher = &PodFetcher{}
	convey.Convey("TestMarkWorkerInstanceWillDelete ok", t, func() {
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID",
		}
		flag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				jsonBytes, _ := json.Marshal(workerIns)
				return string(jsonBytes), nil
			}),
			gomonkey.ApplyFunc(etcdhandler.PutInstanceToETCDWithErr,
				func(ctx context.Context, instance v1alpha1.WorkerInstance, operation string) error {
					flag = instance.WillDelete
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		_ = podFetcher.markWorkerInstanceWillDelete(workerIns, 3600)
		_ = podFetcher.markWorkerInstanceWillDelete(workerIns, 3600)
		convey.So(flag, convey.ShouldBeTrue)
		CancelWillDeleteTask("instanceID")
		time.Sleep(time.Millisecond)
	})
	convey.Convey("TestMarkWorkerInstanceWillDelete QueryETCDWithRetry err", t, func() {
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID1",
		}
		errMsg := "QueryETCDWithRetry error happened"
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				return "", errors.New(errMsg)
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		err := podFetcher.markWorkerInstanceWillDelete(workerIns, 3600)
		convey.So(err.Error(), convey.ShouldEqual, errMsg)
	})
	convey.Convey("TestMarkWorkerInstanceWillDelete WorkerInstance unmarshal err", t, func() {
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID2",
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				return "{", nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		err := podFetcher.markWorkerInstanceWillDelete(workerIns, 3600)
		convey.So(strings.Contains(err.Error(), "unexpected end of JSON input"), convey.ShouldBeTrue)
	})
	convey.Convey("TestMarkWorkerInstanceWillDelete ok", t, func() {
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID3",
		}
		errMsg := "QueryETCDWithRetry error happened"
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				jsonBytes, _ := json.Marshal(workerIns)
				return string(jsonBytes), nil
			}),
			gomonkey.ApplyFunc(etcdhandler.PutInstanceToETCDWithErr,
				func(ctx context.Context, instance v1alpha1.WorkerInstance, operation string) error {
					return errors.New(errMsg)
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		err := podFetcher.markWorkerInstanceWillDelete(workerIns, 3600)
		convey.So(err.Error(), convey.ShouldEqual, errMsg)
	})
}

func TestRunWillDeleteCheckTask(t *testing.T) {
	convey.Convey("TestRunWillDeleteCheckTask closeCh closed", t, func() {
		task := &willDeleteCheckTask{
			closeCh: make(chan interface{}),
		}
		close(task.closeCh)
		task.run()
	})
	convey.Convey("TestRunWillDeleteCheckTask timeout", t, func() {
		timer := time.NewTimer(10 * time.Millisecond)
		pf := &PodFetcher{}
		task := &willDeleteCheckTask{
			podFetcher: pf,
			closeCh:    make(chan interface{}),
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pf), "DeletePodByK8S",
				func(p *PodFetcher, podName, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context, instance v1alpha1.WorkerInstance) {}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		task.run()
	})
}

func TestClearWillDeleteInsRecord(t *testing.T) {
	convey.Convey("TestClearWillDeleteInsRecord ok", t, func() {
		timeCh := make(chan time.Time)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{
					C: timeCh,
				}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		willDeletedInstance.Store("aKey", int64(1))
		timestampIf, _ := willDeletedInstance.Load("aKey")
		timestamp, _ := timestampIf.(int64)
		convey.So(timestamp == 1, convey.ShouldBeTrue)
		ch := make(chan struct{})
		go clearWillDeleteInsRecord(ch)
		timeCh <- time.Time{}
		timeCh <- time.Time{}
		close(ch)
		timestampIf, _ = willDeletedInstance.Load("aKey")
		timestamp, _ = timestampIf.(int64)
		convey.So(timestamp == 0, convey.ShouldBeTrue)
	})
}
