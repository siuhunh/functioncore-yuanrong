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

package podfetcher

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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	k8cache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/types"
	commontypes "functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podhandler"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	tools "functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

func TestInitFuncRequest(t *testing.T) {
	convey.Convey("Test Init FuncRequest", t, func() {
		convey.Convey("success", func() {
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
			service := &faasfunction.FunctionService{}
			conf := &config.RequestInfo{}
			service.Spec.Configuration.Role.XRole = "xRole"
			service.Spec.Configuration.Role.AppXRole = "appRole"
			service.Spec.Configuration.DomainID = "domainID"
			commontypes, _ := initFuncRequest(service, conf)
			convey.So(commontypes.Message.Function.Role.XRole, convey.ShouldEqual, "xRole")
		})
	})

	convey.Convey("Test Init FuncRequest", t, func() {
		convey.Convey("success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(utils.IsCustomContainerSpec,
					func(*faasfunction.FunctionServiceSpec) bool {
						return true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			service := &faasfunction.FunctionService{}
			conf := &config.RequestInfo{
				SnapshotDeployment: &types.SnapshotDeploymentSpec{
					AppID: "111",
				},
			}
			service.Spec.Configuration.Role.XRole = "xRole"
			service.Spec.Configuration.Role.AppXRole = "appRole"
			service.Spec.Configuration.DomainID = "domainID"
			service.Spec.Configuration.DNSDomainCfg = []types.DNSDomainInfo{
				{
					ID: "1",
				},
			}
			commontypes, _ := initFuncRequest(service, conf)
			convey.So(commontypes.Message.Function.Role.XRole, convey.ShouldEqual, "xRole")
		})
	})
}

type FakePodInformer struct {
}

func (f FakePodInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f FakePodInformer) Lister() listercorev1.PodLister {
	return nil
}

func TestCreateClient(t *testing.T) {
	convey.Convey("Test Create Client", t, func() {
		convey.Convey("create failed", func() {
			stopCh := make(chan struct{})
			workerClient := &fhttp.Client{}
			err := CreatePodFetcher(workerClient, stopCh)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("create succeed", func() {
			defer ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}).
				ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
					return &poolmanager.PoolMgr{}
				}).
				ApplyFunc(utils.GetPodInformer, func() informercorev1.PodInformer {
					return FakePodInformer{}
				}).
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{
							AKey: "test",
							SKey: "test",
						},
					}, nil
				}).
				ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
					return &vpcmanager.VpcHandler{}, nil
				}).
				ApplyFunc((*alarm.Client).RegisterAlarmType,
					func(a *alarm.Client, almType alarm.Type) {
						return
					}).Reset()
			stopCh := make(chan struct{})
			workerClient := &fhttp.Client{}
			err := CreatePodFetcher(workerClient, stopCh)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestGetPodFromNodeManager(t *testing.T) {
	convey.Convey("Test Get Pod From Node Manager", t, func() {
		p := &PodFetcher{
			PoolMgr: &poolmanager.PoolMgr{},
		}
		var ctx context.Context
		funSvc := &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					Resource: faasfunction.ResourceSpec{
						CPU: 10,
						Mem: 4,
					},
					AutoScalerConfig: faasfunction.AutoScalerConfig{
						InstanceType: "vip",
					},
				},
			},
		}
		patch := gomonkey.ApplyFunc((*podhandler.Manager).FilterByPodCreateReq,
			func(_ *podhandler.Manager, req *podhandler.PodCreateReq) podhandler.PodHandler {
				return nil
			})
		patch2 := gomonkey.ApplyFunc(config.GetK8sCidr,
			func() *config.K8sCidr {
				return &config.K8sCidr{}
			})
		defer patch.Reset()
		defer patch2.Reset()
		req := &config.RequestInfo{
			TraceID:     "aaa",
			CPU:         0,
			Memory:      0,
			ForceDeploy: "true",
		}
		funSvc.Spec.Configuration.AutoScalerConfig.InstanceType = "vip"
		pod, _ := p.getPodFromNodeManager(ctx, funSvc, req)
		convey.So(pod, convey.ShouldBeNil)
	})
}

func TestInitRequestToWorker(t *testing.T) {
	convey.Convey("Test Init Request To Worker", t, func() {
		patches := [...]*Patches{
			ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					LocalAuth: &localauth.AuthConfig{},
				}, nil
			}),
			ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
				return &config.K8sCidr{
					SvcCidr: "256.256.256.256",
				}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				PodIP: "123",
			},
		}
		functionRequest := types.FunctionSpecRequest{}
		reqInfo := &config.RequestInfo{}
		httpsEnable := true
		req, response, err := initSpecializeRequest(pod, functionRequest, reqInfo, httpsEnable, false)
		convey.So(req, convey.ShouldNotBeNil)
		convey.So(response, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		convey.Convey("Test Init Duration", func() {
			functionRequest.Message.Function.Role.XRole = "role"
			req, response, err = initSpecializeRequest(pod, functionRequest, reqInfo, httpsEnable, false)
			convey.So(req, convey.ShouldNotBeNil)
			convey.So(response, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestInitDuration(t *testing.T) {
	convey.Convey("Test Init Duration", t, func() {
		chooseStartTime := time.Time{}
		chooseEndTime := time.Second
		workerDuration := "a"
		convey.So(initDuration(chooseStartTime, chooseEndTime, workerDuration), convey.ShouldNotBeNil)
		convey.Convey("Test Init Duration", func() {
			workerDuration = ""
			convey.So(initDuration(chooseStartTime, chooseEndTime, workerDuration), convey.ShouldNotBeNil)
		})
	})
}

func TestProcessItem(t *testing.T) {
	convey.Convey("Test Process Item", t, func() {
		q := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},
		))
		p := &PodFetcher{
			Queue: queue{
				RateLimitingInterface: q,
				processingQueue:       make(map[string]interface{}, utils.DefaultMapSize),
			},
		}
		p.Queue.Add("a")
		p.processItem()
	})
}

func TestPodFetcher_GeneralizePodInternal(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		KubeClient           *kubernetes.Clientset
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		ctx     context.Context
		reqInfo *config.RequestInfo
		service *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with success",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*PodFetcher).generalizeRequestToWorker,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo,
							service *faasfunction.FunctionService, chooseStartTime time.Time) error {
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-func-vpc-pat-container-ip",
								},
							}
							requestInfo.EndpointTalentPatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-endpoint-pat-container-ip",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandlePatServiceList,
						func(_ *vpcmanager.VpcHandler, tenantID, funcSvcID string,
							patPodInfo []types.NATConfigure, podName string) {
							return
						}),
				})
				return patches
			},
			wantErr: false,
		}, // test with success
		{
			name: "test with HandleVpcFunctionInfo ErrNoOperationalPermissionsVpc error",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							return utils.ErrNoOperationalPermissionsVpc
						}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S,
						func(_ *PodFetcher, podName, traceID string) error {
							return errors.New("mock delete pod by k8s error")
						}),
				})
				return patches
			},
			wantErr: true,
		}, // test with HandleVpcFunctionInfo ErrNoOperationalPermissionsVpc error
		{
			name: "test with HandleVpcFunctionInfo ErrNoAvailableVpcPatInstance error",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							return utils.ErrNoAvailableVpcPatInstance
						}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S,
						func(_ *PodFetcher, podName, traceID string) error {
							return errors.New("mock delete pod by k8s error")
						}),
				})
				return patches
			},
			wantErr: true,
		}, // test with HandleVpcFunctionInfo ErrNoAvailableVpcPatInstance error
		{
			name: "test with success",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*PodFetcher).generalizeRequestToWorker,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo,
							service *faasfunction.FunctionService, chooseStartTime time.Time) error {
							return errors.New("mock generalizeRequestToWorker error")
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
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandlePatServiceList,
						func(_ *vpcmanager.VpcHandler, tenantID, funcSvcID string,
							patPodInfo []types.NATConfigure, podName string) {
							return
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							return errors.New("mock IdleDeletePatServiceList error")
						}),
				})
				return patches
			},
			wantErr: true,
		}, // test with generalizeRequestToWorker error
		{
			name: "test with vpc item not found error",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							return utils.ErrVPCNotFound
						}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S,
						func(_ *PodFetcher, podName, traceID string) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test with custom container success",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
							CustomContainerConfig: types.CustomContainerConfig{
								ControlPath: "ControlPath",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*PodFetcher).generalizePod,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo, service *faasfunction.FunctionService,
							chooseStartTime time.Time, enableCircuitBreaker bool) error {
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test with custom container handle vpc failed",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
							CustomContainerConfig: types.CustomContainerConfig{
								ControlPath: "ControlPath",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*PodFetcher).generalizePod,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo, service *faasfunction.FunctionService,
							chooseStartTime time.Time, enableCircuitBreaker bool) error {
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							return utils.ErrVPCNotFound
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test with custom container create pod failed",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
							CustomContainerConfig: types.CustomContainerConfig{
								ControlPath: "ControlPath",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return nil, errors.New("failed")
						}),
					gomonkey.ApplyFunc((*PodFetcher).generalizePod,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo, service *faasfunction.FunctionService,
							chooseStartTime time.Time, enableCircuitBreaker bool) error {
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PodFetcher{
				workerClient:         tt.fields.workerClient,
				KubeClient:           tt.fields.KubeClient,
				VpcHandler:           tt.fields.VpcHandler,
				PoolMgr:              tt.fields.PoolMgr,
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
			_, err := p.GeneralizePodInternal(tt.args.ctx, tt.args.reqInfo, tt.args.service)
			patches.ResetAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func TestPodFetcher_GeneralizePodInternalWithSpan(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		KubeClient           *kubernetes.Clientset
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		ctx     context.Context
		reqInfo *config.RequestInfo
		service *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with error with get pod from pool span",
			fields: fields{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args: args{
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-func-name",
					TraceID:      "mock-trace-id",
					DeployNode:   "mock-request-node",
				},
				service: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mock-service-id",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "mock-func-vpc-id",
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*PodFetcher).generalizeRequestToWorker,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo,
							service *faasfunction.FunctionService, chooseStartTime time.Time) error {
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-func-vpc-pat-container-ip",
								},
							}
							requestInfo.EndpointTalentPatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-endpoint-pat-container-ip",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandlePatServiceList,
						func(_ *vpcmanager.VpcHandler, tenantID, funcSvcID string,
							patPodInfo []types.NATConfigure, podName string) {
							return
						}),
					gomonkey.ApplyFunc(tracer.EnableCommonTracer, func() bool {
						return true
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test with error with get pod from pool span
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PodFetcher{
				workerClient:         tt.fields.workerClient,
				KubeClient:           tt.fields.KubeClient,
				VpcHandler:           tt.fields.VpcHandler,
				PoolMgr:              tt.fields.PoolMgr,
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
			_, err := p.GeneralizePodInternal(tt.args.ctx, tt.args.reqInfo, tt.args.service)
			patches.ResetAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func TestPodFetcherSendIdleModeRequestToWorker(t *testing.T) {
	convey.Convey("Test Send Generalize Request To Worker", t, func() {
		instance := v1alpha1.WorkerInstance{}
		p := &PodFetcher{
			workerClient:  &fhttp.Client{},
			IdleRetryInfo: &IdleRetryList{},
		}
		patches := [...]*Patches{
			ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					LocalAuth: &localauth.AuthConfig{},
				}, nil
			}),
			ApplyMethod(reflect.TypeOf(p.workerClient), "DoTimeout", func(f *fhttp.Client, req *fhttp.Request,
				resp *fhttp.Response, timeout time.Duration) error {
				return nil
			}),
			ApplyFunc(cache.CheckReserveInstanceExist, func(string) bool {
				return true
			}),
			ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
				func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
					return
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		ctx, _ := context.WithCancel(context.Background())
		p.SendIdleModeRequestByRetry(ctx, instance, false, "mock-traceID", 0)
		err := p.sendIdleModeRequestToWorker(instance, false, "mock-traceID", 3*time.Second)
		convey.So(err, convey.ShouldBeNil)
	})

	type args struct {
		instance v1alpha1.WorkerInstance
		idleMode bool
		traceID  string
		timeout  time.Duration
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to send idle mode,get config error", &PodFetcher{PoolMgr: &poolmanager.PoolMgr{},
			IdleRetryInfo: &IdleRetryList{}},
			args{
				instance: v1alpha1.WorkerInstance{},
				idleMode: false,
				traceID:  "abc",
				timeout:  0}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, errors.New("test")
					}),
					ApplyFunc(cache.CheckReserveInstanceExist, func(string) bool {
						return true
					}),
					ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
						func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
							return
						}),
					ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
						func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
							return
						}),
					ApplyMethodSeq(reflect.TypeOf(&PodFetcher{}), "SendIdleModeRequestToWorkerInternal", []OutputCell{
						{Values: Params{nil}},
					}),
					ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
				})
				return patches
			}},

		{"case2 failed to send idle mode,DoTimeout error", &PodFetcher{IdleRetryInfo: &IdleRetryList{}}, args{
			instance: v1alpha1.WorkerInstance{},
			idleMode: false,
			traceID:  "abc",
			timeout:  0,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{
						AKey:     "000",
						SKey:     "000",
						Duration: 5,
					}}, nil
				}),
				ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return errors.New("test")
					}),
				ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
					resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
					resp.Header.SetStatusCode(http.StatusInternalServerError)
					return resp
				}),
				ApplyFunc(cache.CheckReserveInstanceExist, func(string) bool {
					return true
				}),
				ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
					func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
						return
					}),
				ApplyMethodSeq(reflect.TypeOf(&PodFetcher{}), "SendIdleModeRequestToWorkerInternal", []OutputCell{
					{Values: Params{nil}},
				}),
				ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					c := make(chan time.Time, 1)
					t := &time.Ticker{
						C: c,
					}
					c <- time.Now()
					return t
				}),
			})
			return patches
		}},

		{"case3 failed to send idle mode,resp status code not 200, unmarshal error", &PodFetcher{IdleRetryInfo: &IdleRetryList{}}, args{
			instance: v1alpha1.WorkerInstance{},
			idleMode: false,
			traceID:  "abc",
			timeout:  0,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{
						AKey:     "000",
						SKey:     "000",
						Duration: 5,
					}}, nil
				}),
				ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return nil
					}),
				ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
					resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
					resp.Header.SetStatusCode(http.StatusInternalServerError)
					return resp
				}),
				ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					return errors.New("test")
				}),
				ApplyFunc(cache.CheckReserveInstanceExist, func(string) bool {
					return true
				}),
				ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
					func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
						return
					}),
				ApplyMethodSeq(reflect.TypeOf(&PodFetcher{}), "SendIdleModeRequestToWorkerInternal", []OutputCell{
					{Values: Params{nil}},
				}),
				ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					c := make(chan time.Time, 1)
					t := &time.Ticker{
						C: c,
					}
					c <- time.Now()
					return t
				}),
			})
			return patches
		}},

		{"case4 failed to send idle mode,resp status code not 200, snerror", &PodFetcher{IdleRetryInfo: &IdleRetryList{}}, args{
			instance: v1alpha1.WorkerInstance{},
			idleMode: false,
			traceID:  "abc",
			timeout:  0,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{
						AKey:     "000",
						SKey:     "000",
						Duration: 5,
					}}, nil
				}),
				ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return nil
					}),
				ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
					resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
					resp.Header.SetStatusCode(http.StatusInternalServerError)
					body, _ := json.Marshal(snerror.BadResponse{Code: snerror.DownLoadCodeErrorCode, Message: "test"})
					resp.SetBody(body)
					return resp
				}),
				ApplyFunc(cache.CheckReserveInstanceExist, func(string) bool {
					return true
				}),
				ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
					func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
						return
					}),
				ApplyMethodSeq(reflect.TypeOf(&PodFetcher{}), "SendIdleModeRequestToWorkerInternal", []OutputCell{
					{Values: Params{nil}},
				}),
				ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					c := make(chan time.Time, 1)
					t := &time.Ticker{
						C: c,
					}
					c <- time.Now()
					return t
				}),
			})
			return patches
		}},

		{"case5 failed to send idle mode, cache is not exit, return", &PodFetcher{IdleRetryInfo: &IdleRetryList{}}, args{
			instance: v1alpha1.WorkerInstance{},
			idleMode: false,
			traceID:  "abc",
			timeout:  0,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{
						AKey:     "000",
						SKey:     "000",
						Duration: 5,
					}}, nil
				}),
				ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return nil
					}),
				ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
					resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
					resp.Header.SetStatusCode(http.StatusInternalServerError)
					body, _ := json.Marshal(snerror.BadResponse{Code: snerror.DownLoadCodeErrorCode, Message: "test"})
					resp.SetBody(body)
					return resp
				}),
				ApplyFunc(cache.CheckReserveInstanceExist, func(string) bool {
					return false
				}),
				ApplyMethod(reflect.TypeOf(&PodFetcher{}), "RemoveRetryAlarm",
					func(_ *PodFetcher, instance v1alpha1.WorkerInstance) {
						return
					}),
				ApplyMethodSeq(reflect.TypeOf(&PodFetcher{}), "SendIdleModeRequestToWorkerInternal", []OutputCell{
					{Values: Params{nil}},
				}),
				ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					c := make(chan time.Time, 1)
					t := &time.Ticker{
						C: c,
					}
					c <- time.Now()
					return t
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ctx, _ := context.WithCancel(context.Background())
			tt.p.SendIdleModeRequestByRetry(ctx, tt.args.instance, tt.args.idleMode, tt.args.traceID, 0)
			patches.ResetAll()
		})
	}
}

func TestSendGeneralizeRequestToWorker(t *testing.T) {
	convey.Convey("Test Send Generalize Request To Worker", t, func() {
		convey.Convey("case 1 success", func() {
			pod := &corev1.Pod{}
			reqInfo := &config.RequestInfo{}
			service := &faasfunction.FunctionService{}
			p := &PodFetcher{
				workerClient: &fhttp.Client{},
			}
			patches := [...]*Patches{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, nil
				}),
				ApplyMethod(reflect.TypeOf(p.workerClient), "DoTimeout", func(f *fhttp.Client, req *fhttp.Request,
					resp *fhttp.Response, timeout time.Duration) error {
					return nil
				}),
				ApplyFunc((*alarm.Client).ReportAlarm,
					func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
				ApplyFunc((*alarm.Client).ClearAlarm,
					func(a *alarm.Client, almType alarm.Type) { return }),
				ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
					return &config.K8sCidr{
						SvcCidr: "256.256.256.256",
					}
				}),
				ApplyFunc((*PodFetcher).safetyVpcPodSetting, func(_ *PodFetcher, _ *faasfunction.FunctionService) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			workerDuration, err := p.sendGeneralizeRequestToWorker(pod, reqInfo, service)
			convey.So(workerDuration, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case 2 success, logPaths size is larger than 20", func() {
			pod := &corev1.Pod{}
			reqInfo := &config.RequestInfo{}
			service := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						LogPolicy: commontypes.LogPolicy{
							LogPaths: constructLogPaths(),
						},
					},
				},
			}
			p := &PodFetcher{
				workerClient: &fhttp.Client{},
			}
			patches := [...]*Patches{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, nil
				}),
				ApplyMethod(reflect.TypeOf(p.workerClient), "DoTimeout", func(f *fhttp.Client, req *fhttp.Request,
					resp *fhttp.Response, timeout time.Duration) error {
					return nil
				}),
				ApplyFunc((*alarm.Client).ReportAlarm,
					func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
				ApplyFunc((*alarm.Client).ClearAlarm,
					func(a *alarm.Client, almType alarm.Type) { return }),
				ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
					return &config.K8sCidr{
						SvcCidr: "256.256.256.256",
					}
				}),
				ApplyFunc((*PodFetcher).safetyVpcPodSetting, func(_ *PodFetcher, _ *faasfunction.FunctionService) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			workerDuration, err := p.sendGeneralizeRequestToWorker(pod, reqInfo, service)
			convey.So(workerDuration, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	type args struct {
		pod     *corev1.Pod
		reqInfo *config.RequestInfo
		service *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		want        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to init specialize request for pod,get config error", &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}},
			args{
				pod:     &corev1.Pod{},
				reqInfo: &config.RequestInfo{},
				service: &faasfunction.FunctionService{},
			}, "", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, errors.New("test")
					}),
					ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{
							SvcCidr: "256.256.256.256",
						}
					}),
				})
				return patches
			}},

		{"case2 resp statuscode not 200, unmarshal error", &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}},
			args{
				pod:     &corev1.Pod{},
				reqInfo: &config.RequestInfo{},
				service: &faasfunction.FunctionService{
					Version: "version",
					Spec: faasfunction.FunctionServiceSpec{
						SnapshotDeployment: types.SnapshotDeploymentSpec{
							IsSnapshotExpire: true,
						},
						Configuration: faasfunction.ConfigurationSpec{
							Initializer: faasfunction.Initializer{Timeout: 1},
						}},
				},
			}, "", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&poolmanager.PoolMgr{}), "GetPod", func(_ *poolmanager.PoolMgr,
						ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest,
					) (*corev1.Pod, error) {
						return &corev1.Pod{}, nil
					}),
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{},
						}, nil
					}),
					ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
						resp.Header.Add(constants.HeaderLoadDuration, "1")
						return resp
					}),
					ApplyMethod(reflect.TypeOf(&fhttp.ResponseHeader{}), "StatusCode",
						func(_ *fhttp.ResponseHeader) int {
							return http.StatusInternalServerError
						}),
					ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return nil
						}),
					ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("test")
					}),
					ApplyFunc((*alarm.Client).ClearAlarm,
						func(a *alarm.Client, almType alarm.Type) { return }),
					ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{
							SvcCidr: "256.256.256.256",
						}
					}),
					ApplyFunc(etcd3.CreateEtcdCtxInfoWithTimeout,
						func(ctx context.Context, duration time.Duration) etcd3.EtcdCtxInfo {
							return etcd3.EtcdCtxInfo{}
						}),
					ApplyFunc(etcd3.GetMetaEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{Client: &clientv3.Client{}}
						}),
					ApplyFunc((*etcd3.EtcdClient).Put,
						func(client *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			}},

		{"case3 resp statuscode not 200, DownLoadCodeErrorCode", &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}},
			args{
				pod:     &corev1.Pod{},
				reqInfo: &config.RequestInfo{},
				service: &faasfunction.FunctionService{
					Version: "version",
					Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
						Initializer: faasfunction.Initializer{Timeout: 1},
					}},
				},
			}, "", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&poolmanager.PoolMgr{}), "GetPod", func(_ *poolmanager.PoolMgr,
						ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest,
					) (*corev1.Pod, error) {
						return &corev1.Pod{}, nil
					}),
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{},
						}, nil
					}),
					ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
						resp.Header.Add(constants.HeaderLoadDuration, "1")
						body, _ := json.Marshal(snerror.BadResponse{Code: snerror.DownLoadCodeErrorCode, Message: "test"})
						resp.SetBody(body)
						return resp
					}),
					ApplyFunc(etcd3.GetMetaEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{Client: &clientv3.Client{}}
						}),
					ApplyMethod(reflect.TypeOf(&fhttp.ResponseHeader{}), "StatusCode",
						func(_ *fhttp.ResponseHeader) int {
							return http.StatusInternalServerError
						}),
					ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return nil
						}),
					ApplyFunc((*alarm.Client).ReportAlarm,
						func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
					ApplyFunc((*alarm.Client).ClearAlarm,
						func(a *alarm.Client, almType alarm.Type) { return }),
					ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{
							SvcCidr: "256.256.256.256",
						}
					}),
					ApplyFunc((*PodFetcher).safetyVpcPodSetting, func(_ *PodFetcher, _ *faasfunction.FunctionService) {}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := tt.p.sendGeneralizeRequestToWorker(tt.args.pod, tt.args.reqInfo, tt.args.service)
			if (err != nil) != tt.wantErr {
				t.Errorf("sendGeneralizeRequestToWorker() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("sendGeneralizeRequestToWorker() got = %v, want %v", got, tt.want)
			}
			time.Sleep(100 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestSetLabelsAndOwnerReference(t *testing.T) {
	convey.Convey("Test Send Generalize Request To Worker", t, func() {
		p := &PodFetcher{}
		pod := &corev1.Pod{}
		reqInfo := &config.RequestInfo{}
		service := &faasfunction.FunctionService{}
		service.Labels = map[string]string{"a": "b"}
		patches := [...]*Patches{
			ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("fail")
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		err := p.SetLabelsAndOwnerReference(service, pod, reqInfo)
		convey.So(err, convey.ShouldNotBeNil)
	})

	type args struct {
		functionMeta *faasfunction.FunctionService
		workerIns    *corev1.Pod
		requestInfo  *config.RequestInfo
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 pod not found", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{Labels: map[string]string{"a": "b"}}},
			workerIns:   &corev1.Pod{},
			requestInfo: &config.RequestInfo{},
		}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Patch",
					func(_ *fake.FakePods, ctx context.Context, name string,
						pt k8stypes.PatchType, data []byte,
						opts metav1.PatchOptions, subresources ...string) (result *corev1.Pod, err error) {
						return &corev1.Pod{}, errors.New("not found")
					}),
				ApplyFunc(k8serror.IsNotFound,
					func(err error) bool {
						if err.Error() == "not found" {
							return true
						}
						return false
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			})
			return patches
		}},

		{"case2 pod patch error", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{Labels: map[string]string{"a": "b"}}},
			workerIns:   &corev1.Pod{},
			requestInfo: &config.RequestInfo{},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Patch",
					func(_ *fake.FakePods, ctx context.Context, name string,
						pt k8stypes.PatchType, data []byte,
						opts metav1.PatchOptions, subresources ...string) (result *corev1.Pod, err error) {
						return &corev1.Pod{}, errors.New("other error")
					}),
				ApplyFunc(k8serror.IsNotFound,
					func(err error) bool {
						if err.Error() == "not found" {
							return true
						}
						return false
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			})
			return patches
		}},

		{"case3 succeed to SetLabelsAndOwnerReference", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			functionMeta: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{Labels: map[string]string{"a": "b"}}},
			workerIns:   &corev1.Pod{},
			requestInfo: &config.RequestInfo{},
		}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Patch",
					func(_ *fake.FakePods, ctx context.Context, name string,
						pt k8stypes.PatchType, data []byte,
						opts metav1.PatchOptions, subresources ...string) (result *corev1.Pod, err error) {
						return &corev1.Pod{}, nil
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := tt.p.SetLabelsAndOwnerReference(tt.args.functionMeta, tt.args.workerIns, tt.args.requestInfo); (err != nil) != tt.wantErr {
				t.Errorf("SetLabelsAndOwnerReference() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func constructLogPaths() []string {
	var logPaths []string
	for i := 0; i < 30; i++ {
		logPaths = append(logPaths, "/tmp"+strconv.Itoa(i))
	}
	return logPaths
}

// test for delete processing event in queue by pod name
func Test_queue_DeleteEventInQueue(t *testing.T) {
	type fields struct {
		RateLimitingInterface workqueue.RateLimitingInterface
		mutex                 sync.Mutex
		processingQueue       map[string]interface{}
	}
	type args struct {
		podName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "case_01",
			fields: fields{
				RateLimitingInterface: nil,
				mutex:                 sync.Mutex{},
				processingQueue:       map[string]interface{}{},
			},
			args: args{
				podName: "pod_001",
			},
			want: "",
		},
		{
			name: "case_02 succeed to delete event",
			fields: fields{
				RateLimitingInterface: workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
					workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
					&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},
				)),
				mutex: sync.Mutex{},
				processingQueue: map[string]interface{}{"pod_002": CREvent{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}},
					functionService: &faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: "abc"}}}},
			},
			args: args{
				podName: "pod_002",
			},
			want: "abc",
		},
		{
			name: "case_03 event.(CREvent) false",
			fields: fields{
				RateLimitingInterface: workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
					workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
					&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},
				)),
				mutex:           sync.Mutex{},
				processingQueue: map[string]interface{}{"pod_002": "abc"},
			},
			args: args{
				podName: "pod_002",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &queue{
				RateLimitingInterface: tt.fields.RateLimitingInterface,
				mutex:                 tt.fields.mutex,
				processingQueue:       tt.fields.processingQueue,
			}
			if got := q.DeleteEventInQueue(tt.args.podName); got != tt.want {
				t.Errorf("DeleteEventInQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for get MaxStateSize in env
func Test_getMaxStateSize(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc func() []*Patches
		want        string
	}{
		{
			name: "case_01",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.Getenv, func(key string) string {
					return ""
				}))
				return patches
			},
			want: "256",
		},
		{
			name: "case_02",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.Getenv, func(key string) string {
					return "128"
				}))
				return patches
			},
			want: "128",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getMaxStateSize(); got != tt.want {
				t.Errorf("getMaxStateSize() = %v, want %v", got, tt.want)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

func TestCreatePodFetcher(t *testing.T) {
	type args struct {
		workerClient *fhttp.Client
		stopCh       <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to create podfetcher",
			args{workerClient: &fhttp.Client{}, stopCh: make(chan struct{})},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
						return &poolmanager.PoolMgr{}
					}),
					ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
						return &vpcmanager.VpcHandler{}, nil
					}),
					ApplyFunc(getGeneralizeTimeoutFromCfg,
						func() (int, error) {
							return 1, nil
						}),
					ApplyFunc(utils.GetPodInformer, func() informercorev1.PodInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Pods()
					}),
					ApplyFunc((*alarm.Client).RegisterAlarmType, func(a *alarm.Client, almType alarm.Type) {
						return
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := CreatePodFetcher(tt.args.workerClient, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("CreatePodFetcher() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_getGeneralizeTimeoutFromCfg(t *testing.T) {
	tests := []struct {
		name        string
		want1       int
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to get",
			defaultPodGeneralizeTimeout, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{
							AKey: "test", SKey: "test", Duration: 1,
						}, PodGeneralizeTimeout: defaultPodGeneralizeTimeout}, nil
					}),
				})
				return patches
			}},
		{"case2 failed to get, get config error",
			defaultPodGeneralizeTimeout, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{
							AKey: "test", SKey: "test", Duration: 1,
						}}, errors.New("test")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got1, err := getGeneralizeTimeoutFromCfg()
			if (err != nil) != tt.wantErr {
				t.Errorf("getGeneralizeTimeoutFromCfg() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got1 != tt.want1 {
				t.Errorf("getGeneralizeTimeoutFromCfg() got1 = %v, want %v", got1, tt.want1)
			}
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_processCREvent(t *testing.T) {
	type args struct {
		event interface{}
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to processCREvent", &PodFetcher{}, args{
			CREvent{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}},
				requestInfo: &config.RequestInfo{WorkerID: "test", TraceID: "test"}}},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*PodFetcher).SetLabelsAndOwnerReference, func(_ *PodFetcher,
						functionMeta *faasfunction.FunctionService, workerIns *corev1.Pod,
						requestInfo *config.RequestInfo) error {
						return nil
					}),
				})
				return patches
			}},
		{"case2 failed to processCREvent, set labels error", &PodFetcher{}, args{
			CREvent{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}},
				requestInfo: &config.RequestInfo{WorkerID: "test", TraceID: "test"}}},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*PodFetcher).SetLabelsAndOwnerReference, func(_ *PodFetcher,
						functionMeta *faasfunction.FunctionService, workerIns *corev1.Pod,
						requestInfo *config.RequestInfo) error {
						return errors.New("test")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := tt.p.processCREvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("processCREvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_GeneralizePod(t *testing.T) {
	q := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second), // for queue capacity
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},        // for queue rate
	))
	p := &PodFetcher{
		Queue: queue{
			RateLimitingInterface: q,
			processingQueue:       make(map[string]interface{}, utils.DefaultMapSize),
		},
	}
	p.Queue.Add("a")
	type args struct {
		ctx             context.Context
		requestInfo     *config.RequestInfo
		functionService *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		want        *corev1.Pod
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to GeneralizePod, getPodFromNodeManager error", &PodFetcher{
			PoolMgr: &poolmanager.PoolMgr{},
		}, args{ctx: context.Background(),
			requestInfo: &config.RequestInfo{WorkerID: "test", TraceID: "test"}, functionService: &faasfunction.FunctionService{}},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case2 succeed to GeneralizePod", p, args{ctx: context.Background(),
			requestInfo: &config.RequestInfo{WorkerID: "test", TraceID: "test"},
			functionService: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						NetworkController: types.NetworkController{
							DisablePublicNetwork: true,
						},
					},
				},
			}},
			nil, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*PodFetcher).GeneralizePodInternal, func(_ *PodFetcher,
						ctx context.Context, reqInfo *config.RequestInfo,
						service *faasfunction.FunctionService) (*corev1.Pod, error) {
						return &corev1.Pod{}, nil
					}),
					ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
						funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
						return
					}),
					ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
						return context.Background()
					}),
					ApplyFunc(etcdhandler.PutInstanceToETCD, func(ctx context.Context, instance v1alpha1.WorkerInstance) {
						return
					}),
				})
				return patches
			}},
		{"case3 no need to GeneralizePod when stateful function instance exists", p, args{ctx: context.Background(),
			requestInfo: &config.RequestInfo{WorkerID: "test", TraceID: "test"}, functionService: &faasfunction.FunctionService{}},
			nil, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(statefulFilter, func(requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						return &corev1.Pod{}, nil
					}),
				})
				return patches
			}},
		{"case4 need to GeneralizePod when stateful function instance exists in migrating phase", p, args{ctx: context.Background(),
			requestInfo: &config.RequestInfo{WorkerID: "test", TraceID: "test", IsMigrating: true},
			functionService: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						NetworkController: types.NetworkController{
							DisablePublicNetwork: true,
						},
					},
				},
			}},
			nil, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(statefulFilter, func(requestInfo *config.RequestInfo,
						functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
						return &corev1.Pod{}, errors.New("The method should not be called.")
					}),
					ApplyFunc((*PodFetcher).GeneralizePodInternal, func(_ *PodFetcher,
						ctx context.Context, reqInfo *config.RequestInfo,
						service *faasfunction.FunctionService) (*corev1.Pod, error) {
						return &corev1.Pod{}, nil
					}),
					ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
						funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
						return
					}),
					ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
						return context.Background()
					}),
					ApplyFunc(etcdhandler.PutInstanceToETCD, func(ctx context.Context, instance v1alpha1.WorkerInstance) {
						return
					}),
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := tt.p.GeneralizePod(tt.args.ctx, tt.args.requestInfo, tt.args.functionService)
			if (err != nil) != tt.wantErr {
				t.Errorf("GeneralizePod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestGeneralizePod_GetCAEBusInstance(t *testing.T) {
	convey.Convey("TestGeneralizePod_GetCAEBusInstance", t, func() {
		convey.Convey("TestGeneralizePod_GetCAEBusInstance success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc((*PodFetcher).GeneralizePodInternal, func(_ *PodFetcher,
					ctx context.Context, reqInfo *config.RequestInfo,
					service *faasfunction.FunctionService) (*corev1.Pod, error) {
					return &corev1.Pod{
						Status: corev1.PodStatus{
							PodIP: "127.1.1.1",
						},
						Spec: corev1.PodSpec{
							NodeName: "node001",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name: "pool25-800-768-fusion-86bf4c8b78-xqtqz",
						},
					}, nil
				}),
				ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
					funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					return
				}),
				ApplyFunc((*PodFetcher).CreateNewContext, func(_ *PodFetcher, funcSvcID string) context.Context {
					return context.Background()
				}),
				ApplyFunc(etcdhandler.PutInstanceToETCD, func(ctx context.Context, instance v1alpha1.WorkerInstance) {
					return
				}),
				ApplyFunc((*queue).AddRateLimited, func(_ *queue, event interface{}) {
					return
				}),
			})
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			podFetcher := &PodFetcher{
				PoolMgr: &poolmanager.PoolMgr{},
			}
			podFetcher.InitCAEBusInstanceMutexes()
			requestInfo := &config.RequestInfo{Applier: "functiontask-2ggqv",
				BusinessID: "businessID", FunctionName: "funcA", WorkerID: "test", TraceID: "test"}
			functionService := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						NetworkController: types.NetworkController{
							DisablePublicNetwork: true,
						},
					},
				},
			}
			functionService.Spec.Configuration.BusinessType = "CAE"
			pod, err := podFetcher.GeneralizePod(context.Background(), requestInfo, functionService)
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(pod.Name, convey.ShouldEqual, "pool25-800-768-fusion-86bf4c8b78-xqtqz")
			convey.So(pod.Status.PodIP, convey.ShouldEqual, "127.1.1.1")

			workerInstance := v1alpha1.WorkerInstance{
				Applier: "functiontask-AAA",
				IP:      "127.1.1.2",
				PodName: "pool25-800-768-fusion-86bf4c8b78-xqtqz2",
			}
			cache.WorkerCache.StoreWorkerInstance("busi-funcA--1103317980", workerInstance)
			pod, err = podFetcher.GeneralizePod(context.Background(), requestInfo, functionService)
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(pod.Name, convey.ShouldEqual, "pool25-800-768-fusion-86bf4c8b78-xqtqz2")
			convey.So(pod.Status.PodIP, convey.ShouldEqual, "127.1.1.2")
		})
	})
}

func TestPodFetcher_GetPodByName(t *testing.T) {
	type args struct {
		podName   string
		namespace string
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		want        *corev1.Pod
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to GetPodByName",
			&PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{podName: "case1", namespace: "default"},
			&corev1.Pod{},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get",
						func(_ *fake.FakePods, ctx context.Context,
							name string, opts metav1.GetOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
				})
				return patches
			}},
		{"case1 failed to GetPodByName",
			&PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{podName: "case1", namespace: "default"},
			nil,
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get",
						func(_ *fake.FakePods, ctx context.Context,
							name string, opts metav1.GetOptions) (*corev1.Pod, error) {
							return nil, errors.New("get pod error")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := tt.p.GetPodByName(tt.args.podName, tt.args.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPodByName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPodByName() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_DeletePod(t *testing.T) {
	type args struct {
		pod     *corev1.Pod
		traceID string
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 DeletionTimestamp is not nil",
			&PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{}}}, traceID: "a"},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case2 pod is nil",
			&PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{pod: nil, traceID: "a"},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case3 DeletePodByWorkerInstance",
			&PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{pod: &corev1.Pod{}, traceID: "a"},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
						func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
							return nil
						}),
					ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
						instance v1alpha1.WorkerInstance) {
						return
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.p.DeletePod(tt.args.pod, tt.args.traceID)
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_CreateNewContext(t *testing.T) {
	type args struct {
		functionServiceID string
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		want        context.Context
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to create new context", &PodFetcher{contexts: map[string]*Ctx{}}, args{"test"},
			nil, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got := tt.p.CreateNewContext(tt.args.functionServiceID)
			if !strings.Contains(fmt.Sprintf(" %s", got), commonutils.LayerListSep) {
				t.Errorf("CreateNewContext() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_CancelAndCreateNewContext(t *testing.T) {
	type args struct {
		functionServiceID string
	}
	tests := []struct {
		name string
		p    *PodFetcher
		args args
		want context.Context
	}{
		{"case1 create newContext for functionService",
			&PodFetcher{contexts: map[string]*Ctx{}}, args{"test"},
			nil},
		{"case2 cancel and create newContext for functionService",
			&PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{ctx: context.TODO(), cancelFunc: func() {}}}},
			args{"test"},
			nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.p.CancelAndCreateNewContext(tt.args.functionServiceID)
			if !strings.Contains(fmt.Sprintf(" %s", got), commonutils.LayerListSep) {
				t.Errorf("CancelAndCreateNewContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPodFetcher_CancelAndDeleteContext(t *testing.T) {
	type args struct {
		functionServiceID string
	}
	tests := []struct {
		name string
		p    *PodFetcher
		args args
	}{
		{"case1 cancel and delete context for functionService",
			&PodFetcher{contexts: map[string]*Ctx{"test": &Ctx{ctx: context.TODO(), cancelFunc: func() {}}},
				caeBusInstanceMutexes: make(map[string]*sync.Mutex)},
			args{"test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.CancelAndDeleteContext(tt.args.functionServiceID)
		})
	}
}

func TestPodFetcher_DeleteCAEBusInstanceMutexes(t *testing.T) {
	type args struct {
		functionServiceID string
	}
	tests := []struct {
		name string
		p    *PodFetcher
		args args
	}{
		{"case1 TestPodFetcher_DeleteCAEBusInstanceMutexes",
			&PodFetcher{caeBusInstanceMutexes: map[string]*sync.Mutex{"test": new(sync.Mutex)}},
			args{"test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.DeleteCAEBusInstanceMutexes(tt.args.functionServiceID)
			if _, ok := tt.p.caeBusInstanceMutexes[tt.args.functionServiceID]; ok {
				t.Errorf("failed to DeleteCAEBusInstanceMutexes")
			}
		})
	}
}

func TestPodFetcher_GeneralizePodWithRetry(t *testing.T) {
	type args struct {
		ctx             context.Context
		requestInfo     *config.RequestInfo
		functionService *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		want        *corev1.Pod
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 HandleVpcFunctionInfo error",
			&PodFetcher{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args{
				ctx:             context.TODO(),
				requestInfo:     &config.RequestInfo{TraceID: "test1"},
				functionService: &faasfunction.FunctionService{},
			}, nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case2 GeneralizePod error1",
			&PodFetcher{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args{
				ctx:             context.TODO(),
				requestInfo:     &config.RequestInfo{TraceID: "test1"},
				functionService: &faasfunction.FunctionService{},
			}, nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return nil
					}),
				})
				return patches
			}},
		{"case3 GeneralizePod error2",
			&PodFetcher{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args{
				ctx:             context.TODO(),
				requestInfo:     &config.RequestInfo{TraceID: "test1"},
				functionService: &faasfunction.FunctionService{},
			}, nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*PodFetcher).GeneralizePod,
						func(_ *PodFetcher, ctx context.Context, requestInfo *config.RequestInfo,
							functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
							return &corev1.Pod{}, utils.ErrReachMaxInstanceNum
						}),
					ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return nil
					}),
				})
				return patches
			},
		},

		{"case4 succeed to GeneralizePod",
			&PodFetcher{
				VpcHandler: &vpcmanager.VpcHandler{},
				PoolMgr:    &poolmanager.PoolMgr{},
			},
			args{
				ctx:             context.TODO(),
				requestInfo:     &config.RequestInfo{TraceID: "test1"},
				functionService: &faasfunction.FunctionService{},
			}, &corev1.Pod{}, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*PodFetcher).GeneralizePod,
						func(_ *PodFetcher, ctx context.Context, requestInfo *config.RequestInfo,
							functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
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
			got, err := tt.p.GeneralizePodWithRetry(tt.args.ctx, tt.args.requestInfo, tt.args.functionService)
			if (err != nil) != tt.wantErr {
				t.Errorf("GeneralizePodWithRetry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GeneralizePodWithRetry() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_DeletePodByK8S(t *testing.T) {
	convey.Convey("DeletePodByK8S", t, func() {
		p := &PodFetcher{KubeClient: &kubernetes.Clientset{}}
		defer gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
			func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
				return &fake.FakeCoreV1{}
			}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
			func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
				return &fake.FakePods{}
			}).Reset()

		convey.Convey("case 1  delete bridge ok", func() {
			patch1 := ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
				func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				})
			err := p.DeletePodByK8S("pool0", "deleteBridgePod-pool0")
			convey.So(err, convey.ShouldBeNil)
			patch1.Reset()
		})
	})
}

func Test_DeletePodByForce(t *testing.T) {
	convey.Convey("DeletePodByForce", t, func() {
		p := &PodFetcher{KubeClient: &kubernetes.Clientset{}}
		defer gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
			func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
				return &fake.FakeCoreV1{}
			}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
			func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
				return &fake.FakePods{}
			}).Reset()

		convey.Convey("case 1  delete ok", func() {
			patch1 := ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
				func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				})
			err := p.DeletePodByForce(&corev1.Pod{})
			convey.So(err, convey.ShouldBeNil)
			patch1.Reset()
		})
		convey.Convey("case 2 delete error", func() {
			utils.RetryPodOperationList = utils.NewPodOperationRetryList()
			patch2 := ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
				func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return errors.New("error")
				})
			patch4 := ApplyFunc((*utils.RetryPodOperationList).AddPodOperation, func(podOpEle *utils.PodOperationEle) {
				return
			})
			err := p.DeletePodByForce(&corev1.Pod{})
			convey.So(err, convey.ShouldNotBeNil)
			patch2.Reset()
			patch4.Reset()
		})
		convey.Convey("case 3 pod is not empty.", func() {
			patch3 := ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
				func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				})
			pods := &corev1.Pod{}
			pods.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: tools.WorkerContainerName, Ready: true}}
			err := p.DeletePodByForce(pods)
			convey.So(err, convey.ShouldBeNil)
			patch3.Reset()
		})
	})
}

func TestPodFetcher_DeletePodByWorkerInstance(t *testing.T) {
	utils.RetryPodOperationList = utils.NewPodOperationRetryList()
	type args struct {
		funcSvcID string
		namespace string
		traceID   string
		instance  v1alpha1.WorkerInstance
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 k8s delete pod error", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			funcSvcID: "abc",
			namespace: "default",
			traceID:   "11111",
			instance:  v1alpha1.WorkerInstance{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
				}),
				ApplyFunc((*utils.RetryPodOperationList).AddPodOperation, func(podOpEle *utils.PodOperationEle) {
					return
				}),
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
					func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
						return errors.New("test")
					}),
				ApplyFunc(etcdhandler.DeleteInstanceFromETCD,
					func(ctx context.Context, instance v1alpha1.WorkerInstance) {

					}),
				ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
					funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					return
				}),
			})
			return patches
		}},

		{"case2 k8s delete pod, delete cache", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			funcSvcID: "abc",
			namespace: "default",
			traceID:   "11111",
			instance:  v1alpha1.WorkerInstance{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
				}),
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
					func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
						return nil
					}),
				ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
					funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					return
				}),
				ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
					ins v1alpha1.WorkerInstance) {
				}),
			})
			return patches
		}},
		{"case3 notify the worker to exit", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			funcSvcID: "abc",
			namespace: "default",
			traceID:   "11111",
			instance:  v1alpha1.WorkerInstance{PodName: "custom-"},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
					funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					return
				}),
				ApplyFunc((*PodFetcher).DeletePodByK8S, func(_ *PodFetcher, podName, traceID string) error {
					return nil
				}),
				ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
					ins v1alpha1.WorkerInstance) {
				}),
			})
			return patches
		}},
		{"case4 notify the worker to exit, send err", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			funcSvcID: "abc",
			namespace: "default",
			traceID:   "11111",
			instance:  v1alpha1.WorkerInstance{PodName: "custom-"},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
					funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					return
				}),
				ApplyFunc((*PodFetcher).DeletePodByK8S, func(_ *PodFetcher, podName, traceID string) error {
					return nil
				}),
				ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
					ins v1alpha1.WorkerInstance) {
				}),
			})
			return patches
		}},
		{"case5 notify the worker to exit, send err", &PodFetcher{KubeClient: &kubernetes.Clientset{}}, args{
			funcSvcID: "abc",
			namespace: "default",
			traceID:   "11111",
			instance:  v1alpha1.WorkerInstance{PodName: "custom-"},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc((*PodFetcher).DoActions, func(_ *PodFetcher,
					funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					return
				}),
				ApplyFunc((*PodFetcher).DeletePodByK8S, func(_ *PodFetcher, podName, traceID string) error {
					return nil
				}),
				ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
					ins v1alpha1.WorkerInstance) {
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.p.DeletePodByWorkerInstance(tt.args.funcSvcID, tt.args.namespace, tt.args.traceID, tt.args.instance)
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_CleanPodsInNode(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 List pod error", &PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{node: &corev1.Node{}}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "List",
						func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
							return nil, errors.New("test")
						}),
				})
				return patches
			}},

		{"case2 succeed to CleanPodsInNode", &PodFetcher{KubeClient: &kubernetes.Clientset{}},
			args{node: &corev1.Node{}}, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "List",
						func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
							return &corev1.PodList{Items: []corev1.Pod{corev1.Pod{}}}, nil
						}),
					ApplyFunc((*PodFetcher).DeletePod, func(_ *PodFetcher,
						pod *corev1.Pod, traceID string) {
						return
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := tt.p.CleanPodsInNode(tt.args.node, "isPoolPod", "mock-traceID"); (err != nil) != tt.wantErr {
				t.Errorf("CleanPodsInNode() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_getFuncMountConfig(t *testing.T) {
	type args struct {
		service *faasfunction.FunctionService
	}
	tests := []struct {
		name string
		args args
		want types.FuncMountConfig
	}{
		{"case1", args{service: &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					FuncMountConfig: faasfunction.FuncMountConfig{
						FuncMounts: []faasfunction.FuncMount{faasfunction.FuncMount{MountType: "test1"},
							faasfunction.FuncMount{MountType: "test2"}},
						FuncMountUser: faasfunction.FuncMountUser{UserID: 1, GroupID: 2}}}}}},
			types.FuncMountConfig{FuncMountUser: commontypes.FuncMountUser{UserID: 1, GroupID: 2},
				FuncMounts: []commontypes.FuncMount{
					commontypes.FuncMount{MountType: "test1"},
					commontypes.FuncMount{MountType: "test2"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFuncMountConfig(tt.args.service); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getFuncMountConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_setRequestMessage(t *testing.T) {
	type args struct {
		functionRequest *commontypes.FunctionSpecRequest
		service         *faasfunction.FunctionService
	}
	tests := []struct {
		name string
		args args
	}{
		{"case1", args{
			functionRequest: &commontypes.FunctionSpecRequest{},
			service: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Env: []corev1.EnvVar{
							corev1.EnvVar{Name: "ENVIRONMENT_KEY", Value: "test"},
							corev1.EnvVar{Name: "WORKER_ENV", Value: "test"},
							corev1.EnvVar{Name: "USER_DATA", Value: "test"},
						},
					},
				}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequestMessage(tt.args.functionRequest, tt.args.service)
		})
	}
}

func TestPodFetcher_CreateContext_ConcurrentMap(t *testing.T) {
	loopNum := 100

	mockFuncSvcID := "mockFuncSvcID"
	mockPodFetcher := &PodFetcher{
		contexts: make(map[string]*Ctx),
	}
	getWaitGroup := &sync.WaitGroup{}
	getWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockPodFetcher.GetContext(mockFuncSvcID)
			getWaitGroup.Done()
		}()
	}
	createWaitGroup := &sync.WaitGroup{}
	createWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockPodFetcher.CreateNewContext(mockFuncSvcID)
			createWaitGroup.Done()
		}()
	}
	// no panic means test case is ok
	getWaitGroup.Wait()
	createWaitGroup.Wait()
}

func TestPodFetcher_CancelAndCreateContext_ConcurrentMap(t *testing.T) {
	loopNum := 100

	mockFuncSvcID := "mockFuncSvcID"
	mockPodFetcher := &PodFetcher{
		contexts: make(map[string]*Ctx),
	}
	getWaitGroup := &sync.WaitGroup{}
	getWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockPodFetcher.GetContext(mockFuncSvcID)
			getWaitGroup.Done()
		}()
	}
	createWaitGroup := &sync.WaitGroup{}
	createWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockPodFetcher.CancelAndCreateNewContext(mockFuncSvcID)
			createWaitGroup.Done()
		}()
	}
	// no panic means test case is ok
	getWaitGroup.Wait()
	createWaitGroup.Wait()
}

func TestPodFetcher_CancelAndDeleteContext_ConcurrentMap(t *testing.T) {
	loopNum := 100

	mockFuncSvcID := "mockFuncSvcID"
	mockPodFetcher := &PodFetcher{
		contexts:              make(map[string]*Ctx),
		caeBusInstanceMutexes: make(map[string]*sync.Mutex),
	}
	getWaitGroup := &sync.WaitGroup{}
	getWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockPodFetcher.GetContext(mockFuncSvcID)
			getWaitGroup.Done()
		}()
	}
	deleteWaitGroup := &sync.WaitGroup{}
	deleteWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockPodFetcher.CancelAndDeleteContext(mockFuncSvcID)
			deleteWaitGroup.Done()
		}()
	}
	// no panic means test case is ok
	getWaitGroup.Wait()
	deleteWaitGroup.Wait()
}

func TestPodFetcher_SendIdleModeRequestToWorkerInternal(t *testing.T) {
	convey.Convey("Test SendIdleModeRequestToWorkerInternal", t, func() {
		convey.Convey("success to send idle mode request to worker internal", func() {
			instance := v1alpha1.WorkerInstance{}
			p := &PodFetcher{
				workerClient:  &fhttp.Client{},
				IdleRetryInfo: &IdleRetryList{},
			}
			patches := [...]*Patches{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, nil
				}),
				ApplyMethod(reflect.TypeOf(p.workerClient), "DoTimeout", func(f *fhttp.Client, req *fhttp.Request,
					resp *fhttp.Response, timeout time.Duration) error {
					return nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := p.SendIdleModeRequestToWorkerInternal(instance, false, "mock-traceID", 1)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("failed to send idle mode request to worker internal", func() {
			instance := v1alpha1.WorkerInstance{}
			p := &PodFetcher{
				workerClient:  &fhttp.Client{},
				IdleRetryInfo: &IdleRetryList{},
			}
			patches := [...]*Patches{
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, errors.New("test")
				}),
				ApplyMethod(reflect.TypeOf(p.workerClient), "DoTimeout", func(f *fhttp.Client, req *fhttp.Request,
					resp *fhttp.Response, timeout time.Duration) error {
					return nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := p.SendIdleModeRequestToWorkerInternal(instance, false, "mock-traceID", 1)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestPodFetcher_SendPatPodIPToWorkerByRetry(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		KubeClient           kubernetes.Interface
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		retryPeriod time.Duration
		param       *config.PatPodRequestParam
	}
	mockFunSvcID := "mock-function-service-id"
	mockPodName := "mock-pod-name"
	mockTraceID := "mock-trace-id"
	deletedPodName := "mock-pat-pod-name-deleted"
	updatedPodName := "mock-pat-pod-name-updated"
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockUtils.PatchesFunc
	}{
		{
			name: "test with initPatPodIPRequest return nil",
			fields: fields{
				workerClient: &fhttp.Client{},
				httpsEnable:  false,
				stopCh:       make(<-chan struct{}),
			},
			args: args{
				retryPeriod: 1 * time.Second,
				param: &config.PatPodRequestParam{
					FuncSvcID: mockFunSvcID,
					PodName:   mockPodName,
					TraceID:   mockTraceID,
					Timeout:   3 * time.Second,
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: deletedPodName,
							IsDeleted:  true,
						},
						{
							PatPodName: updatedPodName,
						},
					},
					EndpointPatPodInfo: nil,
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(initPatPodIPRequest, func(podIP string, httpsEnable bool,
						param config.PatPodRequestParam) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					ApplyFunc((*fhttp.Client).DoTimeout,
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							resp.Header.SetStatusCode(fhttp.StatusOK)
							return nil
						}),
					ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							return errors.New("mock IdleDeletePatServiceList return error")
						}),
				})
				return patches
			},
		},
		{
			name: "test with fhttp client do timeout return error",
			fields: fields{
				workerClient: &fhttp.Client{},
				httpsEnable:  false,
				stopCh:       make(<-chan struct{}),
			},
			args: args{
				retryPeriod: 1 * time.Second,
				param: &config.PatPodRequestParam{
					FuncSvcID: mockFunSvcID,
					PodName:   mockPodName,
					TraceID:   mockTraceID,
					Timeout:   3 * time.Second,
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: deletedPodName,
							IsDeleted:  true,
						},
						{
							PatPodName: updatedPodName,
						},
					},
					EndpointPatPodInfo: nil,
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(initPatPodIPRequest, func(podIP string, httpsEnable bool,
						param config.PatPodRequestParam) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					ApplyFunc((*fhttp.Client).DoTimeout,
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return errors.New("mock do timeout error")
						}),
					ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							return errors.New("mock IdleDeletePatServiceList return error")
						}),
				})
				return patches
			},
		},
		{
			name: "test with response is not ok",
			fields: fields{
				workerClient: &fhttp.Client{},
				httpsEnable:  false,
				stopCh:       make(<-chan struct{}),
			},
			args: args{
				retryPeriod: 1 * time.Second,
				param: &config.PatPodRequestParam{
					FuncSvcID: mockFunSvcID,
					PodName:   mockPodName,
					TraceID:   mockTraceID,
					Timeout:   3 * time.Second,
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: deletedPodName,
							IsDeleted:  true,
						},
						{
							PatPodName: updatedPodName,
						},
					},
					EndpointPatPodInfo: nil,
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(initPatPodIPRequest, func(podIP string, httpsEnable bool,
						param config.PatPodRequestParam) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					ApplyFunc((*fhttp.Client).DoTimeout,
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
							snErr := &snerror.BadResponse{
								Code:    fhttp.StatusInternalServerError,
								Message: "mock internal server error",
							}
							body, err := json.Marshal(snErr)
							if err != nil {
								return err
							}
							resp.SetBody(body)
							return nil
						}),
					ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							return errors.New("mock IdleDeletePatServiceList return error")
						}),
				})
				return patches
			},
		},
		{
			name: "test with initPatPodIPRequest return error",
			fields: fields{
				workerClient: &fhttp.Client{},
				httpsEnable:  false,
				stopCh:       make(<-chan struct{}),
			},
			args: args{
				retryPeriod: 1 * time.Second,
				param: &config.PatPodRequestParam{
					FuncSvcID: mockFunSvcID,
					PodName:   mockPodName,
					TraceID:   mockTraceID,
					Timeout:   3 * time.Second,
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: deletedPodName,
							IsDeleted:  true,
						},
						{
							PatPodName: updatedPodName,
						},
					},
					EndpointPatPodInfo: nil,
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(initPatPodIPRequest, func(podIP string, httpsEnable bool,
						param config.PatPodRequestParam) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), errors.New("mock initPatPodIPRequest error")
					}),
					ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							return errors.New("mock IdleDeletePatServiceList return error")
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtx, cancelFunc := context.WithCancel(context.Background())
			p := &PodFetcher{
				workerClient:         tt.fields.workerClient,
				KubeClient:           tt.fields.KubeClient,
				VpcHandler:           tt.fields.VpcHandler,
				PoolMgr:              tt.fields.PoolMgr,
				podLister:            tt.fields.podLister,
				Queue:                tt.fields.Queue,
				stopCh:               tt.fields.stopCh,
				contexts:             tt.fields.contexts,
				podGeneralizeTimeout: tt.fields.podGeneralizeTimeout,
				httpsEnable:          tt.fields.httpsEnable,
				IdleRetryInfo:        tt.fields.IdleRetryInfo,
			}
			patches := tt.patchFunc()
			// cache.WorkerCache.StoreWorkerInstance(mockFunSvcID, v1alpha1.WorkerInstance{PodName: mockPodName})
			go func() {
				time.Sleep(1 * time.Second)
				cancelFunc()
			}()
			p.SendPatPodIPToWorkerByRetry(mockCtx, tt.args.retryPeriod, tt.args.param)
			cache.WorkerCache.DeleteWorkerInstance(mockFunSvcID, mockPodName, false)
			patches.ResetAll()
		})
	}
}

func Test_initPatPodIPRequest(t *testing.T) {
	type args struct {
		podIP              string
		traceID            string
		httpsEnable        bool
		patPodRequestParam config.PatPodRequestParam
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test normal",
			args: args{
				podIP:       "",
				traceID:     "",
				httpsEnable: false,
				patPodRequestParam: config.PatPodRequestParam{
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: "mock-pat-pod-name",
						},
					},
					EndpointPatPodInfo: []types.NATConfigure{
						{
							PatPodName: "mock-pat-pod-name",
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}}, nil
					}),
					ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
						return "auth", "10000000"
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := initPatPodIPRequest(tt.args.podIP, tt.args.httpsEnable, tt.args.patPodRequestParam)
			if (err != nil) != tt.wantErr {
				t.Errorf("initPatPodIPRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_getRequestPriority(t *testing.T) {
	tests := []struct {
		name string

		isFirstInstance bool
		applier         string

		excepted int
	}{
		{
			name:            "first instance, not reserved",
			isFirstInstance: true,
			applier:         "task",

			excepted: PriorityFirstInstance,
		},
		{
			name:            "first instance and reserved",
			isFirstInstance: true,
			applier:         constants.WorkerManagerApplier,

			excepted: PriorityReservedInstance,
		},
		{
			name:    "not first, but reserved",
			applier: constants.WorkerManagerApplier,

			excepted: PriorityReservedInstance,
		},
		{
			name: "not first, not reserved",

			excepted: PriorityDefault,
		},
		{
			name:     "prewarm instance",
			applier:  constants.PredictionApplier,
			excepted: PriorityPrewarmInstance,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.excepted, getRequestPriority(&config.RequestInfo{Applier: test.applier, IsFirstInstance: test.isFirstInstance}), test.name)
	}
}

func TestPodFetcher_ScalePrewarmPod(t *testing.T) {
	q := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},
	))
	podFetcher = &PodFetcher{
		Queue: queue{RateLimitingInterface: q},
	}
	funcSvcID := "mockFuncSvcID"
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc((*PodFetcher).GeneralizePodInternal, func(_ *PodFetcher,
			ctx context.Context, reqInfo *config.RequestInfo,
			service *faasfunction.FunctionService) (*corev1.Pod, error) {
			return &corev1.Pod{}, nil
		}),
		gomonkey.ApplyFunc((*PodFetcher).WithContext, func(_ *PodFetcher,
			f decorateFunc, requestInfo *config.RequestInfo,
			functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
			log.GetLogger().Infof("mock WithContext")
			return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "abc"}}, nil
		}),
		gomonkey.ApplyFunc((*PodFetcher).generalizeRequestToWorker, func(_ *PodFetcher, pod *corev1.Pod,
			reqInfo *config.RequestInfo, service *faasfunction.FunctionService, chooseStartTime time.Time) error {
			return nil
		}),
		gomonkey.ApplyFunc((*queue).AddRateLimited, func(_ *queue, event interface{}) {
			return
		}),
		gomonkey.ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
			func(_ *vpcmanager.VpcHandler, instanceName string) error {
				fmt.Println("succeed to mock IdleDeletePatServiceList")
				return nil
			}),
		ApplyFunc((*PodFetcher).DeletePodByK8S, func(_ *PodFetcher, podName, traceID string) error {
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	utils.SetSmartWarmerEnable(true)
	cache.WorkerCache.CreateFunctionCacheByFuncMeta(funcSvcID, &faasfunction.FunctionService{})
	podFetcher.ScaleUpPrewarmPods(funcSvcID, 1)
	time.Sleep(100 * time.Millisecond)

	pod := podFetcher.GetPodFromPrewarmPool(funcSvcID, &config.RequestInfo{}, &faasfunction.FunctionService{})

	assert.Equal(t, pod != nil, true)
	podFetcher.ScaleDownPrewarmPod(funcSvcID, []*corev1.Pod{pod})

	pod = podFetcher.GetPodFromPrewarmPool(funcSvcID, &config.RequestInfo{}, &faasfunction.FunctionService{})
	assert.Equal(t, pod != nil, false)
	time.Sleep(100 * time.Millisecond)
}

func Test_sendPatPodIPToWorker(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, _ *fhttp.Request, _ *fhttp.Response, _ time.Duration) error {
						return nil
					}),
					gomonkey.ApplyFunc(initPatPodIPRequest, func(podIP string, httpsEnable bool,
						param config.PatPodRequestParam) (*fhttp.Request, error) {
						return &fhttp.Request{}, nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case2",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, _ *fhttp.Request, _ *fhttp.Response, _ time.Duration) error {
						return nil
					}),
					gomonkey.ApplyFunc(initPatPodIPRequest, func(podIP string, httpsEnable bool,
						param config.PatPodRequestParam) (*fhttp.Request, error) {
						return &fhttp.Request{}, nil
					}),
					gomonkey.ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						response := &fhttp.Response{}
						response.Header = fhttp.ResponseHeader{}
						response.Header.SetStatusCode(1)
						return response
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc
			patches()
			podFetcher = &PodFetcher{
				workerClient: &fhttp.Client{},
			}
			instance := v1alpha1.WorkerInstance{}
			var patPodInfo, endpointTalentPatPodInfo, publicPatPodInfo []commontypes.NATConfigure
			err := podFetcher.sendPatPodIPToWorker(instance, config.PatPodRequestParam{
				TraceID:            "mockTraceID",
				Timeout:            time.Second,
				PatPodInfo:         patPodInfo,
				EndpointPatPodInfo: endpointTalentPatPodInfo,
				PublicPatPodInfo:   publicPatPodInfo,
			})
			assert.Equal(t, tt.wantErr, err == nil)
			patches().ResetAll()
		})
	}
}

func TestUpdateRestoreFailNum(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc
			patches()
			defer patches().ResetAll()
			info := commontypes.FailedInfo{}
			updateRestoreFailNum("key", info)
		})
	}
}

func TestSendPatPodIPToWorkerByRetry(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			fields: fields{
				workerClient: &fhttp.Client{},
				stopCh:       make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initIdleModeRequest, func(podIP, traceID string, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
						return fhttp.AcquireRequest(), fhttp.AcquireResponse(), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&vpcmanager.VpcHandler{}), "DeletePatPods",
						func(v *vpcmanager.VpcHandler, podName, funcSvcID, traceID string) {
						}),
				})

				return patches
			},
			wantErr: true,
		},
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
				stopCh:               make(chan struct{}),
				contexts:             tt.fields.contexts,
				contextsMutex:        tt.fields.contextsMutex,
				podGeneralizeTimeout: tt.fields.podGeneralizeTimeout,
				httpsEnable:          tt.fields.httpsEnable,
				IdleRetryInfo:        tt.fields.IdleRetryInfo,
			}
			patches := tt.patchesFunc
			patches()
			ctx := context.Background()
			param := config.PatPodRequestParam{
				FuncSvcID: "id",
			}
			p.SendPatPodIPToWorkerByRetry(ctx, 10, &param)
			ctx.Done()
			patches().ResetAll()
		})
	}
}

func TestPodFetcher_sendIdleModeRequestToWorker(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		instance v1alpha1.WorkerInstance
		idleMode bool
		traceID  string
		timeout  time.Duration
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance: v1alpha1.WorkerInstance{},
				idleMode: false,
				traceID:  "test-trace-id",
				timeout:  10,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initIdleModeRequest, func(podIP, traceID string, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
						return fhttp.AcquireRequest(), fhttp.AcquireResponse(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test success
		{
			name: "test with do timeout method return error",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance: v1alpha1.WorkerInstance{},
				idleMode: false,
				traceID:  "test-trace-id",
				timeout:  10,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initIdleModeRequest, func(podIP, traceID string, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
						return fhttp.AcquireRequest(), fhttp.AcquireResponse(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return errors.New("mock do timeout error")
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test with do timeout method return error
		{
			name: "test with status code is not ok",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance: v1alpha1.WorkerInstance{},
				idleMode: false,
				traceID:  "test-trace-id",
				timeout:  10,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initIdleModeRequest, func(podIP, traceID string, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
						return fhttp.AcquireRequest(), fhttp.AcquireResponse(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
						resp.SetBody([]byte("mock response body error"))
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test with status code is not ok
		{
			name: "test with status code is not ok but unmarshal success",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance: v1alpha1.WorkerInstance{},
				idleMode: false,
				traceID:  "test-trace-id",
				timeout:  10,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initIdleModeRequest, func(podIP, traceID string, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
						return fhttp.AcquireRequest(), fhttp.AcquireResponse(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
						errBytes, _ := json.Marshal(snerror.New(150500, "mock http response error"))
						resp.SetBody(errBytes)
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test with status code is not ok but unmarshal success
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
			patches := tt.patchesFunc
			patches()
			defer patches().ResetAll()
			err := p.sendIdleModeRequestToWorker(tt.args.instance, tt.args.idleMode, tt.args.traceID, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func TestPodFetcher_setBillingSwitchToWorker(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		instance      v1alpha1.WorkerInstance
		enableBilling bool
		traceID       string
		timeout       time.Duration
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance:      v1alpha1.WorkerInstance{},
				enableBilling: false,
				traceID:       "test-trace-id",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initRequestToWorker, func(podIP, requestPath string, httpsEnable bool) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test success
		{
			name: "test failed with initBillingSwitchRequest error",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance:      v1alpha1.WorkerInstance{},
				enableBilling: false,
				traceID:       "test-trace-id",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initRequestToWorker, func(podIP, requestPath string, httpsEnable bool) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), errors.New("mock init billing error")
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test failed with initBillingSwitchRequest error
		{
			name: "test failed with DoTimeout error",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance:      v1alpha1.WorkerInstance{},
				enableBilling: false,
				traceID:       "test-trace-id",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initRequestToWorker, func(podIP, requestPath string, httpsEnable bool) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return errors.New("mock DoTimeout error")
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test failed with DoTimeout error
		{
			name: "test failed with status code is not ok and unmarshal is failed",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance:      v1alpha1.WorkerInstance{},
				enableBilling: false,
				traceID:       "test-trace-id",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initRequestToWorker, func(podIP, requestPath string, httpsEnable bool) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
						resp.SetBody([]byte("mock response body error"))
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test failed with status code is not ok and unmarshal is failed
		{
			name: "test failed with status code is not ok and unmarshal is success",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				instance:      v1alpha1.WorkerInstance{},
				enableBilling: false,
				traceID:       "test-trace-id",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initRequestToWorker, func(podIP, requestPath string, httpsEnable bool) (*fhttp.Request, error) {
						return fhttp.AcquireRequest(), nil
					}),
					gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
						errBytes, _ := json.Marshal(snerror.New(150500, "mock http response error"))
						resp.SetBody(errBytes)
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test failed with status code is not ok and unmarshal is success
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
			err := p.setBillingSwitchToWorker(tt.args.instance, tt.args.enableBilling, tt.args.traceID, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
			patches.ResetAll()
		})
	}
}

func TestUpdateSnapshotInEtcd(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test updateSnapshotInEtcd",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Put",
						func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
						func() *etcd3.EtcdClient {
							client := etcd3.EtcdClient{}
							client.Client = &clientv3.Client{}
							return &client
						}),
				})
				return patches
			},
		},
		{
			name: "test updateSnapshotInEtcd failed",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Put",
						func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
						func() *etcd3.EtcdClient {
							client := etcd3.EtcdClient{}
							client.Client = &clientv3.Client{}
							return &client
						}),
					gomonkey.ApplyFunc(json.Marshal,
						func(v any) ([]byte, error) {
							err := snerror.New(1, "error")
							return nil, err
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &faasfunction.FunctionService{}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			updateSnapshotInEtcd("key", service, "traceId")
		})
	}
}

func TestGetDefaultSnapshotDeployment(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test getDefaultSnapshotDeployment",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &faasfunction.FunctionService{}
			assert.Equal(t, true, getDefaultSnapshotDeployment(service).IsSnapshotExpire)

		})
	}
}

func TestPodFetcher_generalizeRequestToWorker(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		pod             *corev1.Pod
		reqInfo         *config.RequestInfo
		service         *faasfunction.FunctionService
		chooseStartTime time.Time
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test generalizeRequestToWorker",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				pod: &corev1.Pod{},
				reqInfo: &config.RequestInfo{
					TraceID:     "aaa",
					CPU:         0,
					Memory:      0,
					ForceDeploy: "true",
				},
				service:         &faasfunction.FunctionService{},
				chooseStartTime: time.Time{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).sendGeneralizeRequestToWorker,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo,
							service *faasfunction.FunctionService) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test generalizeRequestToWorker error",
			fields: fields{
				workerClient: &fhttp.Client{},
			},
			args: args{
				pod: &corev1.Pod{},
				reqInfo: &config.RequestInfo{
					TraceID:     "aaa",
					CPU:         0,
					Memory:      0,
					ForceDeploy: "true",
				},
				service:         &faasfunction.FunctionService{},
				chooseStartTime: time.Time{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).sendGeneralizeRequestToWorker,
						func(_ *PodFetcher, pod *corev1.Pod, reqInfo *config.RequestInfo,
							service *faasfunction.FunctionService) (string, error) {
							return "", errors.New("test")
						}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S,
						func(_ *PodFetcher, podName, traceID string) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
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
			patches := tt.patchesFunc
			patches()
			defer patches().ResetAll()
			err := p.generalizeRequestToWorker(tt.args.pod, tt.args.reqInfo, tt.args.service, tt.args.chooseStartTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func TestPodFetcher_handleVpcHook(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		pod     *corev1.Pod
		reqInfo *config.RequestInfo
		service *faasfunction.FunctionService
	}
	mockService := &faasfunction.FunctionService{}
	mockService.ID = "mock-function-service"
	actualIsMocked := false
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		isMocked    bool
	}{
		{
			name:   "test success with 2 pats",
			fields: fields{},
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "mock-pod-name",
					},
				},
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-function-name",
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: "PatPodName01",
						},
						{
							PatPodName: "PatPodName02",
						},
					},
				},
				service: mockService,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			isMocked: false,
		}, // test success with 2 pats
		{
			name:   "test success with 1 pats and create resource return 2 pats",
			fields: fields{},
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "mock-pod-name",
					},
				},
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-function-name",
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: "PatPodName01",
						},
					},
				},
				service: mockService,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							actualIsMocked = true
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-func-vpc-pat-container-ip-001",
								},
								{
									PatContainerIP: "mock-func-vpc-pat-container-ip-002",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandlePatServiceList,
						func(_ *vpcmanager.VpcHandler, tenantID, funcSvcID string,
							patPodInfo []types.NATConfigure, podName string) {
							return
						}),
					gomonkey.ApplyFunc((*PodFetcher).SendPatPodIPToWorkerByRetry,
						func(_ *PodFetcher, ctx context.Context, retryPeriod time.Duration,
							param *config.PatPodRequestParam) {
							actualIsMocked = true
							log.GetLogger().Infof("mock SendPatPodIPToWorkerByRetry success")
							return
						}),
				})
				return patches
			},
			isMocked: true,
		}, // test success with 1 pats and create resource return 2 pats
		{
			name:   "test success with 1 pats and create resource return 1 pats",
			fields: fields{},
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "mock-pod-name",
					},
				},
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-function-name",
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: "PatPodName01",
						},
					},
					TraceID: "mock-trace-id",
				},
				service: mockService,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							actualIsMocked = true
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-func-vpc-pat-container-ip-001",
								},
							}
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandlePatServiceList,
						func(_ *vpcmanager.VpcHandler, tenantID, funcSvcID string,
							patPodInfo []types.NATConfigure, podName string) {
							return
						}),
					gomonkey.ApplyFunc((*PodFetcher).SendPatPodIPToWorkerByRetry,
						func(_ *PodFetcher, ctx context.Context, retryPeriod time.Duration,
							param *config.PatPodRequestParam) {
							actualIsMocked = true
							log.GetLogger().Infof("mock SendPatPodIPToWorkerByRetry success")
							return
						}),
				})
				return patches
			},
			isMocked: true,
		}, // test success with 1 pats and create resource return 1 pats
		{
			name:   "test with create vpc resource error",
			fields: fields{},
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "mock-pod-name",
					},
				},
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-function-name",
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: "PatPodName01",
						},
					},
				},
				service: mockService,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							actualIsMocked = true
							return errors.New("mock create vpc resource error")
						}),
				})
				return patches
			},
			isMocked: true,
		}, // test with create vpc resource error
		{
			name:   "test error with 1 pats and create resource return nil",
			fields: fields{},
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "mock-pod-name",
					},
				},
				reqInfo: &config.RequestInfo{
					FunctionName: "mock-function-name",
					PatPodInfo: []types.NATConfigure{
						{
							PatPodName: "PatPodName01",
						},
					},
					TraceID: "mock-trace-id",
				},
				service: mockService,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							actualIsMocked = true
							requestInfo.PatPodInfo = nil
							return nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandlePatServiceList,
						func(_ *vpcmanager.VpcHandler, tenantID, funcSvcID string,
							patPodInfo []types.NATConfigure, podName string) {
							return
						}),
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualIsMocked = false
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
			p.handleVpcHook(tt.args.pod, tt.args.reqInfo, tt.args.service)
			time.Sleep(50 * time.Millisecond)
			patches.ResetAll()
			if tt.isMocked != actualIsMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, actualIsMocked)
			}
		})
	}
}

func TestHandleRestoreDead(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		nErr        *snerror.BadResponse
		service     *faasfunction.FunctionService
	}{
		{
			name: "case1_fail_num_not_enough",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HGet",
						func(_ *redisclient.Client, ctx context.Context, key, field string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result",
						func(_ *redis.StringCmd) (string, error) {
							return "{\"failedCount\":6,\"time\":1698407137}", nil
						}),
					gomonkey.ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								PoolMgrConfig: config.PoolMgrConfig{
									CheckpointPodConfig: config.CheckpointPodConfig{
										RestoreFailureThreshold: 10,
									},
								},
							}, nil
						}),
				})
				return patches
			},
			nErr: &snerror.BadResponse{
				Code:    snerror.RestoreDeadErrorCode,
				Message: "",
			},
			service: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{
					ID: "mock-service-id",
				},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FuncVpc: faasfunction.Vpc{
							VpcID: "mock-func-vpc-id",
						},
					},
				},
			},
		},
		{
			name: "case2_no_need_update",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HGet",
						func(_ *redisclient.Client, ctx context.Context, key, field string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result",
						func(_ *redis.StringCmd) (string, error) {
							return "{\"failedCount\":6,\"time\":1698407137}", nil
						}),
					gomonkey.ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								PoolMgrConfig: config.PoolMgrConfig{
									CheckpointPodConfig: config.CheckpointPodConfig{
										RestoreFailureThreshold: 5,
									},
								},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetValues",
						func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							snapshotInfo := "{\n    \"appId\": \"\",\n    \"bucketId\": \"bucket-test-log1\",\n    \"objectId\": \"khj.zip.VERSION201.0.0.B001.20231027092259\",\n    \"bucketUrl\": \"\",\n    \"snapshotState\":\"circuit_breaking\",\n    \"sha256\": \"18d411bd7239c1168795c30c37225c2245ce1ef346c57f619d2f2d2b7c5c90e2\",\n    \"size\": 18110172}"
							arr := strings.Split(snapshotInfo, "\n")
							return arr, nil
						}),
				})
				return patches
			},
			nErr: &snerror.BadResponse{
				Code:    snerror.RestoreDeadErrorCode,
				Message: "",
			},
			service: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{
					ID: "mock-service-id",
				},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FuncVpc: faasfunction.Vpc{
							VpcID: "mock-func-vpc-id",
						},
					},
				},
			},
		},
		{
			name: "case3_need_update",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HGet",
						func(_ *redisclient.Client, ctx context.Context, key, field string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result",
						func(_ *redis.StringCmd) (string, error) {
							return "{\"failedCount\":6,\"time\":1698407137}", nil
						}),
					gomonkey.ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								PoolMgrConfig: config.PoolMgrConfig{
									CheckpointPodConfig: config.CheckpointPodConfig{
										RestoreFailureThreshold: 5,
									},
								},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetValues",
						func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							snapshotInfo := "{\n    \"appId\": \"\",\n    \"bucketId\": \"bucket-test-log1\",\n    \"objectId\": \"khj.zip.VERSION201.0.0.B001.20231027092259\",\n    \"bucketUrl\": \"\",\n       \"sha256\": \"18d411bd7239c1168795c30c37225c2245ce1ef346c57f619d2f2d2b7c5c90e2\",\n    \"size\": 18110172\n}"
							arr := strings.Split(snapshotInfo, "\n")
							return arr, nil
						}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
						func() *etcd3.EtcdClient {
							client := etcd3.EtcdClient{}
							return &client
						}),
				})
				return patches
			},
			nErr: &snerror.BadResponse{
				Code:    snerror.RestoreDeadErrorCode,
				Message: "",
			},
			service: &faasfunction.FunctionService{
				ObjectMeta: faasfunction.ObjectMeta{
					ID: "mock-service-id",
				},
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FuncVpc: faasfunction.Vpc{
							VpcID: "mock-func-vpc-id",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handleRestoreDead("traceId", tt.service)
			patches.ResetAll()
		})
	}
}

func TestPodFetcher_GeneralizePodInternal1(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		ctx     context.Context
		reqInfo *config.RequestInfo
		service *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test for internal vpc error",
			fields: fields{
				PoolMgr: &poolmanager.PoolMgr{
					Config: config.PoolMgrConfig{
						DisableCircuitBreaker: 1,
					},
				},
			},
			args: args{
				reqInfo: &config.RequestInfo{},
				service: &faasfunction.FunctionService{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PodFetcher).getPodFromNodeManager,
						func(_ *PodFetcher, ctx context.Context, funSvc *faasfunction.FunctionService,
							req *config.RequestInfo) (*corev1.Pod, error) {
							return &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name: "mock-pod-name",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).HandleVpcFunctionInfo,
						func(_ *vpcmanager.VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							requestInfo.PatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-func-vpc-pat-container-ip",
								},
							}
							requestInfo.EndpointTalentPatPodInfo = []types.NATConfigure{
								{
									PatContainerIP: "mock-endpoint-pat-container-ip",
								},
							}
							fmt.Println("mock HandleVpcFunctionInfo return internal vpc error")
							return errors.New("mock internal vpc error")
						}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S,
						func(_ *PodFetcher, podName, traceID string) error {
							fmt.Println("mock DeletePodByK8S return nil")
							return nil
						}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == snerror.New(snerror.InternalVPCError, err.Error())
			},
		},
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
			_, err := p.GeneralizePodInternal(tt.args.ctx, tt.args.reqInfo, tt.args.service)
			if !tt.wantErr(t, err, fmt.Sprintf("GeneralizePodInternal(%v, %v, %v)", tt.args.ctx, tt.args.reqInfo, tt.args.service)) {
				return
			}
		})
	}
}

func TestPodFetcher_getPodFromNodeManager(t *testing.T) {
	type fields struct {
		workerClient         *fhttp.Client
		VpcHandler           *vpcmanager.VpcHandler
		PoolMgr              *poolmanager.PoolMgr
		KubeClient           kubernetes.Interface
		podLister            listercorev1.PodLister
		Queue                queue
		stopCh               <-chan struct{}
		contexts             map[string]*Ctx
		contextsMutex        sync.Mutex
		podGeneralizeTimeout time.Duration
		httpsEnable          bool
		IdleRetryInfo        *IdleRetryList
	}
	type args struct {
		ctx    context.Context
		funSvc *faasfunction.FunctionService
		req    *config.RequestInfo
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     error
	}{
		{
			name: "test for JSONParseError",
			args: args{
				req: &config.RequestInfo{
					NodeLabel: "mockNodeLabel",
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(strconv.ParseBool, func(str string) (bool, error) {
						return true, nil
					}),
				})
				return patches
			},
			wantErr: snerror.New(utils.JSONParseError, "failed to parse node label"),
		},
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
			_, err := p.getPodFromNodeManager(tt.args.ctx, tt.args.funSvc, tt.args.req)
			if errors.Is(err, tt.wantErr) {
				t.Errorf("expect %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func TestSendUpdateAgencyInfoRequestToWorker(t *testing.T) {
	type args struct {
		workerInstance v1alpha1.WorkerInstance
		traceID        string
		workerID       string
		service        *faasfunction.FunctionService
		podName        string
	}
	tests := []struct {
		name        string
		p           *PodFetcher
		args        args
		want        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{name: "case1 failed to init update agency info request for pod,initUpdateAgencyInfoRequest error", p: &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}},
			args: args{
				workerInstance: v1alpha1.WorkerInstance{},
				traceID:        "test1",
				workerID:       "test2",
				service:        &faasfunction.FunctionService{},
				podName:        "testPod1",
			}, wantErr: true, patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(initUpdateAgencyInfoRequest, func(workerInstance v1alpha1.WorkerInstance, service *faasfunction.FunctionService,
						httpsEnable bool) (*fhttp.Request, *fhttp.Response, error) {
						return &fhttp.Request{}, &fhttp.Response{}, errors.New("test")
					}),
				})
				return patches
			}},

		{"case2 resp statuscode not 200, unmarshal error", &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}, workerClient: &fhttp.Client{}},
			args{
				workerInstance: v1alpha1.WorkerInstance{},
				traceID:        "test1",
				workerID:       "test2",
				podName:        "testPod1",
				service: &faasfunction.FunctionService{
					Version: "version",
					Spec: faasfunction.FunctionServiceSpec{
						SnapshotDeployment: types.SnapshotDeploymentSpec{
							IsSnapshotExpire: true,
						},
						Configuration: faasfunction.ConfigurationSpec{
							Initializer: faasfunction.Initializer{Timeout: 1},
						}},
				},
			}, "", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(initUpdateAgencyInfoRequest, func(workerInstance v1alpha1.WorkerInstance, service *faasfunction.FunctionService,
						httpsEnable bool) (*fhttp.Request, *fhttp.Response, error) {
						return &fhttp.Request{}, &fhttp.Response{}, nil
					}),
					ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return nil
						}),
					ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
						resp.Header.Add(constants.HeaderLoadDuration, "1")
						return resp
					}),
					ApplyMethod(reflect.TypeOf(&fhttp.ResponseHeader{}), "StatusCode",
						func(_ *fhttp.ResponseHeader) int {
							return http.StatusInternalServerError
						}),

					ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("test")
					}),
				})
				return patches
			}},

		{"case3 resp statuscode not 200", &PodFetcher{PoolMgr: &poolmanager.PoolMgr{}, workerClient: &fhttp.Client{}},
			args{
				workerInstance: v1alpha1.WorkerInstance{},
				traceID:        "test1",
				workerID:       "test2",
				podName:        "testPod1",
				service: &faasfunction.FunctionService{
					Version: "version",
					Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
						Initializer: faasfunction.Initializer{Timeout: 1},
					}},
				},
			}, "", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						resp := &fhttp.Response{Header: fhttp.ResponseHeader{}}
						resp.Header.Add(constants.HeaderLoadDuration, "1")
						body, _ := json.Marshal(snerror.BadResponse{Code: snerror.DownLoadCodeErrorCode, Message: "test"})
						resp.SetBody(body)
						return resp
					}),
					ApplyMethod(reflect.TypeOf(&fhttp.ResponseHeader{}), "StatusCode",
						func(_ *fhttp.ResponseHeader) int {
							return http.StatusInternalServerError
						}),
					ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.p.SendUpdateAgencyInfoRequestToWorker(tt.args.workerInstance, tt.args.traceID, tt.args.workerID, tt.args.service, tt.args.podName)
			patches.ResetAll()
		})
	}
}

func TestInitUpdateAgencyInfoRequest(t *testing.T) {
	type args struct {
		workerInstance v1alpha1.WorkerInstance
		service        *faasfunction.FunctionService
		httpsEnable    bool
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1 succeed to init update agency info request for pod",
			args: args{
				workerInstance: v1alpha1.WorkerInstance{},
				service:        &faasfunction.FunctionService{},
				httpsEnable:    false,
			}, wantErr: false, patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				now := time.Now().String()
				patches.Append(mockUtils.PatchSlice{ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, nil
				}),
					ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
						return "asdfsdffsdafasdf", now
					}),
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, nil })})
				return patches
			}},

		{
			name: "case2 failed to init update agency info request for pod, config.GetConfig() error",
			args: args{
				workerInstance: v1alpha1.WorkerInstance{},
				service:        &faasfunction.FunctionService{},
				httpsEnable:    false,
			}, wantErr: true, patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				now := time.Now().String()
				patches.Append(mockUtils.PatchSlice{ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, errors.New("test")
				}),
					ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
						return "asdfsdffsdafasdf", now
					})})
				return patches
			}},

		{
			name: "case3 failed to init update agency info request for pod, json.Marshal error",
			args: args{
				workerInstance: v1alpha1.WorkerInstance{},
				service:        &faasfunction.FunctionService{},
				httpsEnable:    false,
			}, wantErr: true, patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				now := time.Now().String()
				patches.Append(mockUtils.PatchSlice{ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{},
					}, nil
				}),
					ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
						return "asdfsdffsdafasdf", now
					}),
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("s") })})
				return patches
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, _, err := initUpdateAgencyInfoRequest(tt.args.workerInstance, tt.args.service, tt.args.httpsEnable)
			if (err != nil) != tt.wantErr {
				t.Errorf("initUpdateAgencyInfoRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_setAgencyRequestHeader(t *testing.T) {
	convey.Convey("Test setAgencyRequestHeader", t, func() {
		patches := [...]*Patches{
			ApplyFunc(utils.GetUserAgencyFromCacheMap, func(tenant string, domainID string,
				agencyID string) tools.UserAgency {
				return tools.UserAgency{
					AccessKey:     "test",
					SecretKey:     "test",
					Token:         "test",
					SecurityAk:    "test",
					SecuritySk:    "test",
					SecurityToken: "test",
				}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		var functionRequest commontypes.FunctionSpecRequest
		functionRequest.Message.Function.Role.AppXRole = "appTest"
		functionRequest.Message.Function.URN = "sn:cn:yrk:tenantIDTest:function:0@default@disableAKSK:latest"
		functionRequest.Message.Function.DomainID = "domainIDTest"
		convey.Convey("Test DisableInsecureAkSk:false", func() {
			req := fhttp.AcquireRequest()
			functionRequest.Message.Function.DisableInsecureAkSk = false
			setAgencyRequestHeader(req, functionRequest)
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderAccessKey))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecretKey))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderAuthToken))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecurityAccessKey))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecuritySecretKey))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecurityToken))
		})
		convey.Convey("Test DisableInsecureAkSk:true", func() {
			req := fhttp.AcquireRequest()
			functionRequest.Message.Function.DisableInsecureAkSk = true
			setAgencyRequestHeader(req, functionRequest)
			convey.ShouldBeNil(req.Header.Peek(constants.HeaderAccessKey))
			convey.ShouldBeNil(req.Header.Peek(constants.HeaderSecretKey))
			convey.ShouldBeNil(req.Header.Peek(constants.HeaderAuthToken))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecurityAccessKey))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecuritySecretKey))
			convey.ShouldNotBeNil(req.Header.Peek(constants.HeaderSecurityToken))
		})
	})
}

func TestDealWithVPCError(t *testing.T) {
	convey.Convey("TestDealWithVPCError", t, func() {
		convey.Convey("VPCXRoleNotFound", func() {
			err := errors.New(utils.ErrVPCXRoleNotFound.Error())
			err = dealWithVPCError(err)
			var snErr snerror.SNError
			ok := errors.As(err, &snErr)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(snErr.Error(), convey.ShouldEqual, snerror.ErrText(snerror.VPCXRoleNotFound))
		})
		convey.Convey("ErrNoOperationalPermissionsVpc", func() {
			err := errors.New(utils.ErrNoOperationalPermissionsVpc.Error())
			err = dealWithVPCError(err)
			var snErr snerror.SNError
			ok := errors.As(err, &snErr)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(snErr.Error(), convey.ShouldEqual, utils.ErrNoOperationalPermissionsVpc.Error())
		})
		convey.Convey("NotEnoughNIC", func() {
			err := errors.New(utils.ErrNoAvailableVpcPatInstance.Error())
			err = dealWithVPCError(err)
			var snErr snerror.SNError
			ok := errors.As(err, &snErr)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(snErr.Error(), convey.ShouldEqual, snerror.ErrText(snerror.NotEnoughNIC))
		})
		convey.Convey("ErrVPCNotFound", func() {
			err := errors.New(utils.ErrVPCNotFound.Error())
			err = dealWithVPCError(err)
			var snErr snerror.SNError
			ok := errors.As(err, &snErr)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(snErr.Error(), convey.ShouldEqual, snerror.ErrText(snerror.VPCNotFound))
		})
		convey.Convey("NotVPCError", func() {
			notVPCError := "not a vpc error"
			err := errors.New(notVPCError)
			err = dealWithVPCError(err)
			var snErr snerror.SNError
			ok := errors.As(err, &snErr)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(snErr.Error(), convey.ShouldEqual, notVPCError)
		})
	})
}

func Test_checkReachMaxInstance(t *testing.T) {
	type args struct {
		functionService     *faasfunction.FunctionService
		requestInfo         *config.RequestInfo
		maxInstance         int64
		maxReversedInstance int64
	}
	tests := []struct {
		name        string
		args        args
		wantErr     error
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test reachMaxOnDemandInsNum and not reversed-ins",
			args{
				functionService: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							TenantID: "a",
						},
					},
				},
				requestInfo:         &config.RequestInfo{},
				maxInstance:         -2,
				maxReversedInstance: 0,
			},
			utils.ErrReachMaxOnDemandInstanceNumPerTenant,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadOrStore",
						func(syncMap *sync.Map, key, value any) (actual any, loaded bool) {
							return value, false
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load",
						func(_ *sync.Map, key any) (value any, ok bool) {
							return "", true
						}),
				})
				return patches
			},
		},
		{
			"test reachMaxOnDemandInsNum but reversed-ins ",
			args{
				functionService: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							TenantID: "a",
						},
					},
				},
				requestInfo: &config.RequestInfo{
					Applier: constants.WorkerManagerApplier,
				},
				maxInstance:         -2,
				maxReversedInstance: 0,
			},
			nil,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadOrStore",
						func(_ *sync.Map, key, value any) (actual any, loaded bool) {
							return value, false
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load",
						func(_ *sync.Map, key any) (value any, ok bool) {
							return "", true
						}),
				})
				return patches
			},
		},
		{
			"test reachMaxReversedInsNum not reversed-ins ",
			args{
				functionService: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							TenantID: "a",
						},
					},
				},
				requestInfo:         &config.RequestInfo{},
				maxInstance:         0,
				maxReversedInstance: -2,
			},
			nil,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadOrStore",
						func(_ *sync.Map, key, value any) (actual any, loaded bool) {
							return value, false
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load",
						func(_ *sync.Map, key any) (value any, ok bool) {
							return "", true
						}),
				})
				return patches
			},
		},
		{
			"test reachMaxReversedInsNum not reversed-ins ",
			args{
				functionService: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							TenantID: "",
						},
					},
				},
				requestInfo: &config.RequestInfo{
					Applier: constants.WorkerManagerApplier,
				},
				maxInstance:         0,
				maxReversedInstance: -2,
			},
			nil,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadOrStore",
						func(_ *sync.Map, key, value any) (actual any, loaded bool) {
							return value, false
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load",
						func(_ *sync.Map, key any) (value any, ok bool) {
							return "", true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		temp := cache.DefaultMaxOnDemandInstanceNum
		tempReversed := cache.DefaultMaxReversedInstanceNum
		cache.DefaultMaxOnDemandInstanceNum = tt.args.maxInstance
		cache.DefaultMaxReversedInstanceNum = tt.args.maxReversedInstance

		err := checkReachMaxInstance(tt.args.functionService, tt.args.requestInfo)
		if !errors.Is(err, tt.wantErr) {
			t.Errorf("failed to test checkReachMaxInstance, err: %v", err)
		}
		cache.DefaultMaxOnDemandInstanceNum = temp
		cache.DefaultMaxReversedInstanceNum = tempReversed
		patches.ResetAll()
	}
}

func Test_StatefulInstanceFlowOutDelete(t *testing.T) {
	type args struct {
		podAction []cache.PodAction
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test podfetcher is nil",
			args{
				podAction: []cache.PodAction{},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(GetPodFetcher, func() *PodFetcher {
						return nil
					}),
				})
				return patches
			},
		},
		{
			"test length of podAction is 0",
			args{
				podAction: []cache.PodAction{},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(GetPodFetcher, func() *PodFetcher {
						return &PodFetcher{}
					}),
				})
				return patches
			},
		},
		{
			"test functionService is nil",
			args{
				podAction: []cache.PodAction{{FuncSvc: "test", StatefulMeta: cache.Metadata{}}},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(GetPodFetcher, func() *PodFetcher {
						return &PodFetcher{}
					}),
				})
				return patches
			},
		},
		{
			"test length GetFunctionPreStopHandler is not 0",
			args{
				podAction: []cache.PodAction{{FuncSvc: "testsvc", StatefulMeta: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}}}},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadOrStore",
						func(_ *sync.Map, key, value any) (actual any, loaded bool) {
							return value, false
						}),
					gomonkey.ApplyFunc((*PodFetcher).SendUpdateAgencyInfoRequestToWorker, func(p *PodFetcher, workerInstance v1alpha1.WorkerInstance, traceID string,
						workerID string, service *faasfunction.FunctionService, podName string) {
					}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S, func(p *PodFetcher, podName, traceID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(cache.WorkerCache.GetFunctionPreStopHandler, func(_ string) string {
						return "testtest"
					}),
				})
				return patches
			},
		},
		{
			"test length GetFunctionPreStopHandler is not 0, delete pod error",
			args{
				podAction: []cache.PodAction{{FuncSvc: "testsvc", StatefulMeta: cache.Metadata{FuncMeta: &faasfunction.FunctionService{}}}},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadOrStore",
						func(_ *sync.Map, key, value any) (actual any, loaded bool) {
							return value, false
						}),
					gomonkey.ApplyFunc((*PodFetcher).SendUpdateAgencyInfoRequestToWorker, func(p *PodFetcher, workerInstance v1alpha1.WorkerInstance, traceID string,
						workerID string, service *faasfunction.FunctionService, podName string) {
					}),
					gomonkey.ApplyFunc((*PodFetcher).DeletePodByK8S, func(p *PodFetcher, podName, traceID string) error {
						return errors.New("error")
					}),
					gomonkey.ApplyFunc(cache.WorkerCache.GetFunctionPreStopHandler, func(_ string) string {
						return "testtest"
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		StatefulInstanceFlowOutDelete(tt.args.podAction)
		patches.ResetAll()
	}
}

func TestPodFetcher_refreshDynamicResources(t *testing.T) {
	convey.Convey("Test RefreshDynamicResources", t, func() {
		p := &PodFetcher{
			PoolMgr: &poolmanager.PoolMgr{},
		}
		requestInfo := &config.RequestInfo{
			StateID: "123",
			CreateOptions: &types.StateCreateOptions{
				CPU:    2,
				Memory: 4096,
			},
			TraceID: "456",
		}
		functionService := &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					Resource: faasfunction.ResourceSpec{
						CPU: 4,
						Mem: 8192,
					},
				},
			},
		}
		patch := gomonkey.ApplyMethod(reflect.TypeOf(p.PoolMgr), "GetStatefulDynamicClosestStandard", func(_ *poolmanager.PoolMgr, cpu, mem int64) (int64, int64) {
			return 10, 100
		})
		defer patch.Reset()
		p.refreshDynamicResources(requestInfo, functionService)
		convey.Convey("Should set CPU and Memory correctly when StateID and CreateOptions are provided", func() {
			convey.So(requestInfo.CPU, convey.ShouldEqual, 10)
			convey.So(requestInfo.Memory, convey.ShouldEqual, 100)
		})
		convey.Convey("Should set CPU and Memory from FunctionService when StateID and CreateOptions are not provided", func() {
			requestInfo.StateID = ""
			requestInfo.CreateOptions = nil
			requestInfo.CPU = 0
			requestInfo.Memory = 0
			p.refreshDynamicResources(requestInfo, functionService)
			convey.So(requestInfo.CPU, convey.ShouldEqual, 4)
			convey.So(requestInfo.Memory, convey.ShouldEqual, 8192)
		})
	})
}

func Test_statefulFilter(t *testing.T) {
	convey.Convey("Test statefulFilter", t, func() {
		cache.StateWorkersList.Store("123", "test")
		cache.StateWorkersList.Store("1234", "test1")
		requestInfo := &config.RequestInfo{
			StateID: "123",
		}
		functionService := &faasfunction.FunctionService{
			ObjectMeta: faasfunction.ObjectMeta{
				ID: "1",
			},
		}
		convey.Convey("stateID is empty", func() {
			requestInfo.StateID = ""
			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("reloading and cluster flowout", func() {
			requestInfo.IsReload = true
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.IsCurrentClustersStatusRunning, func() bool {
					return false
				}),
			})
			defer patches.ResetAll()
			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "cluster status is not running")
		})
		convey.Convey("not reloading but has instance", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.IsCurrentClustersStatusRunning, func() bool {
					return true
				}),
			})
			defer patches.ResetAll()
			cache.WorkerCache.StoreWorkerInstance(functionService.ID, v1alpha1.WorkerInstance{
				InstanceID: "new",
				IP:         "1.1.1.1",
				NodeIP:     "2.2.2.2",
				PodName:    "podA",
				NodeName:   "nodeA",
				StateID:    requestInfo.StateID,
			})
			cache.WorkerCache.CheckAndInsertStateIDMaxInstance(functionService.ID, requestInfo.StateID)
			pod, err := statefulFilter(requestInfo, functionService)
			convey.So(pod.Status.PodIP, convey.ShouldEqual, "1.1.1.1")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("not reloading", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.IsCurrentClustersStatusRunning, func() bool {
					return true
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "1234"
			functionService.ID = "new"
			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "instance: new with StateID: 1234 is not exist and not reloading")
		})
		convey.Convey("not in this cluster", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.IsCurrentClustersStatusRunning, func() bool {
					return true
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "12345-notexist"
			functionService.ID = "new"
			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, snerror.ErrText(snerror.StatefulFunctionReloading))
		})
		convey.Convey("test refresh cpu and memory", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.GetCPUMemoryOfStateRouters, func(value interface{}) (int64, int64) {
					return 128, 256
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "1234"
			functionService.ID = "new"
			statefulFilter(requestInfo, functionService)
			convey.So(requestInfo.CreateOptions, convey.ShouldNotBeNil)
			convey.So(requestInfo.CreateOptions.CPU, convey.ShouldEqual, 128)
			convey.So(requestInfo.CreateOptions.Memory, convey.ShouldEqual, 256)
		})
		convey.Convey("test no need refresh cpu and memory", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.GetCPUMemoryOfStateRouters, func(value interface{}) (int64, int64) {
					return 128, 256
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "1234"
			functionService.ID = "new"
			requestInfo.CreateOptions = &types.StateCreateOptions{
				CPU:    300,
				Memory: 400,
			}
			statefulFilter(requestInfo, functionService)
			convey.So(requestInfo.CreateOptions, convey.ShouldNotBeNil)
			convey.So(requestInfo.CreateOptions.CPU, convey.ShouldEqual, 300)
			convey.So(requestInfo.CreateOptions.Memory, convey.ShouldEqual, 400)
		})
		convey.Convey("refresh creator", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.GetCPUMemoryOfStateRouters, func(value interface{}) (int64, int64) {
					return 128, 256
				}),
				gomonkey.ApplyFunc(getCreatorFromStateMetaInfo, func(value interface{}) string {
					return "testCreator"
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "1234"
			functionService.ID = "new"
			requestInfo.CreateOptions = &types.StateCreateOptions{
				CPU:    300,
				Memory: 400,
			}
			statefulFilter(requestInfo, functionService)
			if requestInfo.Creator != "testCreator" {
				t.Errorf("this is error")
			}
		})
		convey.Convey("not in StateWorkersList but in etcd", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.GetCPUMemoryOfStateRouters, func(value interface{}) (int64, int64) {
					return 200, 256
				}),
				gomonkey.ApplyFunc(cache.UpdateStateWorkerList, func(stateID string) (bool, error) {
					return true, nil
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "1234-not-exist-but-in-etcd"
			functionService.ID = "new"
			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "instance: new with StateID: 1234-not-exist-but-in-etcd is not exist and not reloading")
			convey.So(requestInfo.CreateOptions.CPU, convey.ShouldEqual, 200)
			convey.So(requestInfo.CreateOptions.Memory, convey.ShouldEqual, 256)
		})
		convey.Convey("not in StateWorkersList but in etcd with another cluster", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.GetCPUMemoryOfStateRouters, func(value interface{}) (int64, int64) {
					return 200, 256
				}),
				gomonkey.ApplyFunc(cache.UpdateStateWorkerList, func(stateID string) (bool, error) {
					return false, nil
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "1234-not-exist-but-in-redis-and-in-other-cluster"
			functionService.ID = "new"

			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, snerror.ErrText(snerror.StatefulFunctionReloading))
			convey.So(requestInfo.CreateOptions, convey.ShouldBeNil)
		})
		convey.Convey("not in StateWorkersList but UpdateStateWorkerList error", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.GetCPUMemoryOfStateRouters, func(value interface{}) (int64, int64) {
					return 200, 256
				}),
				gomonkey.ApplyFunc(cache.UpdateStateWorkerList, func(stateID string) (bool, error) {
					return false, errors.New("test error")
				}),
			})
			defer patches.ResetAll()
			requestInfo.StateID = "in-another-cluster"
			functionService.ID = "new"

			_, err := statefulFilter(requestInfo, functionService)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, snerror.ErrText(snerror.StatefulFunctionReloading))
			convey.So(requestInfo.CreateOptions, convey.ShouldBeNil)
		})
	})
}

func Test_getCreatorFromStateCache(t *testing.T) {
	convey.Convey("Test StateWorkersList", t, func() {
		convey.Convey("test StateWorkersList load ok, struct not cache.StateMetadata", func() {
			cache.StateWorkersList.Delete("stateID")
			cache.StateWorkersList.Store("stateID", "123")
			res := getCreatorFromStateMetaInfo("stateID")
			if res != "" {
				t.Errorf("this is error ")
			}
			cache.StateWorkersList.Delete("stateID")
		})
		convey.Convey("test StateWorkersList load ok", func() {
			cache.StateWorkersList.Delete("stateID")
			cache.StateWorkersList.Store("stateID", cache.StateMetadata{Creator: "testCreator"})
			val := cache.StateMetadata{Creator: "testCreator"}
			res := getCreatorFromStateMetaInfo(val)
			if res != "testCreator" {
				t.Errorf("this is error ")
			}
			cache.StateWorkersList.Delete("stateID")
		})
	})
}

func TestPodFetcher_generalizePod_withErrorMessage(t *testing.T) {
	q := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},
	))
	podFetcher = &PodFetcher{
		Queue: queue{RateLimitingInterface: q},
	}
	var setErrorFlag bool
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc((*PodFetcher).generalizeRequestToWorker, func(_ *PodFetcher, pod *corev1.Pod,
			reqInfo *config.RequestInfo, service *faasfunction.FunctionService, chooseStartTime time.Time) error {
			return errors.New("test")
		}),
		gomonkey.ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
			func(_ *vpcmanager.VpcHandler, instanceName string) error {
				fmt.Println("succeed to mock IdleDeletePatServiceList")
				return nil
			}),
		gomonkey.ApplyFunc(poderrormessage.ReportPodErrMsgToRedis, func(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
			podErrInfo poderrormessage.PodErrorInfo) {
			setErrorFlag = true
			return
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()

	err := podFetcher.generalizePod(&corev1.Pod{}, &config.RequestInfo{}, &faasfunction.FunctionService{
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"},
		},
	}, time.Now(), false)
	assert.Equal(t, err != nil, true)
	assert.True(t, setErrorFlag)
}

func TestUpdateAllFuncAgency(t *testing.T) {
	type args struct {
		stopCh   chan struct{}
		interval time.Duration
	}
	flag := 0
	stopCh := make(chan struct{})
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(GetPodFetcher, func() *PodFetcher {
			return &PodFetcher{
				workerClient: &fhttp.Client{},
			}
		}),
		gomonkey.ApplyFunc((*PodFetcher).SendUpdateAgencyInfoRequestToWorker, func(p *PodFetcher, workerInstance v1alpha1.WorkerInstance, traceID string,
			workerID string, service *faasfunction.FunctionService, podName string) {
			flag = 1
		}),
		gomonkey.ApplyFunc(utils.GetFunctionPodsByInformer, func(_ *faasfunction.FunctionService) []*corev1.Pod {
			return []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod",
						Labels: map[string]string{
							utils.LabelApplier: "worker-manager",
						},
					},
				},
			}
		}),
	})
	defer patches.ResetAll()
	funcMeta := &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
		Runtime: faasfunction.RuntimeSpec{Language: "java"}}}}
	funcMeta.Spec.Configuration.Runtime.Language = "java17"
	cache.WorkerCache.CreateFunctionCacheByFuncMeta("funcSvcID", funcMeta)
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test update all function agency",
			args: args{
				stopCh:   stopCh,
				interval: 3 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan time.Time)
			defer gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
				return &time.Ticker{C: ch}
			}).Reset()
			go func(stop chan struct{}) {
				ch <- time.Time{}
				ch <- time.Time{}
				close(stopCh)
			}(tt.args.stopCh)
			UpdateAllFuncAgency(tt.args.stopCh, tt.args.interval)
			assert.Equal(t, 1, flag)
		})
	}
}

func Test_podFetcher_getWorkerLog(t *testing.T) {
	podFetcher = &PodFetcher{KubeClient: &kubernetes.Clientset{}}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
		func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
			return &fake.FakeCoreV1{}
		})
	patches.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
		func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
			return &fake.FakePods{}
		})
	patches.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "GetLogs",
		func(_ *fake.FakePods, name string, opts *corev1.PodLogOptions) *rest.Request {
			return &rest.Request{}
		})
	patches.ApplyMethod(reflect.TypeOf(&rest.Request{}), "Stream",
		func(_ *rest.Request, ctx context.Context) (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewReader([]byte("xxxxxxx panic: runtime error [signal SIGSEGV: segmer xx]"))), nil
		})
	str := podFetcher.getWorkerLog(&corev1.Pod{})
	assert.Equal(t, "panic: runtime error [signal SIGSEGV: segmer xx]", str)
	patches.Reset()
}

func Test_safetyVpcPodSetting(t *testing.T) {
	convey.Convey("Test safetyVpcPodSetting", t, func() {
		podFetcher = &PodFetcher{}
		convey.Convey("function not IsSafety", func() {
			service := &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{FuncVpc: faasfunction.Vpc{IsSafety: false}}}}
			podFetcher.safetyVpcPodSetting(service)
		})
		convey.Convey("k8s get service error", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Services",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.ServiceInterface {
						return &fake.FakeServices{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeServices{}), "Get",
					func(_ *fake.FakeServices, ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Service, error) {
						return nil, errors.New("this is error")
					}),
			})
			podFetcher.KubeClient = &kubernetes.Clientset{}
			defer patches.ResetAll()
			service := &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{FuncVpc: faasfunction.Vpc{IsSafety: true}}}}
			podFetcher.safetyVpcPodSetting(service)
		})
		convey.Convey("clear", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
					func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Services",
					func(_ *fake.FakeCoreV1, namespace string) typecorev1.ServiceInterface {
						return &fake.FakeServices{}
					}),
				ApplyMethod(reflect.TypeOf(&fake.FakeServices{}), "Get",
					func(_ *fake.FakeServices, ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Service, error) {
						return &corev1.Service{}, nil
					}),
			})
			podFetcher.KubeClient = &kubernetes.Clientset{}
			defer patches.ResetAll()
			service := &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{FuncVpc: faasfunction.Vpc{IsSafety: true}}}}
			podFetcher.safetyVpcPodSetting(service)
		})
	})

}

func TestGetKmsCache(t *testing.T) {
	mockInfo := utils.CheckKmsCipherInfo{
		DecType:       "test",
		TraceID:       "test",
		KmsKeyID:      "test",
		CipherKey:     "test",
		EncryptCipher: "test",
		TenantID:      "test",
	}

	p := gomonkey.ApplyFuncReturn(utils.GetAgencyIDFromFunctionService, "test")
	defer p.Reset()
	p.ApplyFuncReturn(utils.GetUserCipherCache, &utils.UserCipherCache{}, false)
	p.ApplyFuncReturn(utils.CheckAndStoreCipherCache, &utils.UserCipherCache{}, errors.New("test"))

	_, err := getKmsCache(mockInfo, &faasfunction.FunctionService{})
	assert.Error(t, err)
}
