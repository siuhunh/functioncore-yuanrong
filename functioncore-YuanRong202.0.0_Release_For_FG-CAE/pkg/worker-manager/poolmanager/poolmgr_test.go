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

// Package poolmgr manage pool
package poolmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	appsv1fake "k8s.io/client-go/kubernetes/typed/apps/v1/fake"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corefake "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snapshotmanager"
	commonTypes "functioncore/pkg/common/types"
	wcache "functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/detector"
	"functioncore/pkg/worker-manager/podhandler"
	"functioncore/pkg/worker-manager/podtemplate"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func Test_getStandardFromPodName(t *testing.T) {
	type args struct {
		podName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"normal case",
			args{podName: "pool7-500-500-python3-c84c95788-gfkd4"},
			"500-500-python3",
		},
		{
			"custom runtime",
			args{podName: "pool14-500-500-custom-runtime-6dfc7659cf-ngzh7"},
			"500-500-custom-runtime",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := getStandardFromPodName(tt.args.podName); string(got) != tt.want {
				t.Errorf("getStandardFromPodName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPoolMgr_UpdatePod(t *testing.T) {
	poolMgr, pod := newTestPoolMgr()
	spec := faasfunction.FunctionServiceSpec{}
	spec.Configuration.Runtime.Language = "python3"
	poolMgr.DeletePod(pod)
	pod.Annotations[usedLabelKey] = utils.FalseString
	poolMgr.UpdatePod(pod)
	gotPod, _ := poolMgr.GetPod(context.Background(), &spec, common.PodRequest{
		CPU: 500,
		Mem: 500,
	})
	if gotPod != nil {
		if gotPod.Name != pod.Name {
			t.Errorf("GetPod() = %v, wanted pod name %v", gotPod.Name, pod.Name)
		}
	}
}

// TestPoolMgr_GetPod -
func TestPoolMgr_GetPod(t *testing.T) {
	convey.Convey("test: get pod", t, func() {
		convey.Convey("test: decode custom resources err", func() {
			patches := ApplyFunc(decodeCustomResources, func(rawJSON string) (map[string]int64, error) {
				return nil, errors.New("test err")
			})
			patches.ApplyFunc((*PoolMgr).updatePodLabels, func(_ *PoolMgr, pod *corev1.Pod,
				enableTMPExpansion bool) error {
				return nil
			})
			defer patches.Reset()
			pod, err := (&PoolMgr{}).GetPod(context.Background(),
				&faasfunction.FunctionServiceSpec{}, common.PodRequest{})
			convey.So(pod, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

// TestPoolMgr_createPoolDeployByRetry -
func TestPoolMgr_createPoolDeployByRetry(t *testing.T) {
	convey.Convey("test: create pool deploy by retry", t, func() {
		convey.Convey("test: retry max times err", func() {
			stopCh := make(chan struct{})
			errMax := false
			patch := ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client,
				almType alarm.Type, specAddition string) {
				close(stopCh)
				errMax = true
			})
			patch.ApplyFunc((*PoolMgr).createPoolDeploy, func(_ *PoolMgr,
				curPool *Pool, deployment *appsv1.Deployment) error {
				return errors.New("test err")
			})
			patch.ApplyFunc(time.Sleep, func(d time.Duration) {})
			defer patch.Reset()
			poolMgr := &PoolMgr{}
			poolMgr.createPoolDeployByRetry(&Pool{}, &appsv1.Deployment{}, stopCh)
			convey.So(errMax, convey.ShouldBeTrue)
		})
		convey.Convey("test: succeed to create pool deploy", func() {
			stopCh := make(chan struct{})
			ok := false
			patch := ApplyFunc((*PoolMgr).createPoolDeploy, func(_ *PoolMgr,
				curPool *Pool, deployment *appsv1.Deployment) error {
				ok = true
				return nil
			})
			defer patch.Reset()
			poolMgr := &PoolMgr{}
			poolMgr.createPoolDeployByRetry(&Pool{}, &appsv1.Deployment{}, stopCh)
			convey.So(ok, convey.ShouldBeTrue)
		})
	})
}

func TestPoolMgr_customContainer(t *testing.T) {
	poolMgr, pod := newTestPoolMgr()
	spec := faasfunction.FunctionServiceSpec{}
	spec.Configuration.CustomContainerConfig.ControlPath = "event"
	poolMgr.DeletePod(pod)
	pod.Annotations[usedLabelKey] = utils.FalseString
	poolMgr.UpdatePod(pod)
	gotPod, _ := poolMgr.GetPod(context.Background(), &spec, common.PodRequest{
		CPU: 500,
		Mem: 500,
	})
	if gotPod != nil {
		if gotPod.Name != pod.Name {
			t.Errorf("GetPod() = %v, wanted pod name %v", gotPod.Name, pod.Name)
		}
	}
}

func TestPoolMgr_checkpointPod(t *testing.T) {
	poolMgr, pod := newTestPoolMgr()
	pod.Labels[utils.StandardLabelKey] = utils.CheckpointPodStandard
	spec := faasfunction.FunctionServiceSpec{}
	spec.SnapshotDeployment.BucketID = "id"
	poolMgr.DeletePod(pod)
	poolMgr.UpdatePod(pod)
	gotPod, _ := poolMgr.GetPod(context.Background(), &spec, common.PodRequest{
		CPU: 500,
		Mem: 500,
	})
	if gotPod != nil {
		if gotPod.Name != pod.Name {
			t.Errorf("GetPod() = %v, wanted pod name %v", gotPod.Name, pod.Name)
		}
	}
}
func TestPoolMgr_DsWorkerPod(t *testing.T) {
	poolMgr, pod := newTestPoolMgr()
	pod.Name = "ds-worker123123"
	patch := ApplyFunc(handleDsWorkerDeleteEvent, func(pod *corev1.Pod) {})
	poolMgr.DeletePod(pod)
	patch.Reset()

	patch = ApplyFunc(handleDsWorkerPodEvent, func(pod *corev1.Pod) {})
	poolMgr.UpdatePod(pod)
	patch.Reset()
}
func TestPoolMgr_GetOwnerKindForPod(t *testing.T) {
	podKind := getOwnerKindForPod(nil)
	if podKind != "" {
		t.Errorf("getOwnerKindForPod() = %s, , wanted pod owner kind empty str", podKind)
	}

	pod := &corev1.Pod{}
	podKind = getOwnerKindForPod(pod)
	if podKind != "" {
		t.Errorf("getOwnerKindForPod() = %s, , wanted pod owner kind empty str", podKind)
	}

	testKind := "testKind"
	ownerReference := metav1.OwnerReference{
		Kind: testKind,
	}
	pod.OwnerReferences = append(pod.OwnerReferences, ownerReference)
	podKind = getOwnerKindForPod(pod)
	if podKind != testKind {
		t.Errorf("getOwnerKindForPod() = %s, , wanted pod owner kind %s", podKind, testKind)
	}
}

func TestPoolMgr_AddPod(t *testing.T) {
	poolMgr, pod := newTestPoolMgr()
	spec := faasfunction.FunctionServiceSpec{}
	spec.Configuration.Runtime.Language = "python3"
	pod.Annotations[usedLabelKey] = utils.FalseString
	poolMgr.AddPod(pod)
	gotPod, _ := poolMgr.GetPod(context.Background(), &spec, common.PodRequest{
		CPU: 500,
		Mem: 500,
	})
	if gotPod != nil {
		if gotPod.Name != pod.Name {
			t.Errorf("GetPod() = %v, wanted pod name %v", gotPod.Name, pod.Name)
		}
	}
}

func TestPoolMgr_NoPod(t *testing.T) {
	poolMgr, _ := newTestPoolMgr()
	spec := faasfunction.FunctionServiceSpec{}
	spec.Configuration.Runtime.Language = "python3"
	_, err := poolMgr.GetPod(context.Background(), &spec, common.PodRequest{
		CPU: 500,
		Mem: 500,
	})
	if err != nil {
		if err.Error() != ErrNoSuchStandard.Error() {
			t.Errorf("GetPod() = %s, wanted error info %v", err.Error(), ErrNoSuchStandard)
		}
	}
}

func TestPoolMgr_Start(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		podDetector        *detector.PodDetector
		podHandlerManager  *podhandler.Manager
		poolList           map[standard]*Pool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with deployment is nil",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
								ScalingUpTriggerTimeThreshold: 5,
								EnableFusionPolicy:            1,
							},
						}, nil
					}),
					ApplyFunc(config.GetElasticScalingConfig, func() *config.ElasticScalingConfig {
						return nil
					}),
					ApplyFunc(alarm.GetClient, func() *alarm.Client {
						return &alarm.Client{}
					}),
					ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(_ *alarm.Client, almType alarm.Type) {
							fmt.Println("mock RegisterAlarmType method")
							return
						}),
					ApplyFunc((*PoolMgr).splitPoolConfigListByName,
						func(_ *PoolMgr, runtimeName string) []config.PoolConfig {
							return []config.PoolConfig{
								{
									Name: "mock pool config1",
								},
							}
						}),
					ApplyFunc(utils.GetWorkerManagerDeployUID, func() types.UID {
						return "111"
					}),
					ApplyFunc((*poolDeploymentBuilder).build, func(_ *poolDeploymentBuilder, pool *Pool, ownerUID types.UID) *appsv1.Deployment {
						return &appsv1.Deployment{}
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test with deployment is nil
		{
			name: "test with deployment is nil",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
								ScalingUpTriggerTimeThreshold: 5,
								EnableFusionPolicy:            1,
							},
						}, nil
					}),
					ApplyFunc(config.GetElasticScalingConfig, func() *config.ElasticScalingConfig {
						return nil
					}),
					ApplyFunc(alarm.GetClient, func() *alarm.Client {
						return &alarm.Client{}
					}),
					ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(_ *alarm.Client, almType alarm.Type) {
							fmt.Println("mock RegisterAlarmType method")
							return
						}),
					ApplyFunc((*PoolMgr).splitPoolConfigListByName,
						func(_ *PoolMgr, runtimeName string) []config.PoolConfig {
							return []config.PoolConfig{
								{
									Name: "mock pool config1",
								},
							}
						}),
					ApplyFunc(utils.GetWorkerManagerDeployUID, func() types.UID {
						return "111"
					}),
					ApplyFunc((*poolDeploymentBuilder).build, func(_ *poolDeploymentBuilder, pool *Pool, ownerUID types.UID) *appsv1.Deployment {
						return &appsv1.Deployment{}
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test with deployment is not nil
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				podDetector:        tt.fields.podDetector,
				podHandlerManager:  tt.fields.podHandlerManager,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
				PodUsedRecord: &PodUsedRecord{
					usedPodRecordVal: make(map[standard]int64),
				},
			}
			patches := tt.patchesFunc()
			err := p.Start(tt.args.stopCh)
			patches.ResetAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func TestPoolMgr_Start1(t *testing.T) {
	poolMgr := PoolMgr{
		KubeClient: fake.NewSimpleClientset(),
		Config:     config.PoolMgrConfig{},
	}
	patches := [...]*Patches{
		ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{
				LocalAuth: &localauth.AuthConfig{
					AKey: "mock-akey",
					SKey: "mock-skey",
				},
			}, nil
		}),
		ApplyFunc(runtime.HandleCrash, func(additionalHandlers ...func(interface{})) {
			return
		}),
		ApplyFunc(informers.NewSharedInformerFactory, func(client kubernetes.Interface, defaultResync time.Duration) informers.SharedInformerFactory {
			client = fake.NewSimpleClientset()
			return informers.NewSharedInformerFactoryWithOptions(client, defaultResync)
		}),
		ApplyFunc(cache.WaitForCacheSync, func(stopCh <-chan struct{}, cacheSyncs ...cache.InformerSynced) bool {
			return true
		}),
		ApplyFunc(detector.InitPodDetector, func() (*detector.PodDetector, error) {
			return &detector.PodDetector{}, nil
		}),
		ApplyFunc((*alarm.Client).RegisterAlarmType, func(a *alarm.Client, almType alarm.Type) {
			return
		}),
		ApplyFunc(utils.GetWorkerManagerDeployUID, func() types.UID {
			return "111"
		}),
		ApplyFunc((*poolDeploymentBuilder).build, func(_ *poolDeploymentBuilder, pool *Pool, ownerUID types.UID) *appsv1.Deployment {
			return &appsv1.Deployment{}
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	_ = poolMgr.Start(nil)
	poolMgr.poolList = map[standard]*Pool{}
	if len(poolMgr.poolList) != 0 {
		t.Errorf("poolMgr.poolList isn't 0")
	}
	err := poolMgr.Start(nil)
	assert.Nil(t, err)
}

func newTestPoolMgr() (*PoolMgr, *corev1.Pod) {
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
			return &config.K8sCidr{
				SvcCidr:  "aaa",
				PodCidr:  "bbb",
				HostCidr: "ccc",
			}
		}),
		ApplyFunc(detector.InitPodDetector, func() (*detector.PodDetector, error) {
			return &detector.PodDetector{}, nil
		}),
	})
	defer patches.ResetAll()
	podDetector, err := detector.InitPodDetector()
	if err != nil {
		return nil, nil
	}
	poolMgr := PoolMgr{
		KubeClient: fake.NewSimpleClientset(),
		Config: config.PoolMgrConfig{
			IsSharedContainer: false,
		},
		podDetector: podDetector,
		podHandlerManager: createPodHandlerManager(fake.NewSimpleClientset(), config.PoolMgrConfig{
			IsSharedContainer: false,
		}),
	}

	poolMgr.poolList = make(map[standard]*Pool)

	pool := &Pool{}
	pool.mux = new(sync.Mutex)

	pool.PodList = make(map[nodeName]*NodePod)
	node := nodeName(fmt.Sprintf("node-1"))
	pool.PodList[node] = &NodePod{}

	// add pod to pool in node
	pod := &corev1.Pod{}
	pod.Annotations = map[string]string{
		usedLabelKey: utils.FalseString,
	}
	pod.Labels = map[string]string{
		specializeLabelKey: specializeLabelValuePreStart,
	}
	podName := "pool7-500-500-python3-c84c95788-gfkd4"
	pod.Name = podName
	pod.Spec.NodeName = fmt.Sprintf("node-1")
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "1.2.3.4"
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name:         utils.WorkerContainerName,
			RestartCount: 10,
		},
		{
			Name:         utils.RuntimeContainerName,
			RestartCount: 15,
		},
	}

	pool.PodList[node].podsQueue.items = append(pool.PodList[node].podsQueue.items, &podObj{pod, false})
	pool.PodNum = 1

	poolMgr.getDeployment(pool)
	poolMgr.poolList["500-500-python3"] = pool

	return &poolMgr, pod
}

func TestCreatePoolMgr(t *testing.T) {
	type args struct {
		workerClient *fhttp.Client
		stopCh       <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test create pool mgr with get k8s client nil error",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.PatchSlice{}
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return nil
					}),
					ApplyFunc(detector.InitPodDetector, func() (*detector.PodDetector, error) {
						return &detector.PodDetector{}, nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test create pool mgr with get config error",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.PatchSlice{}
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("mock error")
					}),
					ApplyFunc(detector.InitPodDetector, func() (*detector.PodDetector, error) {
						return &detector.PodDetector{}, nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test create pool mgr with no error",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.PatchSlice{}
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{},
						}, nil
					}),
					ApplyFunc(detector.InitPodDetector, func() (*detector.PodDetector, error) {
						return &detector.PodDetector{}, nil
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
			if err := CreatePoolMgr(tt.args.workerClient, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("CreatePoolMgr return error: %v wantErr: %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_getPoolStandard(t *testing.T) {
	type args struct {
		poolConfig config.PoolConfig
	}
	tests := []struct {
		name    string
		args    args
		wantNil bool
	}{
		{
			name:    "test get pool standard",
			args:    args{},
			wantNil: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantNil, getPoolStandard(tt.args.poolConfig) == nil, "getPoolStandard(%v)", tt.args.poolConfig)
		})
	}
}

func Test_getLabelDiffFlag(t *testing.T) {
	type args struct {
		curPool *Pool
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "get label diff flag with empty pod",
			args: args{
				curPool: &Pool{},
			},
			want: false,
		},
		{
			name: "get label diff flag with no empty pod",
			args: args{
				curPool: &Pool{
					Label: map[string]string{"mock-key": "mock-value"},
					Deployment: &v1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"mock-key": "mock-value-diff"},
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getLabelDiffFlag(tt.args.curPool), "getLabelDiffFlag(%v)", tt.args.curPool)
		})
	}
}

func TestPoolMgr_updatePoolDeploy(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		poolList           map[standard]*Pool
		DeployMap          map[string]bool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		curPool    *Pool
		deployment *v1.Deployment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test update pool deploy with fake k8s client",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
			},
			args: args{
				curPool: &Pool{
					Deployment: &v1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{},
						},
					},
				},
				deployment: &v1.Deployment{},
			},
			wantErr: false,
		},
		{
			name: "test update pool deploy with fake k8s client and different spec",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
			},
			args: args{
				curPool: &Pool{
					Deployment: &v1.Deployment{
						Spec: v1.DeploymentSpec{
							Paused: true,
						},
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{},
						},
					},
				},
				deployment: &v1.Deployment{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
			}
			assert.Equalf(t, tt.wantErr, p.updatePoolDeploy(tt.args.curPool, tt.args.deployment) != nil, fmt.Sprintf("updatePoolDeploy(%v, %v)", tt.args.curPool, tt.args.deployment))
		})
	}
}

func TestPoolMgr_deletePoolDeploy(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		poolList           map[standard]*Pool
		DeployMap          map[string]bool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		curPool *Pool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test delete pool deploy",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
			},
			args: args{
				curPool: &Pool{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
			}
			if err := p.deletePoolDeploy(tt.args.curPool); (err != nil) != tt.wantErr {
				t.Errorf("deletePoolDeploy return %v want error is %v", err, tt.wantErr)
			}
		})
	}
}

func TestPoolMgr_updateDeployReplicas(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		poolList           map[standard]*Pool
		DeployMap          map[string]bool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		pool     *Pool
		replicas int32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test updateDeployReplicas",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
			},
			args: args{
				replicas: 1,
				pool:     &Pool{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
			}
			if err := p.updateDeployReplicas(tt.args.pool, tt.args.replicas); (err != nil) != tt.wantErr {
				t.Errorf("updateDeployReplicas return %v want error is %v", err, tt.wantErr)
			}
		})
	}
}

func TestPoolMgr_createPoolDeploy(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		poolList           map[standard]*Pool
		DeployMap          map[string]bool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		curPool    *Pool
		deployment *appsv1.Deployment
	}
	isCalled := false
	tests := []struct {
		name         string
		fields       fields
		args         args
		patchesFunc  mockutils.PatchesFunc
		expectCalled bool
		deployName   string
	}{
		{
			name: "first simple test",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
				poolList:   map[standard]*Pool{},
			},
			args: args{
				curPool: &Pool{
					Label: map[string]string{
						"mock-key": "mock-value",
					},
					Name: "newName",
				},
				deployment: &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Get",
						func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string,
							opts metav1.GetOptions) (*v1.Deployment, error) {
							isCalled = true
							return &v1.Deployment{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"mock-key2": "mock-value2",
									},
								},
							}, nil
						}),
				})
				return patches
			},
			expectCalled: true,
		},
		{
			name: "first simple test with mock delete deployments",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
				poolList: map[standard]*Pool{
					"pool23-400-256-fusion": {},
				},
			},
			args: args{
				curPool: &Pool{
					Label: map[string]string{
						"mock-key": "mock-value",
					},
					Standard: "pool23-400-256-fusion",
					Name:     "newName",
				},
				deployment: &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "newName"},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Get",
						func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string,
							opts metav1.GetOptions) (*v1.Deployment, error) {
							isCalled = true
							return &v1.Deployment{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"mock-key2": "mock-value2",
									},
								},
							}, nil
						}),
					ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Delete",
						func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string,
							opts metav1.DeleteOptions) error {
							return nil
						}),
					ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Update",
						func(_ *appsv1fake.FakeDeployments, ctx context.Context, deployment *v1.Deployment,
							opts metav1.UpdateOptions) (*v1.Deployment, error) {
							return &v1.Deployment{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										"mock-key2": "mock-value2",
									},
								},
							}, nil
						}),
				})
				return patches
			},
			expectCalled: true,
		},
		{
			name: "first simple test with error",
			fields: fields{
				KubeClient: fake.NewSimpleClientset(),
				poolList: map[standard]*Pool{
					"pool23-400-256-fusion": {},
				},
			},
			args: args{
				curPool: &Pool{
					Label: map[string]string{
						"mock-key": "mock-value",
					},
					Standard: "pool23-400-256-fusion",
					Name:     "newName",
				},
				deployment: &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "newName"},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			expectCalled: false,
			deployName:   "newName",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isCalled = false
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
			}
			patches := tt.patchesFunc()
			p.createPoolDeploy(tt.args.curPool, tt.args.deployment)
			if isCalled != tt.expectCalled {
				t.Errorf("isCalled is %v want %v", isCalled, tt.expectCalled)
			}
			if tt.deployName != "" {
				if p.poolList[tt.args.curPool.Standard].Name != tt.deployName {
					t.Errorf("deployName is %v want %v", p.poolList[tt.args.curPool.Standard].Name, tt.deployName)
				}
			}
			patches.ResetAll()
		})
	}
}

func TestPoolMgr_UpdatePoolManagerConfig(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		Config             config.PoolMgrConfig
	}
	type args struct {
		newConfig *config.FaasConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "case_01 new config is nil",
			fields:  fields{&customContainerMGR{}, &checkpointPodMGR{}, config.PoolMgrConfig{}},
			args:    args{newConfig: nil},
			wantErr: true,
		}, {
			name:   "case_02 normal scenario",
			fields: fields{&customContainerMGR{config: config.PoolMgrConfig{}}, &checkpointPodMGR{}, config.PoolMgrConfig{}},
			args: args{newConfig: &config.FaasConfig{PoolMgrConfig: config.PoolMgrConfig{PoolList: config.PoolList{Pool: []config.PoolConfig{
				{
					Name:          "test",
					RunTimeName:   "test",
					LimitCPU:      1,
					LimitMemory:   1000,
					RequestCPU:    1,
					RequestMemory: 1000,
					PoolSize:      15,
					Annotations: map[string]string{
						"a": "a",
					},
				},
			}}}}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				Config:             tt.fields.Config,
			}
			if err := p.UpdatePoolManagerConfig(tt.args.newConfig); (err != nil) != tt.wantErr {
				t.Errorf("UpdatePoolManagerConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPoolMgr_ProcessPoolSizeUpdate(t *testing.T) {
	type fields struct {
		poolList map[standard]*Pool
		Config   config.PoolMgrConfig
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name:   "case_01 poolList is nil",
			fields: fields{poolList: nil, Config: config.PoolMgrConfig{}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case_02 normal scenario",
			fields: fields{
				poolList: map[standard]*Pool{
					"1-1000-python": {PoolSize: 10, scalingConfig: &ScalingConfig{replicas: 10, basePoolSize: 10}, mux: &sync.Mutex{}},
					"1-1000-java":   {PoolSize: 10, scalingConfig: &ScalingConfig{replicas: 10, basePoolSize: 10}, mux: &sync.Mutex{}},
					"1-1000-cpp":    {PoolSize: 10, scalingConfig: &ScalingConfig{replicas: 10, basePoolSize: 10}, mux: &sync.Mutex{}},
				},
				Config: config.PoolMgrConfig{PoolList: config.PoolList{Pool: []config.PoolConfig{
					{
						Name:          "pool22",
						RunTimeName:   "python",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      10,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool23",
						RunTimeName:   "java",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      20,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool24",
						RunTimeName:   "test",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      5,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool25",
						RunTimeName:   "cpp",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      -1,
						Annotations: map[string]string{
							"a": "a",
						},
					},
				}}},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).updateDeployReplicas,
						func(_ *PoolMgr, pool *Pool, replicas int32) error {
							return fmt.Errorf("error")
						}),
					ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case_03 curPool is nil",
			fields: fields{
				poolList: map[standard]*Pool{
					"1-1000-python": nil,
					"1-1000-java":   nil,
				},
				Config: config.PoolMgrConfig{PoolList: config.PoolList{Pool: []config.PoolConfig{
					{
						Name:          "pool22",
						RunTimeName:   "python",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      10,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool23",
						RunTimeName:   "java",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      20,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool24",
						RunTimeName:   "test",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      5,
						Annotations: map[string]string{
							"a": "a",
						},
					},
				}}},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).updateDeployReplicas,
						func(_ *PoolMgr, pool *Pool, replicas int32) error {
							return fmt.Errorf("error")
						}),
					ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case_04 scalingConfig is nil",
			fields: fields{
				poolList: map[standard]*Pool{
					"1-1000-python": {PoolSize: 10, scalingConfig: nil, mux: &sync.Mutex{}},
					"1-1000-java":   {PoolSize: 10, scalingConfig: nil, mux: &sync.Mutex{}},
				},
				Config: config.PoolMgrConfig{PoolList: config.PoolList{Pool: []config.PoolConfig{
					{
						Name:          "pool22",
						RunTimeName:   "python",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      10,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool23",
						RunTimeName:   "java",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      20,
						Annotations: map[string]string{
							"a": "a",
						},
					}, {
						Name:          "pool24",
						RunTimeName:   "test",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      5,
						Annotations: map[string]string{
							"a": "a",
						},
					},
				}}},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).updateDeployReplicas,
						func(_ *PoolMgr, pool *Pool, replicas int32) error {
							return fmt.Errorf("error")
						}),
					ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &PoolMgr{
				poolList: tt.fields.poolList,
				Config:   tt.fields.Config,
			}
			p.ProcessPoolSizeUpdate()
			patches.ResetAll()
		})
	}
}

func TestPoolMgr_deleteRedundantPool(t *testing.T) {
	labels := make(map[string]string)
	labels["standard"] = "pool22"
	deployments := make([]v1.Deployment, 0)
	deployments = append(deployments, v1.Deployment{Spec: v1.DeploymentSpec{
		Selector: &metav1.LabelSelector{MatchLabels: labels},
	}})
	type fields struct {
		poolList map[standard]*Pool
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name:   "case01 failed to list pool",
			fields: fields{poolList: nil},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.ListDeployByK8S,
						func(k8sClient kubernetes.Interface, options metav1.ListOptions) (*v1.DeploymentList, error) {
							return nil, errors.New("failed to list pool")
						}),
				})
				return patches
			},
		}, {
			name:   "case02 failed to delete redundant pool",
			fields: fields{poolList: make(map[standard]*Pool)},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.ListDeployByK8S,
						func(k8sClient kubernetes.Interface, options metav1.ListOptions) (*v1.DeploymentList, error) {
							return &v1.DeploymentList{Items: deployments}, nil
						}),
					ApplyFunc(time.Sleep, func(_ time.Duration) {}),
				})
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.DeleteDeployByK8S,
						func(k8sClient kubernetes.Interface, name string) error {
							return errors.New("failed to delete pool")
						}),
				})
				return patches
			},
		}, {
			name:   "case03 snapshot manager skip to delete redundant pool",
			fields: fields{poolList: make(map[standard]*Pool)},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.ListDeployByK8S,
						func(k8sClient kubernetes.Interface, options metav1.ListOptions) (*v1.DeploymentList, error) {
							return &v1.DeploymentList{Items: deployments}, nil
						}),
				})
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.DeleteDeployByK8S,
						func(k8sClient kubernetes.Interface, name string) error {
							return errors.New("failed to delete pool")
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &PoolMgr{
				poolList: tt.fields.poolList,
			}
			if strings.Contains(tt.name, "snapshot manager") {
				snapshotmanager.SetSnapshotManagerMode(true)
			} else {
				snapshotmanager.SetSnapshotManagerMode(false)
			}
			p.deleteRedundantPool()
			patches.ResetAll()
			snapshotmanager.SetSnapshotManagerMode(false)
		})
	}
}

func TestPoolMgr_createPod(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		podHandlerManager  *podhandler.Manager
		poolList           map[standard]*Pool
	}
	type args struct {
		req *podhandler.PodCreateReq
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        *corev1.Pod
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case001 create pool pod failed",
			fields: fields{
				customContainerMgr: &customContainerMGR{},
				checkpointPodMgr:   &checkpointPodMGR{},
				podHandlerManager:  &podhandler.Manager{},
				poolList:           make(map[standard]*Pool),
			},
			args: args{req: &podhandler.PodCreateReq{
				Spec: nil,
				PodReq: &common.PodRequest{
					FunSvcID: "test",
				},
			}},
			want:    nil,
			wantErr: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.IsNeedRetryScheduling,
						func(err error, labels map[string]string) bool {
							return true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				podHandlerManager:  tt.fields.podHandlerManager,
				poolList:           tt.fields.poolList,
			}
			got, _, err := p.selectPod(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("createPod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createPod() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestSelectPod(t *testing.T) {
	convey.Convey("TestSelectPod", t, func() {
		convey.Convey("TestSelectPod success", func() {
			podHandlerManager := &podhandler.Manager{}
			customContainerMgr := &customContainerMGR{
				config: config.PoolMgrConfig{
					CustomContainerConfig: config.CustomContainerConfig{},
				},
			}
			p := &PoolMgr{
				podHandlerManager:  podHandlerManager,
				customContainerMgr: customContainerMgr,
			}
			req := &podhandler.PodCreateReq{
				Spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						CustomContainerConfig: commonTypes.CustomContainerConfig{
							ControlPath: "fakeControlPath",
						},
						BusinessType: "CAE",
					},
				},
				PodReq: &common.PodRequest{
					FunSvcID: "test",
					CPU:      500,
					Mem:      1024,
				},
			}
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(podHandlerManager), "FilterByPodCreateReq",
					func(m *podhandler.Manager, req *podhandler.PodCreateReq) podhandler.PodHandler {
						return nil
					}),
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						CaeCustomizePoolSpec: map[string]config.PoolSpec{
							"pool-500-1024":  {LimitCPU: 500, LimitMEM: 1024},
							"pool-1000-2048": {LimitCPU: 1000, LimitMEM: 2048},
						},
					}, nil
				}),
				ApplyPrivateMethod(customContainerMgr, "createPod",
					func(c *customContainerMGR, param createCustomContainerPodParam) (*corev1.Pod, error) {
						return &corev1.Pod{}, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			pod, _, _ := p.selectPod(req)
			convey.So(pod, convey.ShouldNotBeNil)
		})
		convey.Convey("TestSelectPod customize spec not found", func() {
			podHandlerManager := &podhandler.Manager{}
			customContainerMgr := &customContainerMGR{
				config: config.PoolMgrConfig{
					CustomContainerConfig: config.CustomContainerConfig{},
				},
			}
			p := &PoolMgr{
				podHandlerManager:  podHandlerManager,
				customContainerMgr: customContainerMgr,
			}
			req := &podhandler.PodCreateReq{
				Spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						CustomContainerConfig: commonTypes.CustomContainerConfig{
							ControlPath: "fakeControlPath",
						},
						BusinessType: "CAE",
					},
				},
				PodReq: &common.PodRequest{
					FunSvcID: "test",
					CPU:      501,
					Mem:      1024,
				},
			}
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(podHandlerManager), "FilterByPodCreateReq",
					func(m *podhandler.Manager, req *podhandler.PodCreateReq) podhandler.PodHandler {
						return nil
					}),
				ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						CaeCustomizePoolSpec: map[string]config.PoolSpec{
							"pool-500-1024":  {LimitCPU: 500, LimitMEM: 1024},
							"pool-1000-2048": {LimitCPU: 1000, LimitMEM: 2048},
						},
					}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			pod, _, err := p.selectPod(req)
			convey.So(pod, convey.ShouldBeNil)
			convey.So(err.Error(), convey.ShouldEqual, ErrNoSuchStandard.Error())
		})
	})
}

// TestPoolMgr_ReclaimingSelectedButUnusedPod -
func TestPoolMgr_ReclaimingSelectedButUnusedPod(t *testing.T) {
	convey.Convey("test: reclaiming selected bus unused pod", t, func() {
		p := &PoolMgr{
			poolList: map[standard]*Pool{
				"standard": {
					mux: &sync.Mutex{},
					PodList: map[nodeName]*NodePod{
						"node1": {},
					},
				},
			},
		}
		getStandardFromPodNameErrFlag := false
		notFoundFlag := false
		succeedFlag := false
		patches := ApplyFunc(getStandardFromPodName, func(name string) (standard, error) {
			if strings.Contains(name, "errPod") {
				getStandardFromPodNameErrFlag = true
				return "", errors.New("test err")
			}
			if strings.Contains(name, "notFoundPod") {
				notFoundFlag = true
				return "", nil
			}
			return "standard", nil
		})
		patches.ApplyFunc((*Pool).reclaimUnusedSelectedPod, func(_ *Pool,
			pod *corev1.Pod, enableTMPExpansion bool) {
			succeedFlag = true
		})
		defer patches.Reset()
		errPod := &corev1.Pod{}
		errPod.Name = "errPod"
		p.reclaimUnusedSelectedPod(errPod, false)
		convey.So(getStandardFromPodNameErrFlag, convey.ShouldBeTrue)
		notFoundPod := &corev1.Pod{}
		notFoundPod.Name = "notFoundPod"
		p.reclaimUnusedSelectedPod(notFoundPod, false)
		convey.So(notFoundFlag, convey.ShouldBeTrue)
		normalPod := &corev1.Pod{}
		normalPod.Name = "testPod"
		p.reclaimUnusedSelectedPod(normalPod, false)
		convey.So(succeedFlag, convey.ShouldBeTrue)
	})
}

// TestUpdatePodLabels
func TestUpdatePodLabels(t *testing.T) {
	convey.Convey("test: update pod labels", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, nameSpace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
		})
		defer patches.ResetAll()
		convey.Convey("test: failed to update pod labels for tmp expansion", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "Patch",
					func(_ *corefake.FakePods, ctx context.Context, name string, pt types.PatchType,
						data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
						return nil, errors.New("test err")
					}),
			})
			defer patches.ResetAll()
			p := &PoolMgr{
				KubeClient: &kubernetes.Clientset{},
			}
			err := p.updatePodLabels(&corev1.Pod{}, true)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test: succeed to update pod labels for tmp expansion", func() {
			patches := mockutils.InitPatchSlice()
			pod := &corev1.Pod{}
			pod.Labels = map[string]string{}
			patches.Append(mockutils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "Patch",
					func(_ *corefake.FakePods, ctx context.Context, name string, pt types.PatchType,
						data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
						pod.Labels[utils.LabelEnableTMPExpansion] = utils.TrueString
						return pod, nil
					}),
			})
			defer patches.ResetAll()
			p := &PoolMgr{
				KubeClient: &kubernetes.Clientset{},
			}
			err := p.updatePodLabels(pod, true)
			convey.So(err, convey.ShouldBeNil)
			convey.So(pod.Labels[utils.LabelEnableTMPExpansion], convey.ShouldEqual, utils.TrueString)
		})
	})
}

// TestPoolMgr_Run -
func TestPoolMgr_Run(t *testing.T) {
	convey.Convey("TestPoolMgr_Run", t, func() {
		convey.Convey("TestPoolMgr_Run_snapshotmanager", func() {
			stopCh := make(chan struct{})
			patch := ApplyFunc((*PoolMgr).Start, func(_ *PoolMgr, stopCh <-chan struct{}) error {
				return nil
			})
			patch.ApplyGlobalVar(&poolMgr, &PoolMgr{})
			snapshotmanager.SetSnapshotManagerMode(true)
			defer patch.Reset()
			Run(stopCh)
		})
	})
}

func Test_ProcessPoolScaleLimitUpdate(t *testing.T) {
	label := make(map[string]string)
	label["node-role"] = "data-plane"
	label["node-type"] = "highend"
	convey.Convey("Test_ProcessPoolScaleLimitUpdate", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc((*PoolMgr).splitPoolConfigListByName,
				func(_ *PoolMgr, runtimeName string) []config.PoolConfig {
					return []config.PoolConfig{
						{
							Name:        "pool22-0",
							LimitCPU:    300,
							LimitMemory: 128,
							RunTimeName: "fusion",
							Label:       label,
						},
						{
							Name:           "pool24-0",
							LimitCPU:       600,
							LimitMemory:    512,
							RunTimeName:    "fusion",
							Label:          label,
							PoolScaleLimit: 0.1,
						},
						{
							Name:           "pool25-0",
							LimitCPU:       800,
							LimitMemory:    768,
							RunTimeName:    "fusion",
							Label:          label,
							PoolScaleLimit: 0.1,
						},
						{
							Name:           "pool26-0",
							LimitCPU:       1000,
							LimitMemory:    1024,
							RunTimeName:    "fusion",
							Label:          label,
							PoolScaleLimit: 0.1,
						},
					}
				}),
			ApplyFunc(wcache.GetTotalNodeInfoWithFilters,
				func(filters ...wcache.Filter) *config.NodeInfo {
					return &config.NodeInfo{
						CPUTotal:    50000,
						MemoryTotal: 100000 * 1024 * 1024,
					}
				}),
			ApplyFunc((*PoolMgr).updateDeployReplicas,
				func(_ *PoolMgr, pool *Pool, replicas int32) error {
					return nil
				}),
		})
		defer patches.ResetAll()
		convey.Convey("test: poolList is nil", func() {
			poolMgr := &PoolMgr{}
			poolMgr.ProcessPoolScaleLimitUpdate()
			assert.Nil(t, poolMgr.poolList)
		})
		convey.Convey("test: failed to update pod labels for tmp expansion", func() {
			poolMgr := &PoolMgr{
				poolList: make(map[standard]*Pool, defaultPoolNum),
			}
			scalingConfig := &ScalingConfig{
				replicas:      10,
				basePoolSize:  5,
				poolSizeLimit: 100,
			}

			scalingConfig2 := &ScalingConfig{
				replicas:      10,
				basePoolSize:  5,
				poolSizeLimit: 100,
			}

			scalingConfig3 := &ScalingConfig{
				replicas:      10,
				basePoolSize:  6,
				poolSizeLimit: 100,
			}
			poolMgr.poolList["300-128-fusion"] = &Pool{
				Standard:      "300-128-fusion",
				scalingConfig: scalingConfig,
				mux:           new(sync.Mutex),
			}
			poolMgr.poolList["600-512-fusion"] = &Pool{
				Standard:      "600-512-fusion",
				scalingConfig: scalingConfig2,
				mux:           new(sync.Mutex),
			}
			poolMgr.poolList["800-768-fusion"] = &Pool{
				Standard: "800-768-fusion",
				mux:      new(sync.Mutex),
			}
			poolMgr.poolList["1000-1024-fusion"] = &Pool{
				Standard:      "1000-1024-fusion",
				scalingConfig: scalingConfig3,
				mux:           new(sync.Mutex),
			}
			poolMgr.poolList["1200-1280-fusion"] = &Pool{
				Standard:      "1200-1280-fusion",
				scalingConfig: scalingConfig,
				mux:           new(sync.Mutex),
			}
			poolMgr.ProcessPoolScaleLimitUpdate()

			assert.Equal(t, int32(10), poolMgr.poolList["300-128-fusion"].GetReplicas())
			assert.Equal(t, int32(8), poolMgr.poolList["600-512-fusion"].GetReplicas())
			assert.Equal(t, int32(6), poolMgr.poolList["1000-1024-fusion"].GetReplicas())
			assert.Equal(t, int32(10), poolMgr.poolList["1200-1280-fusion"].GetReplicas())
		})
	})
}

func TestPoolMgr_createTMPExpansionPod(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		podDetector        *detector.PodDetector
		podHandlerManager  *podhandler.Manager
		poolList           map[standard]*Pool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		req *podhandler.PodCreateReq
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		want        *corev1.Pod
		want1       bool
		wantErr     bool
	}{
		{
			name: "case01: no pool of corresponding standard found",
			args: args{req: &podhandler.PodCreateReq{
				PodReq: &common.PodRequest{
					TraceID: "mock-trace-id",
				},
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).getPoolFromPoolList,
						func(_ *PoolMgr, req common.PodRequest, language string,
							customResource map[string]int64, labels map[string]string) *Pool {
							return nil
						}),
				})
				return patches
			},
			want:    nil,
			want1:   false,
			wantErr: true,
		},
		{
			name: "case02: no nodes available",
			args: args{req: &podhandler.PodCreateReq{
				PodReq: &common.PodRequest{
					TraceID: "mock-trace-id",
				},
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).getPoolFromPoolList,
						func(_ *PoolMgr, req common.PodRequest, language string,
							customResource map[string]int64, labels map[string]string) *Pool {
							return &Pool{
								PodNum:   0,
								Standard: "mock-standard",
							}
						}),
				})
				return patches
			},
			want:    nil,
			want1:   false,
			wantErr: true,
		},
		{
			name: "case03: failed to create tmp expansion pod template",
			args: args{req: &podhandler.PodCreateReq{
				PodReq: &common.PodRequest{
					TraceID: "mock-trace-id",
				},
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).getPoolFromPoolList,
						func(_ *PoolMgr, req common.PodRequest, language string,
							customResource map[string]int64, labels map[string]string) *Pool {
							return &Pool{
								PodNum:   0,
								Standard: "mock-standard",
								Name:     "mock-pool-name",
								Deployment: &v1.Deployment{
									Spec: v1.DeploymentSpec{
										Template: corev1.PodTemplateSpec{
											Spec: corev1.PodSpec{},
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{},
											},
										},
									},
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"mock-key": "mock-value-diff"},
									},
								},
							}
						}),
					ApplyFunc(wcache.GetAvailableNodesForTMPExpansion,
						func() map[string]*wcache.NodeInfo {
							return map[string]*wcache.NodeInfo{
								"mock-node": {},
							}
						}),
					ApplyFunc(podtemplate.CreateTmpExpansionPodTemplate,
						func(podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta, podName string,
							traceID string, demandNodes []string) *utils.CreatePodTemplate {
							return nil
						}),
					ApplyFunc(utils.CreatePod,
						func(ctx context.Context, template *utils.CreatePodTemplate) (*corev1.Pod, error) {
							return nil, errors.New("create pod error")
						}),
				})
				return patches
			},
			want:    nil,
			want1:   false,
			wantErr: true,
		},
		{
			name: "case04: success to create TMP expansion pod",
			args: args{req: &podhandler.PodCreateReq{
				PodReq: &common.PodRequest{
					TraceID: "mock-trace-id",
				},
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).getPoolFromPoolList,
						func(_ *PoolMgr, req common.PodRequest, language string,
							customResource map[string]int64, labels map[string]string) *Pool {
							return &Pool{
								PodNum:   0,
								Standard: "mock-standard",
								Name:     "mock-pool-name",
								Deployment: &v1.Deployment{
									Spec: v1.DeploymentSpec{
										Template: corev1.PodTemplateSpec{
											Spec: corev1.PodSpec{},
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{},
											},
										},
									},
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"mock-key": "mock-value-diff"},
									},
								},
							}
						}),
					ApplyFunc(wcache.GetAvailableNodesForTMPExpansion,
						func() map[string]*wcache.NodeInfo {
							return map[string]*wcache.NodeInfo{
								"mock-node": {},
							}
						}),
					ApplyFunc(podtemplate.CreateTmpExpansionPodTemplate,
						func(podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta, podName string,
							traceID string, demandNodes []string) *utils.CreatePodTemplate {
							return &utils.CreatePodTemplate{}
						}),
					ApplyFunc(utils.CreatePod,
						func(ctx context.Context, template *utils.CreatePodTemplate) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			want1:   false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				podDetector:        tt.fields.podDetector,
				podHandlerManager:  tt.fields.podHandlerManager,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
			}
			patches := tt.patchesFunc()
			got, got1, err := p.createTMPExpansionPod(tt.args.req)
			patches.ResetAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("createTMPExpansionPod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createTMPExpansionPod() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("createTMPExpansionPod() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestPoolMgrInitTerminatingLongRun(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-no-label"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "pool22-300-128-test1",
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "pool22-300-128-test2",
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "invalid_pod_name",
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.2"},
		},
	}

	p := &PoolMgr{
		synced:             make(chan struct{}),
		terminatingLongRun: make(map[string]*PodsOnNode),
	}

	p.InitTerminatingLongRun(pods)

	assert.Equal(t, len(p.terminatingLongRun), 2)
	for _, v := range p.terminatingLongRun {
		assert.Equal(t, len(v.Pods), 1)
	}
}

func TestPoolMgrAddTerminatingLongRun(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-no-label"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "pool22-300-128-not-terminating",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels:            map[string]string{utils.LabelLongRun: "true"},
				Name:              "pool22-300-128-no-hostIP",
				DeletionTimestamp: &metav1.Time{},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels:            map[string]string{utils.LabelLongRun: "true"},
				Name:              "pool22-300-128-test1",
				DeletionTimestamp: &metav1.Time{},
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels:            map[string]string{utils.LabelLongRun: "true"},
				Name:              "pool22-300-128-test2",
				DeletionTimestamp: &metav1.Time{},
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels:            map[string]string{utils.LabelLongRun: "true"},
				Name:              "invalid_pod_name",
				DeletionTimestamp: &metav1.Time{},
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.2"},
		},
	}

	p := &PoolMgr{
		synced:             make(chan struct{}),
		terminatingLongRun: make(map[string]*PodsOnNode),
	}

	for i := range pods {
		p.AddTerminatingLongRun(&pods[i])
	}

	assert.Equal(t, len(p.terminatingLongRun), 2)
	for _, v := range p.terminatingLongRun {
		assert.Equal(t, len(v.Pods), 1)
	}
}

func TestPoolMgrRemoveTerminatingLongRun(t *testing.T) {
	p := &PoolMgr{
		synced: make(chan struct{}),
		terminatingLongRun: map[string]*PodsOnNode{
			"127.0.0.1": {
				Pods: map[string]commonTypes.InstanceRouter{
					"pod1": {},
					"pod2": {},
				},
			},
			"127.0.0.2": {
				Pods: map[string]commonTypes.InstanceRouter{
					"pod3": {},
				},
			},
		},
	}

	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-no-label"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "pool22-300-128-no-hostIP",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "hostIP-not-in-cache",
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.3"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "pod1",
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{utils.LabelLongRun: "true"},
				Name:   "pod3",
			},
			Status: corev1.PodStatus{HostIP: "127.0.0.2"},
		},
	}

	for i := range pods {
		p.RemoveTerminatingLongRun(&pods[i])
	}

	assert.Equal(t, len(p.terminatingLongRun), 1)
}

func TestPoolMgrQueryTerminatingLongRunByHostIP(t *testing.T) {
	p := &PoolMgr{
		synced: make(chan struct{}),
		terminatingLongRun: map[string]*PodsOnNode{
			"127.0.0.1": {
				Pods: map[string]commonTypes.InstanceRouter{
					"pod1": {},
					"pod2": {},
				},
			},
			"127.0.0.2": {
				Pods: map[string]commonTypes.InstanceRouter{
					"pod3": {},
				},
			},
		},
	}
	close(p.synced)

	tests := []struct {
		name   string
		hostIP string

		excepted map[string]commonTypes.InstanceRouter
	}{
		{
			name:   "no such hostIP",
			hostIP: "127.0.0.9",
		},
		{
			name:   "hostIP exits",
			hostIP: "127.0.0.1",
			excepted: map[string]commonTypes.InstanceRouter{
				"pod1": {},
				"pod2": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.excepted, p.QueryTerminatingLongRunByHostIP(tt.hostIP))
		})
	}
}

func TestCheckPodReadyStatus(t *testing.T) {
	p := &PoolMgr{}
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-",
		},
		Spec: corev1.PodSpec{
			NodeName: "nodeName",
		},
		Status: corev1.PodStatus{},
	}
	assert.True(t, p.checkPodReadyStatus(pod))
	pod.Name = "tmpexpansion-123"
	assert.True(t, p.checkPodReadyStatus(pod))
	pod.Labels = map[string]string{"migratedFrom": "1223"}
	assert.True(t, p.checkPodReadyStatus(pod))
}

func TestPoolMgr_AddPod_CleanPod(t *testing.T) {
	type fields struct {
		customContainerMgr     *customContainerMGR
		checkpointPodMgr       *checkpointPodMGR
		podDetector            *detector.PodDetector
		podHandlerManager      *podhandler.Manager
		PodUsedRecord          *PodUsedRecord
		poolList               map[standard]*Pool
		Config                 config.PoolMgrConfig
		KubeClient             kubernetes.Interface
		terminatingLongRun     map[string]*PodsOnNode
		terminatingLongRunLock sync.RWMutex
		synced                 chan struct{}
		projectID              string
		projectName            string
		clusterID              string
		clusterName            string
		stopCh                 <-chan struct{}
	}
	type args struct {
		pod *corev1.Pod
	}
	isMocked := false
	pod001 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-mock-pod-name-001",
			Labels: map[string]string{
				utils.IsPoolPodLabelKey: trueString,
			},
		},
	}
	strength.GetPodCache().SetState(pod001.Name, "mockFuncSvcID002", strength.BeforeSpecialized)
	strength.GetPodCache().DelState(pod001.Name)
	pod002 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-mock-pod-name-002",
			Labels: map[string]string{
				utils.IsPoolPodLabelKey: trueString,
			},
		},
	}
	strength.GetPodCache().SetState(pod002.Name, "mockFuncSvcID002", strength.BeforeSpecialized)
	tests := []struct {
		name           string
		fields         fields
		args           args
		patches        mockutils.PatchesFunc
		expectIsMocked bool
	}{
		{
			name:   "test clean remaining custom container pod",
			fields: fields{},
			args: args{
				pod: pod001,
			},
			patches: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.DeletePod, func(pod *corev1.Pod) error {
						isMocked = true
						return nil
					}),
					ApplyFunc(utils.IsPodReady, func(pod *corev1.Pod) bool {
						isMocked = true
						return true
					}),
				})
				return patches
			},
			expectIsMocked: true,
		}, // test clean remaining custom container pod
		{
			name:   "test no clean remaining custom container pod when pod is specializing",
			fields: fields{},
			args: args{
				pod: pod002,
			},
			patches: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.DeletePod, func(pod *corev1.Pod) error {
						isMocked = true
						return nil
					}),
				})
				return patches
			},
			expectIsMocked: false,
		},
		{
			name:   "test ds-worker",
			fields: fields{},
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "ds-worker-123123",
						Labels: map[string]string{
							utils.IsPoolPodLabelKey: trueString,
						},
					},
				},
			},
			patches: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(utils.DeletePod, func(pod *corev1.Pod) error {
						isMocked = true
						return nil
					}),
					ApplyFunc(handleDsWorkerPodEvent, func(pod *corev1.Pod) {}),
				})
				return patches
			},
			expectIsMocked: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMocked = false
			p := &PoolMgr{
				customContainerMgr:     tt.fields.customContainerMgr,
				checkpointPodMgr:       tt.fields.checkpointPodMgr,
				podDetector:            tt.fields.podDetector,
				podHandlerManager:      tt.fields.podHandlerManager,
				PodUsedRecord:          tt.fields.PodUsedRecord,
				poolList:               tt.fields.poolList,
				Config:                 tt.fields.Config,
				KubeClient:             tt.fields.KubeClient,
				terminatingLongRun:     tt.fields.terminatingLongRun,
				terminatingLongRunLock: tt.fields.terminatingLongRunLock,
				synced:                 tt.fields.synced,
				projectID:              tt.fields.projectID,
				projectName:            tt.fields.projectName,
				clusterID:              tt.fields.clusterID,
				clusterName:            tt.fields.clusterName,
				stopCh:                 tt.fields.stopCh,
			}
			patches := tt.patches()
			defer patches.ResetAll()
			p.AddPod(tt.args.pod)
			if isMocked != tt.expectIsMocked {
				t.Errorf("expect %v but found %v", tt.expectIsMocked, isMocked)
			}
		})
	}
}

func TestPoolMgr_GetStatefulDynamicClosestStandard(t *testing.T) {
	convey.Convey("Test get stateful dynamic closest standard", t, func() {
		poolMgr := &PoolMgr{
			poolList: map[standard]*Pool{
				standard("300-128-fusion"): {
					LimitCPU:    300,
					LimitMemory: 128,
				},
				standard("400-256-fusion"): {
					LimitCPU:    400,
					LimitMemory: 256,
				},
				standard("600-512-fusion"): {
					LimitCPU:    600,
					LimitMemory: 512,
				},
				standard("800-768-fusion"): {
					LimitCPU:    800,
					LimitMemory: 768,
				},
				standard("1000-1024-fusion"): {
					LimitCPU:    1000,
					LimitMemory: 1024,
				},
				standard("1200-1280-fusion"): {
					LimitCPU:    1200,
					LimitMemory: 1280,
				},
				standard("1400-1536-fusion"): {
					LimitCPU:    1400,
					LimitMemory: 1536,
				},
				standard("1600-1792-fusion"): {
					LimitCPU:    1600,
					LimitMemory: 1792,
				},
				standard("1800-2048-fusion"): {
					LimitCPU:    1800,
					LimitMemory: 2048,
				},
			},
		}
		cpu, memory := poolMgr.GetStatefulDynamicClosestStandard(int64(0), int64(512))
		convey.So(cpu, convey.ShouldEqual, 600)
		convey.So(memory, convey.ShouldEqual, 512)
		cpu, memory = poolMgr.GetStatefulDynamicClosestStandard(int64(300), int64(0))
		convey.So(cpu, convey.ShouldEqual, 300)
		convey.So(memory, convey.ShouldEqual, 128)
		cpu, memory = poolMgr.GetStatefulDynamicClosestStandard(int64(900), int64(1300))
		convey.So(cpu, convey.ShouldEqual, 1400)
		convey.So(memory, convey.ShouldEqual, 1536)
		cpu, memory = poolMgr.GetStatefulDynamicClosestStandard(int64(0), int64(0))
		convey.So(cpu, convey.ShouldEqual, 0)
		convey.So(memory, convey.ShouldEqual, 0)
		cpu, memory = poolMgr.GetStatefulDynamicClosestStandard(int64(5000), int64(5000))
		convey.So(cpu, convey.ShouldEqual, 1800)
		convey.So(memory, convey.ShouldEqual, 2048)
	})
	convey.Convey("Test get stateful dynamic closest standard when loading", t, func() {
		poolMgr := &PoolMgr{}
		cpu, memory := poolMgr.GetStatefulDynamicClosestStandard(int64(500), int64(512))
		convey.So(cpu, convey.ShouldEqual, 0)
		convey.So(memory, convey.ShouldEqual, 0)
	})
}

func TestPoolMgr_UpdateRestorePool(t *testing.T) {
	p := &PoolMgr{DecoyFileEnabled: true}
	newConfig := &config.FaasConfig{DecoyFileEnabled: true}
	p.RestorePool(newConfig)
	assert.Equal(t, p.DecoyFileEnabled, newConfig.DecoyFileEnabled)
	p.DecoyFileEnabled = false
	p.RestorePool(newConfig)
	assert.Equal(t, p.DecoyFileEnabled, newConfig.DecoyFileEnabled)
}

func TestPoolMgr_processRestorePool(t *testing.T) {
	type fields struct {
		customContainerMgr *customContainerMGR
		checkpointPodMgr   *checkpointPodMGR
		podDetector        *detector.PodDetector
		podHandlerManager  *podhandler.Manager
		poolList           map[standard]*Pool
		Config             config.PoolMgrConfig
		KubeClient         kubernetes.Interface
		projectID          string
		projectName        string
		clusterID          string
		clusterName        string
		stopCh             <-chan struct{}
	}
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with deployment is nil",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc((*PoolMgr).splitPoolConfigListByName,
						func(_ *PoolMgr, runtimeName string) []config.PoolConfig {
							return []config.PoolConfig{
								{
									Name: "mock pool config1",
								},
							}
						}),

					ApplyFunc((*PoolMgr).createPoolDeploy,
						func(_ *PoolMgr, curPool *Pool, deployment *appsv1.Deployment) error {
							return nil
						}),
					ApplyFunc(utils.GetWorkerManagerDeployUID, func() types.UID {
						return "111"
					}),
					ApplyFunc((*poolDeploymentBuilder).build, func(_ *poolDeploymentBuilder, pool *Pool, ownerUID types.UID) *appsv1.Deployment {
						return &appsv1.Deployment{}
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PoolMgr{
				customContainerMgr: tt.fields.customContainerMgr,
				checkpointPodMgr:   tt.fields.checkpointPodMgr,
				podDetector:        tt.fields.podDetector,
				podHandlerManager:  tt.fields.podHandlerManager,
				poolList:           tt.fields.poolList,
				Config:             tt.fields.Config,
				KubeClient:         tt.fields.KubeClient,
				projectID:          tt.fields.projectID,
				projectName:        tt.fields.projectName,
				clusterID:          tt.fields.clusterID,
				clusterName:        tt.fields.clusterName,
				stopCh:             tt.fields.stopCh,
				PodUsedRecord: &PodUsedRecord{
					usedPodRecordVal: make(map[standard]int64),
				},
			}
			patches := tt.patchesFunc()
			err := p.processRestorePool()
			patches.ResetAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func Test_handleDsWorkerPodEvent(t *testing.T) {
	convey.Convey("condition.Type == corev1.PodReady && condition.Status == corev1.ConditionFalse, ok", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(setDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		handleDsWorkerPodEvent(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})
	convey.Convey("condition.Type == corev1.PodReady && condition.Status == corev1.ConditionFalse, !ok", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(setDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})
		pod.Spec.NodeName = "nodeName"
		handleDsWorkerPodEvent(pod)
	})
	convey.Convey("condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(recoverDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
			ApplyFunc(setDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		handleDsWorkerPodEvent(pod)
	})
}

func Test_handleDsWorkerDeleteEvent(t *testing.T) {
	convey.Convey("cache.NodeTaintCache.Load(pod.Spec.NodeName); ok ", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(setDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
			ApplyFunc(recoverDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		handleDsWorkerDeleteEvent(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})
	convey.Convey("cache.NodeTaintCache.Load(pod.Spec.NodeName); ok ", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(setDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		handleDsWorkerDeleteEvent(pod)
	})
}

func Test_setDsWorkerHostTaintLabel(t *testing.T) {
	convey.Convey("nodeCache is nil", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(recoverDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node { return nil }),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		setDsWorkerHostTaintLabel(pod)
	})

	convey.Convey("Update error", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(recoverDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node {
				return &corev1.Node{Spec: corev1.NodeSpec{}}
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Nodes",
				func(_ *corefake.FakeCoreV1) typecorev1.NodeInterface {
					return &corefake.FakeNodes{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeNodes{}), "Update",
				func(_ *corefake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
					return nil, errors.New("this is error")
				}),
			ApplyFunc(processLongTimeFailure, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		setDsWorkerHostTaintLabel(pod)
	})
	convey.Convey("has tainted", t, func() {
		var flag bool
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(recoverDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node {
				return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nodeName"}, Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
					Key:    utils.DsWorkerTaintKey,
					Value:  utils.DsWorkerTaintValue,
					Effect: corev1.TaintEffectPreferNoSchedule,
				}}}}
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Nodes",
				func(_ *corefake.FakeCoreV1) typecorev1.NodeInterface {
					return &corefake.FakeNodes{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeNodes{}), "Update",
				func(_ *corefake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
					flag = true
					return nil, nil
				}),
			ApplyFunc(processLongTimeFailure, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		setDsWorkerHostTaintLabel(pod)
		if flag {
			t.Errorf("this is error")
		}
		defer func() { wcache.NodeTaintCache.Delete("nodeName") }()
	})
	convey.Convey("all clear", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(recoverDsWorkerHostTaintLabel, func(pod *corev1.Pod) {}),
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node {
				return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nodeName"}, Spec: corev1.NodeSpec{}}
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Nodes",
				func(_ *corefake.FakeCoreV1) typecorev1.NodeInterface {
					return &corefake.FakeNodes{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeNodes{}), "Update",
				func(_ *corefake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
					return nil, nil
				}),
			ApplyFunc(processLongTimeFailure, func(pod *corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		setDsWorkerHostTaintLabel(pod)
		defer func() { wcache.NodeTaintCache.Delete("nodeName") }()
	})
}

func Test_recoverDsWorkerHostTaintLabel(t *testing.T) {
	convey.Convey("NodeTaintCache.Load(pod.Spec.NodeName); !ok ", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		recoverDsWorkerHostTaintLabel(pod)
	})

	convey.Convey("!hasTaint", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node {
				return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nodeHost"}, Spec: corev1.NodeSpec{}}
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Nodes",
				func(_ *corefake.FakeCoreV1) typecorev1.NodeInterface {
					return &corefake.FakeNodes{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeNodes{}), "Update",
				func(_ *corefake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
					return nil, errors.New("this is error")
				}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		recoverDsWorkerHostTaintLabel(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("hasTaint, update error", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node {
				res := make([]corev1.Taint, 1)
				res = append(res, corev1.Taint{Key: utils.DsWorkerTaintKey, Value: utils.DsWorkerTaintValue, Effect: corev1.TaintEffectPreferNoSchedule})
				return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nodeHost"}, Spec: corev1.NodeSpec{Taints: res}}
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Nodes",
				func(_ *corefake.FakeCoreV1) typecorev1.NodeInterface {
					return &corefake.FakeNodes{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeNodes{}), "Update",
				func(_ *corefake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
					return nil, errors.New("this is error")
				}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		recoverDsWorkerHostTaintLabel(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("hasTaint, update clear", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(wcache.GetK8sNodeCacheByDeepCopy, func(podName string) *corev1.Node {
				res := make([]corev1.Taint, 1)
				res = append(res, corev1.Taint{Key: utils.DsWorkerTaintKey, Value: utils.DsWorkerTaintValue, Effect: corev1.TaintEffectPreferNoSchedule})
				return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nodeHost"}, Spec: corev1.NodeSpec{Taints: res}}
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Nodes",
				func(_ *corefake.FakeCoreV1) typecorev1.NodeInterface {
					return &corefake.FakeNodes{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeNodes{}), "Update",
				func(_ *corefake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
					return nil, nil
				}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		recoverDsWorkerHostTaintLabel(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})
}

func Test_processLongTimeFailure(t *testing.T) {
	convey.Convey("nodeCache, load !ok", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 1
			}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		processLongTimeFailure(pod)
	})

	convey.Convey("nodeCache, timeStamp.(time.Time) !ok", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 1
			}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", "a")
		processLongTimeFailure(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("nodeCache, duration < ", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 1
			}),
			ApplyFunc(time.Duration.Seconds, func(_ time.Duration) float64 {
				return 0
			}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		processLongTimeFailure(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("nodeCache, pod list error", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 0
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, namespace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "List",
				func(_ *corefake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
					return nil, errors.New("error")
				}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		time.Sleep(1 * time.Second)
		processLongTimeFailure(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("nodeCache, pod list, poolList.Items == nil", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 0
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, namespace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "List",
				func(_ *corefake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
					return &corev1.PodList{}, errors.New("error")
				}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		time.Sleep(1 * time.Second)
		processLongTimeFailure(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("nodeCache, pod list, pod.Labels == nil", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 0
			}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, namespace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "List",
				func(_ *corefake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
					podList := make([]corev1.Pod, 0)
					podList = append(podList, corev1.Pod{})
					return &corev1.PodList{Items: podList}, errors.New("error")
				}),
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				var KubeClient kubernetes.Interface
				KubeClient = &kubernetes.Clientset{}
				return KubeClient
			}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		time.Sleep(1 * time.Second)
		processLongTimeFailure(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})

	convey.Convey("nodeCache, pod list, pod.Labels: stateID=testStateID", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, namespace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
			ApplyFunc(utils.GetEnvDsWorkerCrashTimeout, func() int64 {
				return 0
			}),
			ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "List",
				func(_ *corefake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
					podList := make([]corev1.Pod, 0)
					testMap := make(map[string]string)
					testMap["stateID"] = "testStateID"
					podList = append(podList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: testMap}})
					return &corev1.PodList{Items: podList}, errors.New("error")
				}),
			ApplyFunc(deletePodByName, func(pod corev1.Pod) {}),
		})
		defer patches.ResetAll()
		_, pod := newTestPoolMgr()
		pod.Name = "ds-worker123123"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		wcache.NodeTaintCache.Store("nodeName", time.Now())
		time.Sleep(1 * time.Second)
		processLongTimeFailure(pod)
		wcache.NodeTaintCache.Delete("nodeName")
	})
}

func Test_deletePodByName(t *testing.T) {
	convey.Convey("pod.Labels[utils.LabelIsBridgeFunction], error", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, namespace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "Delete",
				func(_ *corefake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return errors.New("error")
				}),
			ApplyFunc(utils.FunctionNameSpace, func() string {
				return "default"
			}),
		})
		defer patches.ResetAll()
		label := make(map[string]string)
		label[utils.LabelIsBridgeFunction] = utils.TrueString
		pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: label}}
		pod.Name = "pool22-pod11223344"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		deletePodByName(pod)
	})

	convey.Convey("not bridgeFunction pod, clear", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &corefake.FakeCoreV1{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakeCoreV1{}), "Pods",
				func(_ *corefake.FakeCoreV1, namespace string) typecorev1.PodInterface {
					return &corefake.FakePods{}
				}),
			ApplyMethod(reflect.TypeOf(&corefake.FakePods{}), "Delete",
				func(_ *corefake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				}),
			ApplyFunc(utils.FunctionNameSpace, func() string {
				return "default"
			}),
		})
		defer patches.ResetAll()
		label := make(map[string]string)
		pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: label}}
		pod.Name = "pool22-pod11223344"
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		pod.Spec.NodeName = "nodeName"
		deletePodByName(pod)
	})
}

func Test_parsePoolHangingTimeout(t *testing.T) {
	defer os.Unsetenv("POOL_HANGING_TIMEOUT")
	tests := []struct {
		name     string
		val      string
		expected time.Duration
	}{
		{
			name:     "empty env",
			val:      "",
			expected: defaultPoolHangingTimeout * time.Second,
		},
		{
			name:     "invalid int",
			val:      "some value",
			expected: defaultPoolHangingTimeout * time.Second,
		},
		{
			name:     "too small",
			val:      "0",
			expected: defaultPoolHangingTimeout * time.Second,
		},
		{
			name:     "valid value",
			val:      "66",
			expected: 66 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("POOL_HANGING_TIMEOUT", tt.val)
			assert.Equal(t, tt.expected, parsePoolHangingTimeout())
		})
	}
}
