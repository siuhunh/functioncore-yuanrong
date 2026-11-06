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
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/schedulequeue"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	utils2 "functioncore/pkg/worker-manager/utils"
	"functioncore/test/utils"
)

func poolFactory() *Pool {
	pool := &Pool{}
	pool.PodList = make(map[nodeName]*NodePod)
	pool.mux = new(sync.Mutex)
	for i := 0; i < 3; i++ {
		pod := &corev1.Pod{}
		pod.Name = fmt.Sprintf("pod-%d", i)
		pod.Labels = make(map[string]string)
		pod.Annotations = make(map[string]string)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = "1.2.3.4"
		node := nodeName(fmt.Sprintf("node-%d", i))
		pool.PodList[node] = &NodePod{}
		podObj := &podObj{
			pod:      pod,
			usedFlag: false,
		}
		if i == 2 {
			podObj.usedFlag = true
			pool.PodList[node].readyPodNum = 0
		}
		pool.PodList[node].podsQueue.items = append(pool.PodList[node].podsQueue.items, podObj)
		pool.PodList[node].readyPodNum = 1
	}
	return pool
}

func TestPool_addPodToPool(t *testing.T) {
	type fields struct {
		Deployment    *appsv1.Deployment
		mux           *sync.Mutex
		PodList       map[nodeName]*NodePod
		PodNum        int64
		Name          string
		NameSpace     string
		Standard      standard
		Language      string
		LimitCPU      int64
		LimitMemory   int64
		RequestCPU    int64
		RequestMemory int64
		PoolSize      int32
		OndemandChan  map[string]chan *corev1.Pod
		Label         map[string]string
		Annotations   map[string]string
	}
	type args struct {
		pod *corev1.Pod
	}
	var f fields
	var mutex sync.Mutex
	f.mux = &mutex
	info := &NodePod{}
	f.PodList = map[nodeName]*NodePod{}
	f.PodList["nodeName2"] = info

	podChan := make(chan *corev1.Pod)
	fValue := map[string]chan *corev1.Pod{
		"podName":  podChan,
		"podName2": podChan,
	}
	f.OndemandChan = fValue

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f,
			args{
				pod: &corev1.Pod{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "podName",
					},
					Spec: corev1.PodSpec{
						NodeName: "nodeName",
					},
					Status: corev1.PodStatus{},
				},
			},
		},
		{"case2", f,
			args{
				pod: &corev1.Pod{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "podName2",
						Annotations: map[string]string{
							constants.OndemandKey: "true",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "nodeName2",
					},
					Status: corev1.PodStatus{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:    tt.fields.Deployment,
				mux:           tt.fields.mux,
				PodList:       tt.fields.PodList,
				PodNum:        tt.fields.PodNum,
				Name:          tt.fields.Name,
				NameSpace:     tt.fields.NameSpace,
				Standard:      tt.fields.Standard,
				Language:      tt.fields.Language,
				LimitCPU:      tt.fields.LimitCPU,
				LimitMemory:   tt.fields.LimitMemory,
				RequestMemory: tt.fields.RequestMemory,
				RequestCPU:    tt.fields.RequestCPU,
				PoolSize:      tt.fields.PoolSize,
				OndemandChan:  tt.fields.OndemandChan,
				Label:         tt.fields.Label,
				Annotations:   tt.fields.Annotations,
			}
			p.addPodToPool(tt.args.pod)
		})
	}
}

func TestPool_deletePodFromPool(t *testing.T) {
	type fields struct {
		Deployment    *appsv1.Deployment
		mux           *sync.Mutex
		PodList       map[nodeName]*NodePod
		PodNum        int64
		Name          string
		NameSpace     string
		Standard      standard
		Language      string
		LimitCPU      int64
		LimitMemory   int64
		RequestCPU    int64
		RequestMemory int64
		PoolSize      int32
		Label         map[string]string
		Annotations   map[string]string
	}
	type args struct {
		pod *corev1.Pod
	}
	var f fields
	var mutex sync.Mutex
	f.mux = &mutex
	info := &NodePod{}
	f.PodList = map[nodeName]*NodePod{}
	f.PodList["nodeName"] = info

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f,
			args{
				pod: &corev1.Pod{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "podName",
					},
					Spec: corev1.PodSpec{
						NodeName: "nodeName",
					},
					Status: corev1.PodStatus{},
				},
			},
		},
		{"case2", f,
			args{
				pod: &corev1.Pod{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "podName2",
						Annotations: map[string]string{
							constants.OndemandKey: "true",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "nodeName2",
					},
					Status: corev1.PodStatus{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:    tt.fields.Deployment,
				mux:           tt.fields.mux,
				PodList:       tt.fields.PodList,
				PodNum:        tt.fields.PodNum,
				Name:          tt.fields.Name,
				NameSpace:     tt.fields.NameSpace,
				Standard:      tt.fields.Standard,
				Language:      tt.fields.Language,
				LimitCPU:      tt.fields.LimitCPU,
				LimitMemory:   tt.fields.LimitMemory,
				RequestMemory: tt.fields.RequestMemory,
				RequestCPU:    tt.fields.RequestCPU,
				PoolSize:      tt.fields.PoolSize,
				Label:         tt.fields.Label,
				Annotations:   tt.fields.Annotations,
			}
			p.deletePodFromPool(tt.args.pod)
		})
	}
}

func TestPool_GetReplicas(t *testing.T) {
	convey.Convey("TestInitConfig", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetElasticScalingConfig, func() *config.ElasticScalingConfig {
				return nil
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{
					PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
						ScalingUpTriggerTimeThreshold: 5,
						EnableFusionPolicy:            0,
					},
				}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		scalingCfg := newScalingConfig(2, math.MaxInt32, standard("pool22-300-128"))
		p := &Pool{
			scalingConfig: scalingCfg,
		}
		replicas := p.GetReplicas()
		var res int32 = 2
		assert.Equal(t, res, replicas)
	})
}

func TestPool_string(t *testing.T) {
	type fields struct {
		NameSpace string
		Standard  standard
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "test001",
			fields: fields{
				NameSpace: "mock-namespace",
				Standard:  standard("pool22-300-128"),
			},
			want: "mock-namespace/pool22-300-128",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				NameSpace: tt.fields.NameSpace,
				Standard:  tt.fields.Standard,
			}
			if got := p.string(); got != tt.want {
				t.Errorf("string() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_configCustomResources(t *testing.T) {
	called := false

	customResource2 := make(map[string]int64)
	customResource2["test"] = 100

	type args struct {
		pod            *corev1.Pod
		customResource map[string]int64
	}
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		args        args
	}{
		// test get config error
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						called = true
						return &config.FaasConfig{}, errors.New("get config error")
					}),
				})
				return patches
			},
			args{},
		},
		// test success
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						called = true
						return &config.FaasConfig{
							PoolMgrConfig: config.PoolMgrConfig{
								VolumeConfig: config.VolumeConfig{
									CustomResourceMountPaths: []config.CustomResourceMountPath{
										{},
									},
								},
							},
						}, nil
					}),
				})
				return patches
			},
			args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: utils2.RuntimeContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{},
									Limits:   corev1.ResourceList{},
								},
							},
						},
					},
				},
				customResource: customResource2,
			},
		},
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						called = true
						return &config.FaasConfig{
							PoolMgrConfig: config.PoolMgrConfig{
								VolumeConfig: config.VolumeConfig{
									CustomResourceMountPaths: []config.CustomResourceMountPath{
										{},
									},
								},
							},
						}, nil
					}),
				})
				return patches
			},
			args{
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "gotime",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{},
									Limits:   corev1.ResourceList{},
								},
							},
						},
					},
				},
				customResource: customResource2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			patches := tt.patchesFunc()
			configCustomResources(tt.args.pod, tt.args.customResource)
			if called != true {
				t.Errorf("Test_configCustomResources error")
			}
			patches.ResetAll()
		})
	}
}

func TestPool_waitPodReadyOrTimeout(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		clientSet kubernetes.Interface
		curPod    *corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1",
			fields{
				mux:          &sync.Mutex{},
				OndemandChan: make(map[string]chan *corev1.Pod),
			},
			args{
				clientSet: utils.K8sFakeClient,
				curPod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
						timerC := make(chan time.Time)
						close(timerC)
						timer := &time.Timer{
							C: timerC,
						}
						return timer
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&time.Timer{}), "Stop", func(_ *time.Timer) bool {
						return true
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			patches := tt.patchesFunc()
			_, err := p.waitPodReadyOrTimeout(tt.args.clientSet, tt.args.curPod)
			if (err != nil) != tt.wantErr {
				t.Errorf("Test_configCustomResources error :%v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_demandPodInNode(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		nodeLabels     []map[string]string
		customResource map[string]int64
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test client nil
		{
			"case1",
			fields{
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
			},
			args{
				nodeLabels: []map[string]string{
					{
						"test": "test",
					},
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		{
			"case2",
			fields{
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
			},
			args{
				nodeLabels: []map[string]string{
					{
						"test": "test",
					},
				},
				customResource: map[string]int64{
					"test": 0x10,
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test create error
		{
			"case3",
			fields{
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
			},
			args{
				nodeLabels: []map[string]string{
					{
						"test": "test",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, errors.New("create pod error")
						}),
				})
				return patches
			},
			false,
		},
		{
			"case4",
			fields{
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
				mux:          &sync.Mutex{},
				OndemandChan: make(map[string]chan *corev1.Pod),
			},
			args{
				nodeLabels: []map[string]string{
					{
						"test": "test",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
						timerC := make(chan time.Time)
						close(timerC)
						timer := &time.Timer{
							C: timerC,
						}
						return timer
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&time.Timer{}), "Stop", func(_ *time.Timer) bool {
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
						func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
							return nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			patches := tt.patchesFunc()
			if got := p.demandPodInNode(tt.args.nodeLabels, tt.args.customResource); (got != nil) != tt.want {
				t.Errorf("demandPodInNode() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_choosePodInNode(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		node string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test not exist
		{
			"case1",
			fields{
				mux:     &sync.Mutex{},
				PodList: make(map[nodeName]*NodePod),
			},
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test in map
		{
			"case2",
			fields{
				mux: &sync.Mutex{},
				PodList: map[nodeName]*NodePod{
					nodeName("test"): &NodePod{
						podsQueue: podsObjQueue{
							items: []*podObj{
								{
									usedFlag: false,
								}},
						},
					},
				},
			},
			args{
				node: "test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			patches := tt.patchesFunc()
			if got := p.choosePodInNode(tt.args.node); (got != nil) != tt.want {
				t.Errorf("choosePodInNode() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_choosePodRandom(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			fields{
				mux: &sync.Mutex{},
				PodList: map[nodeName]*NodePod{
					nodeName("test"): {
						readyPodNum: 0,
					},
					nodeName("t"): {
						podsQueue: podsObjQueue{
							items: []*podObj{
								{
									pod: &corev1.Pod{
										ObjectMeta: metav1.ObjectMeta{Name: "t"},
									},
									usedFlag: false,
								}},
						},
						readyPodNum: 100,
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			patches := tt.patchesFunc()
			if got := p.choosePodRandom(false, []string{}); (got != nil) != tt.want {
				t.Errorf("choosePodRandom() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_getPodInternal(t *testing.T) {
	_, _ = cache.WorkerCache.HandleInsMetadata("yyrkc536-0-default-test-reserved-latest-634330035", config.InstanceMetaData{
		MinInstance: 0, MaxInstance: 1000, IdleMode: false}, types.ProfileMetaData{}, utils2.FunctionInfo{})
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		ctx          context.Context
		demandNodes  []string
		req          common.PodRequest
		reservedAnti string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1 PollImmediate error",
			fields{
				mux: &sync.Mutex{},
			},
			args{reservedAnti: weakAntiAffinity},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(wait.PollImmediate,
						func(interval, timeout time.Duration, condition wait.ConditionFunc) error {
							return errors.New("PollImmediate error")
						}),
				})
				return patches
			},
			true,
		},
		{
			"case2 succeed to chose pod with reserved req",
			fields{
				mux: &sync.Mutex{},
				PodList: map[nodeName]*NodePod{nodeName("192.168.77.88"): &NodePod{
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "172.168.77.88"},
								},
								usedFlag: false,
							}},
					},
					readyPodNum: 1,
				}},
			},
			args{req: common.PodRequest{
				CPU:      300,
				Mem:      128,
				FuncName: "test-reserved",
				TraceID:  "reserved-6aceb507-52fe-4e8f-ada5-5ebd76e129c1",
				FunSvcID: "yyrkc536-0-default-test-reserved-latest-634330035",
				Applier:  constants.WorkerManagerApplier,
			}, reservedAnti: weakAntiAffinity,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*Pool).choosePodFromNodes,
						func(_ *Pool, ctx context.Context, nodes []string, _ []string, timeout time.Duration,
							req common.PodRequest) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
				})
				return patches
			},
			false,
		},
		{
			"case3 failed to chose pod with reserved req, pod is nil and err is not nil",
			fields{
				mux: &sync.Mutex{},
				PodList: map[nodeName]*NodePod{nodeName("192.168.77.88"): &NodePod{
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "172.168.77.88"},
								},
								usedFlag: false,
							}},
					},
					readyPodNum: 1,
				}, nodeName("192.168.77.89"): &NodePod{
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "172.168.77.88"},
								},
								usedFlag: false,
							}},
					},
					readyPodNum: 1,
				}},
			},
			args{req: common.PodRequest{
				CPU:      300,
				Mem:      128,
				FuncName: "test-reserved",
				TraceID:  "reserved-6aceb507-52fe-4e8f-ada5-5ebd76e129c1",
				FunSvcID: "yyrkc536-0-default-test-reserved-latest-634330035",
				Applier:  constants.WorkerManagerApplier,
			}, reservedAnti: weakAntiAffinity,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(wait.PollImmediate,
						func(interval, timeout time.Duration, condition wait.ConditionFunc) error {
							return errors.New("PollImmediate error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
				queue:                         schedulequeue.NewPriorityScheduleQueue("test", podRequestPriority, make(chan struct{})),
			}
			p.queue.Start()
			patches := tt.patchesFunc()
			_, err := p.getPodInternal(context.TODO(), tt.args.demandNodes, tt.args.req, tt.args.reservedAnti, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPodInternal() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_getPodInternal_wait_schedule_timeout(t *testing.T) {
	p := &Pool{
		mux:   &sync.Mutex{},
		queue: schedulequeue.NewPriorityScheduleQueue("testqueue", podRequestPriority, make(chan struct{})),
	}
	p.queue.Start()

	pathes := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*Pool).choosePodFromNodes,
			func(_ *Pool, ctx context.Context, nodes []string, _ []string, timeout time.Duration,
				req common.PodRequest) (*corev1.Pod, error) {
				time.Sleep(100 * time.Millisecond)
				return &corev1.Pod{}, nil
			}),
		gomonkey.ApplyFunc(context.WithTimeout, func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
			return context.WithDeadline(parent, time.Now().Add(1*time.Millisecond))
		}),
	}
	defer func() {
		for i := range pathes {
			pathes[i].Reset()
		}
	}()

	go p.getPodInternal(context.TODO(), nil, common.PodRequest{}, weakAntiAffinity, true)
	time.Sleep(50 * time.Millisecond)
	pod, err := p.getPodInternal(context.TODO(), nil, common.PodRequest{}, weakAntiAffinity, true)

	assert.Equal(t, ErrRequestExceedLimit, err, "wait pod timeout")
	assert.Nil(t, pod, "pod should be nil when wait timeout")
}

func TestPool_getPodInternalWithPriority(t *testing.T) {
	p := &Pool{
		mux:   &sync.Mutex{},
		queue: schedulequeue.NewPriorityScheduleQueue("testqueue", podRequestPriority, make(chan struct{})),
	}
	p.queue.Start()

	count := uint32(0)

	n := 10

	pods := make([]*corev1.Pod, 0, n+1)

	for i := 0; i <= n; i++ {
		pods = append(pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}})
	}

	pathes := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*Pool).choosePodFromNodes,
			func(_ *Pool, ctx context.Context, nodes []string, _ []string, timeout time.Duration,
				req common.PodRequest) (*corev1.Pod, error) {
				time.Sleep(100 * time.Millisecond)
				index := atomic.AddUint32(&count, 1) - 1
				return pods[index], nil
			}),
	}
	defer func() {
		for i := range pathes {
			pathes[i].Reset()
		}
	}()

	// make sure a request is holding schedule queue
	go p.getPodInternal(context.TODO(), nil, common.PodRequest{}, weakAntiAffinity, true)
	time.Sleep(50 * time.Millisecond)

	wg := sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			pod, err := p.getPodInternal(context.TODO(), nil, common.PodRequest{Priority: -i}, weakAntiAffinity, true)
			assert.Nil(t, err, "get pod internal should no error")
			assert.Equal(t, pod.Name, pods[i+1].Name)
		}(i)
	}
	wg.Wait()
}

func TestPool_getPodInternalWithCustomResource(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		customResource map[string]int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"case1",
			fields{
				CustomResources: map[string]int64{
					"test": 1,
				},
				mux: &sync.Mutex{},
			},
			args{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			_, err := p.getPodInternalWithCustomResource(tt.args.customResource)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPodInternalWithCustomResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestPool_choosePodFromNodes(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		ctx             context.Context
		nodes           []string
		noScheduleNodes []string
		timeout         time.Duration
		traceID         string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test PollImmediate error
		{
			"case1",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(wait.PollImmediate,
						func(interval, timeout time.Duration, condition wait.ConditionFunc) error {
							return errors.New("PollImmediate error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			patches := tt.patchesFunc()
			req := common.PodRequest{
				TraceID:            tt.args.traceID,
				EnableTMPExpansion: false,
			}
			_, err := p.choosePodFromNodes(tt.args.ctx, tt.args.nodes, tt.args.noScheduleNodes, tt.args.timeout, req)
			if (err != nil) != tt.wantErr {
				t.Errorf("choosePodFromNodes() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_demandPodFromNodes(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		ctx     context.Context
		req     common.PodRequest
		timeout time.Duration
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(wait.PollImmediate,
						func(interval, timeout time.Duration, condition wait.ConditionFunc) error {
							return errors.New("PollImmediate error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			patches := tt.patchesFunc()
			_, err := p.demandPodFromNodes(tt.args.ctx, tt.args.req, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("demandPodFromNodes() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_findCandidateNode(t *testing.T) {
	type fields struct {
		Deployment                    *appsv1.Deployment
		mux                           *sync.Mutex
		PodList                       map[nodeName]*NodePod
		scalingConfig                 *ScalingConfig
		Label                         map[string]string
		Annotations                   map[string]string
		CustomResources               map[string]int64
		OndemandChan                  map[string]chan *corev1.Pod
		Name                          string
		NameSpace                     string
		Standard                      standard
		Language                      string
		PodNum                        int64
		LimitCPU                      int64
		RequestCPU                    int64
		LimitMemory                   int64
		RequestMemory                 int64
		EphemeralStorage              int64
		LivenessProbePeriodSeconds    int32
		LivenessProbeFailureThreshold int32
		PoolSize                      int32
		NonIsolated                   bool
	}
	type args struct {
		demandNodes []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// test podList empty
		{
			"case1",
			fields{
				mux: &sync.Mutex{},
			},
			args{
				demandNodes: []string{
					"test",
				},
			},
			false,
		},
		// test in podList
		{
			"case2",
			fields{
				mux: &sync.Mutex{},
				PodList: map[nodeName]*NodePod{
					nodeName("test"): {
						readyPodNum: 2,
					},
				},
			},
			args{
				demandNodes: []string{
					"test",
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment:                    tt.fields.Deployment,
				mux:                           tt.fields.mux,
				PodList:                       tt.fields.PodList,
				scalingConfig:                 tt.fields.scalingConfig,
				Label:                         tt.fields.Label,
				Annotations:                   tt.fields.Annotations,
				CustomResources:               tt.fields.CustomResources,
				OndemandChan:                  tt.fields.OndemandChan,
				Name:                          tt.fields.Name,
				NameSpace:                     tt.fields.NameSpace,
				Standard:                      tt.fields.Standard,
				Language:                      tt.fields.Language,
				PodNum:                        tt.fields.PodNum,
				LimitCPU:                      tt.fields.LimitCPU,
				RequestCPU:                    tt.fields.RequestCPU,
				LimitMemory:                   tt.fields.LimitMemory,
				RequestMemory:                 tt.fields.RequestMemory,
				EphemeralStorage:              tt.fields.EphemeralStorage,
				LivenessProbePeriodSeconds:    tt.fields.LivenessProbePeriodSeconds,
				LivenessProbeFailureThreshold: tt.fields.LivenessProbeFailureThreshold,
				PoolSize:                      tt.fields.PoolSize,
				NonIsolated:                   tt.fields.NonIsolated,
			}
			if got := p.findCandidateNode(tt.args.demandNodes); (len(got) > 0) != tt.want {
				t.Errorf("findCandidateNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSelectPodFromRandomNodesForTMPExpansion -
func TestSelectPodFromRandomNodesForTMPExpansion(t *testing.T) {
	convey.Convey("test: test select pod from random nodes for tmp expansion", t, func() {
		p := &Pool{
			mux:    &sync.Mutex{},
			PodNum: 2,
			PodList: map[nodeName]*NodePod{
				"nodePod1": {readyPodNum: 0},
				"node2": {readyPodNum: 1,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod2"},
									Spec: corev1.PodSpec{
										NodeName: "node2",
									},
								},
								usedFlag: false,
							}},
					}},
				"node3": {readyPodNum: 1,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod3"},
									Spec: corev1.PodSpec{
										NodeName: "node3",
									},
								},
								usedFlag: false,
							}},
					},
				},
			},
		}
		patch := gomonkey.ApplyFunc(cache.GetAvailableNodesForTMPExpansion, func() map[string]*cache.NodeInfo {
			return map[string]*cache.NodeInfo{
				"node2": {},
			}
		})
		patch.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
			return true
		})
		patch.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
			return &cache.NodeInfo{}
		})
		defer patch.Reset()
		pod, done, err := p.selectPodFromRandomNodes(true, []string{})
		convey.So(pod, convey.ShouldNotBeNil)
		convey.So(done, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
		pod, done, err = p.selectPodFromRandomNodes(true, []string{})
		convey.So(pod, convey.ShouldBeNil)
		convey.So(done, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
}

// TestSelectPodFromRandomNodes -
func TestSelectPodFromRandomNodes(t *testing.T) {
	// 6 instances are initialized and 1 instance is randomly chosen, the expected value of readyPodNums is 5
	convey.Convey("test: test multi-choice pod protection in cyclic nesting mode", t, func() {
		p := &Pool{
			mux:    &sync.Mutex{},
			PodNum: 2,
			PodList: map[nodeName]*NodePod{
				"nodePod1": {readyPodNum: 2,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
									Spec: corev1.PodSpec{
										NodeName: "nodePod1",
									},
								},
							},
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod2"},
									Spec: corev1.PodSpec{
										NodeName: "nodePod1",
									},
								},
							}},
					},
				},
				"nodePod2": {readyPodNum: 2,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
									Spec: corev1.PodSpec{
										NodeName: "nodePod2",
									},
								},
							},
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod2"},
									Spec: corev1.PodSpec{
										NodeName: "nodePod2",
									},
								},
							}},
					}},
				"nodePod3": {readyPodNum: 2,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
									Spec: corev1.PodSpec{
										NodeName: "nodePod3",
									},
								},
							},
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod2"},
									Spec: corev1.PodSpec{
										NodeName: "nodePod3",
									},
								},
							}},
					}},
			},
		}
		patch := gomonkey.ApplyFunc(cache.GetAvailableNodesForTMPExpansion, func() map[string]*cache.NodeInfo {
			return nil
		})
		patch.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
			return true
		})
		patch.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
			return &cache.NodeInfo{}
		})
		defer patch.Reset()
		pod := p.choosePodRandom(false, []string{})
		convey.So(pod, convey.ShouldNotBeNil)
		totalReadyPodNums := 0
		for _, nodePod := range p.PodList {
			totalReadyPodNums += nodePod.readyPodNum
		}
		convey.So(totalReadyPodNums, convey.ShouldEqual, 5)
	})
}

// TestPool_ReclaimingSelectedButUnusedPod -
func TestPool_ReclaimingSelectedButUnusedPod(t *testing.T) {
	convey.Convey("test: reclaiming selected but unused pod", t, func() {
		node1Pod1 := &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node1"}}
		node1Pod1.Name = "node1Pod1"
		nodePod := &NodePod{
			readyPodNum: 0,
			podsQueue: podsObjQueue{
				items: []*podObj{
					{
						pod:      node1Pod1,
						usedFlag: true,
					}},
			},
		}
		p := &Pool{
			PodList: map[nodeName]*NodePod{
				"node1": nodePod,
			},
		}
		node := &cache.NodeInfo{}
		patch := gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
			return node
		})
		defer patch.Reset()
		p.reclaimUnusedSelectedPod(nil, false)
		p.reclaimUnusedSelectedPod(&corev1.Pod{Spec: corev1.PodSpec{NodeName: "node1"}}, true)
		p.reclaimUnusedSelectedPod(node1Pod1, false)
		convey.So(p.PodNum, convey.ShouldEqual, 1)
		convey.So(nodePod.readyPodNum, convey.ShouldEqual, 1)
		convey.So(nodePod.podsQueue.getSpecificPod("node1Pod1").usedFlag, convey.ShouldBeFalse)
	})
}

// TestSelectPodFromSpecifiedNodes -
func TestSelectPodFromSpecifiedNodes(t *testing.T) {
	convey.Convey("test: select pod from specified nodes", t, func() {
		p := &Pool{
			mux:    &sync.Mutex{},
			PodNum: 2,
			PodList: map[nodeName]*NodePod{
				"node1": {readyPodNum: 1,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
									Spec: corev1.PodSpec{
										NodeName: "node1",
									},
								},
							}},
					}},
				"node2": {readyPodNum: 1,
					podsQueue: podsObjQueue{
						items: []*podObj{
							{
								pod: &corev1.Pod{
									ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
									Spec: corev1.PodSpec{
										NodeName: "node2",
									},
								},
							}}},
				},
			},
		}
		// node2 is valid, node1 is invalid
		patch := gomonkey.ApplyFunc(cache.GetAvailableNodesForTMPExpansion, func() map[string]*cache.NodeInfo {
			return map[string]*cache.NodeInfo{
				"node1": {},
			}
		})
		patch.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool { return true })
		defer patch.Reset()
		pod, done, err := p.selectPodFromSpecifiedNodes([]string{"node1"}, true, []string{})
		convey.So(pod, convey.ShouldNotBeNil)
		convey.So(done, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
		pod, done, err = p.selectPodFromSpecifiedNodes([]string{"node2"}, true, []string{})
		convey.So(pod, convey.ShouldBeNil)
		convey.So(done, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})

}

func Test_getSortedNodeList(t *testing.T) {
	type args struct {
		podList map[nodeName]*NodePod
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		checkFunc   func(result []nodeName) bool
		isChecked   bool
	}{
		{
			name: "test for sorting node list",
			args: args{
				podList: map[nodeName]*NodePod{
					"nodeID1": {readyPodNum: 1},
					"nodeID2": {readyPodNum: 2},
					"nodeID3": {readyPodNum: 3},
					"nodeID4": {readyPodNum: 4},
					"nodeID5": {readyPodNum: 5},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						if nodeName == "nodeID3" {
							return nil
						}
						if nodeName == "nodeID2" || nodeName == "nodeID4" {
							return &cache.NodeInfo{
								Taints: []corev1.Taint{
									{
										Key: corev1.TaintNodeUnschedulable,
									},
								},
							}
						}
						return &cache.NodeInfo{}
					}),
				})
				return patches
			},
			checkFunc: func(result []nodeName) bool {
				if len(result) == 0 {
					return false
				}
				last := len(result) - 1
				if result[last] == "nodeID2" || result[last] == "nodeID4" {
					return true
				}
				if result[last-1] == "nodeID2" || result[last-1] == "nodeID4" {
					return true
				}
				return false
			},
			isChecked: true,
		},
		{
			name: "test for no taints node",
			args: args{
				podList: map[nodeName]*NodePod{
					"nodeID1": {readyPodNum: 1},
					"nodeID2": {readyPodNum: 2},
					"nodeID3": {readyPodNum: 3},
					"nodeID4": {readyPodNum: 4},
					"nodeID5": {readyPodNum: 5},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						if nodeName == "nodeID3" {
							return nil
						}
						return &cache.NodeInfo{}
					}),
				})
				return patches
			},
			checkFunc: func(result []nodeName) bool {
				if len(result) == 0 {
					return false
				}
				return true
			},
			isChecked: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		result := getSortedNodeList(tt.args.podList)
		if tt.checkFunc(result) != tt.isChecked {
			t.Errorf("expect %v but found %v", tt.isChecked, result)
		}
		fmt.Println(result)
		patches.ResetAll()
	}
}

func TestPool_getHangingTime(t *testing.T) {
	tests := []struct {
		name string
		pool *Pool

		wantZero bool
	}{
		{
			name: "pool size is zero",
			pool: &Pool{
				mux:      &sync.Mutex{},
				PoolSize: 0,
			},
			wantZero: true,
		},
		{
			name: "has pod",
			pool: &Pool{
				mux:      &sync.Mutex{},
				PoolSize: 100,
				PodNum:   1,
			},
			wantZero: true,
		},
		{
			name: "pool size is not zero and no pod",
			pool: &Pool{
				mux:      &sync.Mutex{},
				PoolSize: 1,
				PodNum:   0,
			},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pool.getHangingTime()
			if tt.wantZero {
				assert.Equal(t, time.Duration(0), got, tt.name)
			} else {
				assert.Greater(t, got, time.Duration(0), tt.name)
			}
		})
	}
}

func Test_PodsObjQueue(t *testing.T) {
	convey.Convey("Test_PodsObjQueue", t, func() {
		patch := gomonkey.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
			return true
		})
		defer patch.Reset()
		nodePod := &NodePod{}
		assert.Nil(t, nodePod.podsQueue.getAvailablePodList())
		pushed := nodePod.podsQueue.push(&podObj{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podA"}}})
		assert.Equal(t, true, pushed)
		pushed = nodePod.podsQueue.push(&podObj{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podA"}}})
		assert.Equal(t, false, pushed)
		pushed = nodePod.podsQueue.push(&podObj{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podB"}}})
		assert.Equal(t, true, pushed)
		pushed = nodePod.podsQueue.push(&podObj{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podC"}}})
		assert.Equal(t, true, pushed)

		assert.Equal(t, 3, len(nodePod.podsQueue.getAvailablePodList()))

		assert.Equal(t, "podA", nodePod.podsQueue.getSpecificPod("podA").pod.Name)
		assert.Equal(t, "podB", nodePod.podsQueue.getSpecificPod("podB").pod.Name)
		assert.Equal(t, "podC", nodePod.podsQueue.getSpecificPod("podC").pod.Name)
		assert.Nil(t, nodePod.podsQueue.getSpecificPod("podD"))

		pod := nodePod.podsQueue.getReadyPod()
		assert.Equal(t, "podA", pod.pod.Name)
		pod.usedFlag = true

		pod = nodePod.podsQueue.getReadyPod()
		assert.Equal(t, "podB", pod.pod.Name)
		pod.usedFlag = true

		pod = nodePod.podsQueue.getReadyPod()
		assert.Equal(t, "podC", pod.pod.Name)
		pod.usedFlag = true

		pod = nodePod.podsQueue.getReadyPod()
		assert.Nil(t, pod)
		assert.Nil(t, nodePod.podsQueue.getAvailablePodList())
		nodePod.podsQueue.remove("podA")
		pushed = nodePod.podsQueue.push(&podObj{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "podA"}}})
		assert.Equal(t, true, pushed)
		pod = nodePod.podsQueue.getReadyPod()
		assert.Equal(t, "podA", pod.pod.Name)
	})
}

func Test_getDemandPodWithAffinityNodes(t *testing.T) {
	tests := []struct {
		name           string
		pod            *corev1.Pod
		availableNodes []string
		expected       *corev1.Affinity
		patchesFunc    utils.PatchesFunc
	}{
		{
			name:           "podScheduleInfo.TaggedScheduleLabel is true",
			availableNodes: []string{"node1", "node2"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},

			expected: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      k8sHostNameKey,
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"node1", "node2"},
									},
								},
							},
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&corev1.PodSpec{}), "DeepCopy",
						func(_ *corev1.PodSpec) *corev1.PodSpec {
							return &corev1.PodSpec{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&metav1.ObjectMeta{}), "DeepCopy",
						func(_ *metav1.ObjectMeta) *metav1.ObjectMeta {
							return &metav1.ObjectMeta{
								Annotations: make(map[string]string),
								Labels:      make(map[string]string),
							}
						}),
				})
				return patches
			},
		},
	}
	p := &Pool{
		Deployment: &appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{},
					ObjectMeta: metav1.ObjectMeta{
						Annotations:     make(map[string]string),
						Labels:          make(map[string]string),
						OwnerReferences: []metav1.OwnerReference{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			pod := p.getDemandPodWithAffinityNodes(tt.availableNodes)
			assert.Equal(t, tt.expected, pod.Spec.Affinity, tt.name)
			patches.ResetAll()
		})
	}
}

func TestPool_getPodInAffinityNode(t *testing.T) {
	type fields struct {
		Deployment *appsv1.Deployment
	}
	type args struct {
		availableNodes []string
		customResource map[string]int64
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test client nil
		{
			"case1",
			fields{
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
			},
			args{
				availableNodes: []string{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&corev1.PodSpec{}), "DeepCopy",
						func(_ *corev1.PodSpec) *corev1.PodSpec {
							return &corev1.PodSpec{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&metav1.ObjectMeta{}), "DeepCopy",
						func(_ *metav1.ObjectMeta) *metav1.ObjectMeta {
							return &metav1.ObjectMeta{
								Annotations: make(map[string]string),
								Labels:      make(map[string]string),
							}
						}),
				})
				return patches
			},
			false,
		},
		// test create error
		{
			"case2",
			fields{
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
			},
			args{
				availableNodes: []string{"node1", "node2"},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, errors.New("create pod error")
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&corev1.PodSpec{}), "DeepCopy",
						func(_ *corev1.PodSpec) *corev1.PodSpec {
							return &corev1.PodSpec{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&metav1.ObjectMeta{}), "DeepCopy",
						func(_ *metav1.ObjectMeta) *metav1.ObjectMeta {
							return &metav1.ObjectMeta{
								Annotations: make(map[string]string),
								Labels:      make(map[string]string),
							}
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Deployment: tt.fields.Deployment,
			}
			patches := tt.patchesFunc()
			if got := p.getPodInAffinityNode(tt.args.availableNodes, tt.args.customResource); (got != nil) != tt.want {
				t.Errorf("demandPodInNode() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_getPodFromAffinityNode(t *testing.T) {
	type args struct {
		req             common.PodRequest
		podScheduleInfo cache.PodScheduleInfo
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		isErr       bool
		isPodNil    bool
	}{
		{
			"get pod form Candidate Affinity Node",
			args{
				req: common.PodRequest{},
				podScheduleInfo: cache.PodScheduleInfo{
					AffinityNodes:          []string{"node1"},
					PreferredAffinityNodes: []string{"node2"},
					AntiAffinityNodes:      []string{"node3"},
					TaggedScheduleLabel:    true,
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*Pool).demandPodFromAffinityNode,
						func(_ *Pool, ctx context.Context, req common.PodRequest, timeout time.Duration,
							availableNodes []string) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&corev1.PodSpec{}), "DeepCopy",
						func(_ *corev1.PodSpec) *corev1.PodSpec {
							return &corev1.PodSpec{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&metav1.ObjectMeta{}), "DeepCopy",
						func(_ *metav1.ObjectMeta) *metav1.ObjectMeta {
							return &metav1.ObjectMeta{
								Annotations: make(map[string]string),
								Labels:      make(map[string]string),
							}
						}),
				})
				return patches
			},
			false,
			false,
		},
		{
			"get pod form Candidate Affinity Node, requireDeploy is true",
			args{
				req: common.PodRequest{},
				podScheduleInfo: cache.PodScheduleInfo{
					AffinityNodes:          []string{"node1"},
					PreferredAffinityNodes: []string{"node2"},
					AntiAffinityNodes:      []string{"node3"},
					TaggedScheduleLabel:    true,
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*Pool).demandPodFromAffinityNode,
						func(_ *Pool, ctx context.Context, req common.PodRequest, timeout time.Duration,
							availableNodes []string) (*corev1.Pod, error) {
							return nil, errors.New("test")
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&corev1.PodSpec{}), "DeepCopy",
						func(_ *corev1.PodSpec) *corev1.PodSpec {
							return &corev1.PodSpec{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&metav1.ObjectMeta{}), "DeepCopy",
						func(_ *metav1.ObjectMeta) *metav1.ObjectMeta {
							return &metav1.ObjectMeta{
								Annotations: make(map[string]string),
								Labels:      make(map[string]string),
							}
						}),
				})
				return patches
			},
			true,
			true,
		},
		{
			"AffinityNodes is empty",
			args{
				req: common.PodRequest{},
				podScheduleInfo: cache.PodScheduleInfo{
					TaggedScheduleLabel: false,
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			false,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				mux: new(sync.Mutex),
				Deployment: &appsv1.Deployment{
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
							ObjectMeta: metav1.ObjectMeta{
								Annotations:     make(map[string]string),
								Labels:          make(map[string]string),
								OwnerReferences: []metav1.OwnerReference{},
							},
						},
					},
				},
			}
			patches := tt.patchesFunc()
			pod, err := p.getPodFromAffinityNode(context.TODO(), tt.args.req, tt.args.podScheduleInfo, 60*time.Second)
			assert.Equal(t, tt.isErr, err != nil)
			assert.Equal(t, tt.isPodNil, pod == nil)
			patches.ResetAll()
		})
	}
}

func TestPool_filterNoScheduleNode(t *testing.T) {
	convey.Convey("filterNodes", t, func() {
		data := filterNoScheduleNode([]nodeName{"node1", "node2", "node3"}, []string{"node1", "node2", "node3", "node4"})
		convey.So(len(data), convey.ShouldEqual, 0)

		tmp := filterNoScheduleNode([]nodeName{"node1", "node2", "node3"}, []string{"node1", "node3", "node4"})
		convey.So(len(tmp), convey.ShouldEqual, 1)
		convey.So(tmp[0], convey.ShouldEqual, "node2")

		data2 := filterNoScheduleNode([]string{"node1", "node2", "node3"}, []string{"node1", "node2", "node3", "node4"})
		convey.So(len(data2), convey.ShouldEqual, 0)

		tmp2 := filterNoScheduleNode([]string{"node1", "node2", "node3"}, []string{"node1", "node3", "node4"})
		convey.So(len(tmp2), convey.ShouldEqual, 1)
		convey.So(tmp[0], convey.ShouldEqual, "node2")
	})
}
