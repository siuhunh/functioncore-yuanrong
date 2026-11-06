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

// Package poolmanager manages pool
package poolmanager

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"functioncore/pkg/worker-manager/config"
	tools "functioncore/pkg/worker-manager/utils"
	"functioncore/test/utils"
	mockUtils "functioncore/test/utils"
)

func Test_describeNodeAffinity(t *testing.T) {
	key := "tenant_id"
	values := []string{"123456"}
	labels := map[string]string{"node-type": "highend"}
	ssdNodeKey = "ssdNodeKey"
	ssdNodeValue = "ssdNodeValue"
	type args struct {
		key             string
		values          []string
		labels          map[string]string
		isFusionRuntime bool
	}
	tests := []struct {
		name        string
		args        args
		want        *corev1.NodeAffinity
		patchesFunc mockUtils.PatchesFunc
	}{
		{name: "test001", args: args{key: key, values: values, labels: nil, isFusionRuntime: true},
			want: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      key,
								Operator: corev1.NodeSelectorOpIn,
								Values:   values,
							},
							{
								Key:      ssdNodeKey,
								Operator: corev1.NodeSelectorOpNotIn,
								Values:   []string{ssdNodeValue},
							},
						},
					},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case_02 normal heterogeneous scheduling scenario",
			args: args{
				key:             key,
				values:          values,
				labels:          labels,
				isFusionRuntime: true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
			want: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      key,
							Operator: corev1.NodeSelectorOpIn,
							Values:   values,
						},
							{
								Key:      ssdNodeKey,
								Operator: corev1.NodeSelectorOpNotIn,
								Values:   []string{ssdNodeValue},
							},
							{
								Key:      "node-type",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"highend"},
							}},
					},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := describeNodeAffinity(tt.args.key, tt.args.values, tt.args.labels, tt.args.isFusionRuntime); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("describeNodeAffinity() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_describePodAntiAffinity(t *testing.T) {
	value := standard("pool22-300-128")
	type args struct {
		standard standard
	}
	tests := []struct {
		name string
		args args
		want *corev1.PodAntiAffinity
	}{
		{name: "test001", args: args{standard: value},
			want: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: podAntiAffinityWeight,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									tools.StandardLabelKey: string(value),
								},
							},
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := describePodAntiAffinity(tt.args.standard); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("describePodAntiAffinity() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_poolDeploymentBuilder_describeComponentSet(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		pool *Pool
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test failed to get config
		{
			"case1",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("get config error")
					}),
				})
				return patches
			},
			true,
		},
		// test get config success
		{
			"case2",
			fields{},
			args{
				pool: &Pool{
					Language: "zh/cn",
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
					gomonkey.ApplyFunc(GetPoolManager, func() *PoolMgr {
						return &PoolMgr{DecoyFileEnabled: true}
					}),
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{}
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &poolDeploymentBuilder{
				config: tt.fields.config,
			}
			patches := tt.patchesFunc()
			_, err := b.describeComponentSet(tt.args.pool)
			if (err != nil) != tt.wantErr {
				t.Errorf("describeComponentSet() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_poolDeploymentBuilder_getPodTemplateSpec(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		pool *Pool
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test get config error
		{
			"case1",
			fields{},
			args{
				pool: &Pool{
					Language: "zh/cn",
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, errors.New("get config error")
					}),
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{}
					}),
				})
				return patches
			},
			true,
		},
		// test get config success
		{
			"case2",
			fields{
				config: config.PoolMgrConfig{
					PoolList: config.PoolList{
						NodeAffinity: &config.NodeAffinity{
							NodeAffinityKey:   "test",
							NodeAffinityValue: []string{"test"},
						},
					},
				},
			},
			args{
				pool: &Pool{
					Language: "zh/cn",
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{}
					}),
					gomonkey.ApplyFunc(GetPoolManager, func() *PoolMgr {
						return &PoolMgr{DecoyFileEnabled: false}
					}),
				})
				return patches
			},
			false,
		},
		{
			"case3",
			fields{
				config: config.PoolMgrConfig{
					PoolList: config.PoolList{
						NodeAffinity: &config.NodeAffinity{
							NodeAffinityKey:   "test",
							NodeAffinityValue: []string{"test"},
						},
					},
				},
			},
			args{
				pool: &Pool{
					Language: "zh/cn",
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{SchedulingEnable: true}, nil
					}),
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{}
					}),
					gomonkey.ApplyFunc(GetPoolManager, func() *PoolMgr {
						return &PoolMgr{DecoyFileEnabled: true}
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &poolDeploymentBuilder{
				config: tt.fields.config,
			}
			patches := tt.patchesFunc()
			_, err := b.getPodTemplateSpec(tt.args.pool)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPodTemplateSpec() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_poolDeploymentBuilder_build(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		pool     *Pool
		ownerUID types.UID
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 getPodTemplateSpec failed",
			fields: fields{},
			args: args{
				pool: &Pool{
					Name: "test",
					Label: map[string]string{
						"test": "test",
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*poolDeploymentBuilder).getPodTemplateSpec,
						func(_ *poolDeploymentBuilder, pool *Pool) (corev1.PodTemplateSpec, error) {
							return corev1.PodTemplateSpec{}, fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name:   "case_02 normal scenario",
			fields: fields{},
			args: args{
				pool: &Pool{
					Name: "test",
					Label: map[string]string{
						"test": "test",
					},
					PoolSize: 10,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*poolDeploymentBuilder).getPodTemplateSpec,
						func(_ *poolDeploymentBuilder, pool *Pool) (corev1.PodTemplateSpec, error) {
							return corev1.PodTemplateSpec{}, nil
						}),
					gomonkey.ApplyFunc((*poolDeploymentBuilder).describeObjectMeta,
						func(_ *poolDeploymentBuilder, name string, labels map[string]string, ownerUID types.UID) metav1.ObjectMeta {
							return metav1.ObjectMeta{}
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			b := &poolDeploymentBuilder{
				config: tt.fields.config,
			}
			b.build(tt.args.pool, tt.args.ownerUID)
			patches.ResetAll()
		})
	}
}

func Test_newPoolDeploymentBuilder(t *testing.T) {
	type args struct {
		config config.PoolMgrConfig
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"case1",
			args{
				config: config.PoolMgrConfig{},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newPoolDeploymentBuilder(tt.args.config); (got != nil) != tt.want {
				t.Errorf("newPoolDeploymentBuilder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_poolDeploymentBuilder_describeNonIsolatedContainers(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		pool *Pool
		cs   *componentSet
	}
	var p *Pool
	p = &Pool{
		LimitCPU:         10,
		LimitMemory:      512,
		EphemeralStorage: 128,
		CustomResources:  map[string]int64{},
		WorkerLimitCPU:   10,
	}
	resourceBuilder := newResourceRequirementsBuilder()
	resourceBuilder.setCPU(workerRequestCPU, p.LimitCPU)
	resourceBuilder.setMemory(workerRequestMemory, p.LimitMemory)
	resourceBuilder.setEphemeralStorage(defaultRequestEphemeralStorage, p.EphemeralStorage)
	resourceBuilder.setCustomResource(p.CustomResources)
	resources := resourceBuilder.build()
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        []corev1.Container
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{config.PoolMgrConfig{PoolContainerTags: config.PoolContainerImageTags{WorkerImageTag: "1.0"}}},
			args: args{pool: p,
				cs: &componentSet{argsBuilder: &argsBuilder{map[container][]string{tools.WorkerContainerName: {"on"}}},
					envBuilder:    &envBuilder{map[container][]corev1.EnvVar{tools.WorkerContainerName: {}}},
					volumeBuilder: &volumeBuilder{mounts: map[container][]corev1.VolumeMount{tools.WorkerContainerName: {}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(getWorkerEnv,
						func(workerLimitMemory int64) []corev1.EnvVar {
							return []corev1.EnvVar{}
						}),
					gomonkey.ApplyFunc(sanitizeCAP,
						func(caps []string) []corev1.Capability {
							return []corev1.Capability{}
						}),
				})
				return patches
			},
			want: []corev1.Container{
				{
					Name:  tools.WorkerContainerName,
					Image: "1.0",
					Ports: []corev1.ContainerPort{
						{
							Name:          "func",
							Protocol:      corev1.ProtocolTCP,
							ContainerPort: workerFuncPort,
						},
						{
							Name:          "admin",
							Protocol:      corev1.ProtocolTCP,
							ContainerPort: workerAdminPort,
						},
					},
					Args:           []string{"on"},
					Env:            []corev1.EnvVar{},
					Resources:      resources,
					VolumeMounts:   []corev1.VolumeMount{},
					ReadinessProbe: getWorkerReadinessProbe(),
					LivenessProbe: getWorkerLivenessProbe(p.LivenessProbeFailureThreshold,
						p.LivenessProbePeriodSeconds),
					SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
						Add:  []corev1.Capability{},
						Drop: []corev1.Capability{},
					},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			b := &poolDeploymentBuilder{
				config: tt.fields.config,
			}
			if got := b.describeNonIsolatedContainers(tt.args.pool, tt.args.cs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("describeNonIsolatedContainers() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getNodeAffinity(t *testing.T) {
	ssdNodeKey = "ssdNodeKey"
	ssdNodeValue = "ssdNodeValue"
	type args struct {
		pool               *Pool
		nodeAffinityKey    string
		nodeAffinityValues []string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        *corev1.NodeAffinity
	}{
		{
			name: "case ssd runtime name",
			args: args{
				pool: &Pool{
					Standard: standard("300-128-ssd"),
					Label:    map[string]string{"node-type": "highend"},
				},
				nodeAffinityKey:    "node-role",
				nodeAffinityValues: []string{"data-plane"},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
			want: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "ssdNodeKey",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"ssdNodeValue"},
							},
							{
								Key:      "node-type",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"highend"},
							},
						},
					},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			actual := getNodeAffinity(tt.args.pool, tt.args.nodeAffinityKey, tt.args.nodeAffinityValues)
			assert.Equalf(t, tt.want, actual, "getNodeAffinity(%v, %v, %v)", tt.args.pool, tt.args.nodeAffinityKey, tt.args.nodeAffinityValues)
			fmt.Println(tt.want)
			fmt.Println(actual)
			patches.ResetAll()
		})
	}
}
