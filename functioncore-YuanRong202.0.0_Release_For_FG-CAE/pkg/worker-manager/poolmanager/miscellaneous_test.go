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

package poolmanager

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	v1 "k8s.io/api/core/v1"

	logconf "functioncore/pkg/common/logger/config"
	"functioncore/pkg/worker-manager/config"
	mockUtils "functioncore/test/utils"
)

// test for config volume
func Test_cgroupFreezer_configVolume(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "cgroup-freezer",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/sys/fs/cgroup/freezer/kubepods/burstable",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:        "cgroup-freezer",
						MountPath:   "/runtime/freezer",
						SubPathExpr: "pod$(POD_ID)",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &cgroupFreezer{}
			f.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_cgroupMemory_configVolume(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "cgroup-memory",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/sys/fs/cgroup/memory/kubepods/burstable",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:        "cgroup-memory",
						MountPath:   "/runtime/memory",
						SubPathExpr: "pod$(POD_ID)",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &cgroupMemory{}
			f.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_timeZone_configVolume(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "time-zone",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/etc/localtime",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "time-zone",
						MountPath: "/etc/localtime",
						ReadOnly:  true,
					}},
					containerRuntime: {{
						Name:      "time-zone",
						MountPath: "/etc/localtime",
						ReadOnly:  true,
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &timeZone{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_hostPackage_configVolume(t *testing.T) {
	type fields struct {
		path string
	}
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *volumeBuilder
	}{
		{
			name:   "case_01_normal_scenario",
			fields: fields{"/opt/pkg"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "host-package-path",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/opt/pkg",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "host-package-path",
						MountPath: "/home/sn/pkg",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &hostPackage{
				path: tt.fields.path,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config env
func Test_podInfo_configEnv(t *testing.T) {
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name string
		args args
		want *envBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorkerInit: {{
						Name: "POD_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.podIP",
							},
						}}, {
						Name: "HOST_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.hostIP",
							},
						}}},
					containerWorker: {{
						Name: "POD_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.podIP",
							},
						}}, {
						Name: "HOST_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.hostIP",
							},
						}}, {
						Name: "PodName",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						}}, {
						Name: "POD_ID",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "metadata.uid",
							},
						}}, {
						Name: "POD_NAMESPACE",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath:  "metadata.namespace",
								APIVersion: "",
							},
						}},
					},
					containerRuntime: {{
						Name: "HOST_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.hostIP",
							},
						}}, {
						Name: "PodName",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						}}, {
						Name: "POD_ID",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "metadata.uid",
							},
						}}, {
						Name: "POD_NAMESPACE",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath:  "metadata.namespace",
								APIVersion: "",
							},
						}},
					},
					containerAgent: {{
						Name: "POD_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.podIP",
							},
						}}, {
						Name: "HOST_IP",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "status.hostIP",
							},
						}}, {
						Name: "PodName",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						}}, {
						Name: "POD_ID",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath: "metadata.uid",
							},
						}}, {
						Name: "POD_NAMESPACE",
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{
								FieldPath:  "metadata.namespace",
								APIVersion: "",
							},
						}},
					},
				},
			},
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &podInfo{}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b.envs[containerWorkerInit], tt.want.envs[containerWorkerInit]) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b.envs[containerWorkerInit], tt.want.envs[containerWorkerInit])
			}
			if !reflect.DeepEqual(tt.args.b.envs[containerWorker], tt.want.envs[containerWorker]) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b.envs[containerWorker], tt.want.envs[containerWorker])
			}
			if !reflect.DeepEqual(tt.args.b.envs[containerRuntime], tt.want.envs[containerRuntime]) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b.envs[containerRuntime], tt.want.envs[containerRuntime])
			}
			if !reflect.DeepEqual(tt.args.b.envs[containerAgent], tt.want.envs[containerAgent]) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b.envs[containerAgent], tt.want.envs[containerAgent])
			}
		})
	}
}

// test for config env
func Test_localCache_configEnv(t *testing.T) {
	type fields struct {
		enable bool
	}
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *envBuilder
	}{
		{
			name:   "case_01_normal_scenario",
			fields: fields{enable: true},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorkerInit: {{
						Name:  "ENABLE_LOCAL_CACHE",
						Value: "true",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &localCache{
				enable: tt.fields.enable,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config env
func Test_obs_configEnv(t *testing.T) {
	type fields struct {
		enable bool
	}
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *envBuilder
	}{
		{
			name:   "case_01_normal_scenario",
			fields: fields{enable: false},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorkerInit: {{
						Name:  "ENABLE_MINIO",
						Value: "false",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &obs{
				enable: tt.fields.enable,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config env
func Test_workerAuthCheck_configEnv(t *testing.T) {
	type fields struct {
		enable bool
	}
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *envBuilder
	}{
		{
			name:   "case_01_normal_scenario",
			fields: fields{enable: true},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorker: {{
						Name:  "INIT_AUTH_CHECK",
						Value: "CHECK",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &workerAuthCheck{
				enable: tt.fields.enable,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config env
func Test_funcBranch_configEnv(t *testing.T) {
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        *envBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(string) string {
							return "FUNC_BRANCH_KEY"
						}),
				})
				return patches
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorker: {{
						Name:  "FUNC_BRANCH",
						Value: "FUNC_BRANCH_KEY",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			v := &funcBranch{}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
			patches.ResetAll()
		})
	}
}

// test for config env
func Test_runtimeLanguage_configEnv(t *testing.T) {
	type fields struct {
		language string
	}
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        *envBuilder
	}{
		{
			name:   "case_01_normal_scenario",
			fields: fields{"cpp"},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(getRuntimeType,
						func(string) string {
							return "cpp"
						}),
				})
				return patches
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorker: {{
						Name:  "RUNTIME_TYPE",
						Value: "cpp",
					}},
					containerRuntime: {{
						Name:  "RUNTIME_LANGUAGE",
						Value: "cpp",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			v := &runtimeLanguage{
				language: tt.fields.language,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
			patches.ResetAll()
		})
	}
}

// test for config env
func Test_trace_configEnv(t *testing.T) {
	type fields struct {
		runtimeLimitMemory int64
	}
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *envBuilder
	}{
		{
			name:   "case_01_normal_scenario",
			fields: fields{128},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerRuntime: {{
						Name:  "TRACE_DISABLE",
						Value: "DISABLE",
					}},
					containerWorker: {{
						Name:  "TRACE_DISABLE",
						Value: "DISABLE",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &trace{
				runtimeLimitMemory: tt.fields.runtimeLimitMemory,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_dockerRootDir_configVolume(t *testing.T) {
	bidirectional := v1.MountPropagationBidirectional
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "docker-rootdir",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/var/lib/docker",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:             "docker-rootdir",
						MountPath:        "/var/lib/docker",
						MountPropagation: &bidirectional,
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &dockerRootDir{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_dockerSocket_configVolume(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "docker-socket",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/var/run/docker.sock",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "docker-socket",
						MountPath: "/var/run/docker.sock",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &dockerSocket{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_presetDependencyConfig_configVolume(t *testing.T) {
	hostPathType := v1.HostPathDirectoryOrCreate
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "public-dependency",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/paas/dependencies",
							Type: &hostPathType,
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "public-dependency",
						MountPath: "/opt/function/dependencies",
						ReadOnly:  true,
					}},
					containerRuntime: {{
						Name:      "public-dependency",
						MountPath: "/opt/function/dependencies",
						ReadOnly:  true,
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &presetDependencyConfig{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_cudaDependencyConfig_configVolume(t *testing.T) {
	hostPathType := v1.HostPathDirectoryOrCreate
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "cuda-dependency",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/usr/local/cuda",
							Type: &hostPathType,
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerRuntime: {{
						Name:      "cuda-dependency",
						MountPath: "/usr/local/cuda",
						ReadOnly:  true,
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &cudaDependencyConfig{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

func Test_workerLogEnv_configEnv(t *testing.T) {
	logConfigs := logconf.CoreInfo{
		FilePath: "/home/sn/log/service",
		Level:    "INFO",
	}
	logConfEnv, _ := json.Marshal(logConfigs)
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name        string
		args        args
		want        *envBuilder
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(logconf.ExtractCoreInfoFromEnv,
						func(env string) (logconf.CoreInfo, error) {
							return logConfigs, nil
						}),
				})
				return patches
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorker: {{
						Name:  logconf.LogConfigKey,
						Value: string(logConfEnv),
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			v := &workerLogEnv{}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_runtimeMgrLogEnv_configEnv(t *testing.T) {
	logConfigs := logconf.CoreInfo{
		FilePath: "/home/sn/log",
		Level:    "INFO",
	}
	logConfEnv, _ := json.Marshal(logConfigs)
	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name        string
		args        args
		want        *envBuilder
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(logconf.ExtractCoreInfoFromEnv,
						func(env string) (logconf.CoreInfo, error) {
							return logConfigs, nil
						}),
				})
				return patches
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerRuntime: {{
						Name:  logconf.LogConfigKey,
						Value: string(logConfEnv),
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			v := &runtimeMgrLogEnv{}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_customContainerConfigFile_configVolume(t *testing.T) {
	type fields struct {
		fileMountConfigs []*config.MountFile
		secretFiles      []*config.SecretFileMount
		podName          string
	}
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		expectLen int
	}{
		{
			name: "test secret files",
			fields: fields{
				secretFiles: []*config.SecretFileMount{
					{
						EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
						FileMountPath:        "/tmp/abc.txt",
					},
				},
			},
			args: args{
				b: &volumeBuilder{
					mounts: map[container][]v1.VolumeMount{},
				},
			},
			expectLen: 1,
		},
		{
			name: "test secret files EncryptedFileContent is empty",
			fields: fields{
				secretFiles: []*config.SecretFileMount{
					{
						EncryptedFileContent: "",
						FileMountPath:        "/tmp/abc.txt",
					},
				},
			},
			args: args{
				b: &volumeBuilder{
					mounts: map[container][]v1.VolumeMount{},
				},
			},
			expectLen: 0,
		},
		{
			name: "test secret files FileMountPath is empty",
			fields: fields{
				secretFiles: []*config.SecretFileMount{
					{
						EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
						FileMountPath:        "",
					},
				},
			},
			args: args{
				b: &volumeBuilder{
					mounts: map[container][]v1.VolumeMount{},
				},
			},
			expectLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &customContainerConfigFile{
				fileMountConfigs: tt.fields.fileMountConfigs,
				secretFiles:      tt.fields.secretFiles,
				podName:          tt.fields.podName,
			}
			v.configVolume(tt.args.b)
			if len(tt.args.b.mounts) != tt.expectLen {
				t.Errorf("expect %v but found %v", tt.expectLen, len(tt.args.b.mounts))
			}
		})
	}
}
