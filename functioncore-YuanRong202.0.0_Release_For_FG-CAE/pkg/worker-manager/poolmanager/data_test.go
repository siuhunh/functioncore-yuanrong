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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
)

// test for config volume
func Test_userFunctionCodeData_configVolume(t *testing.T) {
	type fields struct {
		deployDir string
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
			fields: fields{deployDir: "/opt/cloud/data"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "function-data",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}, {
					Name: "function-metrics",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}, {
					Name: "system-data",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "function-data",
						MountPath: "/opt/cloud/data",
					}, {
						Name:      "function-metrics",
						MountPath: "/home/sn/metrics",
					}, {
						Name:      "system-data",
						MountPath: "/runtime/var-tmp",
					}},
					containerRuntime: {{
						Name:      "function-data",
						MountPath: "/opt/cloud/data",
					}, {
						Name:      "function-metrics",
						MountPath: "/home/sn/metrics",
					}, {
						Name:      "system-data",
						MountPath: "/var/tmp",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &userFunctionCodeData{
				deployDir:  tt.fields.deployDir,
				metricsDir: "/home/sn/metrics",
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config env
func Test_userFunctionCodeData_configEnv(t *testing.T) {
	type fields struct {
		deployDir string
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
			fields: fields{deployDir: "/opt/cloud/data"},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerRuntime: {{
						Name:  "DEPLOY_DIR",
						Value: "/opt/cloud/data",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &userFunctionCodeData{
				deployDir: tt.fields.deployDir,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_userFunctionTmpData_configVolume(t *testing.T) {
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
					Name: "user-datas",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "user-datas",
						MountPath: "/runtime/tmp/",
					}},
					containerRuntime: {{
						Name:      "user-datas",
						MountPath: "/tmp/",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &userFunctionTmpData{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_shareWorkerSecretData_configVolume(t *testing.T) {
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
					Name: "worker-secret",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorkerInit: {{
						Name:      "worker-secret",
						MountPath: "/home/sn/temp-resource",
					}},
					containerWorker: {{
						Name:      "worker-secret",
						MountPath: "/home/sn/resource",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &shareWorkerSecretData{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config env
func Test_shareWorkerSecretData_configEnv(t *testing.T) {
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
					containerWorker: {{
						Name:  "ResourcePath",
						Value: "/home/sn/resource",
					}, {
						Name:  "WORKER_KEY_PATH",
						Value: "/home/sn/resource",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &shareWorkerSecretData{}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

func Test_caeFunctionCodeData_configVolume(t *testing.T) {
	type fields struct {
		deployDir string
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
			fields: fields{deployDir: "/opt/cloud/data"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "function-data",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}, {
					Name: "function-metrics",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}, {
					Name: "system-data",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "function-data",
						MountPath: "/opt/cloud/data",
					}, {
						Name:      "function-metrics",
						MountPath: "/home/sn/metrics",
					}, {
						Name:      "system-data",
						MountPath: "/runtime/var-tmp",
					}},
					containerRuntime: {{
						Name:      "function-metrics",
						MountPath: "/home/sn/metrics",
					}, {
						Name:      "system-data",
						MountPath: "/var/tmp",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &caeFunctionCodeData{
				deployDir:  tt.fields.deployDir,
				metricsDir: "/home/sn/metrics",
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

func Test_datasystemPath(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	hostPathType := v1.HostPathDirectoryOrCreate
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "test for uds path",
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "datasystem-uds",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/home/uds",
							Type: &hostPathType,
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerRuntime: {{
						Name:      "datasystem-uds",
						MountPath: "/home/uds",
						ReadOnly:  true,
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &datasystemPath{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}
