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

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

// test for config volume
func Test_runtimeLog_configVolume(t *testing.T) {
	type fields struct {
		mountPath string
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
			fields: fields{mountPath: "/opt/cloud/logs"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "runtimelog",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerRuntime: {{
						Name:      "runtimelog",
						MountPath: "/opt/cloud/logs",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &runtimeLog{
				mountPath: tt.fields.mountPath,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_varLog_configVolume(t *testing.T) {
	type fields struct {
		mountPath string
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
			fields: fields{mountPath: "/var/log"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "varlog",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "varlog",
						MountPath: "/var/log",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &varLog{
				mountPath: tt.fields.mountPath,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_fusionSTDLog_configVolume(t *testing.T) {
	hostPathType := v1.HostPathDirectoryOrCreate
	type fields struct {
		hostPath  string
		mountPath string
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
			fields: fields{hostPath: "/opt/std", mountPath: "/tmp/std"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "stdlogs",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/opt/std",
							Type: &hostPathType,
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "stdlogs",
						MountPath: "/tmp/std",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &fusionSTDLog{
				hostPath:  tt.fields.hostPath,
				mountPath: tt.fields.mountPath,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test for config volume
func Test_fusionServiceLog_configVolume(t *testing.T) {
	hostPathType := v1.HostPathDirectoryOrCreate
	type fields struct {
		hostPath            string
		workerMountPath     string
		runtimeMgrMountPath string
		runtimeLogMountPath string
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
			fields: fields{"/opt/cloud/service", "/opt/worker", "/opt/runtime/mgr", "/opt/runtime/log"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "servicelogs",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/opt/cloud/service",
							Type: &hostPathType,
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:        "servicelogs",
						MountPath:   "/opt/worker",
						SubPathExpr: "$(PodName)",
					}},
					containerRuntime: {{
						Name:        "servicelogs",
						MountPath:   "/opt/runtime/mgr",
						SubPathExpr: "$(PodName)",
					}, {
						Name:        "servicelogs",
						MountPath:   "/opt/runtime/log",
						SubPathExpr: "$(PodName)/runtime",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &fusionServiceLog{
				hostPath:            tt.fields.hostPath,
				workerMountPath:     tt.fields.workerMountPath,
				runtimeMgrMountPath: tt.fields.runtimeMgrMountPath,
				runtimeLogMountPath: tt.fields.runtimeLogMountPath,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

func Test_socketFile_configVolume(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test001",
			args: args{
				&volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &socketFile{}
			s.configVolume(tt.args.b)
			assert.Equal(t, len(tt.args.b.volumes), 1)
			assert.Equal(t, len(tt.args.b.mounts), 2)
		})
	}
}

func Test_caePoolWorkerRuntimeShareConfig(t *testing.T) {
	caeShareCfg := &caePoolWorkerRuntimeShareConfig{}
	vb := &volumeBuilder{
		mounts: make(map[container][]v1.VolumeMount),
	}
	caeShareCfg.configVolume(vb)
	assert.Equal(t, 1, len(vb.volumes))
	assert.Equal(t, 2, len(vb.mounts))
}
