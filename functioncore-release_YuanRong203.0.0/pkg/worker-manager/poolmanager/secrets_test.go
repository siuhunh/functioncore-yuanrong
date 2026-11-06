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

// test fot config volume
func Test_localAuthSecret_configVolume(t *testing.T) {
	type fields struct {
		shouldMountRuntime bool
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
			name:   "case_01_not_mount_runtime",
			fields: fields{false},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "localauth",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							DefaultMode: &readOnlyVolumeMode,
							SecretName:  "local-secret",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorkerInit: {{
						Name:      "localauth",
						MountPath: "/home/sn/resource/cipher",
					}},
				},
			},
		},
		{
			name:   "case_02_mount_runtime",
			fields: fields{true},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "localauth",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							DefaultMode: &readOnlyVolumeMode,
							SecretName:  "local-secret",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorkerInit: {{
						Name:      "localauth",
						MountPath: "/home/sn/resource/cipher",
					}},
					containerRuntime: {{
						Name:      "localauth",
						MountPath: "/home/sn/resource/cipher",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &localAuthSecret{
				shouldMountRuntime: tt.fields.shouldMountRuntime,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test fot config env
func Test_localAuthSecret_configEnv(t *testing.T) {
	type fields struct {
		shouldMountRuntime bool
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
			fields: fields{false},
			args: args{
				b: &envBuilder{
					envs: map[container][]v1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]v1.EnvVar{
					containerWorker: {{
						Name:  "PAAS_CRYPTO_PATH",
						Value: "/home/sn/resource/cipher",
					}},
					containerRuntime: {{
						Name:  "PAAS_CRYPTO_PATH",
						Value: "/home/sn/resource/cipher",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &localAuthSecret{
				shouldMountRuntime: tt.fields.shouldMountRuntime,
			}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test fot config volume
func Test_tempWorkerKeySecret_configVolume(t *testing.T) {
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
					Name: "temp-worker-key",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							DefaultMode: &readOnlyVolumeMode,
							SecretName:  "worker-key",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorkerInit: {{
						Name:      "temp-worker-key",
						MountPath: "/home/sn/resource",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &tempWorkerKeySecret{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test decoy config volume
func Test_DecoySecret_configVolume(t *testing.T) {
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
					Name: "decoy-file",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName:  "decoy-secret",
							DefaultMode: &readOnlyVolumeMode,
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerRuntime: {{
						Name:      "decoy-file",
						MountPath: decoyMountPath,
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &decoySecret{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test fot config volume
func Test_caSecret_configVolume(t *testing.T) {
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
			fields: fields{"/opt/cert"},
			args: args{
				b: &volumeBuilder{
					volumes: []v1.Volume{},
					mounts:  map[container][]v1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				volumes: []v1.Volume{{
					Name: "ca",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							DefaultMode: &readOnlyVolumeMode,
							SecretName:  "ca-secret",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorker: {{
						Name:      "ca",
						MountPath: "/opt/cert",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &caSecret{
				mountPath: tt.fields.mountPath,
			}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test fot config volume
func Test_tlsSecret_configVolume(t *testing.T) {
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
					Name: "https",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							DefaultMode: &readOnlyVolumeMode,
							SecretName:  "https-certs",
						},
					},
				}},
				mounts: map[container][]v1.VolumeMount{
					containerWorkerInit: {{
						Name:      "https",
						MountPath: "/home/sn/resource/https",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &tlsSecret{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test fot config env
func Test_tlsSecret_configEnv(t *testing.T) {
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
						Name:  "SSL_ROOT",
						Value: "/home/sn/resource/https",
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &tlsSecret{}
			v.configEnv(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configEnv() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}

// test fot config volume
func Test_authSecret_configVolume(t *testing.T) {
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
				volumes: []v1.Volume{
					{
						Name: "apple",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: "worker-config"},
								DefaultMode:          &readOnlyVolumeMode,
								Items:                []v1.KeyToPath{{Key: "a.txt", Path: "a.txt"}},
							},
						},
					},
					{
						Name: "boy",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: "worker-config"},
								DefaultMode:          &readOnlyVolumeMode,
								Items:                []v1.KeyToPath{{Key: "b.txt", Path: "b.txt"}},
							},
						},
					},
					{
						Name: "cat",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: "worker-config"},
								DefaultMode:          &readOnlyVolumeMode,
								Items:                []v1.KeyToPath{{Key: "c.txt", Path: "c.txt"}},
							},
						},
					},
					{
						Name: "dog",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: "worker-config"},
								DefaultMode:          &readOnlyVolumeMode,
								Items:                []v1.KeyToPath{{Key: "d.txt", Path: "d.txt"}},
							},
						},
					},
					{
						Name: "wdo",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: "worker-config"},
								DefaultMode:          &readOnlyVolumeMode,
								Items:                []v1.KeyToPath{{Key: "wdo.json", Path: "wdo.json"}},
							},
						},
					},
				},
				mounts: map[container][]v1.VolumeMount{
					containerWorkerInit: {
						{
							Name:      "apple",
							MountPath: "/home/sn/resource/rdo/v1/apple",
						},
						{
							Name:      "boy",
							MountPath: "/home/sn/resource/rdo/v1/boy",
						},
						{
							Name:      "cat",
							MountPath: "/home/sn/resource/rdo/v1/cat",
						},
						{
							Name:      "dog",
							MountPath: "/home/sn/resource/rdo/v1/dog",
						},
						{
							Name:      "wdo",
							MountPath: "/home/sn/resource/wdo",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &authSecret{}
			v.configVolume(tt.args.b)
			if !reflect.DeepEqual(tt.args.b, tt.want) {
				t.Errorf("configVolume() = %v, want %v", tt.args.b, tt.want)
			}
		})
	}
}
