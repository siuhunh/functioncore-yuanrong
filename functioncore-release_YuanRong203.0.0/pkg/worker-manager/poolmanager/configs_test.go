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
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/utils"
	mockUtils "functioncore/test/utils"
)

func Test_podConfig_configVolume(t *testing.T) {
	type args struct {
		b *volumeBuilder
	}
	tests := []struct {
		name string
		args args
		want *volumeBuilder
	}{
		{
			name: "case1",
			args: args{
				&volumeBuilder{
					mounts: map[container][]corev1.VolumeMount{},
				},
			},
			want: &volumeBuilder{
				mounts: map[container][]corev1.VolumeMount{
					containerWorker: {
						corev1.VolumeMount{
							Name:      "pod-config",
							MountPath: "/home/sn/restart/",
						},
						corev1.VolumeMount{
							Name:      "has-worker-init",
							MountPath: "/home/sn/worker-init",
						},
					},
					containerWorkerInit: {
						corev1.VolumeMount{
							Name:      "has-worker-init",
							MountPath: "/home/sn/worker-init",
						},
					},
				},
				volumes: []corev1.Volume{
					{
						Name: "pod-config",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					},
					{
						Name: "has-worker-init",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &podConfig{}
			v.configVolume(tt.args.b)
		})
		if !reflect.DeepEqual(tt.args.b, tt.want) {
			t.Errorf("actuality = %v, want %v", tt.args.b, tt.want)
		}
	}
}

func Test_runtimeManagerConfig_configVolume(t *testing.T) {
	type args struct {
		b *envBuilder
	}
	cluserIDEnvKey, clusterIDEnvValue := "CLUSTER_ID", "mock-cluster-id"
	os.Setenv(cluserIDEnvKey, clusterIDEnvValue)
	snapshotStorageType := "mockSnapshotStorageType"
	tests := []struct {
		name string
		args args
		want *envBuilder
	}{
		{
			name: "case1",
			args: args{
				&envBuilder{
					envs: map[container][]corev1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]corev1.EnvVar{
					containerWorker: {
						corev1.EnvVar{
							Name:  "RUNTIME_MANAGER_CONFIG",
							Value: os.Getenv("RUNTIME_MANAGER_CONFIG"),
						},
						corev1.EnvVar{
							Name:  "SNAPSHOT_STORAGE_TYPE",
							Value: snapshotStorageType,
						},
						corev1.EnvVar{
							Name:  "WORKER_CONFIG",
							Value: os.Getenv("WORKER_CONFIG"),
						},
						corev1.EnvVar{
							Name:  "AUTO_GC_MEMORY_THRESHOLD",
							Value: os.Getenv("WORKER_AUTO_GC_MEMORY_THRESHOLD"),
						},
						corev1.EnvVar{
							Name:  cluserIDEnvKey,
							Value: clusterIDEnvValue,
						},
					},
					containerRuntime: {
						corev1.EnvVar{
							Name:  "RUNTIME_MANAGER_CONFIG",
							Value: os.Getenv("RUNTIME_MANAGER_CONFIG"),
						},
						corev1.EnvVar{
							Name:  cluserIDEnvKey,
							Value: clusterIDEnvValue,
						},
						corev1.EnvVar{
							Name:  constants.RuntimeLogPath,
							Value: utils.DefaultStringEnv(constants.RuntimeLogPath, "/home/snuser/log"),
						},
						corev1.EnvVar{
							Name:  constants.RuntimeLogLevel,
							Value: utils.DefaultStringEnv(constants.RuntimeLogLevel, "INFO"),
						},
						corev1.EnvVar{
							Name:  constants.RuntimeLogSingleSize,
							Value: utils.DefaultStringEnv(constants.RuntimeLogSingleSize, "100"),
						},
						corev1.EnvVar{
							Name:  constants.RuntimeLogThreshold,
							Value: utils.DefaultStringEnv(constants.RuntimeLogThreshold, "5"),
						},
						corev1.EnvVar{
							Name:  constants.RuntimeLogCheckInterval,
							Value: utils.DefaultStringEnv(constants.RuntimeLogCheckInterval, "30"),
						},
						corev1.EnvVar{
							Name:  constants.RuntimeTermTimeout,
							Value: "0",
						},
						corev1.EnvVar{
							Name:  constants.CheckpointRestoreOpts,
							Value: "abc",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		defaultClusterName = clusterIDEnvValue
		t.Run(tt.name, func(t *testing.T) {
			rc := &runtimeManagerConfig{restoreOpts: "abc", storageType: snapshotStorageType}
			rc.configEnv(tt.args.b)
			wc := &workerConfig{}
			wc.configEnv(tt.args.b)
		})
		if !reflect.DeepEqual(tt.args.b, tt.want) {
			t.Errorf("actuality = %v\n, want %v", tt.args.b, tt.want)
		}
		defaultClusterName = ""
	}
}

func Test_workerConfig_configEnv(t *testing.T) {
	p := gomonkey.ApplyFunc(os.Getenv, func(s string) string { return "abc" })
	defer p.Reset()

	type args struct {
		b *envBuilder
	}
	tests := []struct {
		name string
		args args
		want *envBuilder
	}{
		{
			name: "case1",
			args: args{
				&envBuilder{
					envs: map[container][]corev1.EnvVar{},
				},
			},
			want: &envBuilder{
				envs: map[container][]corev1.EnvVar{
					containerWorker: {
						{
							Name:  "WORKER_CONFIG",
							Value: "abc",
						},
						{
							Name:  "AUTO_GC_MEMORY_THRESHOLD",
							Value: "abc",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &workerConfig{}
			v.configEnv(tt.args.b)
		})
		if !reflect.DeepEqual(tt.args.b, tt.want) {
			t.Errorf("actuality = %v, want %v", tt.args.b, tt.want)
		}
	}
}

func Test_getConfigStrForCAE(t *testing.T) {
	tests := []struct {
		name                   string
		patchesFunc            mockUtils.PatchesFunc
		wantCAEConfContain     string
		wantRestoreConfContain string
	}{
		{
			name: "test succeed to marshal runtime manager config",
			patchesFunc: func() mockUtils.PatchSlice {
				var runtimeConfigMap = make(map[string]config.RuntimeConfig, 1)
				runtimeConfigMap["java8"] = config.RuntimeConfig{}
				runtimeConfigMap["java17"] = config.RuntimeConfig{}
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							RuntimeConfigs: runtimeConfigMap,
						}, nil
					}),
					gomonkey.ApplyFunc(utils.GetCAEJavaVersion, func() []string {
						return []string{"java17"}
					}),
				})
				return patches
			},
			wantCAEConfContain:     "java17",
			wantRestoreConfContain: "java8",
		},
		{
			name: "test failed to marshal restore config",
			patchesFunc: func() mockUtils.PatchSlice {
				var runtimeConfigMap = make(map[string]config.RuntimeConfig, 1)
				runtimeConfigMap["java8"] = config.RuntimeConfig{}
				runtimeConfigMap["java17"] = config.RuntimeConfig{}
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							RuntimeConfigs: runtimeConfigMap,
						}, nil
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("failed to marshal restore config")
					}),
				})
				return patches
			},
			wantCAEConfContain:     "",
			wantRestoreConfContain: "",
		},

		{
			name: "test failed to marshal cae config",
			patchesFunc: func() mockUtils.PatchSlice {
				var runtimeConfigMap = make(map[string]config.RuntimeConfig, 1)
				runtimeConfigMap["java8"] = config.RuntimeConfig{}
				runtimeConfigMap["java17"] = config.RuntimeConfig{}
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							RuntimeConfigs: runtimeConfigMap,
						}, nil
					}),
					gomonkey.ApplyFuncSeq(json.Marshal, []gomonkey.OutputCell{
						{Values: gomonkey.Params{[]byte("test"), nil}, Times: 0},
						{Values: gomonkey.Params{nil, errors.New("failed to marshal restore config")}, Times: 1},
					}),
				})
				return patches
			},
			wantCAEConfContain:     "",
			wantRestoreConfContain: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			caeConf, restoreConf := getConfigStrForCAE()
			assert.Contains(t, caeConf, tt.wantCAEConfContain)
			assert.Contains(t, restoreConf, tt.wantRestoreConfContain)
			patches.ResetAll()
		})
	}
}
