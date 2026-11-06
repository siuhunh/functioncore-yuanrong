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
	"strconv"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"functioncore/pkg/common/constants"
	commonTypes "functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
)

// test for get runtime type
func Test_getRuntimeType(t *testing.T) {
	type args struct {
		language string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				language: "cpp11",
			},
			want: "cpp",
		},
		{
			name: "case_02_abnormal_scenario",
			args: args{
				language: "cplus",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getRuntimeType(tt.args.language); got != tt.want {
				t.Errorf("getRuntimeType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for calc worker limit mem
func Test_getWorkerLimitMemory(t *testing.T) {
	type args struct {
		runtimeMem       int64
		ephemeralStorage int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "case_01_mem_less_than_200",
			args: args{
				runtimeMem:       500,
				ephemeralStorage: 1,
			},
			want: 200,
		},
		{
			name: "case_02_ephemeralStorage_more_than_500",
			args: args{
				runtimeMem:       1500,
				ephemeralStorage: 1000,
			},
			want: 1800,
		},
		{
			name: "case_03_mem_more_than_900",
			args: args{
				runtimeMem:       10000,
				ephemeralStorage: 400,
			},
			want: 900,
		},
		{
			name: "case_04_other_scenario",
			args: args{
				runtimeMem:       4000,
				ephemeralStorage: 400,
			},
			want: 800,
		},
		{
			name: "case_05_large_ephemeralStorage_more_than_2048",
			args: args{
				runtimeMem:       4000,
				ephemeralStorage: 10240,
			},
			want: 3686,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWorkerLimitMemory(tt.args.runtimeMem, tt.args.ephemeralStorage); got != tt.want {
				t.Errorf("getWorkerLimitMemory() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test fot get grace period seconds
func Test_getGracePeriodSeconds(t *testing.T) {
	type args struct {
		gcConfig config.GcConfig
	}
	var args1 args
	args1.gcConfig = config.GcConfig{TerminationGracePeriodSeconds: 10}

	tests := []struct {
		name string
		args args
		want *int64
	}{
		{
			name: "case_01_period_seconds_zero",
			args: args{
				gcConfig: config.GcConfig{
					TerminationGracePeriodSeconds: 0,
				},
			},
			want: nil,
		},
		{
			name: "case_02_period_seconds_not_zero",
			args: args1,
			want: &args1.gcConfig.TerminationGracePeriodSeconds,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getGracePeriodSeconds(tt.args.gcConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getGracePeriodSeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for get OwnerReference
func Test_getThisOwnerReference(t *testing.T) {
	tt := true
	env := make(map[string]string, 2)
	env["POD_NAME"] = "pod_test_001"
	env["POD_UID"] = "uid_001"

	tests := []struct {
		name        string
		patchesFunc func() []*Patches
		want        []metav1.OwnerReference
	}{
		{
			name: "case_01_normal_scenario",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.Getenv, func(key string) string {
					return env[key]
				}))
				return patches
			},
			want: []metav1.OwnerReference{
				{
					APIVersion:         "apps/v1",
					BlockOwnerDeletion: &tt,
					Controller:         &tt,
					Kind:               "Pod",
					Name:               "pod_test_001",
					UID:                types.UID("uid_001"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getThisOwnerReference(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getThisOwnerReference() = %v, want %v", got, tt.want)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

// test for sanitize CAP
func Test_sanitizeCAP(t *testing.T) {
	type args struct {
		caps []string
	}
	tests := []struct {
		name string
		args args
		want []corev1.Capability
	}{
		{
			name: "case_01_no_repeat",
			args: args{
				caps: []string{"abc"},
			},
			want: []corev1.Capability{"ABC"},
		},
		{
			name: "case_02_part_repeat",
			args: args{
				caps: []string{"abc", "dD", "dD"},
			},
			want: []corev1.Capability{"ABC", "DD"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeCAP(tt.args.caps); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sanitizeCAP() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for get ImagePullSecrets
func Test_getImagePullSecrets(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.LocalObjectReference
	}{
		{
			name: "case_01_normal_scenario",
			want: []corev1.LocalObjectReference{
				{
					Name: "default-secret",
				},
				{
					Name: "myregcred",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getImagePullSecrets(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getImagePullSecrets() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for get worker env
func Test_getWorkerEnv(t *testing.T) {
	var workerLimitMemory = int64(10)
	defer ApplyGlobalVar(&streamHandlerHostCidr, "123").Reset()
	defer ApplyGlobalVar(&streamHandlerServicePort, "123").Reset()

	type args struct {
		workerLimitMemory int64
	}
	tests := []struct {
		name string
		args args
		want []corev1.EnvVar
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				workerLimitMemory: workerLimitMemory,
			},
			want: []corev1.EnvVar{
				{
					Name:  constants.ResourceLimitsMemory,
					Value: strconv.FormatInt(workerLimitMemory<<mbShift, decimal),
				},
				{
					Name:  constants.FunctionLoadTimeoutEnvKey,
					Value: strconv.FormatInt(loadFunctionTimeoutInSeconds, decimal),
				},
				{
					Name:  constants.StreamHandlerHostCidr,
					Value: "123",
				},
				{
					Name:  constants.StreamHandlerServicePort,
					Value: "123",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWorkerEnv(tt.args.workerLimitMemory); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getWorkerEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for get worker port
func Test_getWorkerPort(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ContainerPort
	}{
		{
			name: "case_01_normal_scenario",
			want: []corev1.ContainerPort{
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWorkerPort(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getWorkerPort() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for get worker health handler
func Test_getWorkerHealthHandler(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc func() []*Patches
		want        corev1.ProbeHandler
	}{
		{
			name: "case_01_http_scenario",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(config.EnableHTTPS, func() bool {
					return false
				}))
				return patches
			},
			want: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 8081,
					},
				},
			},
		},
		{
			name: "case_02_https_scenario",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(config.EnableHTTPS, func() bool {
					return true
				}))
				return patches
			},
			want: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 8081,
					},
					Scheme: "HTTPS",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getWorkerHealthHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getWorkerHealthHandler() = %v, want %v", got, tt.want)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

// test fot get worker LivenessProbe
func Test_getWorkerLivenessProbe(t *testing.T) {
	type args struct {
		livenessProbeFailureThreshold int32
		livenessProbePeriodSeconds    int32
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc func() []*Patches
		want        *corev1.Probe
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				livenessProbeFailureThreshold: 3,
				livenessProbePeriodSeconds:    10,
			},
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(getWorkerHealthHandler, func() corev1.ProbeHandler {
					return corev1.ProbeHandler{}
				}))
				return patches
			},
			want: &corev1.Probe{
				FailureThreshold:    3,
				InitialDelaySeconds: livenessProbeDelay, // probe delay detect 5s.
				PeriodSeconds:       10,
				TimeoutSeconds:      livenessProbeTimeout, // probe timeout 5s
				ProbeHandler:        corev1.ProbeHandler{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getWorkerLivenessProbe(tt.args.livenessProbeFailureThreshold, tt.args.livenessProbePeriodSeconds); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getWorkerLivenessProbe() = %v, want %v", got, tt.want)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

// test fot get worker ReadinessProbe
func Test_getWorkerReadinessProbe(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc func() []*Patches
		want        *corev1.Probe
	}{
		{
			name: "case_01_normal_scenario",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(getWorkerHealthHandler, func() corev1.ProbeHandler {
					return corev1.ProbeHandler{}
				}))
				return patches
			},
			want: &corev1.Probe{
				InitialDelaySeconds: readinessProbeDelay,  // probe delay detect 3s.
				PeriodSeconds:       readinessProbePeriod, // probe detect period 1s
				TimeoutSeconds:      readinessProbeTimeout,
				ProbeHandler:        corev1.ProbeHandler{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getWorkerReadinessProbe(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getWorkerReadinessProbe() = %v, want %v", got, tt.want)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

// test for get EnvVarSource
func Test_getEnvVarSource(t *testing.T) {
	type args struct {
		fieldPath string
	}
	tests := []struct {
		name string
		args args
		want *corev1.EnvVarSource
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				fieldPath: "spec.nodeName",
			},
			want: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getEnvVarSource(tt.args.fieldPath); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getEnvVarSource() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for build SecretVolume
func Test_buildVolumeOfSecretSource(t *testing.T) {
	type args struct {
		name       string
		secretName string
	}
	tests := []struct {
		name string
		args args
		want corev1.Volume
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				name:       "vol_001",
				secretName: "secret_001",
			},
			want: corev1.Volume{
				Name: "vol_001",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						DefaultMode: &readOnlyVolumeMode,
						SecretName:  "secret_001",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildVolumeOfSecretSource(tt.args.name, tt.args.secretName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildVolumeOfSecretSource() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for build Volume
func Test_buildVolume(t *testing.T) {
	type args struct {
		name string
		key  string
		path string
	}
	tests := []struct {
		name string
		args args
		want corev1.Volume
	}{
		{
			name: "case_01_normal_scenario",
			args: args{
				name: "vol_001",
				key:  "log",
				path: "/opt/cloud/logs",
			},
			want: corev1.Volume{
				Name: "vol_001",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "worker-config",
						},
						DefaultMode: &readOnlyVolumeMode,
						Items: []corev1.KeyToPath{
							{
								Key:  "log",
								Path: "/opt/cloud/logs",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildVolume(tt.args.name, tt.args.key, tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildVolume() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMaxIntAndMinInt(t *testing.T) {
	assert.Equal(t, MaxInt(0, 1), int32(1))
	assert.Equal(t, MaxInt(1, 0), int32(1))

	assert.Equal(t, MinInt(1, 0), int32(0))
	assert.Equal(t, MinInt(1, 0), int32(0))
}

func Test_getCustomWorkerEnv(t *testing.T) {
	type args struct {
		req               common.PodRequest
		networkController commonTypes.NetworkController
	}
	var a args
	patPodInfo := append([]commonTypes.NATConfigure{}, commonTypes.NATConfigure{ContainerCidr: "ContainerCidr"})
	endpointTalentPatPodInfo := append([]commonTypes.NATConfigure{}, commonTypes.NATConfigure{ContainerCidr: "ContainerCidr"})
	publicNetworkPatPodInfo := append([]commonTypes.NATConfigure{}, commonTypes.NATConfigure{ContainerCidr: "ContainerCidr"})
	funcMount := append([]commonTypes.FuncMount{}, commonTypes.FuncMount{MountType: "MountType"})
	req := common.PodRequest{
		PatPodInfo:               patPodInfo,
		EndpointTalentPatPodInfo: endpointTalentPatPodInfo,
		PublicNetworkPatPodInfo:  publicNetworkPatPodInfo,
		RouteInfo:                commonTypes.RouteInfoData{},
		FuncMountConfig:          commonTypes.FuncMountConfig{FuncMounts: funcMount},
		SvcCidr:                  "",
	}
	a.req = req
	tests := []struct {
		name        string
		args        args
		patchesFunc func() []*Patches
		want        []corev1.EnvVar
	}{
		{
			name: "case_01_normal_scenario",
			args: a,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
					return &config.K8sCidr{}
				}))
				return patches
			},
			want: getMockCustomWorkerEnvResult(),
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getCustomWorkerEnv(tt.args.req), "getCustomWorkerEnv(%v)", tt.args.req)
		})
		for _, v := range patches {
			v.Reset()
		}
	}
}

func getMockCustomWorkerEnvResult() []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name:  constants.SvcCidr,
			Value: "",
		},
	}
	patPodInfo := append([]commonTypes.NATConfigure{}, commonTypes.NATConfigure{ContainerCidr: "ContainerCidr"})
	val1, _ := json.Marshal(patPodInfo)
	envs = append(envs, corev1.EnvVar{
		Name:  constants.NATConfigures,
		Value: string(val1),
	})
	endpointTalentPatPodInfo := append([]commonTypes.NATConfigure{}, commonTypes.NATConfigure{ContainerCidr: "ContainerCidr"})
	val2, _ := json.Marshal(endpointTalentPatPodInfo)
	envs = append(envs, corev1.EnvVar{
		Name:  constants.EndpointVpcNATConfigure,
		Value: string(val2),
	})
	publicPatPodInfo := append([]commonTypes.NATConfigure{}, commonTypes.NATConfigure{ContainerCidr: "ContainerCidr"})
	val3, _ := json.Marshal(publicPatPodInfo)
	envs = append(envs, corev1.EnvVar{
		Name:  constants.PublicNetworkVpcNATConfigure,
		Value: string(val3),
	})
	val4, _ := json.Marshal(commonTypes.RouteInfoData{})
	envs = append(envs, corev1.EnvVar{
		Name:  constants.RouteInfo,
		Value: string(val4),
	})
	funcMount := append([]commonTypes.FuncMount{}, commonTypes.FuncMount{MountType: "MountType"})
	val5, _ := json.Marshal(commonTypes.FuncMountConfig{FuncMounts: funcMount})
	envs = append(envs, corev1.EnvVar{
		Name:  constants.FuncMountConfig,
		Value: string(val5),
	})
	return envs
}

func TestFilterCheckpointCustom(t *testing.T) {
	type args struct {
		req common.PodRequest
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"case test FilterCheckpointCustom",
			args{
				common.PodRequest{
					SnapshotDeployment: &commonTypes.SnapshotDeploymentSpec{
						BucketID: "testbucket",
					},
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FilterCheckpointCustom(tt.args.req); got != tt.want {
				t.Errorf("FilterCheckpointCustom() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetSeccompProfile(t *testing.T) {
	t.Run("not enable", func(t *testing.T) {
		contain := &corev1.Container{SecurityContext: &corev1.SecurityContext{}}
		SetSeccompProfile(contain)
		if contain.SecurityContext.SeccompProfile != nil {
			t.Errorf("contain.SecurityContext.SeccompProfile should be nil")
		}
	})
	t.Run("enable", func(t *testing.T) {
		enableSeccompProfile = "true"
		defer func() { enableSeccompProfile = "" }()
		contain := &corev1.Container{SecurityContext: &corev1.SecurityContext{}}
		SetSeccompProfile(contain)
		profileName := "yuanrong-sec.json"
		assert.Equal(t, &corev1.SeccompProfile{
			Type:             corev1.SeccompProfileTypeLocalhost,
			LocalhostProfile: &profileName,
		}, contain.SecurityContext.SeccompProfile)
	})
}
