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
package vpcmanager

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func Test_buildEnv(t *testing.T) {
	type args struct {
		options *CreateVpcTriggerInstruct
	}
	var a args
	podType := "podType"
	a.options = &CreateVpcTriggerInstruct{
		Image:           "image",
		SubnetId:        "subnetId",
		Namespace:       "default",
		PulltriggerPort: "28937",
	}
	var vpcNatConfByte []byte = nil
	Envs := make([]corev1.EnvVar, 0)

	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_SSL_MODE", Value: "0"})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_SSL_VERIFY_CLIENT", Value: "1"})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_HOST_IP", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1", FieldPath: "status.hostIP"}}})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1", FieldPath: "metadata.name"}}})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_POD_IP", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1", FieldPath: "status.podIP"}}})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_NAMESPACE", Value: "default"})
	Envs = append(Envs, corev1.EnvVar{
		Name: "POD_TYPE", Value: "podType"})
	Envs = append(Envs, corev1.EnvVar{
		Name: "REGION_ID", Value: os.Getenv("REGION_ID")})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_SK", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "cff-runtime-secret"}, Key: "serverless_skey"}}})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_AK", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "cff-runtime-secret"}, Key: "serverless_akey"}}})
	Envs = append(Envs, corev1.EnvVar{
		Name: "TENANTLB_ADDRESS", Value: os.Getenv("TENANT_TRIGGER_ADDRESS")})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_WEBSOCKET_HOST", Value: fmt.Sprintf("%s:%s", os.Getenv("TENANT_TRIGGER_ADDRESS"), "28937")})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_SUBNETID", Value: "subnetId"})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_VPCNATCONF", Value: string([]byte{})})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_IMAGE_VERSION", Value: "image"})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_CLUSTER_ID", Value: "clusterId"})
	omsvcAddress := os.Getenv("SERVERLESS_OMSVC_ADDRESS")
	index := strings.LastIndex(omsvcAddress, ":")
	if index != -1 {
		Envs = append(Envs, corev1.EnvVar{
			Name:  "SERVERLESS_HOST",
			Value: omsvcAddress[:index],
		})
	}
	Envs = append(Envs, corev1.EnvVar{
		Name: "ENABLE_DATA_PLANE_RDISPATCHER", Value: os.Getenv("ENABLE_DATA_PLANE_RDISPATCHER")})
	tests := []struct {
		name string
		args args
		want []corev1.EnvVar
	}{
		{"case1", a, Envs},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildEnv(podType, tt.args.options, vpcNatConfByte); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_describeVolumes(t *testing.T) {
	secretMode := int32(defaultSecretMode)
	cipherVolume := corev1.Volume{
		Name: "cipher",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-cipher-ssl-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "root.key",
						Path: "root.key",
						Mode: &secretMode,
					},
					{
						Key:  "common_shared.key",
						Path: "common_shared.key",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	sslVolume := corev1.Volume{
		Name: "ssl",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-cipher-ssl-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "ca.crt",
						Path: "trust.cer",
						Mode: &secretMode,
					},
					{
						Key:  "tls.crt",
						Path: "server.cer",
						Mode: &secretMode,
					},
					{
						Key:  "tls.key.pwd",
						Path: "server_key.pem",
						Mode: &secretMode,
					},
					{
						Key:  "pwd",
						Path: "cert_pwd",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	kafkaVolume := corev1.Volume{
		Name: "kafkacert",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-kafka-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "tls.crt",
						Path: "kafka.cer",
						Mode: &secretMode,
					},
					{
						Key:  "tls.key",
						Path: "kafka.key",
						Mode: &secretMode,
					},
					{
						Key:  "tls.ca",
						Path: "kafka.ca",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	authVolume := corev1.Volume{
		Name: "auth",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-common-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "internal_sign_ak",
						Path: "internal_sign_ak",
						Mode: &secretMode,
					},
					{
						Key:  "internal_sign_sk",
						Path: "internal_sign_sk",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	redisVolume := corev1.Volume{
		Name: "redisdb",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-rdb-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "appversion",
						Path: "appversion.json",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	etcdVolume := corev1.Volume{
		Name: "etcd",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-etcd-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "appversion",
						Path: "appversion.json",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	configVolume := corev1.Volume{
		Name: "pulltriggerconfig",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-vpcpulltrigger-config",
				Items: []corev1.KeyToPath{
					{
						Key:  "appversion",
						Path: "appversion.json",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	caVolume := corev1.Volume{
		Name: caName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: caSecretName,
				Items: []corev1.KeyToPath{
					{
						Key:  "ca.pem",
						Path: "ca.pem",
						Mode: &secretMode,
					},
					{
						Key:  "cert.pem",
						Path: "cert.pem",
						Mode: &secretMode,
					},
					{
						Key:  "key.pem",
						Path: "key.pem",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	a := []corev1.Volume{
		{
			Name: "localtime",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc/localtime",
				}},
		},
		cipherVolume,
		sslVolume,
		kafkaVolume,
		authVolume,
		etcdVolume,
		redisVolume,
		configVolume,
		caVolume,
		{
			Name: "etc",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: "vpcpulltrigger",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/paas/sys/log/cff/functiongraph/vpcpulltrigger",
				}},
		},
	}
	a2 := []corev1.Volume{
		{
			Name: "localtime",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc/localtime",
				}},
		},
		cipherVolume,
		sslVolume,
		kafkaVolume,
		authVolume,
		etcdVolume,
		redisVolume,
		configVolume,
		caVolume,
		{
			Name: "etc",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: "vpcpulltrigger",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/functiongraph/vpcpulltrigger",
				}},
		},
	}
	tests := []struct {
		name   string
		setEnv func()
		want   []corev1.Volume
	}{
		{"case1", func() {
			os.Setenv("PULLTRIGGER_LOG_PATH", "")
		}, a},
		{"case2", func() {
			os.Setenv("PULLTRIGGER_LOG_PATH", "/var/functiongraph/vpcpulltrigger")
		}, a2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setEnv()
			if got := describeVolumes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("describeVolumes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_genImagePullSecrets(t *testing.T) {
	a := []corev1.LocalObjectReference{
		{
			Name: "default-secret",
		},
	}
	tests := []struct {
		name string
		want []corev1.LocalObjectReference
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genImagePullSecrets(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("genImagePullSecrets() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_generateSecretVolumes(t *testing.T) {
	secretMode := int32(defaultSecretMode)
	cipherVolume := corev1.Volume{
		Name: "cipher",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-cipher-ssl-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "root.key",
						Path: "root.key",
						Mode: &secretMode,
					},
					{
						Key:  "common_shared.key",
						Path: "common_shared.key",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	sslVolume := corev1.Volume{
		Name: "ssl",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-cipher-ssl-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "ca.crt",
						Path: "trust.cer",
						Mode: &secretMode,
					},
					{
						Key:  "tls.crt",
						Path: "server.cer",
						Mode: &secretMode,
					},
					{
						Key:  "tls.key.pwd",
						Path: "server_key.pem",
						Mode: &secretMode,
					},
					{
						Key:  "pwd",
						Path: "cert_pwd",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	kafkaVolume := corev1.Volume{
		Name: "kafkacert",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-kafka-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "tls.crt",
						Path: "kafka.cer",
						Mode: &secretMode,
					},
					{
						Key:  "tls.key",
						Path: "kafka.key",
						Mode: &secretMode,
					},
					{
						Key:  "tls.ca",
						Path: "kafka.ca",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	authVolume := corev1.Volume{
		Name: "auth",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-common-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "internal_sign_ak",
						Path: "internal_sign_ak",
						Mode: &secretMode,
					},
					{
						Key:  "internal_sign_sk",
						Path: "internal_sign_sk",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	etcdVolume := corev1.Volume{
		Name: "etcd",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-etcd-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "appversion",
						Path: "appversion.json",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	redisVolume := corev1.Volume{
		Name: "redisdb",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cff-rdb-secret",
				Items: []corev1.KeyToPath{
					{
						Key:  "appversion",
						Path: "appversion.json",
						Mode: &secretMode,
					},
				},
			},
		},
	}
	tests := []struct {
		name  string
		want  corev1.Volume
		want1 corev1.Volume
		want2 corev1.Volume
		want3 corev1.Volume
		want4 corev1.Volume
		want5 corev1.Volume
	}{
		{"case1", cipherVolume, sslVolume, kafkaVolume, authVolume, etcdVolume, redisVolume},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volumeMap := generateSecretVolumes()
			got := volumeMap[cipherVolumeType]
			got1 := volumeMap[sslVolumeType]
			got2 := volumeMap[kafkaVolumeType]
			got3 := volumeMap[authVolumeType]
			got4 := volumeMap[etcdVolumeType]
			got5 := volumeMap[redisVolumeType]
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateSecretVolumes() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("generateSecretVolumes() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("generateSecretVolumes() got2 = %v, want %v", got2, tt.want2)
			}
			if !reflect.DeepEqual(got3, tt.want3) {
				t.Errorf("generateSecretVolumes() got3 = %v, want %v", got3, tt.want3)
			}
			if !reflect.DeepEqual(got4, tt.want4) {
				t.Errorf("generateSecretVolumes() got4 = %v, want %v", got4, tt.want4)
			}
			if !reflect.DeepEqual(got5, tt.want5) {
				t.Errorf("generateSecretVolumes() got4 = %v, want %v", got5, tt.want5)
			}
		})
	}
}

func Test_getLifecycle(t *testing.T) {
	a := &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sleep", "3"},
			},
		},
	}
	tests := []struct {
		name string
		want *corev1.Lifecycle
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getLifecycle(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLifecycle() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getLivenessProbe(t *testing.T) {
	a := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/v1.0/healthcheck",
				Port: intstr.IntOrString{
					IntVal: pullTriggerHealthCheckPort,
				},
				Scheme: "HTTPS",
			},
		},
		FailureThreshold:    pullTriggerFailureThreshold,
		InitialDelaySeconds: pullTriggerInitialDelaySeconds,
		PeriodSeconds:       pullTriggerPeriodSeconds,
		TimeoutSeconds:      pullTriggerTimeoutSeconds,
	}
	tests := []struct {
		name string
		want *corev1.Probe
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getLivenessProbe(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLivenessProbe() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getResources(t *testing.T) {
	type args struct {
		cpu int64
		mem int64
	}
	var a args
	a.mem = 10
	a.cpu = 10
	b := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 10)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 10)),
		},
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 0)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 0)),
		},
	}
	var c args
	c.mem = 0
	c.mem = 0
	d := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 500)),
		},
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 0)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 0)),
		},
	}
	tests := []struct {
		name string
		args args
		want corev1.ResourceRequirements
	}{
		{"case1", a, b},
		{"case2", c, d},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getResources(tt.args.cpu, tt.args.mem); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getResources() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSecurityContext(t *testing.T) {
	a := &corev1.SecurityContext{
		Capabilities: &corev1.Capabilities{
			Add: []corev1.Capability{
				"NET_ADMIN",
				"NET_RAW",
				"DAC_OVERRIDE",
				"SETGID",
				"SETUID",
				"CHOWN",
				"FOWNER",
				"KILL",
			},
			Drop: []corev1.Capability{
				"ALL",
			},
		},
	}
	tests := []struct {
		name string
		want *corev1.SecurityContext
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSecurityContext(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSecurityContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getVolumeMount(t *testing.T) {
	a := []corev1.VolumeMount{
		{
			Name:      "etc",
			MountPath: "/opt/CFF/etc",
		},
		{
			Name:      "localtime",
			MountPath: "/etc/localtime",
			ReadOnly:  true,
		},
		{
			Name:      "vpcpulltrigger",
			MountPath: "/var/log/CFF/vpcpulltrigger",
		},
	}
	tests := []struct {
		name string
		want []corev1.VolumeMount
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getVolumeMount(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getVolumeMount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMakePullTriggerDeploy(t *testing.T) {
	convey.Convey("MakePullTriggerDeploy", t, func() {
		triggerInfo := &CreateVpcTriggerInstruct{
			PodName: "podName",
		}
		vpcNatConfByte := []byte("test")
		minScale := int32(1)
		deploy := MakePullTriggerDeploy(triggerInfo, vpcNatConfByte, minScale)
		convey.So(deploy.ObjectMeta.Name, convey.ShouldEqual, "podName")
	})
}

func Test_getInitSecurityContext(t *testing.T) {
	user := int64(0)
	a := &corev1.SecurityContext{
		RunAsUser: &user,
	}
	tests := []struct {
		name string
		want *corev1.SecurityContext
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getInitSecurityContext(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getInitSecurityContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getInitVolumeMount(t *testing.T) {
	a := []corev1.VolumeMount{
		{
			Name:      "etc",
			MountPath: "/tmp/CFF/etc",
		},
		{
			Name:      "cipher",
			MountPath: "/opt/CFF/etc/cipher",
		},
		{
			Name:      "ssl",
			MountPath: "/opt/CFF/etc/ssl",
		},
		{
			Name:      "kafkacert",
			MountPath: "/opt/CFF/etc/kafka",
		},
		{
			Name:      "auth",
			MountPath: "/opt/CFF/etc/auth",
		},
		{
			Name:      "etcd",
			MountPath: "/opt/CFF/etc/etcd",
		},
		{
			Name:      "vpcpulltrigger",
			MountPath: "/var/log/CFF/vpcpulltrigger",
		},
		{
			Name:      "redisdb",
			MountPath: "/opt/CFF/etc/redisdb",
		},
		{
			Name:      "pulltriggerconfig",
			MountPath: "/opt/CFF/etc/config",
		},
		{
			Name:      caName,
			MountPath: "/opt/CFF/etc/ca",
		},
	}
	tests := []struct {
		name string
		want []corev1.VolumeMount
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getInitVolumeMount(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getInitVolumeMount() = %v, want %v", got, tt.want)
			}
		})
	}
}
