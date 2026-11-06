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
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"functioncore/pkg/common/logger/log"
)

// CreateVpcTriggerInstruct define create VpcTrigger instruct
type CreateVpcTriggerInstruct struct {
	PulltriggerPort string `json:"pulltrigger_port"`
	PodName         string `json:"pod_name"`
	MemoryOption    string `json:"memory_option"`
	CpuOption       string `json:"cpu_option"`
	Image           string `json:"image"`
	DomainId        string `json:"domain_id"`
	Namespace       string `json:"namespace"`
	VpcName         string `json:"vpc_name"`
	VpcId           string `json:"vpc_id"`
	SubnetName      string `json:"subnet_name"`
	SubnetId        string `json:"subnet_id"`
	TenantCidr      string `json:"tenant_cidr"`
	HostVmCidr      string `json:"host_vm_cidr"`
	ContainerCidr   string `json:"container_cidr"`
	Gateway         string `json:"gateway"`
	AppXrole        string `json:"app_xrole,omitempty"`
	Xrole           string `json:"xrole,omitempty"`
}

const (
	base10                         = 10
	bitSize                        = 64
	defaultPullTriggerCPU          = 500
	defaultPullTriggerMem          = 500
	defaultSecretMode              = 0400
	pullTriggerHealthCheckPort     = 28917
	pullTriggerFailureThreshold    = 3
	pullTriggerInitialDelaySeconds = 15
	pullTriggerPeriodSeconds       = 30
	pullTriggerTimeoutSeconds      = 3
	labelKey                       = "cff-type"
	labelValue                     = "serverless"
	podAntiAffinityWeight          = 100
)

var (
	omsvcAddress               = os.Getenv("SERVERLESS_OMSVC_ADDRESS")
	enableDataPlaneRdispatcher = os.Getenv("ENABLE_DATA_PLANE_RDISPATCHER")
	regionID                   = os.Getenv("REGION_ID")
	tenantTriggerAddress       = os.Getenv("TENANT_TRIGGER_ADDRESS")
)

// MakePullTriggerDeploy return vpc_pullTrigger deployment
func MakePullTriggerDeploy(triggerInfo *CreateVpcTriggerInstruct, vpcNatConfByte []byte,
	triggerReplicas int32) *appsv1.Deployment {
	labels := map[string]string{}
	labels[labelKey] = labelValue
	terminationGracePeriodSeconds := int64(corev1.DefaultTerminationGracePeriodSeconds)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   triggerInfo.PodName,
			Labels: labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &triggerReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   triggerInfo.PodName,
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets:              genImagePullSecrets(),
					RestartPolicy:                 "Always",
					Volumes:                       describeVolumes(),
					InitContainers:                describeInitContainers(triggerInfo),
					Containers:                    describeContainers(triggerInfo, vpcNatConfByte),
					AutomountServiceAccountToken:  new(bool),
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					Affinity:                      describeAffinity(labels, triggerInfo.SubnetId),
				},
			},
		},
	}
}

func genImagePullSecrets() []corev1.LocalObjectReference {
	return []corev1.LocalObjectReference{
		{
			Name: "default-secret",
		},
	}
}

func describeVolumes() []corev1.Volume {
	triggerLogPath := os.Getenv("PULLTRIGGER_LOG_PATH")
	volumeMap := generateSecretVolumes()
	if triggerLogPath == "" {
		triggerLogPath = "/var/paas/sys/log/cff/functiongraph/vpcpulltrigger"
	}
	return []corev1.Volume{
		{
			Name: "localtime",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc/localtime",
				}},
		},
		volumeMap[cipherVolumeType],
		volumeMap[sslVolumeType],
		// kafkaVolume Volume for vpc config
		volumeMap[kafkaVolumeType],
		volumeMap[authVolumeType],
		volumeMap[etcdVolumeType],
		volumeMap[redisVolumeType],
		volumeMap[configVolumeType],
		volumeMap[caVolumeType],
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
					Path: triggerLogPath,
				}},
		},
	}
}

func generateSecretVolumes() map[Type]corev1.Volume {
	volumeMap := make(map[Type]corev1.Volume)
	for _, volumeType := range getTypes() {
		volume := corev1.Volume{
			Name: volumeType.name(),
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: volumeType.secretName(),
					Items:      volumeType.keyToPath(),
				},
			},
		}
		volumeMap[volumeType] = volume
	}
	return volumeMap
}

func describeContainers(triggerInfo *CreateVpcTriggerInstruct, vpcNatConfByte []byte) []corev1.Container {
	cpu, err := strconv.ParseInt(triggerInfo.CpuOption, base10, bitSize)
	if err != nil {
		log.GetLogger().Warnf("cpu option convert to int failed with error %s", err.Error())
		cpu = defaultPullTriggerCPU
	}
	mem, err := strconv.ParseInt(triggerInfo.MemoryOption, base10, bitSize)
	if err != nil {
		log.GetLogger().Warnf("memory option convert to int failed with error %s", err.Error())
		mem = defaultPullTriggerMem
	}
	ContainerSpec := corev1.Container{
		Name:            "vpcpulltrigger",
		Image:           triggerInfo.Image,
		ImagePullPolicy: "IfNotPresent",
		Env:             buildEnv("ccevpc", triggerInfo, vpcNatConfByte),
		SecurityContext: getSecurityContext(),
		Lifecycle:       getLifecycle(),
		LivenessProbe:   getLivenessProbe(),
		Resources:       getResources(cpu, mem),
		VolumeMounts:    getVolumeMount(),
	}
	return []corev1.Container{
		ContainerSpec,
	}
}

func describeInitContainers(triggerInfo *CreateVpcTriggerInstruct) []corev1.Container {

	cpu, err := strconv.ParseInt(triggerInfo.CpuOption, base10, bitSize)
	if err != nil {
		log.GetLogger().Warnf("cpu option convert to int failed with error %s", err.Error())
		cpu = defaultPullTriggerCPU
	}
	mem, err := strconv.ParseInt(triggerInfo.MemoryOption, base10, bitSize)
	if err != nil {
		log.GetLogger().Warnf("memory option convert to int failed with error %s", err.Error())
		mem = defaultPullTriggerMem
	}
	initContainerSpec := corev1.Container{
		Args:                     []string{"cp -r /opt/CFF/etc /tmp/CFF/;find /tmp/CFF/ -type f  |xargs chmod 600;"},
		Command:                  []string{"/bin/sh", "-c"},
		Name:                     "cff-vpcpulltrigger-bootstrap",
		Image:                    triggerInfo.Image,
		ImagePullPolicy:          "IfNotPresent",
		SecurityContext:          getInitSecurityContext(),
		Resources:                getResources(cpu, mem),
		VolumeMounts:             getInitVolumeMount(),
		TerminationMessagePath:   "/dev/termination-log",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}
	return []corev1.Container{
		initContainerSpec,
	}
}

func describeAffinity(labels map[string]string, subnetID string) *corev1.Affinity {
	subnetNode := fmt.Sprintf("data-plane-pat-only-for-%s", subnetID)
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: podAntiAffinityWeight,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: labels,
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		},
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "node-role",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"data-plane", "data-plane-scaled", subnetNode},
							},
						},
					},
				},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{
					Weight: 50,
					Preference: corev1.NodeSelectorTerm{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "node-role",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{subnetNode},
							},
						},
					},
				},
			},
		},
	}
}

func buildEnv(podType string, triggerInfo *CreateVpcTriggerInstruct, vpcNatConfByte []byte) []corev1.EnvVar {
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
		Name: "SERVERLESS_NAMESPACE", Value: triggerInfo.Namespace})
	Envs = append(Envs, corev1.EnvVar{
		Name: "POD_TYPE", Value: podType})
	Envs = append(Envs, corev1.EnvVar{
		Name: "REGION_ID", Value: regionID})
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
		Name: "TENANTLB_ADDRESS", Value: tenantTriggerAddress})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_WEBSOCKET_HOST", Value: fmt.Sprintf("%s:%s",
			tenantTriggerAddress, triggerInfo.PulltriggerPort)})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_SUBNETID", Value: triggerInfo.SubnetId})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_VPCNATCONF", Value: string(vpcNatConfByte)})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_IMAGE_VERSION", Value: triggerInfo.Image})
	Envs = append(Envs, corev1.EnvVar{
		Name: "SERVERLESS_CLUSTER_ID", Value: "clusterId"})
	index := strings.LastIndex(omsvcAddress, ":")
	if index != -1 {
		Envs = append(Envs, corev1.EnvVar{
			Name:  "SERVERLESS_HOST",
			Value: omsvcAddress[:],
		})
	}
	Envs = append(Envs, corev1.EnvVar{
		Name: "ENABLE_DATA_PLANE_RDISPATCHER", Value: enableDataPlaneRdispatcher})
	return Envs
}

func getSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
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
}

func getInitSecurityContext() *corev1.SecurityContext {
	user := int64(0)
	return &corev1.SecurityContext{
		RunAsUser: &user,
	}
}

func getLifecycle() *corev1.Lifecycle {
	return &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sleep", "3"},
			}},
	}
}

func getLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
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
}

func getResources(cpu, mem int64) corev1.ResourceRequirements {
	if cpu == 0 || mem == 0 {
		cpu = defaultPullTriggerCPU
		mem = defaultPullTriggerMem
	}
	resourceReq := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", cpu)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", mem)),
		},
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 0)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 0)),
		},
	}
	return resourceReq
}

func getVolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
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
}

func getInitVolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
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
}
