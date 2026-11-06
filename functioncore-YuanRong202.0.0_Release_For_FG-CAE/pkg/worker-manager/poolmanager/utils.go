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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snapshotmanager"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
)

const (
	workerUserGroup              = 1000
	workerFuncPort               = 8080
	workerAdminPort              = 8081
	mbShift                      = 20
	decimal                      = 10
	defaultRuntimeMemory         = 500
	workerLimitMemoryLow         = 200
	workerLimitMemoryHigh        = 900
	memFactor                    = 2
	loadFunctionTimeoutInSeconds = 60

	// RuntimeTypeCpp -
	RuntimeTypeCpp = "cpp"
	// RuntimeTypeCppBin -
	RuntimeTypeCppBin = "cppbin"
	// RuntimeTypeJava -
	RuntimeTypeJava = "java"
	// RuntimeTypeNodejs -
	RuntimeTypeNodejs = "nodejs"
	// RuntimeTypePython -
	RuntimeTypePython = "python"
	// RuntimeTypeCustom -
	RuntimeTypeCustom = "custom"
	// RuntimeTypeFusion -
	RuntimeTypeFusion = "fusion"
	// RuntimeTypeHTTP -
	RuntimeTypeHTTP = "http"
)

// these are constants of probe settings, of which unit is second
const (
	readinessProbeDelay      = 3
	readinessProbePeriod     = 1
	readinessProbeTimeout    = 5
	livenessProbeDelay       = 5
	livenessProbeTimeout     = 5
	customLivenessProbeDelay = 1
)

var (
	// RuntimeTypes map runtime language to type
	RuntimeTypes = map[string]string{
		"cpp11":            RuntimeTypeCpp,
		"cpp11-bin":        RuntimeTypeCppBin,
		"java1.8":          RuntimeTypeJava,
		"nodejs10.15.2":    RuntimeTypeNodejs,
		"python3.7":        RuntimeTypePython,
		"python3.8":        RuntimeTypePython,
		"custom-runtime":   RuntimeTypeCustom,
		"fusion":           RuntimeTypeFusion,
		ssdRuntimeName:     RuntimeTypeFusion,
		"vip":              RuntimeTypeFusion,
		"custom-container": RuntimeTypeHTTP,
	}

	readOnlyVolumeMode         int32 = 0440
	readOnlyVolumeModeWithUser int32 = 0444

	streamHandlerHostCidr    = os.Getenv(constants.StreamHandlerHostCidr)
	streamHandlerServicePort = os.Getenv(constants.StreamHandlerServicePort)

	enableSeccompProfile = os.Getenv("ENABLE_SECCOMP_PROFILE")
)

func buildVolume(name string, key string, path string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "worker-config",
				},
				DefaultMode: &readOnlyVolumeMode,
				Items: []corev1.KeyToPath{
					{
						Key:  key,
						Path: path,
					},
				},
			},
		},
	}
}

func buildVolumeOfSecretSource(name string, secretName string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				DefaultMode: &readOnlyVolumeMode,
				SecretName:  secretName,
			},
		},
	}
}

func getEnvVarSource(fieldPath string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		FieldRef: &corev1.ObjectFieldSelector{
			FieldPath: fieldPath,
		},
	}
}

func getRuntimeType(language string) string {
	runtimeType, ok := RuntimeTypes[language]
	if !ok {
		log.GetLogger().Errorf("unknown runtime type %s", language)
	}
	return runtimeType
}

func getWorkerReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		InitialDelaySeconds: readinessProbeDelay,  // probe delay detect 3s.
		PeriodSeconds:       readinessProbePeriod, // probe detect period 1s
		TimeoutSeconds:      readinessProbeTimeout,
		ProbeHandler:        getWorkerHealthHandler(),
	}
}

func getWorkerLivenessProbe(livenessProbeFailureThreshold, livenessProbePeriodSeconds int32) *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    livenessProbeFailureThreshold,
		InitialDelaySeconds: livenessProbeDelay, // probe delay detect 5s.
		PeriodSeconds:       livenessProbePeriodSeconds,
		TimeoutSeconds:      livenessProbeTimeout, // probe timeout 5s
		ProbeHandler:        getWorkerHealthHandler(),
	}
}

func getCustomWorkerLivenessProbe(livenessProbeFailureThreshold, livenessProbePeriodSeconds int32) *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    livenessProbeFailureThreshold,
		InitialDelaySeconds: customLivenessProbeDelay, // probe delay detect 1s.
		PeriodSeconds:       livenessProbePeriodSeconds,
		TimeoutSeconds:      customLivenessProbeDelay, // probe timeout 1s
		ProbeHandler:        getWorkerHealthHandler(),
	}
}

func getWorkerHealthHandler() corev1.ProbeHandler {
	handler := corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: "/healthz",
			Port: intstr.IntOrString{
				Type:   intstr.Int,
				IntVal: workerAdminPort,
			},
		},
	}
	if config.EnableHTTPS() {
		handler.HTTPGet.Scheme = "HTTPS"
	}
	return handler
}

func getWorkerPort() []corev1.ContainerPort {
	return []corev1.ContainerPort{
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
	}
}

func getWorkerEnv(workerLimitMemory int64) []corev1.EnvVar {
	workerLimitBytes := workerLimitMemory << mbShift
	envs := []corev1.EnvVar{
		{
			Name:  constants.ResourceLimitsMemory,
			Value: strconv.FormatInt(workerLimitBytes, decimal),
		},
		{
			Name:  constants.FunctionLoadTimeoutEnvKey,
			Value: strconv.FormatInt(loadFunctionTimeoutInSeconds, decimal),
		},
	}

	if len(streamHandlerHostCidr) != 0 && len(streamHandlerServicePort) != 0 {
		envs = append(envs, corev1.EnvVar{
			Name:  constants.StreamHandlerHostCidr,
			Value: streamHandlerHostCidr,
		})

		envs = append(envs, corev1.EnvVar{
			Name:  constants.StreamHandlerServicePort,
			Value: streamHandlerServicePort,
		})
	}
	return envs
}

func getPatEnv(req common.PodRequest) []corev1.EnvVar {
	var envs []corev1.EnvVar
	if len(req.PatPodInfo) != 0 {
		patPodInfo, err := json.Marshal(req.PatPodInfo)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal pat pod info: %s, traceID: %s", err.Error(), req.TraceID)
		}
		envs = append(envs, corev1.EnvVar{
			Name:  constants.NATConfigures,
			Value: string(patPodInfo),
		})
	}
	if len(req.EndpointTalentPatPodInfo) != 0 {
		endpointTalentPatPodInfo, err := json.Marshal(req.EndpointTalentPatPodInfo)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal endpoint talent pat pod info: %s, traceID: %s",
				err.Error(), req.TraceID)
		}
		envs = append(envs, corev1.EnvVar{
			Name:  constants.EndpointVpcNATConfigure,
			Value: string(endpointTalentPatPodInfo),
		})
	}
	if len(req.PublicNetworkPatPodInfo) != 0 {
		publicNetworkPatPodInfo, err := json.Marshal(req.PublicNetworkPatPodInfo)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal public network pat pod info: %s, traceID: %s",
				err.Error(), req.TraceID)
		}
		envs = append(envs, corev1.EnvVar{
			Name:  constants.PublicNetworkVpcNATConfigure,
			Value: string(publicNetworkPatPodInfo),
		})
	}
	return envs
}

func getCustomWorkerEnv(req common.PodRequest) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name:  constants.SvcCidr,
			Value: config.GetK8sCidr().SvcCidr,
		},
	}

	patEnv := getPatEnv(req)
	if len(patEnv) != 0 {
		envs = append(envs, patEnv...)
	}
	routeInfo := commontypes.RouteInfoData{
		ApigRouteEnable: req.RouteInfo.ApigRouteEnable,
		ApigRoutes:      req.RouteInfo.ApigRoutes,
	}
	routeInfoResult, err := json.Marshal(routeInfo)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal route info: %s, traceID: %s", err.Error(), req.TraceID)
	}
	envs = append(envs, corev1.EnvVar{
		Name:  constants.RouteInfo,
		Value: string(routeInfoResult),
	})
	if len(req.FuncMountConfig.FuncMounts) != 0 {
		funcMountConfig, err := json.Marshal(req.FuncMountConfig)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal func mount config: %s, traceID: %s", err.Error(), req.TraceID)
		}
		envs = append(envs, corev1.EnvVar{
			Name:  constants.FuncMountConfig,
			Value: string(funcMountConfig),
		})
	}
	return envs
}

func getWorkerLimitMemory(runtimeMem, ephemeralStorage int64) int64 {
	mem := (runtimeMem / defaultRuntimeMemory * workerLimitMemoryLow) / memFactor
	if mem < workerLimitMemoryLow {
		return workerLimitMemoryLow
	}
	// avoid calculating large worker limit memory because of large ephemeral storage
	if ephemeralStorage > maxEffectiveEphemeralStorage {
		ephemeralStorage = maxEffectiveEphemeralStorage
	}
	// set worker memory limit in proportion to ephemeralStorage
	if ephemeralStorage > defaultRequestEphemeralStorage {
		return ephemeralStorage * workerLimitMemoryHigh / defaultRequestEphemeralStorage
	}

	if mem > workerLimitMemoryHigh {
		return workerLimitMemoryHigh
	}

	return mem
}

func getImagePullSecrets() []corev1.LocalObjectReference {
	return []corev1.LocalObjectReference{
		{
			Name: "default-secret",
		},
		{
			Name: "myregcred",
		},
	}
}

func sanitizeCAP(caps []string) []corev1.Capability {
	seen := make(map[string]struct{}, len(caps))
	res := make([]corev1.Capability, 0, len(caps))
	for _, cap := range caps {
		cap = strings.ToUpper(strings.TrimSpace(cap))
		if _, exist := seen[cap]; !exist {
			seen[cap] = struct{}{}
			res = append(res, corev1.Capability(cap))
		}
	}
	return res
}

func getThisOwnerReference() []metav1.OwnerReference {
	t := true
	return []metav1.OwnerReference{
		{
			APIVersion:         "apps/v1",
			BlockOwnerDeletion: &t,
			Controller:         &t,
			Kind:               "Pod",
			Name:               os.Getenv("POD_NAME"),
			UID:                types.UID(os.Getenv("POD_UID")),
		},
	}
}

func getGracePeriodSeconds(gcConfig config.GcConfig) *int64 {
	if gcConfig.TerminationGracePeriodSeconds == 0 {
		// Using the default value in kubernetes.
		return nil
	}
	return &gcConfig.TerminationGracePeriodSeconds
}

func getRuntimeResource(pool *Pool) corev1.ResourceRequirements {
	runtimeResourceBuilder := newResourceRequirementsBuilder()
	runtimeResourceBuilder.setCPU(pool.RequestCPU, pool.LimitCPU)
	runtimeResourceBuilder.setMemory(pool.RequestMemory, pool.LimitMemory)
	runtimeResourceBuilder.setEphemeralStorage(defaultRequestEphemeralStorage, pool.EphemeralStorage)
	runtimeResourceBuilder.setCustomResource(pool.CustomResources)
	return runtimeResourceBuilder.build()
}

func getWorkerResource(pool *Pool) corev1.ResourceRequirements {
	workerLimitMemory := getWorkerLimitMemory(pool.LimitMemory, pool.EphemeralStorage)
	workerResourceBuilder := newResourceRequirementsBuilder()
	if pool.LimitMemory == work128 {
		workerResourceBuilder.setCPU(lowerWorkerRequestCPU, workerLimitCPU)
	} else if pool.LimitMemory < cpuRestoreThreshold {
		workerResourceBuilder.setCPU(workerRequestCPU, workerLimitCPU)
	} else {
		workerResourceBuilder.setCPU(workerRequestCPU, pool.WorkerLimitCPU)
	}
	workerResourceBuilder.setMemory(workerRequestMemory, workerLimitMemory)
	workerResourceBuilder.setEphemeralStorage(defaultRequestEphemeralStorage, pool.EphemeralStorage)
	workerResourceBuilder.setCustomResource(pool.CustomResources)
	return workerResourceBuilder.build()
}

// MinInt returns the smaller of x or y.
func MinInt(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

// MaxInt returns the larger of x or y.
func MaxInt(x, y int32) int32 {
	if x > y {
		return x
	}
	return y
}

// FilterCheckpointCustom -
func FilterCheckpointCustom(req common.PodRequest) bool {
	if !snapshotmanager.GetSnapshotManagerMode() {
		// WorkerManager Mode: create snapshots for the first time
		return req.SnapshotDeployment != nil &&
			req.SnapshotDeployment.BucketID != "" && req.SnapshotDeployment.SnapshotSize == 0
	}

	// SnapshotManager Mode: update the existing snapshots
	return req.SnapshotDeployment != nil && req.SnapshotDeployment.BucketID != ""
}

// SetSeccompProfile -
func SetSeccompProfile(container *corev1.Container) {
	if enableSeccompProfile == "true" {
		profileName := "yuanrong-sec.json"
		container.SecurityContext.SeccompProfile = &corev1.SeccompProfile{
			Type:             corev1.SeccompProfileTypeLocalhost,
			LocalhostProfile: &profileName,
		}
	}
}
