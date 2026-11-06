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
	"path"
	"strconv"

	v1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	logconf "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/preload"
)

const (
	firstLimitMemory     = 128
	secondLimitMemory    = 256
	hostAndPathSize      = 2
	workerLogKey         = "WORKER_LOG_CONFIG"
	debugAgentLogKey     = "DEBUG_AGENT_LOG_CONFIG"
	runtimeMgrLogKey     = "RUNTIMEMGR_LOG_CONFIG"
	workerCPULimitKey    = "WORKER_CPU_LIMIT"
	workerMemoryLimitKey = "WORKER_MEMORY_LIMIT"
)

var hcsMode = os.Getenv("HCS_MODE")

type cgroupFreezer struct{}

func (f *cgroupFreezer) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "cgroup-freezer",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/sys/fs/cgroup/freezer/kubepods/burstable",
			},
		},
	})

	mount := v1.VolumeMount{
		Name:        "cgroup-freezer",
		MountPath:   "/runtime/freezer",
		SubPathExpr: "pod$(POD_ID)",
	}
	b.addVolumeMount(containerWorker, mount)
}

type cgroupMemory struct{}

func (f *cgroupMemory) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "cgroup-memory",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/sys/fs/cgroup/memory/kubepods/burstable",
			},
		},
	})

	mount := v1.VolumeMount{
		Name:        "cgroup-memory",
		MountPath:   "/runtime/memory",
		SubPathExpr: "pod$(POD_ID)",
	}
	b.addVolumeMount(containerWorker, mount)
}

type timeZone struct{}

func (v *timeZone) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "time-zone",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/etc/localtime",
			},
		},
	})

	mount := v1.VolumeMount{
		Name:      "time-zone",
		MountPath: "/etc/localtime",
		ReadOnly:  true,
	}
	b.addVolumeMount(containerWorker, mount)
	b.addVolumeMount(containerRuntime, mount)
}

type hostPackage struct {
	path string
}

func (v *hostPackage) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "host-package-path",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: v.path,
			},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "host-package-path",
		MountPath: "/home/sn/pkg",
	})
}

type cidr struct{}

func (v *cidr) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorkerInit, v1.EnvVar{
		Name:  "SVC_CIDR",
		Value: config.GetK8sCidr().SvcCidr,
	})
	b.addEnvVar(containerWorkerInit, v1.EnvVar{
		Name:  "POD_CIDR",
		Value: config.GetK8sCidr().PodCidr,
	})
	b.addEnvVar(containerWorkerInit, v1.EnvVar{
		Name:  "HOST_CIDR",
		Value: config.GetK8sCidr().HostCidr,
	})
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "POD_CIDR",
		Value: config.GetK8sCidr().PodCidr,
	})
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "NETWORK_MODEL",
		Value: constants.GetNetworkModel(),
	})
}

type podInfo struct{}

func (v *podInfo) configEnv(b *envBuilder) {
	addPodIP(b)
	addHostIP(b)
	addPodName(b)
	addPodID(b)
	addPodNamespace(b)
	if utils.GetEnableHCS() {
		addHCSMode(b)
	}
}

func addPodIP(b *envBuilder) {
	envPodIP := v1.EnvVar{
		Name:      "POD_IP",
		ValueFrom: getEnvVarSource("status.podIP"),
	}
	b.addEnvVar(containerWorkerInit, envPodIP)
	b.addEnvVar(containerWorker, envPodIP)
	b.addEnvVar(containerAgent, envPodIP)
}

func addHostIP(b *envBuilder) {
	envHostIP := v1.EnvVar{
		Name:      "HOST_IP",
		ValueFrom: getEnvVarSource("status.hostIP"),
	}
	b.addEnvVar(containerWorkerInit, envHostIP)
	b.addEnvVar(containerWorker, envHostIP)
	b.addEnvVar(containerRuntime, envHostIP)
	b.addEnvVar(containerAgent, envHostIP)
}

func addPodName(b *envBuilder) {
	envPodName := v1.EnvVar{
		Name: "PodName",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "metadata.name",
			},
		},
	}
	b.addEnvVar(containerWorker, envPodName)
	b.addEnvVar(containerRuntime, envPodName)
	b.addEnvVar(containerAgent, envPodName)
}

func addPodID(b *envBuilder) {
	envPodID := v1.EnvVar{
		Name: "POD_ID",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "metadata.uid",
			},
		},
	}
	b.addEnvVar(containerWorker, envPodID)
	b.addEnvVar(containerRuntime, envPodID)
	b.addEnvVar(containerAgent, envPodID)
}

func addPodNamespace(b *envBuilder) {
	envPodNamespace := v1.EnvVar{
		Name: constants.PodNamespaceEnvKey,
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "metadata.namespace",
			},
		},
	}
	b.addEnvVar(containerWorker, envPodNamespace)
	b.addEnvVar(containerRuntime, envPodNamespace)
	b.addEnvVar(containerAgent, envPodNamespace)
}

func addHCSMode(b *envBuilder) {
	envHCSMode := v1.EnvVar{
		Name:  "HCS_MODE",
		Value: hcsMode,
	}
	b.addEnvVar(containerWorker, envHCSMode)
}

type localCache struct {
	enable bool
}

func (v *localCache) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorkerInit, v1.EnvVar{
		Name:  "ENABLE_LOCAL_CACHE",
		Value: strconv.FormatBool(v.enable),
	})
}

type cmNamespace struct {
	nameSpace string
}

func (n cmNamespace) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorkerInit, v1.EnvVar{
		Name:  constants.SystemNameSpaceEnvKey,
		Value: n.nameSpace,
	})
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  constants.SystemNameSpaceEnvKey,
		Value: n.nameSpace,
	})
}

type obs struct {
	enable bool
}

func (v *obs) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorkerInit, v1.EnvVar{
		Name:  "ENABLE_MINIO",
		Value: strconv.FormatBool(v.enable),
	})
}

type workerAuthCheck struct {
	enable bool
}

func (v *workerAuthCheck) configEnv(b *envBuilder) {
	var check string
	if v.enable {
		check = "CHECK"
	}
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "INIT_AUTH_CHECK",
		Value: check,
	})
}

type workerContentSignatureCheck struct {
	enable bool
}

func (v *workerContentSignatureCheck) configEnv(b *envBuilder) {
	var check string
	if v.enable {
		check = "CHECK"
	}
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "CONTENT_SIGNATURE_CHECK",
		Value: check,
	})
}

type funcBranch struct{}

func (v *funcBranch) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  constants.FuncBranchEnvKey,
		Value: os.Getenv(constants.FuncBranchEnvKey),
	})
}

type runtimeLanguage struct {
	language string
}

func (v *runtimeLanguage) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "RUNTIME_TYPE",
		Value: getRuntimeType(v.language),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  "RUNTIME_LANGUAGE",
		Value: v.language,
	})
}

type customContainerConfigFile struct {
	fileMountConfigs []*types.MountFile
	secretFiles      []*types.SecretFileMount
	podName          string
}

func (v *customContainerConfigFile) configVolume(b *volumeBuilder) {
	secretName := v.podName + ".regcred"
	volumeName := "custom-container-mount-file"
	items := make([]v1.KeyToPath, len(v.fileMountConfigs))
	for index, fileMountConfig := range v.fileMountConfigs {
		items[index] = v1.KeyToPath{
			Key:  path.Base(fileMountConfig.FilePath),
			Path: path.Base(fileMountConfig.FilePath),
		}
		b.addVolumeMount(containerRuntime, v1.VolumeMount{
			Name:      volumeName,
			MountPath: fileMountConfig.FilePath,
			SubPath:   path.Base(fileMountConfig.FilePath),
		})
	}
	secretFileItems := make([]v1.KeyToPath, 0, len(v.secretFiles))
	for _, secretFile := range v.secretFiles {
		if secretFile.FileMountPath == "" || secretFile.EncryptedFileContent == "" {
			log.GetLogger().Warnf("ignore to add volume mount when fileMountPath %s or encryptedFileContent %s "+
				"is empty for pod %s", secretFile.FileMountPath, secretFile.EncryptedFileContent, v.podName)
			continue
		}
		secretFileItems = append(secretFileItems, v1.KeyToPath{
			Key:  path.Base(secretFile.FileMountPath),
			Path: path.Base(secretFile.FileMountPath),
		})
		b.addVolumeMount(containerRuntime, v1.VolumeMount{
			Name:      volumeName,
			MountPath: secretFile.FileMountPath,
			SubPath:   path.Base(secretFile.FileMountPath),
		})
	}
	if len(secretFileItems) > 0 {
		items = append(items, secretFileItems...)
	}

	b.addVolume(v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				DefaultMode: &readOnlyVolumeModeWithUser,
				SecretName:  secretName,
				Items:       items,
			},
		},
	})
}

type trace struct {
	runtimeLimitMemory int64
}

func (v *trace) configEnv(b *envBuilder) {
	if v.runtimeLimitMemory == firstLimitMemory || v.runtimeLimitMemory == secondLimitMemory {
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  "TRACE_DISABLE",
			Value: "DISABLE",
		})
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  "TRACE_DISABLE",
			Value: "DISABLE",
		})
	}
}

type workerLogEnv struct{}

func (v *workerLogEnv) configEnv(b *envBuilder) {
	logConfigs, err := logconf.ExtractCoreInfoFromEnv(workerLogKey)
	if err != nil {
		log.GetLogger().Warnf("%s is invalid: %s", workerLogKey, err.Error())
	}
	logConfEnv, err := json.Marshal(logConfigs)
	if err != nil {
		log.GetLogger().Warnf("cannot marshal logConfEnv: %s", err.Error())
		return
	}
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  logconf.LogConfigKey,
		Value: string(logConfEnv),
	})
}

type debugAgentLogEnv struct{}

func (v *debugAgentLogEnv) configEnv(b *envBuilder) {
	logConfigs, err := logconf.ExtractCoreInfoFromEnv(debugAgentLogKey)
	if err != nil {
		log.GetLogger().Warnf("%s is invalid: %s", debugAgentLogKey, err.Error())
		return
	}
	logConfEnv, err := json.Marshal(logConfigs)
	if err != nil {
		log.GetLogger().Warnf("cannot marshal logConfEnv: %s", err.Error())
		return
	}
	b.addEnvVar(containerAgent, v1.EnvVar{
		Name:  logconf.LogConfigKey,
		Value: string(logConfEnv),
	})
}

type runtimeMgrLogEnv struct{}

func (v *runtimeMgrLogEnv) configEnv(b *envBuilder) {
	logConfigs, err := logconf.ExtractCoreInfoFromEnv(runtimeMgrLogKey)
	if err != nil {
		log.GetLogger().Warnf("%s is invalid: %s", runtimeMgrLogKey, err.Error())
	}
	logConfEnv, err := json.Marshal(logConfigs)
	if err != nil {
		log.GetLogger().Warnf("cannot marshal logConfEnv: %s", err.Error())
		return
	}
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  logconf.LogConfigKey,
		Value: string(logConfEnv),
	})
}

type dockerRootDir struct{}

func (v *dockerRootDir) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "docker-rootdir",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/var/lib/docker",
			},
		},
	})

	bidirectional := v1.MountPropagationBidirectional
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:             "docker-rootdir",
		MountPath:        "/var/lib/docker",
		MountPropagation: &bidirectional,
	})
}

type dockerSocket struct{}

func (v *dockerSocket) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "docker-socket",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/var/run/docker.sock",
			},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "docker-socket",
		MountPath: "/var/run/docker.sock",
	})
}

type containerdDir struct{}

func (v *containerdDir) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "containerd-dir",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/var/run/containerd",
			},
		},
	})

	bidirectional := v1.MountPropagationBidirectional
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:             "containerd-dir",
		MountPath:        "/var/run/containerd",
		MountPropagation: &bidirectional,
	})
}

type containerdLogsDir struct{}

func (v *containerdLogsDir) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "containerd-logsdir",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/var/lib/containerd",
			},
		},
	})

	bidirectional := v1.MountPropagationBidirectional
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:             "containerd-logsdir",
		MountPath:        "/var/lib/containerd",
		MountPropagation: &bidirectional,
	})
}

type presetDependencyConfig struct{}

func (v *presetDependencyConfig) configVolume(b *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	b.addVolume(v1.Volume{
		Name: "public-dependency",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/paas/dependencies",
				Type: &hostPathType,
			},
		},
	})
	depend := v1.VolumeMount{
		Name:      "public-dependency",
		MountPath: "/opt/function/dependencies",
		ReadOnly:  true,
	}
	b.addVolumeMount(containerWorker, depend)
	b.addVolumeMount(containerRuntime, depend)
	v.configPreloadVolume(b)
}

func (v *presetDependencyConfig) configPreloadVolume(b *volumeBuilder) {
	if !preload.IsEnable() {
		return
	}
	preloadHostPath := os.Getenv("PRELOAD_HOST_PATH")
	if preloadHostPath == "" {
		return
	}
	hostPathType := v1.HostPathDirectoryOrCreate
	b.addVolume(v1.Volume{
		Name: "preload-host",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: preloadHostPath,
				Type: &hostPathType,
			},
		},
	})
	bidirectional := v1.MountPropagationBidirectional
	preLoadMount := v1.VolumeMount{
		Name:      "preload-host",
		MountPath: "/opt/function/cold-start-preload",
		ReadOnly:  true,
	}
	b.addVolumeMount(containerWorker, preLoadMount)

	b.addVolume(v1.Volume{
		Name: "preload-instance",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: preloadHostPath + "/instance",
				Type: &hostPathType,
			},
		},
	})
	preLoadInstanceMount := v1.VolumeMount{
		Name:      "preload-instance",
		MountPath: "/opt/function/cold-start-preload-instance",
	}
	b.addVolumeMount(containerWorker, preLoadInstanceMount)

	b.addVolume(v1.Volume{
		Name: "cold-start-preload",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	hostToContainer := v1.MountPropagationHostToContainer
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:             "cold-start-preload",
		MountPath:        "/opt/function/preload",
		MountPropagation: &bidirectional,
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:             "cold-start-preload",
		MountPath:        "/opt/function/preload",
		MountPropagation: &hostToContainer,
	})
}

type cudaDependencyConfig struct{}

func (v *cudaDependencyConfig) configVolume(b *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	b.addVolume(v1.Volume{
		Name: "cuda-dependency",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/usr/local/cuda",
				Type: &hostPathType,
			},
		},
	})
	depend := v1.VolumeMount{
		Name:      "cuda-dependency",
		MountPath: "/usr/local/cuda",
		ReadOnly:  true,
	}
	b.addVolumeMount(containerRuntime, depend)
}

type workerResourceLimitEnv struct {
	cpuLimit    int64
	memoryLimit int64
}

func (v *workerResourceLimitEnv) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  workerCPULimitKey,
		Value: strconv.Itoa(int(v.cpuLimit)),
	})

	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  workerMemoryLimitKey,
		Value: strconv.Itoa(int(v.memoryLimit)),
	})
}
