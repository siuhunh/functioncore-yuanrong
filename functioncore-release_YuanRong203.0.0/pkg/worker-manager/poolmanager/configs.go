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
	"path/filepath"
	"strconv"

	v1 "k8s.io/api/core/v1"

	commonconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/config"
)

var (
	defaultClusterName = os.Getenv("CLUSTER_ID")
	limitCodeSize      = os.Getenv("LIMIT_CODE_SIZE")

	caeToolsPath = os.Getenv("CAE_TOOLS_PATH")

	runtimeManagerConfigStr = os.Getenv("RUNTIME_MANAGER_CONFIG")
	// get runtime manager config for cae or restore mode
	runtimeManagerConfigStrForCAE, runtimeManagerConfigStrForCAERestore = getConfigStrForCAE()
)

const (
	defaultCaeToolsPath = "/var/cae/tools"
	fileHandleNum       = "FILE_HANDLE_NUM"
)

type workerConfig struct{}

func (v *workerConfig) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "WORKER_CONFIG",
		Value: os.Getenv("WORKER_CONFIG"),
	})
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "AUTO_GC_MEMORY_THRESHOLD",
		Value: os.Getenv("WORKER_AUTO_GC_MEMORY_THRESHOLD"),
	})
	if defaultClusterName != "" {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  "CLUSTER_ID",
			Value: defaultClusterName,
		})
	}
	if limitCodeSize != "" {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  "LIMIT_CODE_SIZE",
			Value: limitCodeSize,
		})
	}
}

type debugAgentConfig struct{}

func (v *debugAgentConfig) configEnv(b *envBuilder) {
	b.addEnvVar(containerAgent, v1.EnvVar{
		Name:  "DEBUG_AGENT_CONFIG",
		Value: os.Getenv("DEBUG_AGENT_CONFIG"),
	})
}

type podConfig struct{}

func (v *podConfig) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "pod-config",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "pod-config",
		MountPath: "/home/sn/restart/",
	})

	b.addVolume(v1.Volume{
		Name: "has-worker-init",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "has-worker-init",
		MountPath: "/home/sn/worker-init",
	})
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "has-worker-init",
		MountPath: "/home/sn/worker-init",
	})
}

type runtimeConfig struct{}

func (v *runtimeConfig) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  constants.MaxRequestBodySize,
		Value: os.Getenv(constants.MaxRequestBodySize),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.MaxRequestBodySize,
		Value: os.Getenv(constants.MaxRequestBodySize),
	})

	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  constants.EnableVerticalAutoScalingEnv,
		Value: os.Getenv(constants.EnableVerticalAutoScalingEnv),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.EnableVerticalAutoScalingEnv,
		Value: os.Getenv(constants.EnableVerticalAutoScalingEnv),
	})

	readCPUInterval := os.Getenv("READ_CPU_INTERVAL")
	if readCPUInterval != "" {
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  "READ_CPU_INTERVAL",
			Value: readCPUInterval,
		})
	}
}

type runtimeLogPathConfig struct {
	enableLogPath bool
	logPaths      string
}

func (v *runtimeLogPathConfig) configEnv(b *envBuilder) {
	if v.enableLogPath {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  constants.EnableLogPath,
			Value: "true",
		})
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  constants.LogPaths,
			Value: v.logPaths,
		})
	}
}

type runtimeFileHandleNum struct{}

func (v *runtimeFileHandleNum) configEnv(b *envBuilder) {
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  fileHandleNum,
		Value: config.GetFileHandleNum(),
	})
}

type runtimeManagerConfig struct {
	snapshotDeployment        types.SnapshotDeploymentSpec
	disableCAERuntimePreStart uint32
	businessType              string
	restoreOpts               string
	storageType               string
	runtimeTermTimeout        int
	enableCAEDirectConnection bool
}

func (v *runtimeManagerConfig) configEnv(b *envBuilder) {
	configEnv := v.getRuntimeManagerConfigEnv()
	b.addEnvVar(containerWorker, configEnv)
	b.addEnvVar(containerRuntime, configEnv)
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "SNAPSHOT_STORAGE_TYPE",
		Value: v.storageType,
	})
	if v.enableCAEDirectConnection && v.businessType == constants.BusinessTypeCAE {
		b.addEnvVar(containerWorkerInit, v1.EnvVar{
			Name:  "BUSINESS_TYPE",
			Value: v.businessType,
		})
	}
	if defaultClusterName != "" {
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  "CLUSTER_ID",
			Value: defaultClusterName,
		})
	}
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogPath,
		Value: commonutils.DefaultStringEnv(constants.RuntimeLogPath, "/home/snuser/log"),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogLevel,
		Value: commonutils.DefaultStringEnv(constants.RuntimeLogLevel, "INFO"),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogSingleSize,
		Value: commonutils.DefaultStringEnv(constants.RuntimeLogSingleSize, "100"),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogThreshold,
		Value: commonutils.DefaultStringEnv(constants.RuntimeLogThreshold, "5"),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogCheckInterval,
		Value: commonutils.DefaultStringEnv(constants.RuntimeLogCheckInterval, "30"),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeTermTimeout,
		Value: strconv.Itoa(v.runtimeTermTimeout),
	})
	if v.restoreOpts != "" {
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  constants.CheckpointRestoreOpts,
			Value: v.restoreOpts,
		})
	}
}

func (v *runtimeManagerConfig) getRuntimeManagerConfigEnv() v1.EnvVar {
	configEnv := v1.EnvVar{
		Name:  "RUNTIME_MANAGER_CONFIG",
		Value: runtimeManagerConfigStr,
	}
	log.GetLogger().Debugf("disableCAERuntimePreStart: %d, businessType: %s, snapshotSize: %d",
		v.disableCAERuntimePreStart, v.businessType, v.snapshotDeployment.SnapshotSize)
	if v.disableCAERuntimePreStart == 1 && commonutils.IsCAEFunc(v.businessType) {
		if runtimeManagerConfigStrForCAE == "" || runtimeManagerConfigStrForCAERestore == "" {
			log.GetLogger().Debugf("runtimeManagerConfigStrForCAE or runtimeManagerConfigStrForCAERestore is empty")
			return configEnv
		}
		// restore pod
		if v.snapshotDeployment.SnapshotSize != 0 {
			configEnv = v1.EnvVar{
				Name:  "RUNTIME_MANAGER_CONFIG",
				Value: runtimeManagerConfigStrForCAERestore,
			}
		} else {
			configEnv = v1.EnvVar{
				Name:  "RUNTIME_MANAGER_CONFIG",
				Value: runtimeManagerConfigStrForCAE,
			}
		}
	}
	return configEnv
}

type caeTools struct{}

func (v *caeTools) configVolume(vb *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	caeToolsPath = getCaeToolsPath()

	volume := v1.Volume{
		Name: "sn-bin",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "home/sn/bin"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "sn-bin",
		MountPath: "/home/sn/bin",
	})

	volume = v1.Volume{
		Name: "sn-config",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "home/sn/config"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "sn-config",
		MountPath: "/home/sn/config",
	})

	volume = v1.Volume{
		Name: "snuser-runtime-java",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "home/snuser/runtime/java"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "snuser-runtime-java",
		MountPath: "/home/snuser/runtime/java",
	})

	volume = v1.Volume{
		Name: "snuser-bin",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "home/snuser/bin"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "snuser-bin",
		MountPath: "/home/snuser/bin",
	})

	volume = v1.Volume{
		Name: "snuser-config",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "home/snuser/config"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "snuser-config",
		MountPath: "/home/snuser/config",
	})

	volume = v1.Volume{
		Name: "opt-huawei-fastfreeze",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "opt/huawei/fastfreeze"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "opt-huawei-fastfreeze",
		MountPath: "/opt/huawei/fastfreeze",
	})

	volume = v1.Volume{
		Name: "opt-huawei-faststart",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "opt/huawei/faststart"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "opt-huawei-faststart",
		MountPath: "/opt/huawei/faststart",
	})

	volume = v1.Volume{
		Name: "snuser-apm-javaagent",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: filepath.Join(caeToolsPath, "home/snuser/apm-javaagent"),
				Type: &hostPathType,
			},
		},
	}
	vb.addVolume(volume)
	vb.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "snuser-apm-javaagent",
		MountPath: "/home/snuser/apm-javaagent",
	})
}

func getCaeToolsPath() string {
	if caeToolsPath == "" {
		return defaultCaeToolsPath
	}
	return caeToolsPath
}

func getConfigStrForCAE() (string, string) {
	log.GetLogger().Infof("initializing runtime manager config")
	rmConfig, err := commonconfig.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error %s", err.Error())
		return "", ""
	}
	for language := range rmConfig.RuntimeConfigs {
		cfg := rmConfig.RuntimeConfigs[language]
		cfg.Enable = false
		rmConfig.RuntimeConfigs[language] = cfg
	}
	restoreConfigBytes, err := json.Marshal(rmConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal config with error %s", err.Error())
		return "", ""
	}
	// CAE function will pre start java8 and java17 runtime
	for _, language := range commonutils.GetCAEJavaVersion() {
		cfg := rmConfig.RuntimeConfigs[language]
		cfg.Enable = true
		rmConfig.RuntimeConfigs[language] = cfg
	}
	caeConfigBytes, err := json.Marshal(rmConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal config with error %s", err.Error())
		return "", ""
	}
	log.GetLogger().Infof("succeed to marshal runtime manager config")
	return string(caeConfigBytes), string(restoreConfigBytes)
}

type workerIAMTokenEnv struct {
	enableIAMToken bool
}

func (v *workerIAMTokenEnv) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  constants.EnableIAMTokenEnv,
		Value: strconv.FormatBool(v.enableIAMToken),
	})
}
