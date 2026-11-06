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
	"os"
	"path/filepath"

	v1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/utils"
)

var (
	defaultClusterName = os.Getenv("CLUSTER_ID")
	limitCodeSize      = os.Getenv("LIMIT_CODE_SIZE")

	caeToolsPath = os.Getenv("CAE_TOOLS_PATH")
)

const defaultCaeToolsPath = "/var/cae/tools"

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

type runtimeManagerConfig struct {
	restoreOpts string
	storageType string
}

func (v *runtimeManagerConfig) configEnv(b *envBuilder) {
	configEnv := v1.EnvVar{
		Name:  "RUNTIME_MANAGER_CONFIG",
		Value: os.Getenv("RUNTIME_MANAGER_CONFIG"),
	}
	b.addEnvVar(containerWorker, configEnv)
	b.addEnvVar(containerRuntime, configEnv)
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "SNAPSHOT_STORAGE_TYPE",
		Value: v.storageType,
	})
	if defaultClusterName != "" {
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  "CLUSTER_ID",
			Value: defaultClusterName,
		})
	}
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogPath,
		Value: utils.DefaultStringEnv(constants.RuntimeLogPath, "/home/snuser/log"),
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  constants.RuntimeLogLevel,
		Value: utils.DefaultStringEnv(constants.RuntimeLogLevel, "INFO"),
	})
	if v.restoreOpts != "" {
		b.addEnvVar(containerRuntime, v1.EnvVar{
			Name:  constants.CheckpointRestoreOpts,
			Value: v.restoreOpts,
		})
	}
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
