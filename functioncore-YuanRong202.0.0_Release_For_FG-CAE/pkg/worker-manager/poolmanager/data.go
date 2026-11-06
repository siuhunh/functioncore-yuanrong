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

import v1 "k8s.io/api/core/v1"

type userFunctionCodeData struct {
	deployDir  string
	metricsDir string
}

func (v *userFunctionCodeData) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "function-data",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "function-data",
		MountPath: v.deployDir,
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "function-data",
		MountPath: v.deployDir,
	})
	b.addVolume(v1.Volume{
		Name: "function-metrics",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "function-metrics",
		MountPath: v.metricsDir,
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "function-metrics",
		MountPath: v.metricsDir,
	})

	b.addVolume(v1.Volume{
		Name: "system-data",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "system-data",
		MountPath: "/var/tmp",
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "system-data",
		MountPath: "/runtime/var-tmp",
	})
}

type datasystemPath struct{}

func (v *datasystemPath) configVolume(b *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	b.addVolume(v1.Volume{
		Name: "datasystem-uds",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: "/home/uds",
				Type: &hostPathType,
			},
		},
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "datasystem-uds",
		MountPath: "/home/uds",
		ReadOnly:  true,
	})
}

func (v *userFunctionCodeData) configEnv(b *envBuilder) {
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  "DEPLOY_DIR",
		Value: v.deployDir,
	})
}

type caeFunctionCodeData struct {
	deployDir  string
	metricsDir string
}

func (v *caeFunctionCodeData) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "function-data",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolume(v1.Volume{
		Name: "function-metrics",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolume(v1.Volume{
		Name: "system-data",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "function-data",
		MountPath: v.deployDir,
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "function-metrics",
		MountPath: v.metricsDir,
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "function-metrics",
		MountPath: v.metricsDir,
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "system-data",
		MountPath: "/runtime/var-tmp",
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "system-data",
		MountPath: "/var/tmp",
	})
}

func (v *caeFunctionCodeData) configEnv(b *envBuilder) {
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  "DEPLOY_DIR",
		Value: v.deployDir,
	})
}

type userFunctionTmpData struct{}

func (v *userFunctionTmpData) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "user-datas",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "user-datas",
		MountPath: "/runtime/tmp/",
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "user-datas",
		MountPath: "/tmp/",
	})
}

type shareWorkerSecretData struct{}

func (v *shareWorkerSecretData) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "worker-secret",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "worker-secret",
		MountPath: "/home/sn/temp-resource",
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "worker-secret",
		MountPath: "/home/sn/resource",
	})
}

func (v *shareWorkerSecretData) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "ResourcePath",
		Value: "/home/sn/resource",
	})
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "WORKER_KEY_PATH",
		Value: "/home/sn/resource",
	})
}

type remoteDebug struct {
	remoteDebugArgs string
}

func (r *remoteDebug) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "EnableRemoteDebug",
		Value: trueString,
	})
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "RemoteDebugArgs",
		Value: r.remoteDebugArgs,
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  "EnableRemoteDebug",
		Value: trueString,
	})
	b.addEnvVar(containerRuntime, v1.EnvVar{
		Name:  "RemoteDebugArgs",
		Value: r.remoteDebugArgs,
	})
}
