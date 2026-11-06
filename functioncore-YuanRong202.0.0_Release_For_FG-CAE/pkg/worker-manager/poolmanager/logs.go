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
	"fmt"

	v1 "k8s.io/api/core/v1"
)

type runtimeLog struct {
	mountPath string
}

func (v *runtimeLog) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "runtimelog",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "runtimelog",
		MountPath: v.mountPath,
	})
}

type varLog struct {
	mountPath string
}

func (v *varLog) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "varlog",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "varlog",
		MountPath: v.mountPath,
	})
}

func (v *varLog) configArgs(b *argsBuilder) {
	b.addArgs(containerWorker, "-log-dir="+v.mountPath)
}

type socketFile struct {
}

func (s *socketFile) configVolume(b *volumeBuilder) {
	b.addVolume(v1.Volume{
		Name: "socketfile",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "socketfile",
		MountPath: "/home/snuser/socket",
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "socketfile",
		MountPath: "/home/snuser/socket",
	})
}

type fusionSTDLog struct {
	hostPath  string
	mountPath string
}

func (v *fusionSTDLog) configVolume(b *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	b.addVolume(v1.Volume{
		Name: "stdlogs",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: v.hostPath,
				Type: &hostPathType,
			},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "stdlogs",
		MountPath: v.mountPath,
	})
}

func (v *fusionSTDLog) configArgs(b *argsBuilder) {
	b.addArgs(containerWorker, "-log-dir="+v.mountPath)
}

// fusionServiceLog configs the service logs of a pool. These logs are mounted on a hostPath.
type fusionServiceLog struct {
	hostPath            string
	workerMountPath     string
	runtimeMgrMountPath string
	runtimeLogMountPath string
	enableRemoteDebug   bool
}

func (v *fusionServiceLog) configVolume(b *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	b.addVolume(v1.Volume{
		Name: "servicelogs",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: v.hostPath,
				Type: &hostPathType,
			},
		},
	})
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:        "servicelogs",
		MountPath:   v.workerMountPath,
		SubPathExpr: "$(PodName)",
	})
	if v.runtimeMgrMountPath != "" {
		b.addVolumeMount(containerRuntime, v1.VolumeMount{
			Name:        "servicelogs",
			MountPath:   v.runtimeMgrMountPath,
			SubPathExpr: "$(PodName)",
		})
	}
	if v.runtimeLogMountPath != "" {
		b.addVolumeMount(containerRuntime, v1.VolumeMount{
			Name:        "servicelogs",
			MountPath:   v.runtimeLogMountPath,
			SubPathExpr: "$(PodName)/runtime",
		})
	}
	if v.enableRemoteDebug {
		b.addVolumeMount(containerAgent, v1.VolumeMount{
			Name: "servicelogs",
			// debug agent log mount path is same as worker
			MountPath:   v.workerMountPath,
			SubPathExpr: "$(PodName)",
		})
	}
}

type userLogConfig struct {
	hostPath          string
	workerMountPath   string
	runtimeMountPaths map[string]struct{}
}

func (v *userLogConfig) configVolume(b *volumeBuilder) {
	hostPathType := v1.HostPathDirectoryOrCreate
	volume := v1.Volume{
		Name: "logconfig",
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: v.hostPath,
				Type: &hostPathType,
			},
		},
	}
	b.addVolume(volume)
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:        "logconfig",
		MountPath:   v.workerMountPath,
		SubPathExpr: "$(PodName)",
	})
	for path := range v.runtimeMountPaths {
		b.addVolumeMount(containerRuntime, v1.VolumeMount{
			Name:        "logconfig",
			MountPath:   path,
			SubPathExpr: fmt.Sprintf("$(PodName)/userfunc/%s", path),
		})
	}
}
