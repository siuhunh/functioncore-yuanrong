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
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/worker-manager/utils"
)

type container string

const (
	containerWorkerInit container = utils.WorkerInitContainerName
	containerWorker     container = utils.WorkerContainerName
	containerRuntime    container = utils.RuntimeContainerName
	containerAgent      container = utils.AgentContainerName
)

type volumeBuilder struct {
	volumes []corev1.Volume
	mounts  map[container][]corev1.VolumeMount
}

func (vc *volumeBuilder) addVolume(volume corev1.Volume) {
	vc.volumes = append(vc.volumes, volume)
}

func (vc *volumeBuilder) addVolumeMount(name container, mount corev1.VolumeMount) {
	vc.mounts[name] = append(vc.mounts[name], mount)
}

type envBuilder struct {
	envs map[container][]corev1.EnvVar
}

func (b *envBuilder) addEnvVar(name container, env corev1.EnvVar) {
	b.envs[name] = append(b.envs[name], env)
}

type argsBuilder struct {
	args map[container][]string
}

func (b *argsBuilder) addArgs(name container, args ...string) {
	b.args[name] = append(b.args[name], args...)
}

type component interface{}

type volumeConfigger interface {
	configVolume(b *volumeBuilder)
}

type envConfigger interface {
	configEnv(b *envBuilder)
}

type argsConfigger interface {
	configArgs(b *argsBuilder)
}

type componentSet struct {
	volumeBuilder *volumeBuilder
	envBuilder    *envBuilder
	argsBuilder   *argsBuilder
}

func newComponentSet() *componentSet {
	return &componentSet{
		volumeBuilder: &volumeBuilder{
			mounts: make(map[container][]corev1.VolumeMount),
		},
		envBuilder: &envBuilder{
			envs: make(map[container][]corev1.EnvVar),
		},
		argsBuilder: &argsBuilder{
			args: make(map[container][]string),
		},
	}
}

func (b *componentSet) buildVolumes() []corev1.Volume {
	return b.volumeBuilder.volumes
}

func (b *componentSet) buildMounts(container container) []corev1.VolumeMount {
	return b.volumeBuilder.mounts[container]
}

func (b *componentSet) buildEnvVar(container container) []corev1.EnvVar {
	return b.envBuilder.envs[container]
}

func (b *componentSet) buildArgs(container container) []string {
	return b.argsBuilder.args[container]
}

func (b *componentSet) add(component component) {
	if volumeConfigger, ok := component.(volumeConfigger); ok {
		volumeConfigger.configVolume(b.volumeBuilder)
	}
	if envConfigger, ok := component.(envConfigger); ok {
		envConfigger.configEnv(b.envBuilder)
	}
	if argsConfigger, ok := component.(argsConfigger); ok {
		argsConfigger.configArgs(b.argsBuilder)
	}
}
