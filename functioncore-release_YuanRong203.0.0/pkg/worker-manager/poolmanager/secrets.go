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

const (
	decoyMountPath = "/home/sn/decoy/"
)

type localAuthSecret struct {
	shouldMountRuntime bool
}

func (v *localAuthSecret) configVolume(b *volumeBuilder) {
	b.addVolume(buildVolumeOfSecretSource("localauth", "local-secret"))
	mount := v1.VolumeMount{
		Name:      "localauth",
		MountPath: "/home/sn/resource/cipher",
	}
	b.addVolumeMount(containerWorkerInit, mount)
	if v.shouldMountRuntime {
		b.addVolumeMount(containerRuntime, mount)
	}
}

func (v *localAuthSecret) configEnv(b *envBuilder) {
	env := v1.EnvVar{
		Name:  "PAAS_CRYPTO_PATH",
		Value: "/home/sn/resource/cipher",
	}
	b.addEnvVar(containerWorker, env)
	b.addEnvVar(containerRuntime, env)
}

type authSecret struct{}

func (v *authSecret) configVolume(b *volumeBuilder) {
	b.addVolume(buildVolume("apple", "a.txt", "a.txt"))
	b.addVolume(buildVolume("boy", "b.txt", "b.txt"))
	b.addVolume(buildVolume("cat", "c.txt", "c.txt"))
	b.addVolume(buildVolume("dog", "d.txt", "d.txt"))
	b.addVolume(buildVolume("wdo", "wdo.json", "wdo.json"))

	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "apple",
		MountPath: "/home/sn/resource/rdo/v1/apple",
	})
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "boy",
		MountPath: "/home/sn/resource/rdo/v1/boy",
	})
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "cat",
		MountPath: "/home/sn/resource/rdo/v1/cat",
	})
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "dog",
		MountPath: "/home/sn/resource/rdo/v1/dog",
	})
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "wdo",
		MountPath: "/home/sn/resource/wdo",
	})
}

type tempWorkerKeySecret struct{}

func (v *tempWorkerKeySecret) configVolume(b *volumeBuilder) {
	b.addVolume(buildVolumeOfSecretSource("temp-worker-key", "worker-key"))
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "temp-worker-key",
		MountPath: "/home/sn/resource",
	})
}

type caSecret struct {
	mountPath string
}

func (v *caSecret) configVolume(b *volumeBuilder) {
	b.addVolume(buildVolumeOfSecretSource("ca", "ca-secret"))
	b.addVolumeMount(containerWorker, v1.VolumeMount{
		Name:      "ca",
		MountPath: v.mountPath,
	})
}

type tlsSecret struct{}

func (v *tlsSecret) configVolume(b *volumeBuilder) {
	b.addVolume(buildVolumeOfSecretSource("https", "https-certs"))
	b.addVolumeMount(containerWorkerInit, v1.VolumeMount{
		Name:      "https",
		MountPath: "/home/sn/resource/https",
	})
}

func (v *tlsSecret) configEnv(b *envBuilder) {
	b.addEnvVar(containerWorker, v1.EnvVar{
		Name:  "SSL_ROOT",
		Value: "/home/sn/resource/https",
	})
}

type decoySecret struct{}

func (v *decoySecret) configVolume(b *volumeBuilder) {
	b.addVolume(buildVolumeOfSecretSource("decoy-file", "decoy-secret"))
	b.addVolumeMount(containerRuntime, v1.VolumeMount{
		Name:      "decoy-file",
		MountPath: decoyMountPath,
	})
}
