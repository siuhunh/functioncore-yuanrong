/*
 * Copyright (c) 2023 Huawei Technologies Co. Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS WITHOUT WARRANTIES OF ANY KIND
 * EITHER EXPRESS OR IMPLIED INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package vpcmanager is vpc manager module
package vpcmanager

import corev1 "k8s.io/api/core/v1"

// Type volume type
type Type int

const (
	cipherVolumeType Type = iota
	sslVolumeType
	kafkaVolumeType
	authVolumeType
	etcdVolumeType
	redisVolumeType
	configVolumeType
	caVolumeType
)

func getTypes() []Type {
	return []Type{cipherVolumeType, sslVolumeType, kafkaVolumeType, authVolumeType,
		etcdVolumeType, redisVolumeType, configVolumeType, caVolumeType}
}

func (t Type) keyToPath() []corev1.KeyToPath {
	return [...][]corev1.KeyToPath{cipherKeyToPath, sslKeyToPath, kafkaKeyToPath, authKeyToPath, etcdKeyToPath,
		redisKeyToPath, configKeyToPath, caKeyToPath}[t]
}

func (t Type) secretName() string {
	return [...]string{cipherSecretName, sslSecretName, kafkaSecretName, authSecretName, etcdSecretName,
		redisSecretName, configSecretName, caSecretName}[t]
}

func (t Type) name() string {
	return [...]string{cipherName, sslName, kafkaName, authName, etcdName, redisName, pullTriggerConfigName, caName}[t]
}
