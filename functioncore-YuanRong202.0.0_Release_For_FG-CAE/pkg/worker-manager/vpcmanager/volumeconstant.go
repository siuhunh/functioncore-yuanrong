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

const (
	cipherName            = "cipher"
	sslName               = "ssl"
	kafkaName             = "kafkacert"
	authName              = "auth"
	etcdName              = "etcd"
	redisName             = "redisdb"
	pullTriggerConfigName = "pulltriggerconfig"
	caName                = "ca"
)

const (
	cipherSecretName = "cff-cipher-ssl-secret"
	sslSecretName    = "cff-cipher-ssl-secret"
	kafkaSecretName  = "cff-kafka-secret"
	authSecretName   = "cff-common-secret"
	etcdSecretName   = "cff-etcd-secret"
	redisSecretName  = "cff-rdb-secret"
	configSecretName = "cff-vpcpulltrigger-config"
	caSecretName     = "ca-secret"
)

var (
	secretMode      = int32(defaultSecretMode)
	cipherKeyToPath = []corev1.KeyToPath{
		{
			Key:  "root.key",
			Path: "root.key",
			Mode: &secretMode,
		},
		{
			Key:  "common_shared.key",
			Path: "common_shared.key",
			Mode: &secretMode,
		},
	}
	sslKeyToPath = []corev1.KeyToPath{
		{
			Key:  "ca.crt",
			Path: "trust.cer",
			Mode: &secretMode,
		},
		{
			Key:  "tls.crt",
			Path: "server.cer",
			Mode: &secretMode,
		},
		{
			Key:  "tls.key.pwd",
			Path: "server_key.pem",
			Mode: &secretMode,
		},
		{
			Key:  "pwd",
			Path: "cert_pwd",
			Mode: &secretMode,
		},
	}
	kafkaKeyToPath = []corev1.KeyToPath{
		{
			Key:  "tls.crt",
			Path: "kafka.cer",
			Mode: &secretMode,
		},
		{
			Key:  "tls.key",
			Path: "kafka.key",
			Mode: &secretMode,
		},
		{
			Key:  "tls.ca",
			Path: "kafka.ca",
			Mode: &secretMode,
		},
	}
	authKeyToPath = []corev1.KeyToPath{
		{
			Key:  "internal_sign_ak",
			Path: "internal_sign_ak",
			Mode: &secretMode,
		},
		{
			Key:  "internal_sign_sk",
			Path: "internal_sign_sk",
			Mode: &secretMode,
		},
	}
	etcdKeyToPath = []corev1.KeyToPath{
		{
			Key:  "appversion",
			Path: "appversion.json",
			Mode: &secretMode,
		},
	}
	redisKeyToPath = []corev1.KeyToPath{
		{
			Key:  "appversion",
			Path: "appversion.json",
			Mode: &secretMode,
		},
	}
	configKeyToPath = []corev1.KeyToPath{
		{
			Key:  "appversion",
			Path: "appversion.json",
			Mode: &secretMode,
		},
	}
	caKeyToPath = []corev1.KeyToPath{
		{
			Key:  "ca.pem",
			Path: "ca.pem",
			Mode: &secretMode,
		},
		{
			Key:  "cert.pem",
			Path: "cert.pem",
			Mode: &secretMode,
		},
		{
			Key:  "key.pem",
			Path: "key.pem",
			Mode: &secretMode,
		},
	}
)
