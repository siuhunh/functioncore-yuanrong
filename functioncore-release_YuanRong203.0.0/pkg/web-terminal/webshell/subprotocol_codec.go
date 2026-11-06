/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package webshell define function to exec webshell
package webshell

import (
	"encoding/base64"
	"errors"

	"functioncore/pkg/common/logger/log"
)

// POSIXFD define posix fd
type POSIXFD byte

const (
	// STDIN define stdin
	STDIN POSIXFD = '0'
	// STDOUT define stdout
	STDOUT POSIXFD = '1'
	// STDERR define stderr
	STDERR POSIXFD = '2'
)

// String -
func (fd *POSIXFD) String() string {
	switch *fd {
	case STDIN:
		return "stdin"
	case STDOUT:
		return "stdout"
	case STDERR:
		return "stderr"
	default:
		return ""
	}
}

// SubProtocolCodec refer to kubernetes/staging/src/k8s.io/apiserver/pkg/util/wsstream/conn.go
type SubProtocolCodec interface {
	Encode(rawData []byte, posixFd POSIXFD) []byte
	Decode(rawData []byte) (POSIXFD, []byte, error)
}
type subProtocolCodec struct{}

// Encode encode subprotocol base64.channel.k8s.io
func (c *subProtocolCodec) Encode(rawData []byte, posixFd POSIXFD) []byte {
	dat := base64.StdEncoding.EncodeToString(rawData)
	k8sBase64Data := []byte{byte(posixFd)}
	k8sBase64Data = append(k8sBase64Data, dat...)
	log.GetLogger().Debugf("subProtocolCodec.Encode rawData: %s, k8sBase64Data: %s",
		string(rawData), string(k8sBase64Data))
	return k8sBase64Data
}

// Decode decode subprotocol base64.channel.k8s.io
func (c *subProtocolCodec) Decode(rawData []byte) (POSIXFD, []byte, error) {
	if len(rawData) < 1 {
		return '0', nil, errors.New("invalid data from kubernetes")
	}
	posixFd, data := rawData[0], rawData[1:]
	dat, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return POSIXFD(posixFd), nil, err
	}
	log.GetLogger().Debugf("subProtocolCodec.Decode rawData: %s, result data: %s",
		string(rawData), string(dat))
	return POSIXFD(posixFd), dat, nil
}

// NewSubProtocolCodec -
func NewSubProtocolCodec() SubProtocolCodec {
	return &subProtocolCodec{}
}
