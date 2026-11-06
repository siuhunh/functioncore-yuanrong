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

package tls

import (
	"crypto/tls"
	"io/ioutil"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

const (
	defaultCertFile = "/home/sn/resource/secret/cert.pem"
	defaultKeyFile  = "/home/sn/resource/secret/key.pem"
)

func TestNewTLSConfig(t *testing.T) {
	p := gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
		return nil, nil
	})

	p.ApplyFunc(tls.LoadX509KeyPair, func(certFile, keyFile string) (tls.Certificate, error) {
		return tls.Certificate{}, nil
	})
	defer p.Reset()
	actual := NewTLSConfig(WithRootCAs(DefaultCAFile),
		WithCerts(defaultCertFile, defaultKeyFile), WithSkipVerify())
	expect := &tls.Config{
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			// for TLS1.2
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			// for TLS1.3
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
		PreferServerCipherSuites: true,
		Renegotiation:            tls.RenegotiateNever,
		InsecureSkipVerify:       true,
		RootCAs:                  nil,
		Certificates:             []tls.Certificate{{}},
	}

	assert.Equal(t, expect, actual)
}
