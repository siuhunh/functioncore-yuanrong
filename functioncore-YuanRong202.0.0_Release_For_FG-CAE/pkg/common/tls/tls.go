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

// Package tls provides tls utils
package tls

import (
	"crypto/x509"
	"errors"
	"io/ioutil"
	"time"

	"functioncore/pkg/common/logger/log"
)

// LoadRootCAs returns system cert pool with caFiles added
func LoadRootCAs(caFiles ...string) (*x509.CertPool, error) {
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	for _, file := range caFiles {
		cert, err := ioutil.ReadFile(file)
		if err != nil {
			return nil, err
		}
		if !rootCAs.AppendCertsFromPEM(cert) {
			return nil, err
		}
	}
	return rootCAs, nil
}

// VerifyCert Used to verity the server certificate
func VerifyCert(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	certs := make([]*x509.Certificate, len(rawCerts))
	if len(certs) == 0 {
		log.GetLogger().Errorf("cert number is 0")
		return errors.New("cert number is 0")
	}
	opts := x509.VerifyOptions{
		Roots:         tlsConfig.ClientCAs,
		CurrentTime:   time.Now(),
		DNSName:       "",
		Intermediates: x509.NewCertPool(),
	}
	for i, asn1Data := range rawCerts {
		cert, err := x509.ParseCertificate(asn1Data)
		if err != nil {
			log.GetLogger().Errorf("failed to parse certificate from server: %s", err.Error())
			return err
		}
		certs[i] = cert
		if i == 0 {
			continue
		}
		opts.Intermediates.AddCert(cert)
	}
	_, err := certs[0].Verify(opts)
	return err
}
