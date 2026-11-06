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

// Package cert -
package cert

import (
	"crypto/x509"
	"io/ioutil"
	"os"
)

// Pool cert pool
var Pool *x509.CertPool

// Load load certs
func Load() error {
	cliCAFilePath := os.Getenv("CLI_CA_PATH")

	if cliCAFilePath == "" {
		return nil
	}

	Pool = x509.NewCertPool()
	caCrt, err := ioutil.ReadFile(cliCAFilePath)
	if err != nil {
		return err
	}
	Pool.AppendCertsFromPEM(caCrt)

	return nil
}
