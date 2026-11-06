/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package config -
package config

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"functioncore/pkg/common/logger/log"
)

const (
	cipherPathEnv = "PAAS_CRYPTO_PATH"
	sslRoot       = "SSL_ROOT"
)

// Config -
type Config struct {
	AuthConfig   *AuthConfig   `json:"authConfig"`
	LocationInfo *LocationInfo `json:"locationInfo"`
	AlarmInfo    *AlarmInfo    `json:"alarmInfo"`
	Address      string        `json:"address"`
}

// AuthConfig -
type AuthConfig struct {
	TLSProtocol string `json:"tlsProtocol"`
	TLSCiphers  string `json:"tlsCiphers"`
	AKey        string `json:"AKey"`
	SKey        string `json:"SKey"`
	SslPath     string `json:"sslPath"`
	CipherPath  string `json:"cipherPath"`
}

// LocationInfo -
type LocationInfo struct {
	Region  string `json:"region"`
	Cluster string `json:"cluster"`
	NodeIP  string `json:"nodeIP"`
}

// AlarmInfo -
type AlarmInfo struct {
	ImportantLevel string `json:"importantLevel"`
	Content        string `json:"content"`
	Resource       string `json:"resource"`
}

// LoadConfig -
func LoadConfig(configPath string) (*Config, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// SetTLSEnvValues is used setting the SSL_ROOT and PAAS_CRYPTO_PATH Environment Variables
func SetTLSEnvValues(auth *AuthConfig) error {
	if err := os.Setenv(cipherPathEnv, auth.CipherPath); err != nil {
		log.GetLogger().Errorf("set env: %s, val: %s, err: %s", cipherPathEnv, auth.CipherPath, err.Error())
		return err
	}
	if err := os.Setenv(sslRoot, auth.SslPath); err != nil {
		log.GetLogger().Errorf("set env: %s, val: %s, err: %s", sslRoot, auth.SslPath, err.Error())
		return err
	}
	return nil
}
