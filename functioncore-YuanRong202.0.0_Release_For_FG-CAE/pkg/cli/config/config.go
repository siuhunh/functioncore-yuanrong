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

// Package config is config
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"

	"functioncore/pkg/cli/utils"
)

type config struct {
	CLIConfig cliConfig `yaml:"config"`
}

type cliConfig struct {
	Host        string      `yaml:"host"`
	TenantID    string      `yaml:"tenantID"`
	ClusterID   string      `yaml:"clusterID"`
	EnableHTTPS bool        `yaml:"enableHTTPS"`
	Auth        authConfig  `yaml:"auth"`
	Minio       minioConfig `yaml:"minio"`
	ETCD        EtcdConfig  `yaml:"etcd"`
}

type authConfig struct {
	AK          string      `yaml:"ak"`
	SK          string      `yaml:"sk"`
}

type minioConfig struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"accessKeyID"`
	SecretAccessKey string `yaml:"secretAccessKey"`
	Bucket          string `yaml:"bucket"`
	SSLEnable       bool     `yaml:"sslEnable"`
	CertFile        string   `yaml:"certFile"`
}

// EtcdConfig -
type EtcdConfig struct {
	URL       []string `yaml:"url"`
	SSLEnable bool     `yaml:"sslEnable"`
	UserName  string   `yaml:"userName"`
	Password  string   `yaml:"password"`
	CaFile    string   `yaml:"caFile"`
	CertFile  string   `yaml:"certFile"`
	KeyFile   string   `yaml:"keyFile"`
}

const configFile = "config.yaml"

// FConfig -
var FConfig = config{}

// LoadConfig -
func LoadConfig() error {
	configPath, err := getConfigPath()
	if err != nil {
		return err
	}

	configContent, err := ioutil.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read configFile: %s ", configPath)
	}
	if err = yaml.Unmarshal(configContent, &FConfig); err != nil {
		return fmt.Errorf("failed to Unmarshal configConten, %s", err)
	}

	return nil
}

func getConfigPath() (string, error) {
	resourcePath, err := utils.GetResourcePath()
	if err != nil {
		return "", err
	}
	configPath := filepath.Join(resourcePath, configFile)
	if !utils.Exists(configPath) {
		return "", fmt.Errorf("configFile: %s not exists", configPath)
	}
	return configPath, nil
}

const (
	functionConfigFile = "function.json"
	upperMaxInstance   = 100000
	upperMaxLayersSize = 20
)

var (
	// FunctionConfig Configuring Variables
	FunctionConfig = &Config{}
	// RuntimeEnum function runtime enum
	RuntimeEnum = []string{"Python3.6"}
	// DeploymentInfoEnum The deployment information key is memory and value is CPU
	DeploymentInfoEnum = map[int]int{128: 300, 256: 400, 512: 600}
	// MaxMaxInstance default upper limit of the maximum instance
	MaxMaxInstance = float64(1000)
	// MaxLayersSize Maximum number of layers supported by function deployment
	MaxLayersSize = 5
)

// Config function config
type Config struct {
	Runtime        []string     `json:"runtime"`
	DeploymentInfo []Deployment `json:"deploymentInfo"`
	MaxMaxInstance float64      `json:"maxMaxInstance"`
	MaxLayersSize  int          `json:"maxLayersSize,omitempty"`
}

// Deployment function deployment config
type Deployment struct {
	CPU    int `json:"cpu"`
	Memory int `json:"memory"`
}

// LoadFunctionConfig load function.json config
func LoadFunctionConfig() error {
	resourcePath, err := utils.GetResourcePath()
	if err != nil {
		return err
	}

	functionConfigPath := filepath.Join(resourcePath, functionConfigFile)
	if !utils.Exists(functionConfigPath) {
		return errors.New("functionConfigFile not exists")
	}
	configJSON, err := ioutil.ReadFile(functionConfigPath)
	if err != nil {
		return fmt.Errorf("read functionConfigFile: %s fail", functionConfigPath)
	}
	if err = json.Unmarshal(configJSON, FunctionConfig); err != nil {
		return fmt.Errorf("functionConfigFile content has error: %s", err)
	}

	if FunctionConfig.Runtime != nil {
		RuntimeEnum = FunctionConfig.Runtime
	}
	if FunctionConfig.DeploymentInfo != nil {
		DeploymentInfoEnum = setDeploymentInfoEnum(FunctionConfig.DeploymentInfo)
	}
	if FunctionConfig.MaxMaxInstance > 0 && FunctionConfig.MaxMaxInstance < upperMaxInstance {
		MaxMaxInstance = FunctionConfig.MaxMaxInstance
	}
	if FunctionConfig.MaxLayersSize > 0 && FunctionConfig.MaxLayersSize <= upperMaxLayersSize {
		MaxLayersSize = FunctionConfig.MaxLayersSize
	}
	return nil
}

func setDeploymentInfoEnum(deployments []Deployment) map[int]int {
	// 当前deployment为3个，设定size为6一倍
	deploymentEnum := make(map[int]int, 6)
	for _, deployment := range deployments {
		deploymentEnum[deployment.Memory] = deployment.CPU
	}
	return deploymentEnum
}
