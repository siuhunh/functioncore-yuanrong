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

// Package deploy -
package deploy

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/utils"
)

// GetFunctionInfoInCurrentDir -
func GetFunctionInfoInCurrentDir() (*FunctionConfig, error) {
	serviceYamlPath, err := GetFunctionYamlPathInCurrentDir()
	if err != nil {
		return nil, err
	}
	return GetFunctionInfoFromPath(serviceYamlPath)
}

// GetFunctionYamlPathInCurrentDir -
func GetFunctionYamlPathInCurrentDir() (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	localPath := filepath.Join(pwd, "function.yaml")
	if !utils.Exists(localPath) {
		return "", errors.New("current directory does not contain function.yaml")
	}
	return localPath, nil
}

// GetFunctionInfoFromPath get the info from the function.yaml file path
func GetFunctionInfoFromPath(serviceYamlPath string) (*FunctionConfig, error) {
	functionInfo := &FunctionConfig{}
	data, err := getYamlFileData(serviceYamlPath)
	if err != nil {
		return functionInfo, err
	}
	if err = yaml.UnmarshalStrict(data, functionInfo); err != nil {
		return functionInfo, err
	}
	checkFunctionConfig(functionInfo)
	return functionInfo, nil
}

func getYamlFileData(serviceYamlPath string) ([]byte, error) {
	if !utils.Exists(serviceYamlPath) {
		return nil, errors.New("function.yaml not exists")
	}
	data, err := ioutil.ReadFile(serviceYamlPath)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func checkFunctionConfig(funcConfig *FunctionConfig) {
	if funcConfig.MaxInstance == 0 {
		funcConfig.MaxInstance = constant.MaxMaxInstance
	}
	if funcConfig.ConcurrentNum == 0 {
		funcConfig.ConcurrentNum = 1
	}
	funcConfig.Runtime = strings.ToLower(funcConfig.Runtime)
}
