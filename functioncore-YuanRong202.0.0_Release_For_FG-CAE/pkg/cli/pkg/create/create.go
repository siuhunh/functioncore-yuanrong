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

// Package create -
package create

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/deploy"
	"functioncore/pkg/cli/utils"
)

// GetNewProjectPath get the path of the new function project
func GetNewProjectPath(projectName string) (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	projectPath := filepath.Join(pwd, projectName)
	if utils.Exists(projectPath) {
		return "", fmt.Errorf("project %s directory already exists", projectName)
	}
	return projectPath, nil
}

// Function create function
func Function(w io.Writer, templatePath string, projectName string) error {
	fmt.Fprintf(w, "\n------------------------------------"+
		"\nGenerating %s Function"+
		"\n------------------------------------\n", constant.PlatformName)
	projectPath, err := GetNewProjectPath(projectName)
	if err != nil {
		return err
	}
	if err = os.Mkdir(projectPath, utils.MkdirFileMode); err != nil {
		return err
	}
	if err = utils.CopyTree(templatePath, projectPath); err != nil {
		return err
	}
	// update function.yaml
	serviceYamlPath := filepath.Join(projectPath, constant.FunctionYaml)
	functionInfo, err := GetFunctionInfoFromPath(serviceYamlPath)
	if err != nil {
		return err
	}
	functionInfo.Name = projectName
	if err = UpdateInfo(serviceYamlPath, functionInfo); err != nil {
		return err
	}
	return nil
}

// GetFunctionInfoFromPath get the info from the function.yaml file path
func GetFunctionInfoFromPath(serviceYamlPath string) (*deploy.FunctionConfig, error) {
	functionInfo := &deploy.FunctionConfig{}
	data, err := getYamlFileData(serviceYamlPath)
	if err != nil {
		return functionInfo, err
	}
	if err = yaml.UnmarshalStrict(data, functionInfo); err != nil {
		return functionInfo, err
	}

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

// UpdateInfo update the config info of the yaml from the path
func UpdateInfo(yamlPath string, info interface{}) error {
	if !utils.Exists(yamlPath) {
		return errors.New("function.yaml not exists")
	}
	file, err := os.OpenFile(yamlPath, os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := yaml.Marshal(info)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		return err
	}
	return nil
}
