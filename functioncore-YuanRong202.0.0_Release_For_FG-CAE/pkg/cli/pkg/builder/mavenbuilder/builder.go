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

// Package mavenbuilder is java function builder
package mavenbuilder

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/builder/common"
	"functioncore/pkg/cli/utils"
)

// MavenBuilder maven build java code
type MavenBuilder struct{}

// Build maven build java code
func (m MavenBuilder) Build(functionPath string, functionName string) (string, error) {
	absServicePath, err := filepath.Abs(functionPath)
	if err != nil {
		return "", err
	}
	pkgPath, err := common.GetPkgPath(absServicePath)
	if err != nil {
		return "", err
	}

	pomFilePath := filepath.Join(absServicePath, "code", "pom.xml")
	if !utils.Exists(pomFilePath) {
		return "", fmt.Errorf("function %s pom.xml not exists", functionName)
	}

	// 创建mvn打包命令，打的zip压缩包放到service的pkg目录下，zip包名为函数名
	cmd := exec.Command("mvn", "clean", "package", "-f", pomFilePath)
	if err := common.ExecCommand(cmd); err != nil {
		return "", err
	}
	targetPath := filepath.Join(absServicePath, "code", "target")
	files, err := ioutil.ReadDir(targetPath)
	if err != nil {
		return "", err
	}
	jarPath := ""
	for _, file := range files {
		if strings.Contains(file.Name(), ".jar") {
			jarPath = filepath.Join(targetPath, file.Name())
		}
	}
	// 代码zip包路径
	dstPath := filepath.Join(pkgPath, functionName+constant.ZipSuffix)
	if err := utils.ZipOneFile(dstPath, jarPath); err != nil {
		return "", err
	}
	return dstPath, nil
}
