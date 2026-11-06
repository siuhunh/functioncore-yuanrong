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

// Package common common methods of builder
package common

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/utils"
)

// ExecCommand exec command
func ExecCommand(cmd *exec.Cmd) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	// 运行命令
	if err := cmd.Start(); err != nil {
		return err
	}
	slurp, err := ioutil.ReadAll(stdout)
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		fmt.Sprintf("Failed to Build:\n%s\n", slurp)
		return err
	}
	return nil
}

// GetPkgPath get function builder output path
func GetPkgPath(servicePath string) (string, error) {
	pkgPath := filepath.Join(servicePath, constant.ServicePKG)
	if !utils.Exists(pkgPath) {
		if err := os.Mkdir(pkgPath, utils.MkdirFileMode); err != nil {
			return "", err
		}
	}
	return pkgPath, nil
}
