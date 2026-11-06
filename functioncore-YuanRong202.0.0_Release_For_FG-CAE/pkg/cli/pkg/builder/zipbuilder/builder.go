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

// Package zipbuilder is default function builder
package zipbuilder

import (
	"fmt"
	"path/filepath"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/builder/common"
	"functioncore/pkg/cli/utils"
)

// ZipBuilder only zip function code
type ZipBuilder struct{}

// Build only zip function code
func (z ZipBuilder) Build(servicePath string, functionName string) (string, error) {
	stdServicePath, err := filepath.Abs(servicePath)
	if err != nil {
		return "", err
	}
	pkgPath, err := common.GetPkgPath(stdServicePath)
	if err != nil {
		return "", err
	}

	// 函数代码路径
	funcPath := filepath.Join(stdServicePath, "code")
	if !utils.Exists(funcPath) {
		return "", fmt.Errorf("function %s directory not exists", functionName)
	}

	// 代码zip包路径
	dstPath := filepath.Join(pkgPath, functionName+constant.ZipSuffix)
	if err := utils.Zip(dstPath, funcPath); err != nil {
		return "", err
	}
	return dstPath, nil
}
