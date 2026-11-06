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

// Package builder is function builder
package builder

import (
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/builder/mavenbuilder"
	"functioncore/pkg/cli/pkg/builder/zipbuilder"
)

// Builder function builder interface
type Builder interface {
	Build(servicePath string, functionName string) (string, error)
}

// GetBuildByRuntime get builder impl by runtime
func GetBuildByRuntime(runtime, handler string) Builder {
	switch runtime {
	case constant.Java8, constant.Java11:
		return mavenbuilder.MavenBuilder{}
	default:
		return zipbuilder.ZipBuilder{}
	}
}
