/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package utils for common functions
package utils

import "strings"

const (
	// RuntimeAgcPrefix -
	runtimeAgcPrefix = "agc"
	// runtimeAgcCloudObjectNode2015 -
	runtimeAgcCloudObjectNode2015 = "agccloudobjectnodejs20.15"
	// runtimeAgcNode2015 -
	runtimeAgcNode2015 = "agcnodejs20.15"
	// runtimeAgcJava8 -
	runtimeAgcJava8 = "agcjava8"
	// runtimeAgcPython312 -
	runtimeAgcPython312 = "agcpython3.12"
)

var agcRuntimeLanguage = map[string]string{
	runtimeAgcCloudObjectNode2015: "nodejs20.15",
	runtimeAgcNode2015:            "nodejs20.15",
	runtimeAgcJava8:               "java8",
	runtimeAgcPython312:           "python3.12",
}

// GetRuntimeLanguage -
func GetRuntimeLanguage(runtimeName string) string {
	if strings.HasPrefix(runtimeName, runtimeAgcPrefix) {
		return agcRuntimeLanguage[runtimeName]
	}
	return runtimeName
}
