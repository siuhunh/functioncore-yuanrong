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

package tools

import (
	"fmt"
	"path/filepath"
	"strings"

	"functioncore/pkg/common/logger/log"
)

var runtimeExistingDir = map[string]struct{}{
	"bin":   {},
	"dev":   {},
	"etc":   {},
	"home":  {},
	"lib":   {},
	"lib64": {},
	"media": {},
	"mnt":   {},
	"opt":   {},
	"proc":  {},
	"root":  {},
	"run":   {},
	"sbin":  {},
	"srv":   {},
	"sys":   {},
	"tmp":   {},
	"usr":   {},
	"var":   {},
}

// ExtractFirstLevelDir returns func first level dir
func ExtractFirstLevelDir(logPath, funcVersionURN, podName string) (string, bool) {
	cleanLogPath := filepath.Clean(logPath)
	if !filepath.IsAbs(cleanLogPath) {
		log.GetLogger().Warnf("logPath is not absolute path, logPath: %s, funcVersionURN: %s, podName: %s",
			logPath, funcVersionURN, podName)
		return "", false
	}
	dirArr := strings.Split(cleanLogPath[1:], "/")
	dir := dirArr[0]
	if len(dir) == 0 {
		log.GetLogger().Warnf("length of logPath root directory is 0, logPath: %s, funcVersionURN: %s, "+
			"podName: %s", logPath, funcVersionURN, podName)
		return "", false
	}
	if strings.Contains(dir, ".") {
		log.GetLogger().Warnf("logPath root directory is /, logPath: %s, funcVersionURN: %s, podName: %s",
			logPath, funcVersionURN, podName)
		return "", false
	}
	if _, ok := runtimeExistingDir[dir]; ok {
		log.GetLogger().Warnf("logPath root directory already exists, logPath: %s, "+
			"funcVersionURN: %s, podName: %s", logPath, funcVersionURN, podName)
		return "", false
	}
	return fmt.Sprintf("/%s", dir), true
}
