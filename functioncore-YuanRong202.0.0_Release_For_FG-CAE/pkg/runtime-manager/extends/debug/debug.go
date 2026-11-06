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

// Package debug is used for parsing debug logs
package debug

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"functioncore/pkg/common/logger/log"
	workerconfig "functioncore/pkg/worker/config"
)

// extractArgs extract debug args
func extractArgs(envMap map[string]interface{}) (string, string) {
	var arg1, arg2 string
	if value, ok := envMap["JAVA_TOOL_OPTIONS"]; ok {
		if options, check := value.(string); check {
			arg1 = "JAVA_TOOL_OPTIONS=" + options
		}
	} else if nodejsValue, check := envMap["NODE_OPTIONS"]; check {
		if options, checks := nodejsValue.(string); checks {
			arg1 = "NODE_OPTIONS=" + options
		}
	}
	if value, ok := envMap["MARS_OUTPUT"]; ok {
		if output, check := value.(string); check {
			arg2 = "MARS_OUTPUT=" + output
		}
	}
	return arg1, arg2
}

func extractDebugTagReqContainsJavaOrNode(conf *workerconfig.Configuration) (string, map[string]interface{}) {
	var debugger string
	var envMap map[string]interface{}
	envMap = conf.RuntimeConfig.TraceEnv
	if value, ok := envMap["DEBUGGER_TAG"]; ok {
		debugger, ok = value.(string)
		if !ok {
			return "", nil
		}
	}
	log.GetLogger().Infof("request debugger is: %s", debugger)
	return debugger, envMap
}

// ExtractDebugTag -
func ExtractDebugTag(conf *workerconfig.Configuration, currentVersion string) (string, map[string]interface{}) {
	enableTag := os.Getenv("TRACE_DISABLE")
	if conf.RuntimeConfig.EnvKey == "" && enableTag != "DISABLE" {
		if strings.Contains(currentVersion, "java") || strings.Contains(currentVersion, "node") {
			return extractDebugTagReqContainsJavaOrNode(conf)
		}
	}
	return "", nil
}

// GetDebugPara -
func GetDebugPara(debugger string, envMap map[string]interface{}) ([]string, error) {
	if debugger == "" {
		return []string{}, nil
	}
	path, err := filepath.Abs(os.Getenv("DEPLOY_DIR"))
	if err != nil {
		return []string{}, errors.New("failed to get absolute path: " + err.Error())
	}

	cmd := exec.Command("mv", path+"/mars4j", "/tmp")
	err = cmd.Run()
	if err != nil {
		return []string{}, errors.New("failed to move debugger plugins: " + err.Error())
	}
	var debugEnv []string
	arg1, arg2 := extractArgs(envMap)
	debugEnv = append(append(debugEnv, arg1), arg2)
	err = os.Setenv("TRACE_DISABLE", "DISABLE")
	if err != nil {
		return []string{}, errors.New("failed to set disable trace: " + err.Error())
	}
	log.GetLogger().Infof("begin to restart debugger runtime process, debug tag is: %s, "+
		"debug env is %+v", debugger, debugEnv)
	return debugEnv, nil
}
