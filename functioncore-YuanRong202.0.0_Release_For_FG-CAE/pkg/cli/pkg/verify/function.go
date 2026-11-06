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

// Package verify is verification of function params accuracy
package verify

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
)

const (
	// functionNameRegex 函数名正则表达式 可包含字母、数字、下划线和中划线，以大/小写字母开头，以字母或数字结尾
	functionNameRegex = "^[a-zA-Z]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$"
	// functionNameLengthLimit 函数名长度不超过32个字符
	functionNameLengthLimit = 60
	// instanceIDRegex instanceID regex
	instanceIDRegex = "^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$"
	// VersionRegex version regex 以字母/数字开头和结尾，可包含字母、数字、中划线、下划线和点，长度不超过32个字符
	VersionRegex = "^latest$|^[a-zA-Z0-9]([a-zA-Z0-9_-]*\\.)*[a-zA-Z0-9_-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$"
	// VersionLengthLimit 函数版本长度不超过32个字符
	VersionLengthLimit = 32
	// EnvLengthLimit limit total size of user's env variable, current limit is 4096 byte
	EnvLengthLimit          = 4 * (1 << 10)
	referenceLayerSplitSize = 2
)

const (
	// CustomResourceNameLenLimit Maximum length of the resource name
	CustomResourceNameLenLimit = 256
	// CustomResourceValueMin The resource maximum
	CustomResourceValueMin = 1
	// CustomResourceValueMax The resource minimum
	CustomResourceValueMax = 1000000
)

var (
	// ErrFunctionNameRequired function name required
	ErrFunctionNameRequired = errors.New("functionName is required and not blank")
	// ErrFunctionNameFormat function name incorrect
	ErrFunctionNameFormat = errors.New("there is a syntax mistake in the function name. " +
		"The value must start with uppercase or lowercase letter, and end with a letter or a number. " +
		"It can contain letters, numbers, underscores, and hyphens, with a maximum length of 60 characters")
	// ErrInstanceIDRequired instance id required
	ErrInstanceIDRequired = errors.New("instanceID is required and not blank")
	// ErrInstanceIDFormat instance id incorrect
	ErrInstanceIDFormat = errors.New("there is a syntax mistake in the instance id. ")
	// ErrFunctionVersion function version num error
	ErrFunctionVersion = errors.New("the function version is incorrect. " +
		"It should be latest or start and end with a letter/number, and can include letters, " +
		"numbers, hyphens, underscores, and dots, with a length of no more than 32 characters")
)

// timeout
const (
	TimeoutMinValue = 1
	TimeoutMaxValue = 100 * 24 * 3600
)

// SystemReservedFieldsEnum YuanRong system reserved fields
var SystemReservedFieldsEnum = []string{"FAAS_FUNCTION_NAME",
	"FAAS_FUNCTION_VERSION",
	"FAAS_FUNCTION_BUSINESS",
	"FAAS_FUNCTION_TENANTID",
	"FAAS_FUNCTION_USER_FILE_PATH",
	"FAAS_FUNCTION_USER_PATH_LIMITS",
	"FAAS_FUNCTION_DEPLOY_DIR",
	"FAAS_LAYER_DEPLOY_DIR",
	"FAAS_FUNCTION_TIMEOUT",
	"FAAS_FUNCTION_MEMORY",
	"FAAS_FUNCTION_REGION",
	"FAAS_FUNCTION_TIMEZONE",
	"FAAS_FUNCTION_LANGUAGE",
	"FAAS_FUNCTION_LD_LIBRARY_PATH",
	"FAAS_FUNCTION_NODE_PATH",
	"FAAS_FUNCTION_PYTHON_PATH",
	"FAAS_FUNCTION_JAVA_PATH",
}

// MatchFunctionName verify functionName is match functionNameRegex
func MatchFunctionName(functionName string) bool {
	isMatch, err := regexp.MatchString(functionNameRegex, functionName)
	if err != nil {
		return false
	}
	if len(functionName) > functionNameLengthLimit {
		return false
	}
	return isMatch
}

// CheckFunctionName check functionName is empty and correct
func CheckFunctionName(functionName string) error {
	if functionName == "" {
		return ErrFunctionNameRequired
	}
	if !MatchFunctionName(functionName) {
		return ErrFunctionNameFormat
	}
	return nil
}

// MatchInstanceID verify instanceID is match instanceIDRegex
func MatchInstanceID(instanceID string) bool {
	isMatch, err := regexp.MatchString(instanceIDRegex, instanceID)
	if err != nil {
		return false
	}
	return isMatch
}

// CheckInstanceID check instanceID is empty and correct
func CheckInstanceID(instanceID string) error {
	if instanceID == "" {
		return ErrInstanceIDRequired
	}
	if !MatchInstanceID(instanceID) {
		return ErrInstanceIDFormat
	}
	return nil
}

// CheckRuntime check runtime
func CheckRuntime(runtime string) error {
	if !RuntimeExists(runtime) {
		return fmt.Errorf("runtime is not in %s", config.RuntimeEnum)
	}
	return nil
}

// RuntimeExists check runtime
func RuntimeExists(runtime string) bool {
	if IsContain(config.RuntimeEnum, runtime) {
		return true
	}
	return false
}

// IsContain check item is one of the array
func IsContain(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

// CheckMemory verify Memory is right
func CheckMemory(memory int) bool {
	for mem := range config.DeploymentInfoEnum {
		if mem == memory {
			return true
		}
	}
	return false
}

// CheckTimeout verify Timeout is right
func CheckTimeout(timeout int) bool {
	if timeout < TimeoutMinValue || timeout > TimeoutMaxValue {
		return false
	}
	return true
}

// CheckENV verify function environment input
func CheckENV(environment map[string]string) error {
	currentSize := 0
	for envKey, envValue := range environment {
		if envValue == "" {
			return fmt.Errorf("environment %s does not contain the value or the value is empty", envKey)
		}
		currentSize += len(envKey) + len(envValue)
		if currentSize > EnvLengthLimit {
			return fmt.Errorf("env: total size reach limit of %d bytes", EnvLengthLimit)
		}
		if IsContain(SystemReservedFieldsEnum, envKey) {
			return fmt.Errorf("env: %s is system reserved fields", envKey)
		}
	}
	return nil
}

// CheckCustomResources verify function environment input
func CheckCustomResources(customResources map[string]float64) error {
	for resourceKey, resourceValue := range customResources {
		if len(resourceKey) > CustomResourceNameLenLimit {
			return fmt.Errorf("customResource Name reach limit of %d bytes", CustomResourceNameLenLimit)
		}
		if len(strings.Split(fmt.Sprintf("%v", resourceValue), ".")) > 1 {
			return errors.New("the type of customResources's value is not INT")
		}
		if resourceValue < CustomResourceValueMin || resourceValue > CustomResourceValueMax {
			return fmt.Errorf("customResource should in [%d-%d]", CustomResourceValueMin, CustomResourceValueMax)
		}
	}
	return nil
}

// CheckVersion 检查版本号
func CheckVersion(version string) error {
	isMatch := MatchVersion(version)
	if !isMatch {
		return ErrFunctionVersion
	}
	return nil
}

// MatchVersion 版本号匹配
func MatchVersion(version string) bool {
	isMatch, err := regexp.MatchString(VersionRegex, version)
	if err != nil || !isMatch {
		return false
	}
	if len(version) > VersionLengthLimit {
		return false
	}
	return true
}

// CheckMinInstance 检查最小实例树
func CheckMinInstance(minInstance *float64) error {
	if minInstance == nil {
		return nil
	}
	if len(strings.Split(fmt.Sprintf("%v", *minInstance), ".")) > 1 {
		return errors.New("the type of minInstance's value is not INT")
	}
	if *minInstance < 0 {
		return errors.New("minInstance must be at least 0")
	}
	return nil
}

// CheckMaxInstance 最大实例数
func CheckMaxInstance(maxInstance *float64) error {
	if maxInstance == nil {
		return nil
	}
	if len(strings.Split(fmt.Sprintf("%v", *maxInstance), ".")) > 1 {
		return errors.New("the type of maxInstance's value is not INT")
	}
	if *maxInstance < 1 {
		return errors.New("maxInstance must be at least 1")
	}
	if *maxInstance > config.MaxMaxInstance {
		return fmt.Errorf("maxInstance must be less than or equal to %d", int(config.MaxMaxInstance))
	}
	return nil
}

// CheckConcurrentNum 并发
func CheckConcurrentNum(concurrentNum *float64) error {
	if concurrentNum == nil {
		return nil
	}
	if len(strings.Split(fmt.Sprintf("%v", *concurrentNum), ".")) > 1 {
		return errors.New("the type of concurrentNum's value is not INT")
	}
	if *concurrentNum < 1 {
		return errors.New("concurrentNum must be at least 1")
	}
	if *concurrentNum > constant.MaxConcurrentNum {
		return fmt.Errorf("concurrentNum must be less than or equal to %d", constant.MaxConcurrentNum)
	}
	return nil
}

// IsMinInstanceLargeThanMaxInstance 并发
func IsMinInstanceLargeThanMaxInstance(minInstance, maxInstance *float64) bool {
	if minInstance != nil && maxInstance == nil && *minInstance > constant.DefaultMaxInstance {
		return true
	}
	if minInstance != nil && maxInstance != nil && *minInstance > *maxInstance {
		return true
	}
	return false
}
