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

// Package utils interfaces of server
package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
)

// contextKey used for context set string
type contextKey string

// String used for context set string
func (c contextKey) String() string {
	return string(c)
}

var priories = map[string]int{
	"l": LowPriority,
	"m": MediumPriority,
	"h": HighPriority,
}

const (
	funcVersionURNPrefix = "sn:cn:yrk"
	funcInfoMinLen       = 3
	funcNameLen          = 2
	resourceLen          = 2
	cpuIndex             = 3
	memoryIndex          = 4
	workerIDLength       = 8

	defaultLenOfDiskSpace  = 2
	diskSpaceIndex         = 1
	defaultLenOfAvailSpace = 4
	availSpaceIndex        = 3

	defaultSessionConcurrency = 1
)

const (
	// LowPriority mark request low priority.
	LowPriority = iota
	// MediumPriority mark request medium priority.
	MediumPriority
	// HighPriority mark request high priority.
	HighPriority
)

var (
	stopCh <-chan struct{}

	sessionConcurrencyFactor = 10
)

func init() {
	factorStr := os.Getenv("SESSION_CONCURRENCY_FACTOR")
	if factorStr != "" {
		num, err := strconv.Atoi(factorStr)
		if err != nil {
			log.GetLogger().Warnf("failed to parse env "+
				"SESSION_CONCURRENCY_FACTOR %s and will use default factor 10, err: %s", factorStr, err.Error())
			return
		}
		if num > 0 {
			sessionConcurrencyFactor = num
			log.GetLogger().Infof("succeed to set session concurrency factor %d", sessionConcurrencyFactor)
		}
	}
}

// SetGlobalStopCh called at initialization
func SetGlobalStopCh(ch <-chan struct{}) {
	stopCh = ch
}

// GetGlobalStopCh -
func GetGlobalStopCh() <-chan struct{} {
	return stopCh
}

// GetFuncVersion get version: "sn:cn:yrk:i1fe5394:function:functionB:$latest"
func GetFuncVersion(key string) string {
	strs := strings.Split(key, ":")
	if len(strs) != common.URNLen {
		return ""
	}
	return strs[common.URNFuncVersionPosition]
}

// GetFuncVersionURN get version: "sn:cn:yrk:i1fe5394:function:functionB:$latest"
func GetFuncVersionURN(tenantID, functionName, functionVersion string) string {
	return funcVersionURNPrefix + ":" + tenantID + ":" + "function" + ":" + functionName + ":" + functionVersion
}

// GetFuncNameAndVersion get functionName and aliasName/version: "functionB:aliastest"
func GetFuncNameAndVersion(key string) (string, string) {
	strs := strings.Split(key, ":")
	var functionName, aliasName string
	if len(strs) == funcNameLen {
		functionName = strs[0]
		aliasName = strs[1]
	} else if len(strs) == 1 {
		functionName = strs[0]
	}
	return functionName, aliasName
}

// GetAliasURN get aliasURN: "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:test007:alias007"
func GetAliasURN(tenantID, functionName, aliasName string) string {
	return funcVersionURNPrefix + ":" + tenantID + ":" + "function" + ":" + functionName + ":" + aliasName
}

// IsAliasForFuncBranch judge the given version if is an alias
func IsAliasForFuncBranch(version string) bool {
	return strings.HasPrefix(version, urnutils.BranchAliasRule)
}

// ContextWithTimeout set time and kv-string in context, used with ContextWithTimeout
func ContextWithTimeout(ctx context.Context,
	timeout time.Duration, key string, value string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return context.WithValue(ctx, contextKey(key), value), cancel
}

// ContextWithValueString set kv-string in context, used with ContextValueString
func ContextWithValueString(ctx context.Context, key string, value string) context.Context {
	return context.WithValue(ctx, contextKey(key), value)
}

// ContextValueString used with ContextWithValueString
func ContextValueString(ctx context.Context, key string) string {
	value := ctx.Value(contextKey(key))
	if value, ok := value.(string); ok {
		return value
	}
	return ""
}

// GenerateWorkerInfoStr -
func GenerateWorkerInfoStr(funcKeyWithResource, workerAddress, ownerIP string, isDirectFunc,
	hasInitializer bool) string {
	data, err := json.Marshal(common.WorkerInfo{
		FuncKeyWithResource: funcKeyWithResource,
		WorkerAddress:       workerAddress,
		OwnerIP:             ownerIP,
		IsDirectFunc:        isDirectFunc,
		HasInitializer:      hasInitializer,
	})
	if err != nil {
		return ""
	}
	return string(data)
}

// ParseFromWorkerInfoStr get FuncKeyWithResource, WorkerAddress, ownerIP
func ParseFromWorkerInfoStr(workerInfoStr string) *common.WorkerInfo {
	workerInfo := &common.WorkerInfo{}
	err := json.Unmarshal([]byte(workerInfoStr), workerInfo)
	if err != nil {
		return nil
	}
	return workerInfo
}

// AddToUserData add key value to user data in bus server HookArgs
func AddToUserData(userData, key, value string) (string, error) {
	if len(userData) == 0 {
		m := map[string]string{key: value}
		data, err := json.Marshal(m)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	m := make(map[string]string, 1)
	err := json.Unmarshal([]byte(userData), &m)
	if err != nil {
		return "", err
	}
	m[key] = value
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// GetFromUserData get  value from user data in bus server HookArgs by key
func GetFromUserData(userData, key string) (string, error) {
	if len(userData) == 0 {
		return "", errors.New("user data is empty")
	}
	m := make(map[string]string, 1)
	err := json.Unmarshal([]byte(userData), &m)
	if err != nil {
		return "", err
	}
	value, ok := m[key]
	if !ok {
		return "", fmt.Errorf("key:%s is not exists", key)
	}

	return value, nil
}

// GetUserData get  value from user data in bus server HookArgs by key
func GetUserData(userData string) (map[string]string, error) {
	if len(userData) == 0 {
		return nil, errors.New("user data is empty")
	}
	m := make(map[string]string, 1)
	err := json.Unmarshal([]byte(userData), &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// DelFromUserData del key from userData
func DelFromUserData(userData, key string) string {
	m, err := GetUserData(userData)
	if err != nil {
		return userData
	}
	delete(m, key)
	value, err := json.Marshal(m)
	if err != nil {
		return userData
	}
	return string(value)
}

// GetFuncKey -
func GetFuncKey(tenantID, funcName, funcVersion string) string {
	funcKey := tenantID + "/" + funcName + "/" + funcVersion
	return funcKey
}

// GetFuncKeyWithResource gets function key with resource.
// tenantID/funcName/functionVersion/cpu-memory
func GetFuncKeyWithResource(functionKey string, insResource types.InstanceResource) string {
	functionKeyWithResource := functionKey + "/" + insResource.CPU + "-" + insResource.Memory
	return functionKeyWithResource
}

// GetFuncKeyFromFuncKeyWithResource gets function key from function key with resource
// example: tenantID/funcName/functionVersion/cpu-memory  ==> tenantID/funcName/functionVersion
func GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource string) string {
	funcInfo := strings.Split(funcKeyWithResource, "/")
	if len(funcInfo) < funcInfoMinLen {
		return ""
	}

	return funcInfo[0] + "/" + funcInfo[1] + "/" + funcInfo[2]
}

// GetResourceFromFuncKeyWithResource to get cpu and memory from functionKeyWithResource
func GetResourceFromFuncKeyWithResource(funcKeyWithResource string) *types.InstanceResource {
	funcInfo := strings.Split(funcKeyWithResource, "/")
	if len(funcInfo) != funcInfoMinLen+1 {
		return nil
	}

	resourceInfo := strings.Split(funcInfo[3], "-")
	if len(resourceInfo) != resourceLen {
		return nil
	}

	return &types.InstanceResource{CPU: resourceInfo[0], Memory: resourceInfo[1]}
}

// GetMemoryFromFuncKeyResource to get memory from Resource
func GetMemoryFromFuncKeyResource(resource *types.InstanceResource) int {
	if resource == nil {
		return 0
	}
	memoryStr := resource.Memory
	memory, err := strconv.Atoi(memoryStr)
	if err != nil {
		log.GetLogger().Errorf("failed to atoi, memoryStr: %s, err: %s", memoryStr, err.Error())
		return 0
	}
	return memory
}

// ParseFuncKey parse funcKey with format "tenantID/funcName/funcVersion" or "tenantID/funcName/funcVersion/CPU-memory"
func ParseFuncKey(funcKey string) (string, string, string) {
	funcInfo := strings.Split(funcKey, "/")
	if len(funcInfo) < funcInfoMinLen {
		return "", "", ""
	}
	return funcInfo[0], funcInfo[1], funcInfo[2]
}

// CheckIP -
func CheckIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	return true
}

// CheckPort -
func CheckPort(portStr string) bool {
	_, err := regexp.MatchString("^[0-9]*$", portStr)
	if err != nil {
		return false
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}

	if port < common.MinPort || port > common.MaxPort {
		return false
	}
	return true
}

// GetPriority parse http header and return priority of this request.
func GetPriority(priority string) int {
	if val, ok := priories[priority]; ok {
		return val
	}
	return 0
}

// GetCPUMemoryFromInstanceID get cpu and memory
// defaultaz-#-pool23-400-256-fusion-7ddf8c485d-mcmpr
func GetCPUMemoryFromInstanceID(instanceID string) (string, string) {
	split := strings.Split(instanceID, "-")
	if len(split) < workerIDLength {
		return "", ""
	}
	return split[cpuIndex], split[memoryIndex]
}

// BuildFunctionProperty build function property from urn
// if urn is not valid, return err
func BuildFunctionProperty(urn string, demandInstance string, traceID string) (*common.FunctionProperty, error) {
	strs := strings.Split(urn, ":")
	if len(strs) != common.URNLen {
		return nil, fmt.Errorf("invalid function URN: %s traceID %s", urn, traceID)
	}
	dst := &common.FunctionProperty{
		TenantID:        strs[common.URNTenantPosition],
		FunctionName:    strs[common.URNFuncNamePosition],
		FunctionVersion: strs[common.URNFuncVersionPosition],
		FutureID:        traceID,
	}
	if demandInstance != "" {
		dst.InstanceID = demandInstance
	}
	return dst, nil
}

// GetCPUFlagList returns a sorted list of CPU flags.
func GetCPUFlagList() []string {
	cmd := exec.Command("bash", "-c", "cat /proc/cpuinfo | grep flags | awk 'NR==1{print}' | awk -F ':' '{print $2}'")
	output, err := cmd.Output()
	if err != nil {
		log.GetLogger().Errorf("Error get cpu flags: %s", err.Error())
		return nil
	}
	log.GetLogger().Debugf("get cpu flags: %s", string(output))
	flags := strings.Fields(string(output))
	sort.Strings(flags)
	return flags
}

// CheckHasSnError check if a snerror contains error
func CheckHasSnError(err snerror.SNError) bool {
	if err == nil {
		return false
	}
	return err.Code() != proxyerrors.StateOKCode
}

// GetMirrorDiskSpace -
func GetMirrorDiskSpace() (string, error) {
	var cmd *exec.Cmd
	if engine.IsContainerd() {
		cmd = exec.Command("df", "-hl", "/var/lib/containerd")
	} else {
		cmd = exec.Command("df", "-hl", "/var/lib/docker")
	}
	if cmd == nil {
		return "", errors.New("failed to init cmd")
	}
	stdout, err := cmd.Output()
	if err != nil {
		log.GetLogger().Warnf("failed to get df cmd output: %s", err.Error())
		return "", err
	}
	lines := strings.Split(string(stdout), "\n")
	if len(lines) < defaultLenOfDiskSpace {
		log.GetLogger().Warnf("invalid df cmd output")
		return "", errors.New("invalid df cmd output")
	}

	values := strings.Fields(lines[diskSpaceIndex])
	data := make(map[int]string)
	for i, key := range values {
		data[i] = key
	}

	if len(data) < defaultLenOfAvailSpace {
		log.GetLogger().Warnf("invalid avail space output")
		return "", errors.New("invalid avail space output")
	}
	return data[availSpaceIndex], nil
}

// GetSessionConcurrency calculate session concurrency
func GetSessionConcurrency(concurrentNum int) int {
	if concurrentNum <= 0 || concurrentNum > math.MaxUint32 {
		return defaultSessionConcurrency
	}
	result := concurrentNum / sessionConcurrencyFactor
	if result <= 0 {
		return defaultSessionConcurrency
	}
	return result
}
