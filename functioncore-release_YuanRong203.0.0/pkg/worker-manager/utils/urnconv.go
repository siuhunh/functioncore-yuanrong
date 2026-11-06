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

// Package utils gets function services from URNs
package utils

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

//	An example of a worker-manager URN:
//
// /sn/workers/business/iot/tenant/j0f4413f7b4b4c33be576d432f7ee085/function/functest/version/$latest
// /cn-north-1a/cn-north-1a-#-ws-j0f4413f7b-functest-faaslatest-deployment-55b5f9dcb7-r2dsv
const (
	// URNIndexZero URN index 0
	URNIndexZero = iota
	// URNIndexOne URN index 1
	URNIndexOne
	// URNIndexTwo URN index 2
	URNIndexTwo
	// URNIndexThree URN index 3
	URNIndexThree
	// URNIndexFour URN index 4
	URNIndexFour
	// URNIndexFive URN index 5
	URNIndexFive
	// URNIndexSix URN index 6
	URNIndexSix
	// URNIndexSeven URN index 7
	URNIndexSeven
	// URNIndexEight URN index 8
	URNIndexEight
	// URNIndexNine URN index 9
	URNIndexNine
	// URNIndexTen URN index 10
	URNIndexTen
	// URNIndexEleven URN index 11
	URNIndexEleven
	// URNIndexTwelve URN index 12
	URNIndexTwelve
	// URNIndexThirteen URN index 13
	URNIndexThirteen
	// TriggerEtcdKeyLength is the length of a trigger etcd key
	TriggerEtcdKeyLength
	// VersionLatest default version
	VersionLatest     = "$latest"
	versionLatestConv = "l.s.t"

	// yyrki1fe--l.s.t-4043882318 26 size
	// k8s label max len is 63 63 size
	// 63- 26 = 37 funcName max length
	maxLenOfFuncName = 37
	// URNSep URN separator
	URNSep            = "/"
	functionURNPrefix = "sn:cn"
	k8sLabelLen       = 63
	otherStrLen       = 4
)

// FunctionInfo defines Function Info
type FunctionInfo struct {
	Business string
	Tenant   string
	FuncName string
	Version  string
}

// StatusErrorInfo -
type StatusErrorInfo struct {
	ErrorMsg string `json:"errorMsg"`
}

// CrNameByURN returns a CR name by URN
func CrNameByURN(args ...string) string {
	if len(args) == URNIndexZero {
		return ""
	}

	var business, tenant, funcName, version string
	if len(args) < URNIndexFour && len(args) > URNIndexZero {
		functionInfo := GetFunctionInfoByURN(args[URNIndexZero])
		business, tenant, funcName, version = functionInfo.Business,
			functionInfo.Tenant, functionInfo.FuncName, functionInfo.Version
	} else {
		business, tenant, funcName, version = args[URNIndexZero], args[URNIndexOne],
			args[URNIndexTwo], args[URNIndexThree]
	}

	return CrName(business, tenant, funcName, version)
}

// CrNameByKey return Cr Name By function key
func CrNameByKey(funcKey string) string {
	functionInfo := GetFunctionInfoByKey(funcKey)
	business, tenant, funcName, version := functionInfo.Business, functionInfo.Tenant,
		functionInfo.FuncName, functionInfo.Version

	return CrName(business, tenant, funcName, version)
}

// GetFuncNameByKey -
func GetFuncNameByKey(funcKey string) string {
	functionInfo := GetFunctionInfoByKey(funcKey)
	return functionInfo.FuncName
}

// InitInstancePrefix -
var InitInstancePrefix = "/sn/init/workers/cluster/" + clusterID + "/business/"

// GenerateInitInstanceKey -
func GenerateInitInstanceKey(instance v1alpha1.WorkerInstance) string {
	return InitInstancePrefix + instance.BusinessID + "/tenant/" + instance.TenantID + "/function/" +
		instance.FuncName + "/version/" + VersionRestore(instance.Version) + "/" + commonutils.AzEnv() + "/" +
		instance.InstanceID
}

// GetWorkerKeyByInstance -
func GetWorkerKeyByInstance(instance v1alpha1.WorkerInstance) string {
	return "/sn/workers/business/" + instance.BusinessID + "/tenant/" + instance.TenantID + "/function/" +
		instance.FuncName + "/version/" + VersionRestore(instance.Version) + "/" + commonutils.AzEnv() + "/" +
		instance.InstanceID
}

// GetFunctionTaskWorkerNodeNameKeyByInstance -
func GetFunctionTaskWorkerNodeNameKeyByInstance(instance v1alpha1.WorkerInstance) string {
	return "/sn/workers/business/" + instance.BusinessID + "/tenant/" + instance.TenantID + "/function/" +
		"function-task" + "/version/" + VersionRestore(instance.Version) + "/" + commonutils.AzEnv() + "/" +
		instance.NodeName
}

// GetSNStateWorkerPath -
func GetSNStateWorkerPath(instance v1alpha1.WorkerInstance) string {
	return StateWorkerKey + VersionLatest + "/" + commonutils.AzEnv() + "/" + instance.StateID
}

// WorkerKeyByCr return Worker Key By Cr
func WorkerKeyByCr(funcSvc *faasfunction.FunctionService, instanceID string) string {
	business := funcSvc.Spec.Configuration.BusinessID
	tenant := funcSvc.Spec.Configuration.TenantID
	funcName := funcSvc.Spec.Configuration.FunctionName
	version := funcSvc.Spec.Configuration.FunctionVersion

	path := "/sn/workers/business/" + business + "/tenant/" + tenant + "/function/" +
		funcName + "/version/" + VersionRestore(version) + "/" + commonutils.AzEnv() + "/"

	return path + instanceID
}

// FunctionKeyByCr return Function Key By Cr*faasfunction.FunctionService
func FunctionKeyByCr(funcSvc *faasfunction.FunctionService) string {
	business := funcSvc.Spec.Configuration.BusinessID
	tenant := funcSvc.Spec.Configuration.TenantID
	funcName := funcSvc.Spec.Configuration.FunctionName
	version := funcSvc.Spec.Configuration.FunctionVersion

	return FunctionKey(business, tenant, funcName, version)
}

// GetETCDFuncKeyByPod -
func GetETCDFuncKeyByPod(pod *corev1.Pod) string {
	funcName := GetFullFuncNameByPod(pod)
	return FunctionKey(pod.Labels[LabelBusinessID], pod.Labels[LabelTenantID], funcName,
		pod.Labels[LabelFunctionVersion])
}

// FunctionKey -
func FunctionKey(business, tenant, funcName, version string) string {
	return "/sn/functions/business/" + business + "/tenant/" + tenant + "/function/" + funcName +
		"/version/" + VersionRestore(version)
}

// SilentFunctionKey -
func SilentFunctionKey(business, tenant, funcName, version string) string {
	return "/silent/sn/functions/business/" + business + "/tenant/" + tenant + "/function/" + funcName +
		"/version/" + VersionRestore(version)
}

// VersionConvForBranch return version Conv for branch
func VersionConvForBranch(v string) string {
	// cannot contain _. If the version cr contains _, replace it with -.
	return strings.ReplaceAll(v, "_", "-")
}

// VersionRestore return version Latest
func VersionRestore(v string) string {
	if v == versionLatestConv {
		return VersionLatest
	}
	return v
}

func acquireBrief(business, tenant string) string {
	if len(business) > URNIndexFour {
		business = business[:URNIndexFour]
	}

	var product string
	t := strings.Split(tenant, urnutils.TenantProductSplitStr)
	switch l := len(t); {
	case l == URNIndexTwo:
		tenant = t[URNIndexZero]
		product = t[URNIndexOne]
	case l == URNIndexOne:
		// do nothing, use input tenant
	default:
		tenant = ""
	}

	if len(tenant) > URNIndexFour {
		tenant = tenant[:URNIndexFour]
	}

	if len(product) > URNIndexFour {
		product = product[:URNIndexFour]
	}

	return business + tenant + product
}

// CrName CR Name
// [y/z]brief-functionname-version-hash
func CrName(business, tenant, funcName, version string) string {
	hashStr := genFunctionCRStr(business, tenant, funcName, version)
	crHash := FnvHash(hashStr)
	brief := acquireBrief(business, tenant)
	ver := VersionConvForBranch(version)
	crName := brief + "-" + funcName + "-" + ver + "-" + crHash
	return crName
}

func genFunctionCRStr(business string, tenant string, funcName string, version string) string {
	return business + "-" + tenant + "-" + funcName + "-" + version
}

// GetFunctionInfoByURN sn:cn:yrk:xxxxxxx:function:weatherforecast:1
func GetFunctionInfoByURN(urn string) FunctionInfo {
	var functionInfo FunctionInfo
	urnFields := strings.Split(urn, ":")

	switch len(urnFields) {
	case URNIndexSeven:
		functionInfo.Version = urnFields[URNIndexSix]
		fallthrough
	case URNIndexSix:
		functionInfo.FuncName = urnFields[URNIndexFive]
		functionInfo.Tenant = urnFields[URNIndexThree]
		functionInfo.Business = urnFields[URNIndexTwo]
		return functionInfo
	default:
		return functionInfo
	}
}

// GetFunctionInfoByKey /sn/functions/business/iot/tenant/j6db015e6ed74a1cab8644bc78fb22b0/function/func001/version/$latest
// GetFunctionInfoByKey /sn/workers/business/iot/tenant/b4175533838e4f6bbf38705b0fc8fab2/function/func15mb/version/$latest/cn-north-1a/cn-north-1a-#-ws-b417553383-func15mb-faaslatest-deployment-657cf5dccd-h9sbc
func GetFunctionInfoByKey(key string) FunctionInfo {
	etcdKey := key
	if strings.HasPrefix(key, constants.SilentFuncKeyPrefix) {
		etcdKey = strings.TrimPrefix(key, constants.SilentFuncKeyPrefix)
	}
	keyFields := strings.Split(etcdKey, "/")

	var functionInfo FunctionInfo
	if len(keyFields) != URNIndexEleven && len(keyFields) != URNIndexThirteen {
		return functionInfo
	}

	functionInfo.Business = keyFields[URNIndexFour]
	functionInfo.Tenant = keyFields[URNIndexSix]
	functionInfo.FuncName = keyFields[URNIndexEight]
	functionInfo.Version = keyFields[URNIndexTen]

	return functionInfo
}

// GetFunctionURN builds a function URN
func GetFunctionURN(business, tenant, serviceFuncName string) string {
	return functionURNPrefix + ":" + business + ":" + tenant + ":function:" +
		serviceFuncName + ":" + VersionLatest
}

// GetTenantIDFromURN gets tenantID from function URN
func GetTenantIDFromURN(funcUrn string) string {
	items := strings.Split(funcUrn, ":")
	if len(items) < urnutils.URNLenWithVersion {
		return ""
	}
	return items[urnutils.TenantIDIndex]
}

// GetFuncKeyFromURN gets FuncKey from function URN
// sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@zcy_web:V1
// sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest
func GetFuncKeyFromURN(funcUrn string) string {
	items := strings.Split(funcUrn, ":")
	if len(items) < urnutils.URNLenWithVersion {
		log.GetLogger().Errorf("failed to parse funcURN %s", funcUrn)
		return ""
	}
	factors := strings.Split(items[URNIndexFive], "@")
	if len(factors) <= URNIndexTwo {
		log.GetLogger().Errorf("failed to parse Urn with funcURN: %s", funcUrn)
		return ""
	}
	return items[urnutils.TenantIDIndex] + "@" + factors[URNIndexOne] + "@" + factors[URNIndexTwo] + "@" +
		items[urnutils.VersionIndex]
}

// ProjectIDByKey return projectID By publicNetwork key
func ProjectIDByKey(publicNetworkKey string) (string, error) {
	if !strings.HasPrefix(publicNetworkKey, PublicNetworkPrefix) {
		return "", fmt.Errorf("invalid public network key %s in etcd", publicNetworkKey)

	}
	keyFields := strings.Split(publicNetworkKey, "/")
	if len(keyFields) != URNIndexFour {
		return "", fmt.Errorf("invalid public network key %s length in etcd", publicNetworkKey)
	}

	return keyFields[URNIndexThree], nil
}
