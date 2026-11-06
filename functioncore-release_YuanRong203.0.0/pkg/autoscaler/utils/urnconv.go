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

// Package utils defines utils used for autoscaler
package utils

import (
	"hash/fnv"
	"strconv"
	"strings"

	"functioncore/pkg/common/urnutils"
)

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
	// DecimalBase -
	DecimalBase = 10
)

// FunctionInfo defines Function Info
type FunctionInfo struct {
	Business string
	Tenant   string
	FuncName string
	Version  string
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

// CrNameByKey return Cr Name By function key
func CrNameByKey(funcKey string) string {
	functionInfo := GetFunctionInfoByKey(funcKey)
	business, tenant, funcName, version := functionInfo.Business, functionInfo.Tenant,
		functionInfo.FuncName, functionInfo.Version

	return CrName(business, tenant, funcName, version)
}

// GetFunctionInfoByKey /sn/functions/business/iot/tenant/j6db015e6ed74a1cab8644bc78fb22b0/function/func001/version/$latest
// GetFunctionInfoByKey /sn/workers/business/iot/tenant/b4175533838e4f6bbf38705b0fc8fab2/function/func15mb/version/$latest/cn-north-1a/cn-north-1a-#-ws-b417553383-func15mb-faaslatest-deployment-657cf5dccd-h9sbc
func GetFunctionInfoByKey(key string) FunctionInfo {
	var functionInfo FunctionInfo
	keyFields := strings.Split(key, "/")

	if len(keyFields) != URNIndexEleven && len(keyFields) != URNIndexThirteen {
		return functionInfo
	}

	functionInfo.Business = keyFields[URNIndexFour]
	functionInfo.Tenant = keyFields[URNIndexSix]
	functionInfo.FuncName = keyFields[URNIndexEight]
	functionInfo.Version = keyFields[URNIndexTen]

	return functionInfo
}

// GetClusterByInstanceKey GetFunctionInfoByKey
// /instances/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}
// return clusterName and funcSvcID
func GetClusterByInstanceKey(key string) (string, string) {
	keyFields := strings.Split(key, "/")

	if len(keyFields) != URNIndexTwelve {
		return "", ""
	}

	business := keyFields[URNIndexThree]
	tenant := keyFields[URNIndexSeven]
	funcName := keyFields[URNIndexNine]
	version := keyFields[URNIndexEleven]

	return keyFields[5], CrName(business, tenant, funcName, version)
}

// GetFuncSvcIDByCaeAvailableClusterKey GetFuncSvcIDByCaeAvailableClusterKey
// format : /cae/avaiablecluster/business/yrk/tenant/{tenantID}/function/{funcURN}/version/{version}
// note :
// return clusterName and funcSvcID
func GetFuncSvcIDByCaeAvailableClusterKey(key string) string {
	keyFields := strings.Split(key, "/")

	if len(keyFields) != URNIndexEleven {
		return ""
	}

	business := keyFields[URNIndexFour]
	tenant := keyFields[URNIndexSix]
	funcName := keyFields[URNIndexEight]
	version := keyFields[URNIndexTen]

	return CrName(business, tenant, funcName, version)
}

// CrName CR Name
// [y/z]brief-functionname-version-hash
func CrName(business, tenant, funcName, version string) string {
	hashStr := genFunctionCRStr(business, tenant, funcName, version)
	crHash := FunctionHash(hashStr)
	brief := acquireBrief(business, tenant)
	ver := VersionConvForBranch(version)
	crName := brief + "-" + funcName + "-" + ver + "-" + crHash
	return crName
}

func genFunctionCRStr(business string, tenant string, funcName string, version string) string {
	return business + "-" + tenant + "-" + funcName + "-" + version
}

// FunctionHash a hash function
func FunctionHash(s string) string {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return ""
	}

	// for 2 <= base <= 36. The result uses the lower-case letters 'a' to 'z'
	return strconv.FormatUint(uint64(h.Sum32()), DecimalBase)
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

// VersionConvForBranch return version Conv for branch
func VersionConvForBranch(v string) string {
	// cannot contain _. If the version cr contains _, replace it with -.
	return strings.ReplaceAll(v, "_", "-")
}
