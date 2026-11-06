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

// Package urnutils contains URN element definitions and tools
package urnutils

import (
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"

	"functioncore/pkg/common/logger/log"
)

// An example of a function URN: <ProductID>:<RegionID>:<BusinessID>:<TenantID>:<FunctionSign>:<FunctionName>:<Version>
// Indices of elements in BaseURN
const (
	// ProductIDIndex is the index of the product ID in a URN
	ProductIDIndex = iota
	// RegionIDIndex is the index of the region ID in a URN
	RegionIDIndex
	// BusinessIDIndex is the index of the business ID in a URN
	BusinessIDIndex
	// TenantIDIndex is the index of the tenant ID in a URN
	TenantIDIndex
	// FunctionSignIndex is the index of the product ID in a URN
	FunctionSignIndex
	// FunctionNameIndex is the index of the product name in a URN
	FunctionNameIndex
	// VersionIndex is the index of the version in a URN
	VersionIndex
	// URNLenWithVersion is the normal URN length with a version
	URNLenWithVersion
)

// An example of a function functionkey: <TenantID>/<FunctionName>/<Version>
const (
	// TenantIDIndex is the index of the tenant ID in a functionkey
	TenantIDIndexKey = iota
	// FunctionNameIndex is the index of the function name in a functionkey
	FunctionNameIndexKey
	// VersionIndex is the index of the version in a functionkey
	VersionIndexKey
)

const (
	urnLenWithoutVersion = URNLenWithVersion - 1
	// URNSep is a URN separator of functions
	URNSep = ":"
	// FunctionKeySep is a functionkey separator of functions
	FunctionKeySep = "/"
	// DefaultURNProductID is the default product ID of a URN
	DefaultURNProductID = "sn"
	// DefaultURNRegion is the default region of a URN
	DefaultURNRegion = "cn"
	// DefaultURNFuncSign is the default function sign of a URN
	DefaultURNFuncSign  = "function"
	defaultURNLayerSign = "layer"
	// DefaultURNVersion is the default version of a URN
	DefaultURNVersion = "$latest"
	// DefaultVersion is the default version for fusion
	DefaultVersion = "latest"
	anonymization  = "*"
	anonymizeLen   = 3

	// BranchAliasPrefix is used to remove "!" from aliasing rules at the begining of "!"
	BranchAliasPrefix = 1
	// BranchAliasRule is an aliased rule that begins with an "!"
	BranchAliasRule        = "!"
	functionNameStartIndex = 2
	serviceNameIndex       = 1
	funcNameMinLen         = 3
	defaultFunctionMaxLen  = 128

	// FuncETCDKeyLength define the correct ETCD key length
	FuncETCDKeyLength = 11
	functionKeySepCh  = '/'

	funcInfoMinLen     = 3
	funcInfoWithResLen = 4
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
	defaultBusiness = "yrk"
)

const (
	funcNamePrefix = "0@default@"
	workerIDPrefix = "defaultaz-#-"

	funcURNArrLen = 7
)

var (
	funcNameRegexp = regexp.MustCompile("^[a-zA-Z]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$")
)

// BaseURN contains elements of a product URN. It can expand to FunctionURN, LayerURN and WorkerURN
type BaseURN struct {
	ProductID  string
	RegionID   string
	BusinessID string
	TenantID   string
	TypeSign   string
	Name       string
	Version    string
}

// String serializes elements of function URN struct to string
func (p *BaseURN) String() string {
	urn := fmt.Sprintf("%s:%s:%s:%s:%s:%s", p.ProductID, p.RegionID,
		p.BusinessID, p.TenantID, p.TypeSign, p.Name)
	if p.Version != "" {
		return fmt.Sprintf("%s:%s", urn, p.Version)
	}
	return urn
}

// ParseFrom parses elements from a function URN
func (p *BaseURN) ParseFrom(urn string) error {
	elements := strings.Split(urn, URNSep)
	urnLen := len(elements)
	if urnLen < urnLenWithoutVersion || urnLen > URNLenWithVersion {
		return fmt.Errorf("failed to parse urn from %s, invalid length", urn)
	}
	p.ProductID = elements[ProductIDIndex]
	p.RegionID = elements[RegionIDIndex]
	p.BusinessID = elements[BusinessIDIndex]
	p.TenantID = elements[TenantIDIndex]
	p.TypeSign = elements[FunctionSignIndex]
	p.Name = elements[FunctionNameIndex]
	if urnLen == URNLenWithVersion {
		p.Version = elements[VersionIndex]
	}
	return nil
}

// StringWithoutVersion return string without version
func (p *BaseURN) StringWithoutVersion() string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s", p.ProductID, p.RegionID,
		p.BusinessID, p.TenantID, p.TypeSign, p.Name)
}

// GetFunctionInfo collects function information from a URN
func GetFunctionInfo(urn string) (BaseURN, error) {
	var parsedURN BaseURN
	if err := parsedURN.ParseFrom(urn); err != nil {
		log.GetLogger().Errorf("error while parsing an URN: %s", err.Error())
		return BaseURN{}, errors.New("parsing an URN error")
	}
	return parsedURN, nil
}

// GetFuncInfoWithVersion collects function information and distinguishes if the URN contains a version
func GetFuncInfoWithVersion(urn string) (BaseURN, error) {
	parsedURN, err := GetFunctionInfo(urn)
	if err != nil {
		return parsedURN, err
	}
	if parsedURN.Version == "" {
		log.GetLogger().Errorf("incorrect URN length: %s", Anonymize(urn))
		return parsedURN, errors.New("incorrect URN length, no version")
	}
	return parsedURN, nil
}

// ParseAliasURN is used to remove "!" from the beginning of the alias
func ParseAliasURN(aliasURN string) string {
	elements := strings.Split(aliasURN, URNSep)
	if len(elements) == URNLenWithVersion {
		if strings.HasPrefix(elements[VersionIndex], BranchAliasRule) {
			elements[VersionIndex] = elements[VersionIndex][BranchAliasPrefix:]
		}
		return strings.Join(elements, ":")
	}
	return aliasURN
}

// GetAlias returns an alias
func (p *BaseURN) GetAlias() string {
	if p.Version == DefaultURNVersion {
		return ""
	}
	if _, err := strconv.Atoi(p.Version); err == nil {
		return ""
	}
	return p.Version
}

// GetAliasForFuncBranch returns an alias for function branch
func (p *BaseURN) GetAliasForFuncBranch() string {
	if strings.HasPrefix(p.Version, BranchAliasRule) {
		// remove "!" from the beginning of the alias
		return p.Version[BranchAliasPrefix:]
	}
	return ""
}

// Valid check whether the self-verification function name complies with the specifications.
func (p *BaseURN) Valid() error {
	serviceID, functionName, err := GetFunctionNameAndServiceName(p.Name)
	if err != nil {
		log.GetLogger().Errorf("failed to get serviceID and functionName")
		return err
	}
	if !(funcNameRegexp.MatchString(serviceID) ||
		funcNameRegexp.MatchString(functionName)) {
		errmsg := "failed to match reg%s"
		log.GetLogger().Errorf(errmsg, funcNameRegexp)
		return fmt.Errorf(errmsg, funcNameRegexp)
	}
	if len(serviceID) > defaultFunctionMaxLen || len(functionName) > defaultFunctionMaxLen {
		errmsg := "serviceID or functionName's len is out of range %d"
		log.GetLogger().Errorf(errmsg, defaultFunctionMaxLen)
		return fmt.Errorf(errmsg, defaultFunctionMaxLen)
	}
	return nil
}

// GetFunctionNameAndServiceName returns serviceName and FunctionName
func GetFunctionNameAndServiceName(funcName string) (string, string, error) {
	if strings.HasPrefix(funcName, ServiceIDPrefix) {
		split := strings.Split(funcName, Separator)
		if len(split) < funcNameMinLen {
			log.GetLogger().Errorf("incorrect function name length: %s", len(split))
			return "", "", errors.New("parsing a function name error")
		}
		return split[serviceNameIndex], strings.Join(split[functionNameStartIndex:], Separator), nil
	}
	log.GetLogger().Errorf("incorrect function name: %s", funcName)
	return "", "", errors.New("parsing a function name error")
}

// Anonymize anonymize input str to xxx*xxx
func Anonymize(str string) string {
	if len(str) < anonymizeLen+1+anonymizeLen {
		return anonymization
	}
	return str[:anonymizeLen] + anonymization + str[len(str)-anonymizeLen:]
}

// AnonymizeTenantURN Anonymize tenant info in urn
func AnonymizeTenantURN(urn string) string {
	elements := strings.Split(urn, URNSep)
	urnLen := len(elements)
	if urnLen < urnLenWithoutVersion || urnLen > URNLenWithVersion {
		return urn
	}
	elements[TenantIDIndex] = Anonymize(elements[TenantIDIndex])
	return strings.Join(elements, URNSep)
}

// AnonymizeTenantKey Anonymize tenant info in functionkey
func AnonymizeTenantKey(key string) string {
	pos := strings.IndexByte(key, functionKeySepCh)
	if pos == -1 {
		return key
	}
	substr := key[0:pos]
	return strings.Replace(key, substr, Anonymize(substr), 1)
}

// AnonymizeTenantURNSlice Anonymize tenant info in urn slice
func AnonymizeTenantURNSlice(urns []string) []string {
	var anonymizeUrns []string
	for i := 0; i < len(urns); i++ {
		anonymizeUrn := AnonymizeTenantURN(urns[i])
		anonymizeUrns = append(anonymizeUrns, anonymizeUrn)
	}
	return anonymizeUrns
}

// GetBillingFunctionURN -
func GetBillingFunctionURN(funcVersionURN BaseURN, memory string) string {
	c := &ComplexFuncName{}
	c.ParseFrom(funcVersionURN.Name)
	// "projectID@package@functionName@version@memsize"
	return fmt.Sprintf("%s@%s@%s@%s@%s", funcVersionURN.TenantID, c.ServiceID, c.FuncName,
		funcVersionURN.Version, memory)
}

// GetFuncKey -
func GetFuncKey(tenantID, funcName, funcVersion string) string {
	funcKey := tenantID + "/" + funcName + "/" + funcVersion
	return funcKey
}

// GetFuncKeyWithResource -
func GetFuncKeyWithResource(functionKey string, cpu string, memory string) string {
	functionKeyWithResource := functionKey + "/" + cpu + "-" + memory
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

// GetFuncKeyAndResFromFuncKeyWithResource get function key, cpu and memory from function key with resource
// example: tenantID/funcName/functionVersion/cpu-memory  ==> tenantID/funcName/functionVersion, cpu, memory
func GetFuncKeyAndResFromFuncKeyWithResource(funcKeyWithResource string) (string, string, string) {
	funcInfo := strings.Split(funcKeyWithResource, "/")
	if len(funcInfo) < funcInfoWithResLen {
		return "", "", ""
	}
	resInfo := strings.Split(funcInfo[3], "-")

	return funcInfo[0] + "/" + funcInfo[1] + "/" + funcInfo[2], resInfo[0], resInfo[1]
}

// ParseFuncKey parse funcKey with format "tenantID/funcName/funcVersion" or "tenantID/funcName/funcVersion/CPU-memory"
func ParseFuncKey(funcKey string) (string, string, string) {
	funcInfo := strings.Split(funcKey, "/")
	if len(funcInfo) < funcInfoMinLen {
		return "", "", ""
	}
	return funcInfo[0], funcInfo[1], funcInfo[2]
}

// GetProcessorKey -
func GetProcessorKey(instanceKey string, processorID uint32, version uint64) string {
	return fmt.Sprintf("%s/%d/%d", instanceKey, processorID, version)
}

// CrName CR Name
// [y/z]brief-functionname-version-hash
func CrName(tenant, funcName, version string) string {
	business := defaultBusiness
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

// FnvHash a hash function
func FnvHash(s string) string {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return ""
	}

	// for 2 <= base <= 36. The result uses the lower-case letters 'a' to 'z'
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}

func acquireBrief(business, tenant string) string {
	if len(business) > URNIndexFour {
		business = business[:URNIndexFour]
	}

	var product string
	t := strings.Split(tenant, TenantProductSplitStr)
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

// ExtractFuncName extract funcName
// before: 0@default@funcA
// after: funcA
func ExtractFuncName(funcName string) string {
	if strings.HasPrefix(funcName, funcNamePrefix) {
		return funcName[len(funcNamePrefix):]
	}
	return funcName
}

// FuncKeyWithResToShort funcKeyWithRes to short
// before: 123456789/0@default@funcA/latest/300-128
// after: 123*789/funcA/latest/300-128
func FuncKeyWithResToShort(funcKeyWithRes string) string {
	arr := strings.Split(funcKeyWithRes, "/")
	if len(arr) != funcInfoWithResLen {
		return funcKeyWithRes
	}
	return fmt.Sprintf("%s/%s/%s/%s", Anonymize(arr[0]), ExtractFuncName(arr[1]), arr[2], arr[3])
}

// FuncKeyToShort funcKey to short
// before: 123456789/0@default@funcA/latest
// after: 123*789/funcA/latest
func FuncKeyToShort(funcKey string) string {
	arr := strings.Split(funcKey, "/")
	if len(arr) != funcInfoMinLen {
		return funcKey
	}
	return fmt.Sprintf("%s/%s/%s", Anonymize(arr[0]), ExtractFuncName(arr[1]), arr[2])
}

// ExtractPodName extract podName
// before: defaultaz-#-pod-xxx
// after: pod-xxx
func ExtractPodName(workerID string) string {
	if strings.HasPrefix(workerID, workerIDPrefix) {
		return workerID[len(workerIDPrefix):]
	}
	return workerID
}

// FuncURNtoShort funcURN to short
// before: sn:cn:yrk:123456789:function:0@default@funcA:latest
// after: 123*789/funcA/latest
func FuncURNtoShort(funcURN string) string {
	arr := strings.Split(funcURN, ":")
	if len(arr) != funcURNArrLen {
		return funcURN
	}
	return fmt.Sprintf("%s/%s/%s", Anonymize(arr[3]), ExtractFuncName(arr[5]), arr[6])
}
