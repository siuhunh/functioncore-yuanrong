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

// Package function is function api
package function

import (
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/pkg/builder/common"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/deploy"
	"functioncore/pkg/cli/pkg/verify"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
)

const (
	funNameIndex        = 8
	serviceNameIndex    = 1
	funcVersionIndex    = 10
	defaultServiceSize  = 1
	defaultFunctionSize = 10
	defaultEnvSize      = 10
	decimal             = 10
)

func getFuncKey(projectID, packageStr, functionName, version string) string {
	funcKey := "/sn/functions/business/yrk/tenant/" + projectID +
		"/function/0@" + packageStr + "@" + functionName + "/version/" + version
	return funcKey
}

func getInstanceKey(clusterID, projectID, packageStr, functionName, version string) string {
	instanceKey := "/instances/business/yrk/cluster/" + clusterID + "/tenant/" +
		projectID + "/function/0@" + packageStr + "@" + functionName + "/version/" + version
	return instanceKey
}

// QueryServices -
func QueryServices(c client.Client, tenantID string) ([]string, error) {
	prefixKey := "/sn/functions/business/yrk/tenant/" + tenantID + "/function/0@"
	keys, err := c.MatchKeysByPrefixKey(prefixKey)
	if err != nil {
		return nil, err
	}
	serviceMap := make(map[string]string, defaultServiceSize)
	for _, key := range keys {
		funcName := strings.Split(key, "/")[funNameIndex]
		serviceName := strings.Split(funcName, "@")[serviceNameIndex]
		serviceMap[serviceName] = ""
	}
	services := make([]string, 0, len(serviceMap))
	for k := range serviceMap {
		services = append(services, k)
	}
	return services, nil
}

// QueryFunctionWithVersions query function with versions of service
func QueryFunctionWithVersions(c client.Client, tenantID, service string) (map[string][]string, error) {
	if err := verify.CheckServiceName(service); err != nil {
		return nil, err
	}
	prefixKey := "/sn/functions/business/yrk/tenant/" + tenantID + "/function/0@" + service + "@"
	functionKeys, err := c.MatchKeysByPrefixKey(prefixKey)
	if err != nil {
		return nil, err
	}
	functionWithVersionsMap := make(map[string][]string, defaultFunctionSize)
	for _, key := range functionKeys {
		funcKeySplits := strings.Split(key, "/")
		if len(funcKeySplits) < funcVersionIndex {
			continue
		}
		funcName := funcKeySplits[funNameIndex]
		funcName = strings.Split(funcName, "@")[2]
		version := funcKeySplits[funcVersionIndex]
		if len(functionWithVersionsMap[funcName]) > 0 {
			versions := functionWithVersionsMap[funcName]
			versions = append(versions, version)
			functionWithVersionsMap[funcName] = versions
		} else {
			functionWithVersionsMap[funcName] = []string{version}
		}
	}
	return functionWithVersionsMap, nil
}

// QueryFunctions query functions of service
func QueryFunctions(c client.Client, tenantID, service string) ([]string, error) {
	if err := verify.CheckServiceName(service); err != nil {
		return nil, err
	}
	functionMap, err := QueryFunctionWithVersions(c, tenantID, service)
	if err != nil {
		return nil, err
	}
	functions := make([]string, 0, len(functionMap))
	for k := range functionMap {
		functions = append(functions, k)
	}
	return functions, nil
}

// Delete delete function
func Delete(c client.Client, tenantID, serviceName, functionName string) error {
	if err := verify.CheckServiceName(serviceName); err != nil {
		return err
	}
	if err := verify.CheckFunctionName(functionName); err != nil {
		return err
	}
	versions, err := QueryVersionList(c, tenantID, serviceName, functionName)
	if err != nil {
		return err
	}
	for _, version := range versions {
		if err := DeleteByVersion(c, tenantID, serviceName, functionName, version); err != nil {
			return err
		}
	}
	return nil
}

// DeleteByVersion delete function version
func DeleteByVersion(c client.Client, tenantID, serviceName, functionName, version string) error {
	if err := verify.CheckServiceName(serviceName); err != nil {
		return err
	}
	if err := verify.CheckFunctionName(functionName); err != nil {
		return err
	}
	if err := verify.CheckVersion(version); err != nil {
		return err
	}
	funcKey := getFuncKey(tenantID, serviceName, functionName, version)
	results, err := c.GetValuesByKey(funcKey)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return errors.New("function not exists")
	}
	funcData := &EtcdData{}
	err = json.Unmarshal([]byte(results[0]), funcData)
	if err != nil {
		return err
	}
	// clear function code
	if err = c.DeleteObject(funcData.ObjectID); err != nil {
		return err
	}
	for _, layer := range funcData.Layers {
		// clear layers
		if err = c.DeleteObject(layer.ObjectID); err != nil {
			return err
		}
	}
	return c.DeleteByKey(funcKey)
}

// Query query function info
func Query(c client.Client, tenantID, serviceName, functionName, version string) (*Function, error) {
	if err := verify.CheckServiceName(serviceName); err != nil {
		return nil, err
	}
	if err := verify.CheckFunctionName(functionName); err != nil {
		return nil, err
	}
	if err := verify.CheckVersion(version); err != nil {
		return nil, err
	}
	funcKey := getFuncKey(tenantID, serviceName, functionName, version)
	results, err := c.GetValuesByKey(funcKey)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("function not exists")
	}
	funcData := &EtcdData{}
	err = json.Unmarshal([]byte(results[0]), funcData)
	if err != nil {
		return nil, err
	}
	layers := make([]string, len(funcData.Layers))
	for i, layer := range funcData.Layers {
		layers[i] = layer.Name
	}
	deEnv, err := localauth.Decrypt(funcData.Environment)
	if err != nil {
		return nil, err
	}
	userData := make(map[string]string, defaultEnvSize)
	if err := json.Unmarshal(deEnv, &userData); err != nil {
		return nil, err
	}
	url, err := c.GetObjectDownloadURL(funcData.ObjectID)
	if err != nil {
		return nil, err
	}
	functionInfo := generateFunctionInfo(funcData, layers, url.String(), userData)
	return functionInfo, nil
}

func generateFunctionInfo(funcData *EtcdData, layers []string, url string, userData map[string]string) *Function {
	return &Function{
		Name:          funcData.Name,
		Timeout:       funcData.Timeout,
		FunctionURN:   funcData.FunctionUrn,
		Description:   funcData.Description,
		MinInstance:   funcData.MinInstance,
		Runtime:       funcData.Runtime,
		Handler:       funcData.Handler,
		MaxInstance:   funcData.MaxInstance,
		Memory:        funcData.Memory,
		VersionDesc:   funcData.VersionDescription,
		VersionNumber: funcData.Version,
		ConcurrentNum: funcData.ConcurrentNum,
		Layers:        layers,
		Created:       funcData.Created,
		LastModified:  funcData.FunctionUpdateTime.String(),
		DownloadURL:   url,
		Environment:   userData,
	}
}

// QueryVersionList query version list of function
func QueryVersionList(c client.Client, tenantID, serviceName, functionName string) ([]string, error) {
	if err := verify.CheckServiceName(serviceName); err != nil {
		return nil, err
	}
	if err := verify.CheckFunctionName(functionName); err != nil {
		return nil, err
	}
	prefixKey := "/sn/functions/business/yrk/tenant/" + tenantID + "/function/0@" + serviceName + "@" + functionName
	functionKeys, err := c.MatchKeysByPrefixKey(prefixKey)
	if err != nil {
		return nil, err
	}
	versions := make([]string, len(functionKeys))
	for i, key := range functionKeys {
		funcKeySplits := strings.Split(key, "/")
		if len(funcKeySplits) < funcVersionIndex {
			continue
		}
		versions[i] = funcKeySplits[funcVersionIndex]
	}
	return versions, nil
}

// Deploy deploy function
func Deploy(c client.Client, pkgPath string, funcConfig *deploy.FunctionConfig) error {
	if err := checkFunctionConfig(funcConfig); err != nil {
		return err
	}
	fileName := path.Base(pkgPath)
	objectName := "cli-yrst-function-" + funcConfig.TenantID + "-" + funcConfig.Service +
		"-" + funcConfig.Name + "-" + funcConfig.Version + "-" + fileName
	fileSize, err := uploadFile(c, pkgPath, objectName)
	if err != nil {
		return err
	}
	if err := putFuncMetaData(c, funcConfig, objectName, pkgPath, fileSize); err != nil {
		return err
	}
	if err := putInstanceData(c, funcConfig); err != nil {
		return err
	}
	if err := putAgencyData(c, funcConfig); err != nil {
		return err
	}

	return nil
}

func putAgencyData(c client.Client, funcConfig *deploy.FunctionConfig) error {
	key := "/sn/agency/business/yrk/tenant/" + funcConfig.TenantID + "/domain/" + funcConfig.TenantID +
		"/agency/" + funcConfig.AppXRole
	ak, _ := localauth.Encrypt(funcConfig.Agency.AccessKey)
	sk, _ := localauth.Encrypt(funcConfig.Agency.SecretKey)
	token, _ := localauth.Encrypt(funcConfig.Agency.Token)
	securityAK, _ := localauth.Encrypt(funcConfig.Agency.SecurityAk)
	securitySK, _ := localauth.Encrypt(funcConfig.Agency.SecuritySk)
	securityToken, _ := localauth.Encrypt(funcConfig.Agency.SecurityToken)
	agency := Agency{
		AccessKey:     ak,
		SecretKey:     sk,
		Token:         token,
		SecurityAk:    securityAK,
		SecuritySk:    securitySK,
		SecurityToken: securityToken,
	}
	data, err := json.Marshal(agency)
	if err != nil {
		return err
	}
	if err := c.PutKeyWithValue(key, string(data)); err != nil {
		return err
	}
	return nil
}

func checkFunctionConfig(funcConfig *deploy.FunctionConfig) error {
	if err := verify.CheckServiceName(funcConfig.Service); err != nil {
		return err
	}
	if err := verify.CheckFunctionName(funcConfig.Name); err != nil {
		return err
	}
	return nil
}

func uploadFile(c client.Client, path, objectName string) (int64, error) {
	var err error
	if err = verify.CheckZIPFile(path); err != nil {
		return 0, fmt.Errorf("the uploaded function ZIP archive is incorrect, %s", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	fileStat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if err = c.UploadObject(file, fileStat.Size(), objectName); err != nil {
		return 0, err
	}
	return fileStat.Size(), nil
}

func putInstanceData(c client.Client, funcConfig *deploy.FunctionConfig) error {
	instanceKey := getInstanceKey(config.FConfig.CLIConfig.ClusterID, funcConfig.TenantID, funcConfig.Service,
		funcConfig.Name, "latest")
	instanceData := InstanceData{
		InstanceMetaData: InstanceMetaData{
			MaxInstance:   funcConfig.MaxInstance,
			MinInstance:   funcConfig.MinInstance,
			ConcurrentNum: funcConfig.ConcurrentNum,
		},
	}
	data, err := json.Marshal(instanceData)
	if err != nil {
		return err
	}
	if err := c.PutKeyWithValue(instanceKey, string(data)); err != nil {
		fmt.Printf("failed to put InstanceData value to etcd, %s", err)
		return err
	}
	return nil
}

func putFuncMetaData(c client.Client, funcConfig *deploy.FunctionConfig, objectName string,
	filePath string, codeSize int64) error {
	funcKey := getFuncKey(funcConfig.TenantID, funcConfig.Service, funcConfig.Name, funcConfig.Version)
	enUserData, enUserDataEn, err := encryptEnvAndUserData(funcConfig)
	if err != nil {
		return err
	}
	metaData, err := getMetaData(c, filePath, funcConfig, codeSize)
	if err != nil {
		return err
	}
	funcMetaData := &EtcdData{
		MetaData:   metaData,
		S3MetaData: getS3MetaData(objectName, funcConfig.Runtime, filePath),
		EnvMetaData: EnvMetaData{
			Environment:       enUserData,
			EncryptedUserData: enUserDataEn,
		},
		ResourceMetaData: ResourceMetaData{
			CPU:                 funcConfig.CPU,
			Memory:              funcConfig.MemorySize,
			EnableDynamicMemory: funcConfig.EnableDynamicMemory,
		},
		InstanceMetaData: InstanceMetaData{
			MaxInstance:   funcConfig.MaxInstance,
			MinInstance:   funcConfig.MinInstance,
			ConcurrentNum: funcConfig.ConcurrentNum,
		},
	}
	funcMetaData.ExtendedMetaData = getExtendedMetaData(funcConfig)
	data, err := json.Marshal(funcMetaData)
	if err != nil {
		return err
	}
	if err := c.PutKeyWithValue(funcKey, string(data)); err != nil {
		return err
	}
	return nil
}

func getS3MetaData(objectName, Runtime, filePath string) S3MetaData {
	minioConfig := config.FConfig.CLIConfig.Minio
	return S3MetaData{
		BucketID:     minioConfig.Bucket,
		ObjectID:     objectName,
		BucketURL:    minioConfig.Endpoint,
		CodeType:     getCodeType(Runtime),
		CodeURL:      "",
		CodeFilename: path.Base(filePath),
	}
}

func encryptEnvAndUserData(funcConfig *deploy.FunctionConfig) (string, string, error) {
	userData, err := json.Marshal(funcConfig.UserData)
	if err != nil {
		return "", "", err
	}
	userDataEn, err := localauth.Encrypt(string(userData))
	if err != nil {
		return "", "", err
	}
	enUserData, err := json.Marshal(funcConfig.EncryptedUserData)
	if err != nil {
		return "", "", err
	}
	enUserDataEn, err := localauth.Encrypt(string(enUserData))
	if err != nil {
		return "", "", err
	}
	return userDataEn, enUserDataEn, nil
}

func getExtendedMetaData(funcConfig *deploy.FunctionConfig) ExtendedMetaData {
	extendedMetaData := ExtendedMetaData{
		StrategyConfig: StrategyConfig{
			ConcurrentNum: funcConfig.ConcurrentNum,
			Concurrency:   funcConfig.MaxInstance,
		},
		Initializer: Initializer{
			InitializerHandler: funcConfig.InitializerHandler,
			InitializerTimeout: funcConfig.InitializerTimeout,
		},
		PreStop: types.PreStop{
			Handler: funcConfig.PreStopHandler,
			Timeout: funcConfig.PreStopTimeout,
			Type:    funcConfig.PreStopType,
		},
	}
	if funcConfig.Image != "" {
		extendedMetaData = getImageExtendedMetaData(funcConfig)
	}
	extendedMetaData.Role = Role{
		Xrole:    funcConfig.AppXRole,
		AppXrole: funcConfig.AppXRole,
	}
	return extendedMetaData
}

func getMetaData(c client.Client, filePath string, funcConfig *deploy.FunctionConfig,
	codeSize int64) (MetaData, error) {
	funcName := "0@" + funcConfig.Service + "@" + funcConfig.Name
	functionUrn := "sn:cn:yrk:" + funcConfig.TenantID + ":function:" + funcName
	var metaData MetaData
	codeSha512, err := getFileHash512(filePath)
	if err != nil {
		return metaData, err
	}
	layers, err := getLayerInfo(c, funcConfig)
	if err != nil {
		return metaData, err
	}
	metaData = MetaData{
		Layers:                  layers,
		Name:                    funcName,
		Description:             funcConfig.Description,
		FunctionUrn:             functionUrn,
		TenantID:                funcConfig.TenantID,
		Created:                 time.Now().String(),
		FunctionUpdateTime:      time.Now(),
		FunctionVersionUrn:      functionUrn + ":" + funcConfig.Version,
		RevisionID:              strconv.FormatInt(time.Now().UnixNano(), decimal),
		CodeSize:                int(codeSize),
		CodeSha512:              codeSha512,
		Handler:                 funcConfig.Handler,
		Runtime:                 funcConfig.Runtime,
		Timeout:                 funcConfig.Timeout,
		Version:                 funcConfig.Version,
		VersionDescription:      "",
		LatestVersionUpdateTime: time.Now(),
		PublishTime:             "",
		BusinessID:              "yrk",
		ID:                      utils.UniqueID(),
		FuncID:                  utils.UniqueID(),
		FuncName:                funcConfig.Name,
		ProjectName:             "yellow",
		Service:                 funcConfig.Service,
		Dependencies:            "",
		IsStatefulFunction:      funcConfig.IsStatefulFunction,
		Type:                    "v2",
		EnableAuthInHeader:      funcConfig.EnableAuthInHeader,
		DomainID:                funcConfig.TenantID,
		DisableInsecureAkSk:     funcConfig.DisableInsecureAkSk,
	}
	return metaData, nil
}

func getCodeType(runtime string) string {
	codeType := "inline"
	if runtime == "custom image" {
		codeType = "Custom-Image-Swr"
	}
	return codeType
}

func getLayerInfo(c client.Client, funcConfig *deploy.FunctionConfig) ([]Layer, error) {
	var layersInfo []Layer
	currentPath, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	pkgPath, err := common.GetPkgPath(currentPath)
	if err != nil {
		return nil, err
	}
	if len(funcConfig.Layers) > 0 {
		for _, layerName := range funcConfig.Layers {
			layerPath := path.Join(pkgPath, layerName)
			layerObjectName := "cli-yrst-layer-" + funcConfig.TenantID + "-" + funcConfig.Service +
				"-" + funcConfig.Name + "-" + funcConfig.Version + "-" + layerName
			_, err := uploadFile(c, layerPath, layerObjectName)
			if err != nil {
				return nil, err
			}
			layer := Layer{
				BucketURL:      "https://minio.com",
				Sha256:         "",
				DependencyType: "private",
				Name:           layerObjectName,
				ObjectID:       layerObjectName,
				BucketID:       config.FConfig.CLIConfig.Minio.Bucket,
			}
			layersInfo = append(layersInfo, layer)
		}
	}
	return layersInfo, nil
}

func getImageExtendedMetaData(funcConfig *deploy.FunctionConfig) ExtendedMetaData {
	controlPath := "event"
	if funcConfig.Runtime == "http" {
		controlPath = "http"
	}
	return ExtendedMetaData{
		StrategyConfig: StrategyConfig{
			ConcurrentNum: funcConfig.ConcurrentNum,
			Concurrency:   funcConfig.MaxInstance,
		},
		Initializer: Initializer{
			InitializerHandler: funcConfig.InitializerHandler,
			InitializerTimeout: funcConfig.InitializerTimeout,
		},
		CustomContainerConfig: CustomContainerConfig{
			ControlPath: controlPath,
			Image:       funcConfig.Image,
			UID:         -1,
			Gid:         -1,
		},
	}
}

func getFileHash512(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}

	// close file when fail or succeed
	defer func() {
		err := f.Close()
		if err != nil {
		}
	}()

	hasher := sha512.New()
	if _, err = io.Copy(hasher, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
