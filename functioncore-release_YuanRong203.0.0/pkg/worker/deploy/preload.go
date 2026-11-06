/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package deploy deploys Layers and functions
package deploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/utils"
)

const (
	preloadPath                      = "/opt/function/cold-start-preload/"
	preloadInstancePath              = "/opt/function/cold-start-preload-instance/"
	preloadMountPrefixPath           = "/opt/function/preload/"
	currentRevisionFileSplitSize     = 2
	currentRevisionFileRevisionIndex = 1
	retryToUnmountLimit              = 5
	retryToUnmountSleepTime          = 6 * time.Second
)

var (
	podName                     = os.Getenv("PodName")
	deferClearInstanceFiles     []string
	deferUmountPath             []string
	functionEnablePreload       = false
	layerEnablePreload          = false
	functionPreloadErrorMessage string
	layerPreloadResultArray     []types.LayerPreloadResult
)

// SetPreloadEnable -
func SetPreloadEnable(header http.Header) {
	if header == nil || len(header) == 0 {
		return
	}
	functionEnable := header.Get(constants.HeaderPreloadFunctionEnable)
	if functionEnable == "true" {
		functionEnablePreload = true
	}
	layerEnable := header.Get(constants.HeaderPreloadLayerEnable)
	if layerEnable == "true" {
		layerEnablePreload = true
	}
}

// SetPreloadResult -
func SetPreloadResult(w http.ResponseWriter) {
	if functionEnablePreload && functionPreloadErrorMessage != "" {
		w.Header().Set(constants.HeaderPreloadFunctionResult, functionPreloadErrorMessage)
	}
	if layerEnablePreload && len(layerPreloadResultArray) > 0 {
		data, err := json.Marshal(types.LayerPreloadResultArray{Results: layerPreloadResultArray})
		if err != nil {
			log.GetLogger().Warnf("failed to marshal layerPreloadResultArray, err %s", err.Error())
			return
		}
		w.Header().Set(constants.HeaderPreloadLayerResult, string(data))
	}
}

func deployLayerByPreload(layerInfo *types.Layer, language, workerPath string) error {
	if !layerEnablePreload {
		log.GetLogger().Warn("not enable to deploy layer by preload")
		return errors.New("not enable to deploy layer by preload")
	}
	start := time.Now()
	layerVersionName := commonUtils.GetPreloadLayerVersionName(layerInfo.Name, layerInfo.Version)
	layerPreloadPath := preloadPath + utils.LocalFuncURN.TenantID + "/dependencies/" + layerVersionName
	if err := checkLayerPreloadRequirements(layerPreloadPath); err != nil {
		setFunctionLayerPreloadErrorMessage(layerVersionName, err.Error())
		return err
	}
	if err := createInstanceUseFile(layerPreloadPath); err != nil {
		log.GetLogger().Errorf("failed to create preload instance using file, err %s", err.Error())
		return err
	}
	source := layerPreloadPath + "/code"
	layerPreloadMountPath := preloadMountPrefixPath + layerVersionName
	if err := mountAndLinkLayerPreloadFile(source, layerPreloadMountPath, workerPath, language); err != nil {
		return err
	}
	log.GetLogger().Infof("succeed to deploy layer %s by preload, cost %v", layerInfo.Name, time.Now().Sub(start))
	return nil
}

func mountAndLinkLayerPreloadFile(layerPreloadSource, mountBindPath, workerPath, language string) error {
	if err := os.MkdirAll(mountBindPath, worker.DeployFileMode); err != nil {
		log.GetLogger().Errorf("failed to create preload mount bind path %s, err %s", mountBindPath, err.Error())
		return err
	}
	if err := mount(layerPreloadSource, mountBindPath); err != nil {
		log.GetLogger().Errorf("failed to mount preload path to %s, err %s", mountBindPath, err.Error())
		return err
	}
	deferUmountPath = append(deferUmountPath, mountBindPath)
	err := linkPreloadDependencies(language, mountBindPath, workerPath)
	if err != nil {
		log.GetLogger().Errorf("failed to create preload link file, err %s", err.Error())
		return err
	}
	return nil
}

func checkLayerPreloadRequirements(layerPreloadPath string) error {
	if ok := exists(layerPreloadPath); !ok {
		errMsg := fmt.Sprintf("preload path %s not exists", layerPreloadPath)
		log.GetLogger().Errorf(errMsg)
		return errors.New(errMsg)
	}
	if err := checkPreloadSuccessFile(layerPreloadPath); err != nil {
		errMsg := fmt.Sprintf("failed to check preload success file, err %s", err.Error())
		log.GetLogger().Errorf(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func mount(source, target string) error {
	err := syscall.Mount(source, target, "bind", syscall.MS_BIND, "")
	if err != nil {
		log.GetLogger().Errorf("failed to mount %s to %s, err %s", source, target, err.Error())
		return err
	}
	return nil
}

func checkPreloadSuccessFile(path string) error {
	successFilePath := path + "/" + ".success"
	if ok := exists(successFilePath); !ok {
		return errors.New("preload success file not exists")
	}
	return nil
}

func linkPreloadDependencies(language, mountPath, workerPath string) error {
	if strings.Contains(language, "nodejs") {
		nodejsDependenciesPath := workerPath + "/node_modules/"
		mountPath += "/node_modules"
		return linkFiles(nodejsDependenciesPath, mountPath)
	} else if strings.Contains(language, "java") {
		javaDependenciesPath := workerPath + "/lib/"
		return linkFiles(javaDependenciesPath, mountPath)
	} else if strings.Contains(language, "python") || strings.Contains(language, "cangjie") {
		dependenciesPath := workerPath + "/"
		return linkFiles(dependenciesPath, mountPath)
	} else {
		return fmt.Errorf("not support %s preload layer", language)
	}
}

func deployFunctionByPreload(deployPath string, sha512 string, language string) error {
	if !functionEnablePreload {
		log.GetLogger().Warn("not enable to deploy function by preload")
		return errors.New("not enable to deploy function by preload")
	}
	start := time.Now()
	service, funcName, err := urnutils.GetFunctionNameAndServiceName(utils.LocalFuncURN.Name)
	if err != nil {
		log.GetLogger().Errorf("failed to get preload function service and name err: %s", err.Error())
		return err
	}
	funcVersionName := commonUtils.GetPreloadFunctionVersionName(service, funcName, utils.LocalFuncURN.Version)
	funcPreloadPath := preloadPath + utils.LocalFuncURN.TenantID + "/function/" + funcVersionName
	funcPreloadPath, err = checkFunctionPreloadRequirements(funcPreloadPath, sha512)
	if err != nil {
		log.GetLogger().Errorf("failed to check %s preload requirements:%s", funcVersionName, err.Error())
		setDeployFunctionByPreloadErrorMessage(err.Error())
		return err
	}
	if err = createInstanceUseFile(funcPreloadPath); err != nil {
		log.GetLogger().Errorf("failed to create preload instance using file, err %s", err.Error())
		return err
	}
	source := funcPreloadPath + "/code"
	funcPreloadMountPath := preloadMountPrefixPath + funcVersionName
	if err = mountAndLinkPreloadFile(source, funcPreloadMountPath, deployPath+"/", language); err != nil {
		log.GetLogger().Errorf("failed to mount and link preload files, err %s", err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to deploy func by preload, cost %v", time.Now().Sub(start))
	return nil
}

func checkFunctionPreloadRequirements(funcPreloadPath, sha512 string) (string, error) {
	if ok := exists(funcPreloadPath); !ok {
		errMsg := fmt.Sprintf("function preload path %s not exists", funcPreloadPath)
		log.GetLogger().Error(errMsg)
		return "", errors.New(errMsg)
	}
	revision, err := getCurrentRevision(funcPreloadPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get preload function current revision in dir %s, err %s",
			funcPreloadPath, err.Error())
		log.GetLogger().Errorf(errMsg)
		return "", errors.New(errMsg)
	}
	funcPreloadPath = filepath.Join(funcPreloadPath, "revisions", revision)
	if err = checkPreloadSuccessFile(funcPreloadPath); err != nil {
		errMsg := fmt.Sprintf("failed to check preload success file, err %s", err.Error())
		log.GetLogger().Errorf(errMsg)
		return "", errors.New(errMsg)
	}
	if err = checkSha512(funcPreloadPath, sha512); err != nil {
		errMsg := fmt.Sprintf("failed to check preload sha512, err %s", err.Error())
		log.GetLogger().Errorf(errMsg)
		return "", errors.New(errMsg)
	}
	return funcPreloadPath, nil
}

func mountAndLinkPreloadFile(mountSource string, mountBindPath string, linkPath string, language string) error {
	var err error
	if err = os.MkdirAll(mountBindPath, worker.DeployFileMode); err != nil {
		log.GetLogger().Errorf("failed to create preload mount bind path %s, err %s", mountBindPath, err.Error())
		return err
	}
	if err = mount(mountSource, mountBindPath); err != nil {
		log.GetLogger().Errorf("failed to mount preload path to %s, err %s", mountBindPath, err.Error())
		return err
	}
	deferUmountPath = append(deferUmountPath, mountBindPath)
	if err = linkFiles(linkPath, mountBindPath); err != nil {
		log.GetLogger().Errorf("failed to create preload link file, err %s", err.Error())
		return err
	}
	if err = linkSpecialFile(linkPath, mountBindPath, language); err != nil {
		log.GetLogger().Errorf("failed to link special file, err %s", err.Error())
		return err
	}
	return nil
}

func linkSpecialFile(linkPath string, mountBindPath string, language string) error {
	// The /opt/function/code/lib directory exists by default.
	deployLibPath := filepath.Join(linkPath, "lib") + "/"
	preloadLibPath := mountBindPath + "/lib"
	// Therefore, if the lib directory also exists in the preload directory,
	// creating the soft link to the lib directory to /opt/function/code/ will fail because the file already exists.
	if ok := exists(preloadLibPath); ok {
		// Therefore, should create the soft link of the preloaded files in the
		// lib directory to the /opt/function/code/lib directory,
		return linkFiles(deployLibPath, preloadLibPath)
	}
	return nil
}

func setDeployFunctionByPreloadErrorMessage(errorMessage string) {
	functionPreloadErrorMessage = errorMessage
}

func setFunctionLayerPreloadErrorMessage(layerVersionName, errorMessage string) {
	result := types.LayerPreloadResult{
		LayerVersionName: layerVersionName,
		Result:           errorMessage,
	}
	layerPreloadResultArray = append(layerPreloadResultArray, result)
}

func getCurrentRevision(path string) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	var currentRevisionFileName string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "current") {
			currentRevisionFileName = entry.Name()
			break
		}
	}
	if currentRevisionFileName == "" {
		return "", errors.New("current revision file is not found")
	}
	revisionFileNameSplitArray := strings.Split(currentRevisionFileName, ".")
	if len(revisionFileNameSplitArray) != currentRevisionFileSplitSize {
		return "", fmt.Errorf("current revision file %s is not the format (current.n)", currentRevisionFileName)
	}
	return revisionFileNameSplitArray[currentRevisionFileRevisionIndex], nil
}

func createInstanceUseFile(preloadDir string) error {
	realPreloadInstanceDir := path.Join(strings.Replace(preloadDir, preloadPath, preloadInstancePath, 1), "instances")
	if err := os.MkdirAll(realPreloadInstanceDir, worker.DeployFileMode); err != nil &&
		!os.IsExist(err) {
		log.GetLogger().Warnf("failed to create instance dir %s, err %s", realPreloadInstanceDir, err.Error())
		return err
	}
	instanceFilePath := filepath.Join(realPreloadInstanceDir, podName)
	file, err := os.OpenFile(instanceFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, worker.DeployFileMode)
	if err != nil {
		log.GetLogger().Warnf("failed to create instanceFile %s, err %s", instanceFilePath, err.Error())
		return err
	}
	defer file.Close()
	deferClearInstanceFiles = append(deferClearInstanceFiles, instanceFilePath)
	return nil
}

// GracefulClean -
func GracefulClean() {
	for _, instanceFilePath := range deferClearInstanceFiles {
		if err := os.Remove(instanceFilePath); err != nil {
			log.GetLogger().Warnf("failed to remove instanceFile %s, err %s", instanceFilePath, err.Error())
		}
	}
	for _, mountPath := range deferUmountPath {
		err := syscall.Unmount(mountPath, 0)
		if err != nil {
			log.GetLogger().Warnf("failed to umount path %s, err: %s", mountPath, err.Error())
			if err := retryToUnmount(mountPath); err != nil {
				log.GetLogger().Warnf("failed to retry umount path %s, err: %s", mountPath, err.Error())
			}
		}
	}
}

func retryToUnmount(mountPath string) error {
	var err error
	for i := 0; i < retryToUnmountLimit; i++ {
		time.Sleep(retryToUnmountSleepTime)
		err = syscall.Unmount(mountPath, syscall.MNT_FORCE)
		if err != nil {
			log.GetLogger().Warnf("failed to umount path %s, err: %s", mountPath, err.Error())
			continue
		}
		return nil
	}
	return err
}

func checkSha512(path, sha512 string) error {
	sha512FilePath := path + "/" + ".sha512"
	if ok := exists(sha512FilePath); !ok {
		return errors.New("preload sha512 file not exists")
	}
	file, err := os.Open(sha512FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	readSha512 := strings.TrimSpace(string(data))
	if readSha512 != sha512 {
		return errors.New("preload sha512 file not equal")
	}
	return nil
}

func linkFiles(dtsPath, srcPath string) error {
	if ok := exists(srcPath); !ok {
		log.GetLogger().Warnf("src path (%s) does not exist", srcPath)
		return fmt.Errorf("src path (%s) does not exist", srcPath)
	}
	if err := createIfNotExist(dtsPath); err != nil {
		return err
	}
	dir, err := os.ReadDir(srcPath)
	if err != nil {
		log.GetLogger().Errorf("failed to read srcPath : %s", srcPath)
		return err
	}
	for _, fi := range dir {
		linkPath := dtsPath + fi.Name()
		ok := exists(linkPath)
		if ok {
			log.GetLogger().Warnf("link file already exist")
			continue
		}
		err = os.Symlink(srcPath+"/"+fi.Name(), linkPath)
		if err != nil {
			log.GetLogger().Errorf("failed to link public linkPath")
			return err
		}
	}
	return nil
}
