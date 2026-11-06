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

// Package config is common logger client
package config

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/asaskevich/govalidator"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/utils"
)

const (
	configPath = "/home/sn/config/log.json"
	fileMode   = 0750
	// LogConfigKey environment variable key of log config
	LogConfigKey = "LOG_CONFIG"
)

var (
	// defaultCoreInfo default logger config
	defaultCoreInfo = CoreInfo{
		FilePath:   "/home/sn/log",
		Level:      "INFO",
		Tick:       10,    // Unit: Second
		First:      10,    // Unit: Number of logs
		Thereafter: 5,     // Unit: Number of logs
		Tracing:    false, // tracing log switch
		Disable:    false, // Disable file logger
	}
	LogLevel zapcore.Level = zapcore.InfoLevel
)

// CoreInfo contains the core info
type CoreInfo struct {
	FilePath   string `json:"filepath" valid:",required"`
	Level      string `json:"level" valid:",required"`
	Tick       int    `json:"tick" valid:"range(0|86400),optional"`
	First      int    `json:"first" valid:"range(0|20000),optional"`
	Thereafter int    `json:"thereafter" valid:"range(0|1000),optional"`
	Tracing    bool   `json:"tracing" valid:",optional"`
	Disable    bool   `json:"disable" valid:",optional"`
	SingleSize int64  `json:"singlesize" valid:",optional"`
	Threshold  int    `json:"threshold" valid:",optional"`
	IsUserLog  bool   `json:"-"`
}

// GetDefaultCoreInfo get defaultCoreInfo
func GetDefaultCoreInfo() CoreInfo {
	return defaultCoreInfo
}

// GetCoreInfoFromEnv extracts the logger config and ensures that the log file is available
func GetCoreInfoFromEnv() (CoreInfo, error) {
	coreInfo, err := ExtractCoreInfoFromEnv(LogConfigKey)
	if err != nil {
		return defaultCoreInfo, err
	}
	if err = utils.ValidateFilePath(coreInfo.FilePath); err != nil {
		return defaultCoreInfo, err
	}
	if err = os.MkdirAll(coreInfo.FilePath, fileMode); err != nil && !os.IsExist(err) {
		return defaultCoreInfo, err
	}

	return coreInfo, nil
}

// ExtractCoreInfoFromEnv extracts the logger config from ENV
func ExtractCoreInfoFromEnv(env string) (CoreInfo, error) {
	var coreInfo CoreInfo
	config := os.Getenv(env)
	if config == "" {
		return defaultCoreInfo, errors.New(env + " is empty")
	}
	err := json.Unmarshal([]byte(config), &coreInfo)
	if err != nil {
		return defaultCoreInfo, err
	}

	// if the file path is empty, return error
	// if the log file is not writable, zap will create a new file with the configured file path and file name
	if coreInfo.FilePath == "" {
		return defaultCoreInfo, errors.New("log file path is empty")
	}
	if _, err = govalidator.ValidateStruct(coreInfo); err != nil {
		return defaultCoreInfo, err
	}

	return coreInfo, nil
}
