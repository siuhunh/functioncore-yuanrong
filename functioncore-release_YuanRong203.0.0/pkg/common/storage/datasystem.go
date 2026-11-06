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

// Package storage provides common storage interface
package storage

import (
	dsCommon "clients/common"
	"clients/statecache"
	"errors"
	"fmt"
	"os"
	"sync"

	"functioncore/pkg/common/logger/log"
)

const (
	dsWorkerPort = 31501
)

var (
	dsWorkerHost = os.Getenv("HOST_IP")
	dsClientOnce sync.Once
	dsClient     *DSClient
)

// DSClient -
type DSClient struct {
	stateCacheClient statecache.StateClient
}

// GetDSClient -
func GetDSClient() *DSClient {
	dsClientOnce.Do(func() {
		client, err := newDSClient()
		if err != nil {
			log.GetLogger().Errorf("failed to init data system client with error %s", err.Error())
			return
		}
		// succeed to init data system client
		dsClient = client
	})
	return dsClient
}

func newDSClient() (*DSClient, error) {
	connectArgs := dsCommon.ConnectArguments{
		Host: dsWorkerHost,
		Port: dsWorkerPort,
	}
	if config.enableTenantAuth {
		if config.tenantID == "" {
			errMsg := "data system tenant auth is enabled, but tenantID is empty"
			log.GetLogger().Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		if config.iamToken == "" {
			errMsg := fmt.Sprintf("data system tenant auth is enabled, but iamToken is empty, tenantID: %s",
				config.tenantID)
			log.GetLogger().Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		log.GetLogger().Infof("data system tenant auth is enabled, tenantID: %s", config.tenantID)
		connectArgs.TenantID = config.tenantID
		connectArgs.Token = []byte(config.iamToken)
	}
	client := statecache.CreateClient(connectArgs)
	status := client.Init()
	if status.IsError() {
		return nil, errors.New(status.ToString())
	}
	return &DSClient{stateCacheClient: client}, nil
}

// Get -
func (c *DSClient) Get(key string) (string, error) {
	value, status := c.stateCacheClient.Get(key)
	if status.IsError() {
		return "", errors.New(status.ToString())
	}
	return value, nil
}

// Set -
func (c *DSClient) Set(key, value string) error {
	setParam := statecache.SetParam{
		WriteMode: statecache.NoneL2Cache,
		TTLSecond: 0,
	}
	status := c.stateCacheClient.Set(key, value, setParam)
	if status.IsError() {
		return errors.New(status.ToString())
	}
	return nil
}

// Delete -
func (c *DSClient) Delete(key string) error {
	status := c.stateCacheClient.Del(key)
	if status.IsError() {
		return errors.New(status.ToString())
	}
	return nil
}
