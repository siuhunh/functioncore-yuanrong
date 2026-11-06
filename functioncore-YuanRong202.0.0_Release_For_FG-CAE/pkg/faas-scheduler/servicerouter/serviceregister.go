/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package servicerouter contains service route logic
package servicerouter

import (
	"encoding/json"
	"fmt"
	"os"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/faas-scheduler/config"
)

var (
	// RegisterAdapter -
	RegisterAdapter ServiceRegister
	leaseInterval   int64 = 5
	leaseTTL        int64 = 15
)

// Config -
type Config struct {
	Key           string
	Value         string
	LeaseTTL      int64
	LeaseInterval int64
	StopCh        <-chan struct{}
}

// ServiceRegister -
type ServiceRegister interface {
	Register(config Config) error
	UpdateConfig(config Config)
}

// RegisterService register faas scheduler to service center
func RegisterService(stopCh <-chan struct{}) error {
	serviceKey, serviceValue, err := getServiceKeyAndValue()
	if err != nil {
		return err
	}

	config := Config{
		Key:           serviceKey,
		Value:         serviceValue,
		LeaseTTL:      leaseTTL,
		LeaseInterval: leaseInterval,
		StopCh:        stopCh,
	}
	err = RegisterAdapter.Register(config)
	if err != nil {
		return err
	}
	log.GetLogger().Infof("register service success, service key: %s", config.Key)
	return nil
}

func getServiceKeyAndValue() (string, string, error) {
	clusterID := os.Getenv("CLUSTER_ID")
	nodeIP := os.Getenv("NODE_IP")
	podName := os.Getenv("POD_NAME")
	podIP := os.Getenv("POD_IP")

	err := validateEnvs(clusterID, nodeIP, podName, podIP)
	if err != nil {
		return "", "", err
	}

	key := fmt.Sprintf("/sn/faas-scheduler/instances/%s/%s/%s", clusterID, nodeIP, podName)

	serviceInfo := types.FaaSServiceInfo{
		NodeIP:      nodeIP,
		InstanceID:  podName,
		IP:          podIP,
		Port:        config.FConfig.Runtime.ServicePort,
		RunningMode: os.Getenv("RUNNING_MODE"),
	}
	value, err := json.Marshal(serviceInfo)
	if err != nil {
		return "", "", err
	}
	return key, string(value), nil
}

func validateEnvs(clusterID, nodeIP, podName, podIP string) error {
	if clusterID == "" || nodeIP == "" || podName == "" || podIP == "" {
		log.GetLogger().Errorf("can not find envs, clusterID %s, nodeIP %s podName %s podIP %s",
			clusterID, nodeIP, podName, podIP)
		return fmt.Errorf("can not find envs")
	}
	return nil
}
