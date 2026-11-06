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

// Package utils gets function services from URNs
package utils

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"functioncore/pkg/common/logger/log"
	commonutil "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/config"
)

var (
	poolResourceCache   sync.Map
	envCaeRequestFactor float64
)

const (
	work128                        = 128
	cpuRestoreThreshold            = 1024
	workerLimitCPU                 = 1000 // workerLimit improve to 1000 before decompression
	workerRequestCPU               = 100
	workerRequestMemory            = 100
	lowerWorkerRequestCPU          = 50
	defaultRuntimeMemory           = 500
	workerLimitMemoryLow           = 200
	workerLimitMemoryHigh          = 900
	memFactor                      = 2
	maxEffectiveEphemeralStorage   = 2048
	defaultRequestEphemeralStorage = 500

	resourceListDefaultLen = 2
	memIndex               = 1
	cpuIndex               = 0

	caeRequestInitFactor = 0.4
)

// WorkerResource -
type WorkerResource struct {
	RequestCPU int64 `json:"cpuRequest,omitempty"`
	RequestMem int64 `json:"memoryRequest,omitempty"`
	LimitCPU   int64 `json:"cpuLimit,omitempty"`
	LimitMem   int64 `json:"memoryLimit,omitempty"`
}

// RuntimeResource -
type RuntimeResource struct {
	RequestCPU int64 `json:"cpuRequest,omitempty"`
	RequestMem int64 `json:"memoryRequest,omitempty"`
	LimitCPU   int64 `json:"cpuLimit,omitempty"`
	LimitMem   int64 `json:"memoryLimit,omitempty"`
}

// PoolResource -
type PoolResource struct {
	Worker  WorkerResource  `json:"worker,omitempty"`
	Runtime RuntimeResource `json:"runtime,omitempty"`
}

type stateCreateOptions struct {
	CPU    int64 `json:"cpu,omitempty"`
	Memory int64 `json:"memory,omitempty"`
}

func init() {
	envCaeRequestFactor = getEnvCAERequestFactor()
}

func getEnvCAERequestFactor() float64 {
	envCaeRequestFactorStr := os.Getenv("CAE_REQUEST_FACTOR")
	if envCaeRequestFactorStr == "" {
		return caeRequestInitFactor
	}
	caeRequestFactor, err := strconv.ParseFloat(envCaeRequestFactorStr, 64)
	if err != nil {
		log.GetLogger().Warnf("failed to transfer CAE_REQUEST_FACTOR %s to float64", envCaeRequestFactorStr)
		return caeRequestInitFactor
	}
	return caeRequestFactor
}

// InitPoolResourceCache -
func InitPoolResourceCache() {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return
	}
	for _, pool := range cfg.PoolMgrConfig.PoolList.Pool {
		key := strconv.FormatInt(pool.LimitCPU, 10) + "-" + strconv.FormatInt(pool.LimitMemory, 10)
		resource := PoolResource{}
		resource.Runtime.RequestCPU = pool.ActualRequestCPU
		cpuLimit := pool.LimitCPU
		if pool.ActualCPU > 0 {
			cpuLimit = pool.ActualCPU
		}
		resource.Runtime.LimitCPU = cpuLimit
		resource.Runtime.RequestMem = pool.RequestMemory
		resource.Runtime.LimitMem = pool.LimitMemory
		if pool.LimitMemory == work128 {
			resource.Worker.RequestCPU = lowerWorkerRequestCPU
			resource.Worker.LimitCPU = workerLimitCPU
		} else if pool.LimitMemory < cpuRestoreThreshold {
			resource.Worker.RequestCPU = workerRequestCPU
			resource.Worker.LimitCPU = workerLimitCPU
		} else {
			resource.Worker.RequestCPU = workerRequestCPU
			resource.Worker.LimitCPU = pool.LimitCPU
		}
		workerLimitMemory := getWorkerLimitMemory(pool.LimitMemory, pool.EphemeralStorage)
		resource.Worker.RequestMem = workerRequestMemory
		resource.Worker.LimitMem = workerLimitMemory
		poolResourceCache.Store(key, resource)
	}
	log.GetLogger().Infof("finished to init pool resource cache")
}

// GetCAERequestFactor -
func GetCAERequestFactor() float64 {
	return envCaeRequestFactor
}

// GetPoolResource -
func GetPoolResource(limitCPU, limitMemory int64, businessType string) (PoolResource, error) {
	key := strconv.FormatInt(limitCPU, 10) + "-" + strconv.FormatInt(limitMemory, 10)
	result, ok := poolResourceCache.Load(key)
	if !ok {
		if !commonutil.IsCAEFunc(businessType) {
			log.GetLogger().Warnf("failed to get target standard: %s of pool resource", key)
			return PoolResource{}, errors.New("not found")
		}
		return GetCAEPoolResource(limitCPU, limitMemory)
	}

	resources, ok := result.(PoolResource)
	if !ok {
		log.GetLogger().Warnf("failed to get convert the type: %s of pool resource", key)
		return PoolResource{}, errors.New("not found")
	}
	return resources, nil
}

// GetCAEPoolResource -
func GetCAEPoolResource(requestCPU, requestMem int64) (PoolResource, error) {
	cfg, _ := config.GetConfig()
	specKey := fmt.Sprintf("pool-%d-%d", requestCPU, requestMem)
	poolSpec, ok := cfg.CaeCustomizePoolSpec[specKey]
	if !ok {
		log.GetLogger().Warnf("failed to get cae pool spec by key: %s of pool resource", specKey)
		return PoolResource{}, errors.New("cae pool not found")
	}
	caePoolResource := PoolResource{}
	caePoolResource.Runtime.RequestCPU = poolSpec.RequestCPU
	caePoolResource.Runtime.RequestMem = poolSpec.RequestMEM
	caePoolResource.Runtime.LimitCPU = poolSpec.LimitCPU
	caePoolResource.Runtime.LimitMem = poolSpec.LimitMEM

	if poolSpec.LimitMEM == work128 {
		caePoolResource.Worker.RequestCPU = lowerWorkerRequestCPU
		caePoolResource.Worker.LimitCPU = workerLimitCPU
	} else if poolSpec.LimitMEM < cpuRestoreThreshold {
		caePoolResource.Worker.RequestCPU = workerRequestCPU
		caePoolResource.Worker.LimitCPU = workerLimitCPU
	} else {
		caePoolResource.Worker.RequestCPU = workerRequestCPU
		caePoolResource.Worker.LimitCPU = poolSpec.LimitCPU
	}
	workerLimitMemory := getWorkerLimitMemory(poolSpec.LimitMEM, poolSpec.EphemeralStorage)
	caePoolResource.Worker.RequestMem = workerRequestMemory
	caePoolResource.Worker.LimitMem = workerLimitMemory
	return caePoolResource, nil
}

// GetCreateOptionResource -
func GetCreateOptionResource(cpu, mem int64) (int64, int64) {
	if mem == 0 {
		log.GetLogger().Infof("create option memory is 0, ignore obtain resource")
		return 0, 0
	}
	var specs []stateCreateOptions
	poolResourceCache.Range(func(key, value any) bool {
		resources, ok := key.(string)
		if !ok {
			log.GetLogger().Warnf("failed to assert resource: %v", key)
			return true
		}
		resourceList := strings.Split(resources, "-")
		if len(resourceList) != resourceListDefaultLen {
			log.GetLogger().Warnf("invalid length of resource: %s", resources)
			return true
		}
		memoryResource, err := strconv.ParseInt(resourceList[memIndex], 10, 64)
		if err != nil {
			log.GetLogger().Warnf("failed to parse memoryResource: %s", resources)
			return true
		}
		cpuResource, err := strconv.ParseInt(resourceList[cpuIndex], 10, 64)
		if err != nil {
			log.GetLogger().Warnf("failed to parse cpuResource: %s", resources)
			return true
		}
		specs = append(specs, stateCreateOptions{
			CPU:    cpuResource,
			Memory: memoryResource,
		})
		return true
	})
	if len(specs) == 0 {
		log.GetLogger().Warnf("invalid length of specs")
		return 0, 0
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Memory == specs[j].Memory {
			return specs[i].CPU < specs[j].CPU
		}
		return specs[i].Memory < specs[j].Memory
	})

	for _, value := range specs {
		if value.Memory >= mem {
			log.GetLogger().Infof("get create option resource: %d, %d", value.CPU, value.Memory)
			return value.CPU, value.Memory
		}
	}
	log.GetLogger().Warnf("invalid create option resource: %d", mem)
	return 0, 0
}

func getWorkerLimitMemory(runtimeMem, ephemeralStorage int64) int64 {
	mem := (runtimeMem / defaultRuntimeMemory * workerLimitMemoryLow) / memFactor
	if mem < workerLimitMemoryLow {
		return workerLimitMemoryLow
	}
	// avoid calculating large worker limit memory because of large ephemeral storage
	if ephemeralStorage > maxEffectiveEphemeralStorage {
		ephemeralStorage = maxEffectiveEphemeralStorage
	}
	// set worker memory limit in proportion to ephemeralStorage
	if ephemeralStorage > defaultRequestEphemeralStorage {
		return ephemeralStorage * workerLimitMemoryHigh / defaultRequestEphemeralStorage
	}
	if mem > workerLimitMemoryHigh {
		return workerLimitMemoryHigh
	}
	return mem
}

// ConfigureResource -
func ConfigureResource(cpu, mem int64, podSpec *corev1.PodSpec, businessType string) {
	if IsVpaFeatureEnabled() {
		containerResource, _ := GetPoolResource(cpu, mem, businessType)
		for i := range podSpec.Containers {
			if podSpec.Containers[i].Name == RuntimeContainerName {
				podSpec.Containers[i].Resources.Limits["cpu"] = resource.MustParse(fmt.Sprintf("%dm",
					containerResource.Runtime.LimitCPU))
				podSpec.Containers[i].Resources.Requests["cpu"] = resource.MustParse(fmt.Sprintf("%dm",
					containerResource.Runtime.RequestCPU))
				podSpec.Containers[i].Resources.Limits["memory"] = resource.MustParse(fmt.Sprintf("%dMi",
					containerResource.Runtime.LimitMem))
				podSpec.Containers[i].Resources.Requests["memory"] = resource.MustParse(fmt.Sprintf("%dMi",
					containerResource.Runtime.RequestMem))
			}
			if podSpec.Containers[i].Name == WorkerContainerName {
				podSpec.Containers[i].Resources.Limits["cpu"] = resource.MustParse(fmt.Sprintf("%dm",
					containerResource.Worker.LimitCPU))
				podSpec.Containers[i].Resources.Requests["cpu"] = resource.MustParse(fmt.Sprintf("%dm",
					containerResource.Worker.RequestCPU))
				podSpec.Containers[i].Resources.Limits["memory"] = resource.MustParse(fmt.Sprintf("%dMi",
					containerResource.Worker.LimitMem))
				podSpec.Containers[i].Resources.Requests["memory"] = resource.MustParse(fmt.Sprintf("%dMi",
					containerResource.Worker.RequestMem))
			}
		}
	}
}
