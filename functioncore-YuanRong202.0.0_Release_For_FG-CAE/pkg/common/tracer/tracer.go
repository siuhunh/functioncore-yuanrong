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

// Package tracer for init trace provider
package tracer

import (
	"context"
	"fmt"
	"os"

	"functioncore/pkg/common/logger/log"
)

const (
	// OtelGRPCEndpointEnvKey -
	OtelGRPCEndpointEnvKey = "OTEL_GRPC_ENDPOINT"
	// OtelGRPCTokenEnvKey -
	OtelGRPCTokenEnvKey = "OTEL_GRPC_TOKEN"
	// OtelServiceNameEnvKey -
	OtelServiceNameEnvKey = "OTEL_SERVICE_NAME"
	// OtelEnableSampleEnvKey -
	OtelEnableSampleEnvKey = "OTEL_ENABLE_SAMPLE"
)

var (
	hostIP           = os.Getenv("HOST_IP")
	hostName         = os.Getenv("HOSTNAME")
	otelGRPCEndpoint = os.Getenv(OtelGRPCEndpointEnvKey)
	otelGRPCToken    = os.Getenv(OtelGRPCTokenEnvKey)
	otelServiceName  = os.Getenv(OtelServiceNameEnvKey)
	enableOTELTracer = os.Getenv(OtelEnableSampleEnvKey) == "true"
)

// GetOtelGRPCEndpoint -
func GetOtelGRPCEndpoint() string {
	return otelGRPCEndpoint
}

// GetOtelGRPCToken -
func GetOtelGRPCToken() string {
	return otelGRPCToken
}

// GetOtelServiceName -
func GetOtelServiceName() string {
	return otelServiceName
}

// EnableOTELTracer -
func EnableOTELTracer() bool {
	return enableOTELTracer
}

// EnableCommonTracer -
func EnableCommonTracer() bool {
	return enableOTELTracer && otelGRPCEndpoint != ""
}

// InitCommonTracer init common tracer with service name
func InitCommonTracer(shutdown func(), serviceName string) {
	var err error
	shutdown, err = InitProvider(context.Background())
	if err != nil {
		fmt.Printf("failed to init %s trace provider with error %s\n", serviceName, err.Error())
		log.GetLogger().Warnf("failed to init %s trace provider with error %s", serviceName, err.Error())
		return
	}
}

// InitProvider init provider for trace http request
func InitProvider(ctx context.Context) (func(), error) {
	if !EnableCommonTracer() {
		fmt.Println("otel tracer env is empty with ", hostName, otelGRPCEndpoint)
		log.GetLogger().Warnf("otel tracer env is empty with %s, %s", hostName, otelGRPCEndpoint)
		return func() {}, nil
	}
	return func() {}, nil
}
