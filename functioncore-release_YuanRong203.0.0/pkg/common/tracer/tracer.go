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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"

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
	start := time.Now()
	fmt.Println("start to init provider for otel tracer with ", otelGRPCEndpoint)
	log.GetLogger().Infof("start to init provider for otel tracer with %s", otelGRPCEndpoint)
	traceExporter, err := makeTracerExporter(ctx)
	if err != nil {
		return func() {}, err
	}
	tracerProvider, err := makeTraceProvider(ctx, traceExporter)
	if err != nil {
		return func() {}, err
	}

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tracerProvider)
	fmt.Println("succeed to init provider for ", otelGRPCEndpoint, otelServiceName, time.Since(start).String())
	log.GetLogger().Infof("succeed to init provider for otel tracer %s with %s cost %s",
		otelGRPCEndpoint, otelServiceName, time.Since(start).String())

	return func() {
		cxt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := traceExporter.Shutdown(cxt); err != nil {
			otel.Handle(err)
		}
	}, nil
}

func makeTracerExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	headers := map[string]string{}
	if otelGRPCToken != "" {
		headers = map[string]string{"Authentication": otelGRPCToken}
	}
	traceGRPCClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otelGRPCEndpoint),
		otlptracegrpc.WithHeaders(headers),
		otlptracegrpc.WithDialOption(grpc.WithBlock()))

	traceExporter, err := otlptrace.New(ctx, traceGRPCClient)
	if err != nil {
		log.GetLogger().Warnf("failed to create the collector trace exporter with %s", err.Error())
		return nil, err
	}
	return traceExporter, nil
}

func makeTraceProvider(ctx context.Context, traceExporter *otlptrace.Exporter) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(otelServiceName),
			semconv.HostNameKey.String(hostName),
			semconv.NetHostIPKey.String(hostIP),
		),
	)
	if err != nil {
		log.GetLogger().Warnf("failed to create otel resource with %s", err.Error())
		return nil, err
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	sample := sdktrace.NeverSample()
	if enableOTELTracer {
		sample = sdktrace.AlwaysSample()
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sample),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	return tracerProvider, nil
}
