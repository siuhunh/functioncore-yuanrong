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

// Package tracer for gin and fast http
package tracer

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"functioncore/pkg/common/constants"
)

// WrapGinHandler wrap gin handler
func WrapGinHandler(originHandlerFunc func(c *gin.Context)) func(c *gin.Context) {
	return func(c *gin.Context) {
		if EnableCommonTracer() {
			path := c.Request.URL.Path
			tracerName := otelServiceName
			method := c.Request.Method
			tr := otel.Tracer(tracerName)
			traceParent := c.Request.Header.Get(constants.HeaderTraceParent)
			carrier := propagation.HeaderCarrier{}
			carrier.Set(constants.HeaderTraceParent, traceParent)
			propagattor := otel.GetTextMapPropagator()
			// use request context as parent context for current parent span
			pCtx := propagattor.Extract(c.Request.Context(), carrier)

			childCtx, span := tr.Start(pCtx, path, trace.WithSpanKind(trace.SpanKindServer))
			span.SetAttributes(attribute.Key("http.target").String(path))
			span.SetAttributes(attribute.Key("http.method").String(method))
			span.SetAttributes(attribute.Key("http.requestID").String(c.Request.Header.Get(constants.HeaderRequestID)))
			span.SetAttributes(attribute.Key("http.traceID").String(c.Request.Header.Get(constants.HeaderTraceID)))
			defer span.End()
			// set child ctx to request header by carrier
			c.Request = c.Request.WithContext(childCtx)
			childCarrier := propagation.HeaderCarrier{}
			propagattor.Inject(childCtx, childCarrier)
			c.Request.Header.Set(constants.HeaderTraceParent, childCarrier.Get(constants.HeaderTraceParent))
		}
		// call origin handler function
		originHandlerFunc(c)
	}
}

// WrapFastHTTPHandler wrap fast http handler
func WrapFastHTTPHandler(originHandlerFunc func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if EnableCommonTracer() {
			path := ctx.Request.URI().String()
			tracerName := otelServiceName
			method := string(ctx.Method())
			tr := otel.Tracer(tracerName)
			traceParent := string(ctx.Request.Header.Peek(constants.HeaderTraceParent))
			carrier := propagation.HeaderCarrier{}
			carrier.Set(constants.HeaderTraceParent, traceParent)
			propagattor := otel.GetTextMapPropagator()
			// use request context as parent context for current parent span
			pCtx := propagattor.Extract(context.Background(), carrier)

			childCtx, span := tr.Start(pCtx, path, trace.WithSpanKind(trace.SpanKindServer))
			span.SetAttributes(attribute.Key("http.target").String(path))
			span.SetAttributes(attribute.Key("http.method").String(method))
			requestID := string(ctx.Request.Header.Peek(constants.HeaderRequestID))
			traceID := string(ctx.Request.Header.Peek(constants.HeaderTraceID))
			span.SetAttributes(attribute.Key("http.requestID").String(requestID))
			span.SetAttributes(attribute.Key("http.traceID").String(traceID))
			defer span.End()
			// set child ctx to request header by carrier
			childCarrier := propagation.HeaderCarrier{}
			propagattor.Inject(childCtx, childCarrier)
			ctx.Request.Header.Set(constants.HeaderTraceParent, childCarrier.Get(constants.HeaderTraceParent))
		}
		// call origin handler function
		originHandlerFunc(ctx)
	}
}
