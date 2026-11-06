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

package common

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/types"
)

// WrapInvokeAdaptor wrap invoke adaptor
func WrapInvokeAdaptor(originHandlerFunc func(ctx context.Context, dst *FunctionProperty,
	req *types.TCPRequest)) func(ctx context.Context, dst *FunctionProperty, req *types.TCPRequest) {
	return func(ctx context.Context, dst *FunctionProperty, req *types.TCPRequest) {
		if tracer.EnableCommonTracer() {
			path := "SendToRuntime"
			tracerName := tracer.GetOtelServiceName()
			method := "TCP"
			tr := otel.Tracer(tracerName)
			traceParent := req.Extra[constants.HeaderTraceParent]
			carrier := propagation.HeaderCarrier{}
			carrier.Set(constants.HeaderTraceParent, traceParent)
			propagattor := otel.GetTextMapPropagator()
			// use request context as parent context for current parent span
			pCtx := propagattor.Extract(context.Background(), carrier)

			childCtx, span := tr.Start(pCtx, path, trace.WithSpanKind(trace.SpanKindClient))
			span.SetAttributes(attribute.Key("http.target").String(path))
			span.SetAttributes(attribute.Key("http.method").String(method))
			span.SetAttributes(attribute.Key("http.requestID").String(req.Extra[constants.HeaderRequestID]))
			span.SetAttributes(attribute.Key("http.traceID").String(req.Extra[constants.HeaderTraceID]))
			defer span.End()
			// set child ctx to request header by carrier
			childCarrier := propagation.HeaderCarrier{}
			propagattor.Inject(childCtx, childCarrier)
			req.Extra[constants.HeaderTraceParent] = childCarrier.Get(constants.HeaderTraceParent)
		}
		// call origin handler function
		originHandlerFunc(ctx, dst, req)
	}
}
