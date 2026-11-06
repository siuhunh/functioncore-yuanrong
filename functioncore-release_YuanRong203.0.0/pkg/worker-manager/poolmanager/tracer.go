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

package poolmanager

import (
	v1 "k8s.io/api/core/v1"

	commontrace "functioncore/pkg/common/tracer"
)

type traceEnv struct{}

func (v *traceEnv) configEnv(b *envBuilder) {
	if commontrace.GetOtelGRPCEndpoint() != "" {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  commontrace.OtelGRPCEndpointEnvKey,
			Value: commontrace.GetOtelGRPCEndpoint(),
		})
	}
	if commontrace.GetOtelGRPCToken() != "" {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  commontrace.OtelGRPCTokenEnvKey,
			Value: commontrace.GetOtelGRPCToken(),
		})
	}
	if commontrace.GetOtelServiceName() != "" {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  commontrace.OtelServiceNameEnvKey,
			Value: "fgs.worker." + defaultClusterName,
		})
	}
	if commontrace.EnableOTELTracer() {
		b.addEnvVar(containerWorker, v1.EnvVar{
			Name:  commontrace.OtelEnableSampleEnvKey,
			Value: trueString,
		})
	}
}
