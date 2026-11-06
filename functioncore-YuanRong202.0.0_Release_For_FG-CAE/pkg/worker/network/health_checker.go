/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package network

import (
	"context"
	"time"
)

const (
	defaultInterval         = 5
	defaultTimeout          = 2
	defaultRetryTime        = 5 * time.Second
	defaultHealthCheckRetry = 3
	defaultRetryDelay       = 20
)

// Option parameters of the health checker
type Option func(healthChecker interface{})

type healthChecker interface {
	DoHealthCheck(ctx context.Context) error
}
