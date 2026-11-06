/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

package preload

import (
	"context"
	"sync"

	"github.com/panjf2000/ants/v2"

	"functioncore/pkg/common/cffclient"
)

// Client -
type Client struct {
	cffClient      *cffclient.Client
	addTaskPool    *ants.Pool
	deleteTaskPool *ants.Pool

	addToAllNodeTaskMap   sync.Map
	delOfAllNodeTaskMap   sync.Map
	reAddRepeatedCheckMap sync.Map
}

// AddToAllNodesTask -
type AddToAllNodesTask struct {
	id         string
	ctx        context.Context
	cancelFunc context.CancelFunc
	client     *Client
}

// DelOfAllNodesTask -
type DelOfAllNodesTask struct {
	id         string
	ctx        context.Context
	cancelFunc context.CancelFunc
	client     *Client
}

// AddPreloadError -
type AddPreloadError struct {
	ErrCode    string `json:"err_code"`
	ErrMessage string `json:"err_msg"`
}
