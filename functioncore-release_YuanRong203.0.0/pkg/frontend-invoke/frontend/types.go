/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

// Package frontend define types of frontend
package frontend

// InvokeErrorResponse invoke error response
type InvokeErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// workerInstance used for etcd value storage
type workerInstance struct {
	NodeIP     string `json:"nodeIP"`
	NodePort   string `json:"nodePort"`
	Status     string `json:"status"`
	Owner      string `json:"ownerIP"`
	NodeName   string `json:"nodeName"`
	CPU        int    `json:"cpu"`
	Memory     int    `json:"memory"`
	WillDelete bool   `json:"willDelete"`
}
