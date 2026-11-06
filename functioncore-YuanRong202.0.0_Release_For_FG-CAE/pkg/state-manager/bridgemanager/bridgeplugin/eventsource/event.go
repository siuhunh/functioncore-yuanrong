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

// Package eventsource -
package eventsource

import "time"

const (
	// Added define the add event type
	Added = "Added"
	// Deleted define the delete event type
	Deleted = "Deleted"
)

// Event define event source info
type Event struct {
	SourceType string      `json:"sourcetype"`
	EventType  string      `json:"eventtype"`
	Kind       string      `json:"kind"`
	KeyRef     string      `json:"keyref"`
	Object     interface{} `json:"object"`
	StartTime  time.Time   `json:"startTime"`
}

// EventProvisioner define event provision interface
type EventProvisioner interface {
	Start(stopCh <-chan struct{}) error
}
