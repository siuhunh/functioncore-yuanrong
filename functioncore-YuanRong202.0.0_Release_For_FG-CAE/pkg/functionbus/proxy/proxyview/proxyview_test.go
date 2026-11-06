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

// Package proxyview is used to saving available status of proxy
package proxyview

import (
	"testing"
)

func TestUpdateunavailableProxy(t *testing.T) {
	nodeIP := "127.0.0.1"
	status := false
	UpdateProxyAvailableStatus(nodeIP, status)
	_, ok := unavailableProxy[nodeIP]
	if !ok {
		t.Errorf("status of nodeIP %s is error", nodeIP)
		return
	}

	UpdateProxyAvailableStatus(nodeIP, true)
	_, ok = unavailableProxy[nodeIP]
	if ok {
		t.Errorf("failed to delete element in map of nodeIP %s", nodeIP)
		return
	}
}

func TestGetProxyAvailableStatus(t *testing.T) {
	nodeIP := "127.0.0.2"
	UpdateProxyAvailableStatus(nodeIP, false)

	status := GetProxyAvailableStatus(nodeIP)
	if status {
		t.Errorf("status of nodeIP %s is error", nodeIP)
		return
	}
}

func Test_deleteProxy(t *testing.T) {
	unavailableProxy["test"] = &proxyTimer{}
	deleteProxy("wrong key")
	if _, ok := unavailableProxy["test"]; !ok {
		t.Error("delete wrong value")
	}
	deleteProxy("test")
	if _, ok := unavailableProxy["test"]; ok {
		t.Error("not delete")
	}

}
