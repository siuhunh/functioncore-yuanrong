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

// Package preload alarm
package preload

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func resetPreloadRequestFailedTestData() {
	exceedTenantPreloadDiskLimitNodeMap = map[string]map[string]string{}
	otherProblemNodeMap = map[string]map[string]string{}
	requestFailedNodeIPMap.Range(func(key, value interface{}) bool {
		requestFailedNodeIPMap.Delete(key)
		return true
	})
	requestFailedNodeCount.Store(0)
}

func TestReachTenantPreloadDiskLimit(t *testing.T) {
	t.Run("node recover no reach tenant preload disk limit", func(t *testing.T) {
		defer resetPreloadRequestFailedTestData()
		recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
		tenantIDSet, exists := exceedTenantPreloadDiskLimitNodeMap["127.0.0.1"]
		assert.True(t, exists)
		_, exists = tenantIDSet["tID1"]
		assert.True(t, exists)
		removeExceedTenantPreloadDiskLimitIfExists("tID1", "127.0.0.1")
		_, exists = exceedTenantPreloadDiskLimitNodeMap["127.0.0.1"]
		assert.False(t, exists)
	})

	t.Run("node still one tenant preload disk limit", func(t *testing.T) {
		defer resetPreloadRequestFailedTestData()
		recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
		recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.1")
		removeExceedTenantPreloadDiskLimitIfExists("tID1", "127.0.0.1")
		tenantIDSet, exists := exceedTenantPreloadDiskLimitNodeMap["127.0.0.1"]
		assert.True(t, exists)
		_, exists = tenantIDSet["tID2"]
		assert.True(t, exists)
		_, exists = tenantIDSet["tID1"]
		assert.False(t, exists)
	})
}

func TestGetAllReachTenantPreloadNodes(t *testing.T) {
	defer resetPreloadRequestFailedTestData()
	recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
	recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.1")
	recordExceedTenantPreloadDiskLimit(exceedPreloadDiskLimitTenant, "127.0.0.1")
	recordExceedTenantPreloadDiskLimit(exceedPreloadDiskLimitTenant, "127.0.0.2")
	recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.3")
	reachTenantPreloadLimitNodes, reachPreloadLimitNodes := getExceedTenantPreloadDiskLimitNodes()
	assert.ElementsMatch(t, reachTenantPreloadLimitNodes, []string{"127.0.0.1", "127.0.0.3"})
	assert.ElementsMatch(t, reachPreloadLimitNodes, []string{"127.0.0.1", "127.0.0.2"})
}

func TestRemoveRequestFailedNodeWhenOK(t *testing.T) {
	t.Run("exists reach preload disk error", func(t *testing.T) {
		defer resetPreloadRequestFailedTestData()
		recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		removeRequestFailedNodeWhenOK("127.0.0.1", "tID1")
		assert.Equal(t, int64(1), requestFailedNodeCount.Load())
	})
	t.Run("no reach preload disk error", func(t *testing.T) {
		defer resetPreloadRequestFailedTestData()
		recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		assert.Equal(t, int64(1), requestFailedNodeCount.Load())

		removeRequestFailedNodeWhenOK("127.0.0.1", "tID1")
		removeRequestFailedNodeWhenOK("127.0.0.1", "tID2")
		assert.Equal(t, int64(0), requestFailedNodeCount.Load())
	})
	t.Run("no disk error but exists other error", func(t *testing.T) {
		defer resetPreloadRequestFailedTestData()
		recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		recordOtherProblem("tID3", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")

		removeRequestFailedNodeWhenOK("127.0.0.1", "tID1")
		removeRequestFailedNodeWhenOK("127.0.0.1", "tID2")
		assert.Equal(t, int64(1), requestFailedNodeCount.Load())
	})
	t.Run("no error", func(t *testing.T) {
		defer resetPreloadRequestFailedTestData()
		recordExceedTenantPreloadDiskLimit("tID1", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		recordExceedTenantPreloadDiskLimit("tID2", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")
		recordOtherProblem("tID3", "127.0.0.1")
		addRequestFailedNode("127.0.0.1")

		removeRequestFailedNodeWhenOK("127.0.0.1", "tID1")
		removeRequestFailedNodeWhenOK("127.0.0.1", "tID2")
		removeRequestFailedNodeWhenOK("127.0.0.1", "tID3")
		assert.Equal(t, int64(0), requestFailedNodeCount.Load())
	})
}
