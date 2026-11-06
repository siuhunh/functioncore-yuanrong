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

// Package cache is the cache for worker
package cache

import (
	"sync"
	"testing"
)

func Test_customRetryAlarmHelper_AddAlarm(t *testing.T) {
	c := CustomRetryAlarmHelper{
		retryMap:   map[string]struct{}{},
		retryMutex: sync.Mutex{},
	}
	c.AddAlarm("funcSvcID1")
	if !c.TryClearAlarmIfExist("funcSvcID1") {
		t.Errorf("failed to test add alarm")
	}

	if c.TryClearAlarmIfExist("funcSvcID1") {
		t.Errorf("failed to test add alarm")
	}
}
