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

// Package bus contains interface of bus and basic codes
package calls

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
)

func TestCallsCollector(t *testing.T) {
	collector := InitCollector(1)
	_, ch, err := collector.AddRequest()
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		collector.Clear()
		fmt.Println("Collector clear finished")
		wg.Done()
	}()
	go func() {
		// sleep to ensure that request finished after collector clear
		time.Sleep(10 * time.Millisecond)
		resp := &types.TCPResponse{
			ErrorCode:    0,
			ErrorMessage: "",
			RawData:      nil,
			Logs:         "",
			RequestID:    0,
		}
		collector.AddResponse(resp)
		resp.RequestID = 1
		collector.AddResponse(resp)
	}()

	result := <-ch
	assert.Equal(t, result.RequestID, uint64(1))

	// test for GracefulExit
	wg.Wait()
}

func TestCallsCollectorReachLimit(t *testing.T) {
	collector := InitCollectorOnce(0)
	_, _, err := collector.AddRequest()
	assert.NotNil(t, err)
	collector.Clear()
}
