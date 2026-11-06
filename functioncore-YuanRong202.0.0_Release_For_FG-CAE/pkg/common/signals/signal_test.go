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

package signals

import (
	"syscall"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestWaitForSignal(t *testing.T) {
	stopCh := WaitForSignal()

	go func() {
		time.Sleep(200 * time.Millisecond)
		shutdownHandler <- syscall.SIGTERM
	}()
	select {
	case <-stopCh:
		t.Log("received termination signal")
	case <-time.After(time.Second):
		t.Fatal("failed to signal in 1s")
	}

	_, ok := <-stopCh
	assert.Equal(t, ok, false)
}
