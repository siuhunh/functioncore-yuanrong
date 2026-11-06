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

package fusion

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker/runtime"
)

func TestExceptionHandler(t *testing.T) {
	eh := newExceptionHandler()
	err := eh.getException()
	assert.Nil(t, err)

	eh.clearException()

	expectedErr := &runtime.Error{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := eh.getException()
		assert.Equal(t, expectedErr, err)
		wg.Done()
	}()
	time.Sleep(500 * time.Millisecond)
	eh.setException(expectedErr)
	eh.clearException()
	wg.Wait()
}
