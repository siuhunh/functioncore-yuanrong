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

package cache

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"functioncore/pkg/common/snerror"
)

func TestMessageQueue(t *testing.T) {
	queue := GetMessageQueue()
	err := queue.WaitUnblock(1, true)
	assert.NoError(t, err)
	queue.Block()

	backlogCh := make(chan struct{})
	go func() {
		queue.WaitForBacklog()
		close(backlogCh)
	}()
	time.Sleep(1 * time.Second)
	select {
	case <-backlogCh:
		require.FailNow(t, "queue should wait for backlog")
	default:
	}

	ch := make(chan error, 2)
	go func() {
		ch <- queue.WaitUnblock(1, true)
	}()
	go func() {
		ch <- queue.WaitUnblock(1, true)
	}()
	time.Sleep(1 * time.Second)
	select {
	case <-ch:
		require.FailNow(t, "queue should be blocked")
	default:
	}

	select {
	case <-backlogCh:
	default:
		require.FailNow(t, "queue should finish waiting for backlog")
	}

	myerr := errors.New("my error")
	queue.UnblockAndFlush(myerr)

	for i := 0; i < 2; i++ {
		select {
		case err := <-ch:
			assert.True(t, errors.Is(err, myerr))
		default:
			require.FailNow(t, "queue should be unblocked")
		}
	}
}

func BenchmarkWaitUnblock(b *testing.B) {
	queue := GetMessageQueue()

	b.SetParallelism(200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.WaitUnblock(1, true)
		}
	})
}

func TestSetInitType(t *testing.T) {
	queue := GetMessageQueue()
	queue.SetNoBlockType()
	assert.True(t, queue.noBlockType)
}

func TestGetMessageQueue(t *testing.T) {
	queue := newMessageQueue()
	queue.noBlockType = true
	queue.block = true
	queue.Error(errors.New("test"))
	err := queue.WaitUnblock(1, true)
	assert.NotNil(t, err)
	queue.noBlockType = false
	err = queue.WaitUnblock(1, true)
	assert.NotNil(t, err)
}

func TestError(t *testing.T) {
	queue := newMessageQueue()
	queue.Error(snerror.NewSNError(404, "not found"))
	assert.NotNil(t, queue.err)
	err := queue.err
	snErr, ok := err.(snerror.SNError)
	if !ok {
		t.Errorf("this should be ok")
	}
	if snErr.Code() != 404 || !strings.Contains(snErr.Error(), "not found") {
		t.Errorf("thi is error, errCode: %v, errMsg: %s", snErr.Code(), snErr.Error())
	}
}
