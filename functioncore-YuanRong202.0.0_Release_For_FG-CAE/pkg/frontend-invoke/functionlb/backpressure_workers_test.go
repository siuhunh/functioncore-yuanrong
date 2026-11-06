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

// Package functionlb manage Function LB and deal with worker instance
package functionlb

import (
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

func TestBackPressureWorkers(t *testing.T) {
	p := gomonkey.NewPatches()
	c := make(chan time.Time)
	p.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: c}
	})
	defer p.Reset()

	callback := deleteCallback{
		urnWithRes: "7e186a/0@base@testresourcepython36768/latest/300-128",
		function:   func(az, instance, urn string) {},
	}
	bpWorkers := initBackPressureWorkers(&sync.Mutex{})
	{
		bpWorkers.add("127.0.0.1:22423@instanceA", callback)
		assert.Equal(t, 1, len(bpWorkers.workers))
		bpWorkers.delete("127.0.0.1:22423@instanceA")
		assert.Equal(t, 0, len(bpWorkers.workers))
	}

	bpWorkers.checkPeriodically(nil)
	{
		ch := make(chan interface{})
		callback := deleteCallback{
			urnWithRes: "7e186a/0@base@testresourcepython36768/latest/300-128",
			function: func(az, instance, urn string) {
				close(ch)
			},
		}
		bpWorkers.add("127.0.0.1:22423@instanceB", callback)
		stopCh := make(chan struct{})
		go bpWorkers.checkPeriodically(stopCh)
		c <- time.Now()
		c <- time.Now()
		_, ok := <-ch
		assert.Equal(t, false, ok)
		close(c)
	}
	{
		stopCh := make(chan struct{})
		close(stopCh)
		// test checkPeriodically exit
		bpWorkers.checkPeriodically(stopCh)
	}
}
