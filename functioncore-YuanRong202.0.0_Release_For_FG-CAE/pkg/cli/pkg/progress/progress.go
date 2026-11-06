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

// Package progress use to displays work progress.
package progress

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultRepeatCount = 9
	defaultSleepTime   = 300
	// FailedDone Failed to done
	FailedDone = "fail"
)

// Progress process info
type Progress struct {
	Begin        time.Time
	DoingMessage string
	RepeatCount  int
	DoneChan     chan struct{}
}

// CreateAndDoing crate a progress and start doing
func CreateAndDoing(doingMessage string) *Progress {
	progress := &Progress{
		Begin:        time.Now(),
		RepeatCount:  defaultRepeatCount,
		DoingMessage: doingMessage,
		DoneChan:     make(chan struct{}),
	}
	go progress.doing()
	return progress
}

// Doing print doing message until done
func (p *Progress) doing() {
	for i := 1; ; i++ {
		select {
		case _, ok := <-p.DoneChan:
			if !ok {
				// 通道关闭则退出
				return
			}
		default:
			dynamicMessage := strings.Repeat(".", i)
			if i == p.RepeatCount {
				dynamicMessage = strings.Repeat(" ", i)
				i = 0
			}
			if _, err := fmt.Fprintf(os.Stdout, "%s %s\r", p.DoingMessage, dynamicMessage); err != nil {
				continue
			}
			time.Sleep(time.Millisecond * defaultSleepTime)
		}
	}
}

// Done set Progress done
func (p *Progress) Done(doneMessage string) {
	if doneMessage != FailedDone {
		elapsed := time.Since(p.Begin)
		fmt.Printf("%s costs time %dms\n", doneMessage, elapsed.Milliseconds())
	}
	close(p.DoneChan)
}
