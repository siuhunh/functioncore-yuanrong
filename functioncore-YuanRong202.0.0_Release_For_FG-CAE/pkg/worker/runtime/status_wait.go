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

package runtime

import (
	"errors"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tcpmsg"
)

// DirectRuntimeState is used to wait runtime state ok.
type DirectRuntimeState struct {
	isDirectFunc bool
	blockCond    *sync.Cond
	mux          sync.Mutex
	err          error
}

var runtimeState = &DirectRuntimeState{}

// NewRuntimeState -
func NewRuntimeState(isDirect bool) {
	runtimeState.blockCond = sync.NewCond(&runtimeState.mux)
	runtimeState.isDirectFunc = isDirect
	return
}

// GetDirectRuntimeState -
func GetDirectRuntimeState() *DirectRuntimeState {
	return runtimeState
}

// WaitUnblock -
func (q *DirectRuntimeState) WaitUnblock(restartRuntime bool) error {
	q.mux.Lock()
	defer q.mux.Unlock()
	var err error
	if q.isDirectFunc && restartRuntime {
		log.GetLogger().Infof("current status %d,", BackgroundProcessLimit.Status.Get())
		if BackgroundProcessLimit.Status.Get() == tcpmsg.RuntimeProcessStatusRunning ||
			BackgroundProcessLimit.Status.Get() == tcpmsg.RuntimeProcessStatusReconnectFailed {
			msg := tcpmsg.BuildMessage(tcpmsg.KillRuntimeProcess, nil)
			err := RuntimeManagerClient.SendMessageWithNoResp(msg)
			if err != nil {
				log.GetLogger().Warnf("failed to kill runtime: %s", err.Error())
				return err
			}
		}
		log.GetLogger().Info("currently waiting to be unlocked")
		q.blockCond.Wait()
		err = q.err
	}
	return err
}

// Unblock -
func (q *DirectRuntimeState) Unblock() {
	q.mux.Lock()
	defer q.mux.Unlock()
	if q.isDirectFunc {
		log.GetLogger().Info("currently unlocked")
		q.blockCond.Broadcast()
		if BackgroundProcessLimit.Status.Get() != tcpmsg.RuntimeProcessStatusRunning {
			q.err = errors.New("failed to reconnect runtime")
			return
		}
	}
}
