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

// Package statedispatcher -
package statedispatcher

import (
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
)

type MockStateAutoScaler struct {
	mockChan chan bool
}

func (mockSas *MockStateAutoScaler) ScaleUp(req *types.DispatcherRequest) (*types.Instance, error) {
	return nil, nil
}

func (mockSas *MockStateAutoScaler) ScaleDown(workerID string, stateID string) bool {
	mockSas.mockChan <- true
	return true
}

func TestStateSchedule_watchStateIDDelEvent(t *testing.T) {
	convey.Convey("TestStateSchedule_watchStateIDDelEvent", t, func() {
		convey.Convey("testScaleDown", func() {
			ch := make(chan bool, 1)
			schedule := &stateSchedule{
				autoScaler: &MockStateAutoScaler{
					mockChan: ch,
				},
				entries: make(map[string]*scheduleEntry, 0),
			}
			schedule.entries["test"] = &scheduleEntry{}
			stopCh := make(chan struct{})
			insCh := make(chan string, 1)
			go schedule.watchStateIDDelEvent(nil, insCh)
			go schedule.watchStateIDDelEvent(stopCh, nil)
			go schedule.watchStateIDDelEvent(stopCh, insCh)
			convey.So(len(schedule.entries), convey.ShouldEqual, 1)
			insCh <- "test"
			ok := <-ch
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(len(schedule.entries), convey.ShouldEqual, 0)
		})
	})
}

func TestCheckAndPreScaleDownWorker(t *testing.T) {
	ch := make(chan bool, 1)
	entries := make(map[string]*scheduleEntry, 1)
	entries["test"] = &scheduleEntry{currentRequests: -1}
	schedule := &stateSchedule{
		autoScaler: &MockStateAutoScaler{
			mockChan: ch,
		},
		entries: entries,
	}
	schedule.checkAndPreScaleDownWorker()
}

func TestCreateRequestDispatcher(t *testing.T) {
	ch := make(chan bool, 1)
	entries := make(map[string]*scheduleEntry, 1)
	entries["state01"] = &scheduleEntry{
		currentRequests: -1,
		workerID:        "workerId",
	}
	schedule := &stateSchedule{
		autoScaler: &MockStateAutoScaler{
			mockChan: ch,
		},
		entries: entries,
	}
	request := buildTestRequest()
	workerId, _ := schedule.Schedule(request)
	assert.Equal(t, "workerId", workerId)
}

func Test_stateSchedule_forward(t *testing.T) {
	convey.Convey("test state schedule", t, func() {
		patches := []*gomonkey.Patches{}
		oldCall := common.SendCallReq
		common.SendCallReq = func(nodeID, traceID string, callReq *types.TCPRequest, dst *common.FunctionProperty) *types.TCPResponse {
			return &types.TCPResponse{}
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
			common.SendCallReq = oldCall
		}()
		convey.Convey("instance is nil", func() {
			var instance *types.Instance
			var req *types.DispatcherRequest
			dispatcher := &stateSchedule{
				entries: make(map[string]*scheduleEntry, 0),
			}
			isForward := dispatcher.forward(req, instance)
			convey.So(isForward, convey.ShouldBeFalse)
			convey.So(len(dispatcher.entries), convey.ShouldEqual, 0)
		})
		convey.Convey("local bus", func() {
			instance := &types.Instance{
				DeployedIP: "1.1.1.1",
				InstanceID: "instanceID",
			}
			req := &types.DispatcherRequest{
				Request: &types.TCPRequest{
					StateID: stateId,
				},
				Response: make(chan *types.TCPResponse, 1),
			}
			internal.SelfNodeIP = "1.1.1.1"
			dispatcher := &stateSchedule{
				entries: make(map[string]*scheduleEntry, 0),
			}
			isForward := dispatcher.forward(req, instance)
			convey.So(isForward, convey.ShouldBeFalse)
			convey.So(len(dispatcher.entries), convey.ShouldEqual, 1)
			convey.So(dispatcher.entries[stateId].nodeID, convey.ShouldEqual, "1.1.1.1")
			convey.So(dispatcher.entries[stateId].workerID, convey.ShouldEqual, "instanceID")
			convey.So(dispatcher.entries[stateId].stateID, convey.ShouldEqual, stateId)
		})
		convey.Convey("not local bus", func() {
			instance := &types.Instance{
				DeployedIP: "1.1.1.2",
			}
			req := &types.DispatcherRequest{
				Request:  &types.TCPRequest{},
				Response: make(chan *types.TCPResponse, 1),
			}
			internal.SelfNodeIP = "1.1.1.1"
			dispatcher := &stateSchedule{
				entries: make(map[string]*scheduleEntry, 0),
			}
			isForward := dispatcher.forward(req, instance)
			resp := <-req.Response
			convey.So(isForward, convey.ShouldBeTrue)
			convey.So(resp.ErrorCode, convey.ShouldEqual, 0)
			convey.So(len(dispatcher.entries), convey.ShouldEqual, 0)
		})
	})
}

type MockFailedStateAutoScaler struct {
	mockChan chan bool
}

func (mockSas *MockFailedStateAutoScaler) ScaleUp(req *types.DispatcherRequest) (*types.Instance, error) {
	return nil, errors.New("test")
}

func (mockSas *MockFailedStateAutoScaler) ScaleDown(workerID string, stateID string) bool {
	mockSas.mockChan <- true
	return true
}

func Test_stateSchedule_Schedule(t *testing.T) {
	convey.Convey("test state schedule", t, func() {
		convey.Convey("failed to scale up", func() {
			ch := make(chan bool, 1)
			entries := make(map[string]*scheduleEntry, 1)
			entries["state01-test"] = &scheduleEntry{
				currentRequests: -1,
				workerID:        "workerId",
			}
			schedule := &stateSchedule{
				autoScaler: &MockFailedStateAutoScaler{
					mockChan: ch,
				},
				entries: entries,
			}
			request := buildTestRequest()
			_, err := schedule.Schedule(request)
			assert.NotEqual(t, err, nil)
		})
	})
}

func TestTimerForCheckScaleDown(t *testing.T) {
	ddp := &stateSchedule{}
	stopCh := make(chan struct{})
	go ddp.timerForCheckScaleDown(stopCh)
	time.Sleep(100 * time.Millisecond)
	close(stopCh)
	assert.Equal(t, len(ddp.entries), 0)
}
