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

// Package scalehandler -
package scalehandler

import (
	"errors"
	"testing"
	"time"

	"functioncore/pkg/state-manager/config"
	mockutils "functioncore/test/utils"
	"github.com/agiledragon/gomonkey/v2"
)

func TestActions_Actions(t *testing.T) {
	actions := initActions()
	actions.addAction("funcSvc1", &Action{
		NeedPull:   0,
		NeedReduce: 0,
	})
	actions.addAction("funcSvc2", &Action{
		NeedPull:   0,
		NeedReduce: 0,
	})
	if len(actions) != 2 {
		t.Errorf("failed to test add action, because len action not right, len is %d", len(actions))
	}

	actions.addAction("funcSvc1", &Action{
		NeedPull:   0,
		NeedReduce: 0,
	})
	if len(actions) != 2 {
		t.Errorf("failed to test add action funSvc1, because len action not right, len is %d", len(actions))
	}

	actions.addAction("funcSvc3", nil)
	if len(actions) != 2 {
		t.Errorf("failed to test add action funcSvc3, because len action not right, len is %d", len(actions))
	}

	actions = initActions()
	actions.addAction("funcSvc1", &Action{
		Type:       ActionPull,
		NeedPull:   0,
		NeedReduce: 0,
	})

	actions.addAction("funcSvc2", &Action{
		Type:       ActionReduce,
		NeedPull:   0,
		NeedReduce: 0,
	})

	actions.addAction("funcSvc3", &Action{
		Type:       999,
		NeedPull:   0,
		NeedReduce: 0,
	})

	actions.DoActions()

	actions["funcSvc3"].addPods([]string{"podA", "podB"})
	if len(actions["funcSvc3"].pods) != 2 {
		t.Errorf("failed to test addPods")
	}

}

func TestAction_Action(t *testing.T) {
	action := &Action{
		pods: []string{"podA", "podB"},
	}

	failedNum, successNum := action.ScaleUp("funcSvc1", 1)
	if failedNum+successNum != 1 {
		t.Errorf("failed to test scale up")
	}

	action = &Action{
		pods: []string{"podA", "podB"},
	}

	failedNum, successNum = action.ScaleDown("funcSvc1", 1)
	if failedNum+successNum != 1 {
		t.Errorf("failed to test scale down")
	}

}

func Test_checkLastScaleTime(t *testing.T) {

	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
			return &config.FConfig{}, errors.New("failed to get config")
		}),
	})

	ok := checkLastScaleTime("funcSvcID1")
	if ok {
		t.Errorf("failed to test check last time")
	}

	patches.ResetAll()

	patches = mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
			return &config.FConfig{
				ScaleData: config.ScaleData{
					DownscaleStabilization: 1,
				},
			}, nil
		}),
	})

	setLastScaleTime("funcSvcNew")
	ok = checkLastScaleTime("funcSvcNew")
	if ok {
		t.Errorf("failed to test check last time")
	}

	setLastScaleTime("funcSvcNew")

	time.Sleep(2 * time.Second)
	ok = checkLastScaleTime("funcSvcNew")
	if !ok {
		t.Errorf("failed to test check last time")
	}

	setLastScaleTime("funcSvcNew")
	ClearLastScaleTime("funcSvcNew")
	ok = checkLastScaleTime("funcSvcNew")
	if !ok {
		t.Errorf("failed to test check last time")
	}

	patches.ResetAll()

}
