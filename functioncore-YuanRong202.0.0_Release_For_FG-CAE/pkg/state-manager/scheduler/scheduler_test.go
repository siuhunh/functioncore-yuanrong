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

package scheduler

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"
	"gotest.tools/assert"

	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/mock"
	"functioncore/pkg/state-manager/scheduler/metadata"
	"functioncore/pkg/state-manager/scheduler/predicate"
	"functioncore/pkg/state-manager/scheduler/priority"
	"functioncore/pkg/state-manager/utils"
)

type predicateFoo struct{}

func (f *predicateFoo) Filter(
	target *metadata.TargetInfo, infos []*metadata.NodeInfo) ([]*metadata.NodeInfo, error) {
	return nil, errors.New("error")
}

func TestGetPriorityNode(t *testing.T) {
	convey.Convey("GetPriorityNode", t, func() {
		errFoo := errors.New("error")
		targetInfo := &metadata.TargetInfo{}
		infos := []*metadata.NodeInfo{}

		convey.Convey("test 1", func() {
			patches := [1]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetTargetInfo,
					func(_ string) (*metadata.TargetInfo, error) {
						return nil, errFoo
					}),
			}
			defer patches[0].Reset()
			_, err := GetPriorityNode("handler")
			convey.So(err, convey.ShouldEqual, errFoo)
		})

		convey.Convey("test 2", func() {
			patches := [2]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetTargetInfo,
					func(_ string) (*metadata.TargetInfo, error) {
						return &metadata.TargetInfo{}, nil
					}),
				gomonkey.ApplyFunc(metadata.GetNodes,
					func() ([]*metadata.NodeInfo, error) {
						return nil, errFoo
					}),
			}
			defer patches[0].Reset()
			defer patches[1].Reset()
			_, err := GetPriorityNode("handler")
			convey.So(err, convey.ShouldEqual, errFoo)
		})

		convey.Convey("test 3", func() {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockPre := mock.NewMockAbsPredicateAlgorithm(ctl)
			//mockPre.EXPECT().Filter(targetInfo, infos).Return(nil, errFoo)
			filter = mockPre

			patches := [3]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetTargetInfo,
					func(_ string) (*metadata.TargetInfo, error) {
						return &metadata.TargetInfo{}, nil
					}),
				gomonkey.ApplyFunc(metadata.GetNodes,
					func() ([]*metadata.NodeInfo, error) {
						return []*metadata.NodeInfo{}, nil
					}),
			}
			defer patches[0].Reset()
			defer patches[1].Reset()
			_, err := GetPriorityNode("handler")
			convey.So(err, convey.ShouldNotEqual, nil)
		})

		convey.Convey("test 4", func() {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockPre := mock.NewMockAbsPredicateAlgorithm(ctl)
			mockPre.EXPECT().Filter(targetInfo, infos).Return([]*metadata.NodeInfo{}, nil)
			filter = mockPre
			mockPri := mock.NewMockAbsPriorityAlgorithm(ctl)
			mockPri.EXPECT().Score(targetInfo, infos).Return(nil, errFoo)
			scorer = mockPri

			patches := [3]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetTargetInfo,
					func(_ string) (*metadata.TargetInfo, error) {
						return &metadata.TargetInfo{}, nil
					}),
				gomonkey.ApplyFunc(metadata.GetNodes,
					func() ([]*metadata.NodeInfo, error) {
						return []*metadata.NodeInfo{}, nil
					}),
			}
			defer patches[0].Reset()
			defer patches[1].Reset()
			_, err := GetPriorityNode("handler")
			convey.So(err, convey.ShouldEqual, errFoo)
		})

		convey.Convey("test 5", func() {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockPre := mock.NewMockAbsPredicateAlgorithm(ctl)
			mockPre.EXPECT().Filter(targetInfo, infos).Return([]*metadata.NodeInfo{}, nil)
			filter = mockPre
			mockPri := mock.NewMockAbsPriorityAlgorithm(ctl)
			mockPri.EXPECT().Score(targetInfo, infos).Return([]*metadata.NodeScore{}, nil)
			scorer = mockPri

			patches := [4]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetTargetInfo,
					func(_ string) (*metadata.TargetInfo, error) {
						return &metadata.TargetInfo{}, nil
					}),
				gomonkey.ApplyFunc(metadata.GetNodes,
					func() ([]*metadata.NodeInfo, error) {
						return []*metadata.NodeInfo{}, nil
					}),
				gomonkey.ApplyFunc(selectNode,
					func(_ []*metadata.NodeScore) (string, error) {
						return "", errFoo
					}),
			}
			defer patches[0].Reset()
			defer patches[1].Reset()
			defer patches[2].Reset()
			_, err := GetPriorityNode("handler")
			convey.So(err, convey.ShouldEqual, errFoo)
		})

		convey.Convey("test 6", func() {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockPre := mock.NewMockAbsPredicateAlgorithm(ctl)
			mockPre.EXPECT().Filter(targetInfo, infos).Return([]*metadata.NodeInfo{}, nil)
			filter = mockPre
			mockPri := mock.NewMockAbsPriorityAlgorithm(ctl)
			mockPri.EXPECT().Score(targetInfo, infos).Return([]*metadata.NodeScore{}, nil)
			scorer = mockPri

			patches := [6]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetTargetInfo,
					func(_ string) (*metadata.TargetInfo, error) {
						return &metadata.TargetInfo{}, nil
					}),
				gomonkey.ApplyFunc(metadata.GetNodes,
					func() ([]*metadata.NodeInfo, error) {
						return []*metadata.NodeInfo{}, nil
					}),
				gomonkey.ApplyFunc(selectNode,
					func(_ []*metadata.NodeScore) (string, error) {
						return "", nil
					}),
				gomonkey.ApplyFunc(metadata.AddNode,
					func(_, _ string) {
						return
					}),
				gomonkey.ApplyFunc(utils.GetServiceID,
					func(_ string) string {
						return ""
					}),
			}
			defer patches[0].Reset()
			defer patches[1].Reset()
			defer patches[2].Reset()
			defer patches[3].Reset()
			defer patches[4].Reset()
			_, err := GetPriorityNode("handler")
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestInitScheduler(t *testing.T) {
	convey.Convey("InitScheduler", t, func() {
		convey.Convey("ConfigIsNil", func() {
			err := InitScheduler(nil)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("NewPredicateAlgorithmErr", func() {
			patch := gomonkey.ApplyFunc(predicate.NewPredicateAlgorithm,
				func(name string) (predicate.AbsPredicateAlgorithm, error) {
					return nil, errors.New("test err")
				})
			err := InitScheduler(&config.FConfig{ScheCfg: config.SchedulerConfig{
				Filter: config.Filter{},
				Scorer: config.Scorer{},
			}})
			convey.So(err, convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("NewPriorityAlgorithmErr", func() {
			patch := gomonkey.ApplyFunc(priority.NewPriorityAlgorithm,
				func(name string, conf interface{}) (priority.AbsPriorityAlgorithm, error) {
					return nil, errors.New("test err")
				})
			err := InitScheduler(&config.FConfig{ScheCfg: config.SchedulerConfig{
				Filter: config.Filter{},
				Scorer: config.Scorer{},
			}})
			convey.So(err, convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("InitSyncErr", func() {
			patch := gomonkey.ApplyFunc(metadata.InitSync,
				func(conf *config.FConfig) error {
					return errors.New("test err")
				})
			err := InitScheduler(&config.FConfig{ScheCfg: config.SchedulerConfig{
				Filter: config.Filter{AlgorithmName: "overloadfilter"},
				Scorer: config.Scorer{
					AlgorithmName:    "directStateLoadScorer",
					CPUScoreFactor:   0.0,
					MemScoreFactor:   0.0,
					StateScoreFactor: 1.0,
					AffinityFactor:   0.001,
				},
			}})
			convey.So(err, convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("InitSyncOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(predicate.NewPredicateAlgorithm,
					func(name string) (predicate.AbsPredicateAlgorithm, error) {
						return nil, nil
					}),
				gomonkey.ApplyFunc(metadata.InitSync, func(conf *config.FConfig) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitScheduler(&config.FConfig{ScheCfg: config.SchedulerConfig{
				Filter: config.Filter{AlgorithmName: "overloadfilter"},
				Scorer: config.Scorer{
					AlgorithmName:    "overloadfilter",
					CPUScoreFactor:   0.0,
					MemScoreFactor:   0.0,
					StateScoreFactor: 1.0,
					AffinityFactor:   0.001,
				},
			}})
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

// TestGetPriorityNodeWithoutFuncName -
func TestGetPriorityNodeWithoutFuncName(t *testing.T) {
	convey.Convey("TestGetPriorityNodeWithoutFunName", t, func() {
		convey.Convey("GetNodesErr", func() {
			patch := gomonkey.ApplyFunc(metadata.GetNodes, func() ([]*metadata.NodeInfo, error) {
				return nil, errors.New("test err")
			})
			_, err := GetPriorityNodeWithoutFuncName()
			convey.So(err, convey.ShouldBeError)
			patch.Reset()
		})
	})
}

func Test_selectNode(t *testing.T) {
	convey.Convey("selectNode", t, func() {
		nodeID := "node1"
		convey.Convey("test 1", func() {
			scores := make([]*metadata.NodeScore, 2, 2)
			scores[0] = nil
			scores[1] = &metadata.NodeScore{
				ID:    nodeID,
				Score: 100,
			}
			node, _ := selectNode(scores)
			convey.So(node, convey.ShouldEqual, nodeID)
		})

		convey.Convey("test 2", func() {
			scores := make([]*metadata.NodeScore, 2, 2)
			scores[0] = nil
			scores[1] = nil
			_, err := selectNode(scores)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestAddInstanceMap(t *testing.T) {
	convey.Convey("AddInstanceMap", t, func() {
		convey.Convey("test 1", func() {
			functionName := "0-count-addone"
			nodeID := "node1"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready","ownerIP":""}`
			AddInstanceMap(functionName, nodeID, []byte(value))
			AddInstanceMap(functionName, nodeID, []byte(value))
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeID, convey.ShouldEqual, nodeIDGet[0])
		})

		convey.Convey("test 2", func() {
			functionName := "0-count2-addone2"
			nodeID := "node2"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready"}`
			AddInstanceMap(functionName, nodeID, []byte(value))
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeID, convey.ShouldEqual, nodeIDGet[0])
		})

		convey.Convey("test 3", func() {
			functionName := "0-count3-addone3"
			nodeID := "node3"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready","ownerIP":"127.0.0.1"}`
			AddInstanceMap(functionName, nodeID, []byte(value))
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeIDGet, convey.ShouldEqual, nil)
		})

		convey.Convey("test 4", func() {
			functionName := "0-count4-addone4"
			nodeID1 := "node1"
			nodeID2 := "node2"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready","ownerIP":""}`
			AddInstanceMap(functionName, nodeID1, []byte(value))
			AddInstanceMap(functionName, nodeID2, []byte(value))
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeIDGet[0], convey.ShouldEqual, nodeID1)
			convey.So(nodeIDGet[1], convey.ShouldEqual, nodeID2)
		})
	})
}

func TestDeleteInstanceMap(t *testing.T) {
	convey.Convey("DeleteInstanceMap", t, func() {
		convey.Convey("test 1", func() {
			functionName := "0-count-addone"
			nodeID := "node1"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready","ownerIP":""}`
			AddInstanceMap(functionName, nodeID, []byte(value))
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeID, convey.ShouldEqual, nodeIDGet[0])
			DeleteInstanceMap(functionName, nodeID)
			nodeIDGet = GetMinInstanceNode(functionName)
			convey.So(nodeIDGet, convey.ShouldEqual, nil)
		})

		convey.Convey("test 2", func() {
			functionName := "0-count2-addone2"
			nodeID := "node2"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready"}`
			AddInstanceMap(functionName, nodeID, []byte(value))
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeID, convey.ShouldEqual, nodeIDGet[0])
			DeleteInstanceMap(functionName, nodeID)
			nodeIDGet = GetMinInstanceNode(functionName)
			convey.So(nodeIDGet, convey.ShouldEqual, nil)
		})

		convey.Convey("test 3", func() {
			functionName := "0-count3-addone3"
			nodeID := "node1"
			nodeID2 := "node2"
			value := `{"ip":"127.0.0.1","port":"8080","status":"ready","ownerIP":""}`
			AddInstanceMap(functionName, nodeID, []byte(value))
			AddInstanceMap(functionName, nodeID2, []byte(value))
			DeleteInstanceMap(functionName, nodeID)
			nodeIDGet := GetMinInstanceNode(functionName)
			convey.So(nodeIDGet[0], convey.ShouldEqual, nodeID2)
			DeleteInstanceMap(functionName, nodeID2)
			nodeIDGet = GetMinInstanceNode(functionName)
			convey.So(nodeIDGet, convey.ShouldEqual, nil)
		})
	})
}

func TestIsAvailableNode(t *testing.T) {
	availableNodes.LoadOrStore("test1", make(map[string]string, 0))
	result := IsAvailableNode("test1")
	assert.Equal(t, true, result)
}

type testMap struct {
	data map[string]string
}

func TestAddAvailableNodes(t *testing.T) {
	nodeMap := make(map[string]string, 0)
	nodeMap["test1"] = "test1"
	testValue := testMap{
		nodeMap,
	}

	jsonData, err := json.Marshal(testValue)
	if err != nil {
		return
	}
	AddAvailableNodes("test1", jsonData)
}

func TestDelAvailableNodes(t *testing.T) {
	availableNodes.LoadOrStore("test1", make(map[string]string, 0))
	DelAvailableNodes("test1")
}
