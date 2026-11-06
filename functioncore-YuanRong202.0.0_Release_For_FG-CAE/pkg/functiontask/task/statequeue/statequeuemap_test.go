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

package statequeue

import (
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	core "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/statequeue/reqnode"
	"functioncore/pkg/functiontask/task/util"
	mockutils "functioncore/test/utils"
)

type MockStateStore struct{}

// Set the state by state id.
func (s MockStateStore) Set(key string, value string, expireTime int) common.TaskError {
	if value == "false" {
		return common.NewTaskError(1, "aaa")
	}
	return nil
}

// Get the state by state id.
func (s MockStateStore) Get(key string) (string, common.TaskError) {
	return "", nil
}

// DeleteStateValueAndStateMapping delete the state by state id.
func (s MockStateStore) DeleteStateValueAndStateMapping(key string) common.TaskError {
	return s.DeleteState(key, "")
}

// SetStateAndResult Sets the state and result.
func (s MockStateStore) SetStateAndResult(key *metadata.Metadata, state string, TTL int,
	req *common.Request, result *common.Response) common.TaskError {
	if state == "false" {
		return common.NewTaskError(1, "aaa")
	}
	return nil
}

// DeleteState the state.
func (s MockStateStore) DeleteState(key, funcName string) common.TaskError {
	return nil
}

// SetTTL sets the ttl for the state.
func (s MockStateStore) SetTTL(key string, ttl int) common.TaskError {
	if ttl == 1 {
		return common.NewTaskError(1, "aaa")
	}
	return nil
}

// Scan returns state ids with specific service id.
func (s MockStateStore) Scan(pattern string) (map[string]string, common.TaskError) {
	return nil, nil
}

// GetNumsByServiceID get nums of stateID by serviceID
func (s MockStateStore) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	return 0, nil
}

// IsNotExist check whether it is a state not found error.
func (s MockStateStore) IsNotExist(err error) bool {
	return err != nil && err.Error() == common.StateIDNotFoundMsg
}

// StateExists get stateID status directly
func (s MockStateStore) StateExists(key string) (bool, common.TaskError) {
	return true, nil
}

// IsStateIDInHash check stateID is in hash
func (s MockStateStore) IsStateIDInHash(stateHashKey string, stateID string) (bool, common.TaskError) {
	return true, nil
}

func newQueueMap() *Map {
	g := NewStateQueueMap(new(simpleInvoker), stateclient.NewInMemoryStateStore(), DefaultConfig())
	return g
}

// TestMap_GetStateCount tests map'sGetStateCount method.
func TestMap_GetStateCount(t *testing.T) {
	Convey("Map_GetStateCount", t, func(c C) {
		var repeatCnt = 10
		g := newQueueMap()
		So(g.GetStateCount(), ShouldEqual, 0)
		var wg sync.WaitGroup

		wg.Add(repeatCnt)
		for i := 0; i < repeatCnt; i++ {
			go func(i int) {
				defer wg.Done()
				md := &metadata.Metadata{StateID: "xx" + strconv.Itoa(i)}
				_ = g.CreateStateQueue(md)
			}(i)
		}
		wg.Wait()
		So(g.GetStateCount(), ShouldEqual, repeatCnt)
	})
}
func TestMap_GetStateSize(t *testing.T) {
	Convey("TestMap_GetStateSize", t, func() {
		g := newQueueMap()
		md1 := &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "xxx1",
		}
		g.CreateStateQueue(md1)
		queue := g.GetQueue(md1)
		queue.SetState("abcde")
		So(g.GetStateSize(), ShouldEqual, 5)

		queue.SetState("")
		So(g.GetStateSize(), ShouldEqual, 0)
	})
}

func TestMap_DelStateIfIdle(t *testing.T) {
	Convey("TestMap_DelStateIfIdle", t, func() {
		config.Conf.PersistentState = false
		config.Conf.PersistentFuture = false
		g := newQueueMap()
		result := g.DelStateIfIdle("xxx1")
		So(result, ShouldBeFalse)

		md2 := &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "xxx2",
		}
		err := g.CreateStateQueue(md2)
		So(err, ShouldBeNil)
		deleted := g.DelStateIfIdle(md2.Key())
		So(deleted, ShouldBeTrue)

		md3 := &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "xxx3",
		}
		_ = g.CreateStateQueue(md3)
		queue := g.GetQueue(md3)
		queue.addRequest(&common.Request{})
		deleted = g.DelStateIfIdle(md3.Key())
		So(deleted, ShouldBeFalse)
	})
}

func (g *Map) mock_createStateQueue(meta *metadata.Metadata) bool {
	g.CheckAndClearOldStateQueue(meta)
	// clear and create ok
	return true
}

func TestMap_CheckAndClearOldStateQueue(t *testing.T) {
	Convey("CheckAndClearOldStateQueue", t, func() {
		Convey("state queue is not exist", func() {
			g := newQueueMap()
			So(g.mock_createStateQueue(&metadata.Metadata{}), ShouldBeTrue)
		})
		Convey("clear state queue ok", func() {
			g := newQueueMap()
			meta := &metadata.Metadata{
				TenantID:     "TestTenant",
				ServiceID:    "TestService",
				FunctionName: "TestFuncName",
				StateID:      "TestStateID",
				StateKey:     "TestStateKey",
			}
			config.Conf.PersistentState = false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(util.HasExceedNumsError, func(client stateclient.StateClient, tenantID, serviceID string) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			g.CreateStateQueue(meta)
			g.stateQueueMap[meta.Key()].addRequest(&common.Request{})
			So(g.mock_createStateQueue(meta), ShouldBeTrue)
		})
	})
}

func TestRequestSetTimeout(t *testing.T) {
	Convey("success", t, func() {
		q := &StateQueue{}
		q.metadata = &metadata.Metadata{}
		q.head = &reqnode.Node{}
		q.head.Next = q.head
		node := &reqnode.Node{
			Req: &common.Request{},
		}
		config.Conf.PersistentState = true
		g := newQueueMap()
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(node.Req), "SetResponse",
			func(r *common.Request, result *common.Response) {
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(node), "Remove",
			func(node *reqnode.Node) {
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		g.requestSetTimeout(q, node, false)
	})
	Convey("Unsupported method: setTimeout", t, func() {
		q := &StateQueue{}
		q.metadata = &metadata.Metadata{}
		node := &reqnode.Node{
			Req: &common.Request{},
		}
		g := newQueueMap()
		config.Conf.PersistentState = false
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(node.Req), "SetResponse",
			func(r *common.Request, result *common.Response) {
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		g.requestSetTimeout(q, node, true)
	})
}

func TestMap_syncStateToExternalStorage(t *testing.T) {
	var stateClient MockStateStore
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyGlobalVar(&config.Conf.PersistentFuture, true),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		mutex         *sync.RWMutex
		config        *QueueConfig
		stateQueueMap map[string]*StateQueue
		Invoker       common.Invoker
		StateClient   stateclient.StateClient
	}
	type args struct {
		q    *StateQueue
		req  *common.Request
		resp *common.Response
	}
	var f fields
	var a args
	f.StateClient = stateClient
	q := &StateQueue{}
	metadata := &metadata.Metadata{}
	q.metadata = metadata
	q.state = "false"
	req := &common.Request{}
	resp := &common.Response{}
	a.q = q
	a.req = req
	a.resp = resp
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, common.NewTaskError(1, "aaa")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &Map{
				mutex:         tt.fields.mutex,
				config:        tt.fields.config,
				stateQueueMap: tt.fields.stateQueueMap,
				Invoker:       tt.fields.Invoker,
				StateClient:   tt.fields.StateClient,
			}
			if got := g.syncStateToExternalStorage(tt.args.q, tt.args.req, tt.args.resp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("syncStateToExternalStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMap_syncStateToExternalStorage_2(t *testing.T) {
	var stateClient MockStateStore
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyGlobalVar(&config.Conf.PersistentFuture, true),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		mutex         *sync.RWMutex
		config        *QueueConfig
		stateQueueMap map[string]*StateQueue
		Invoker       common.Invoker
		StateClient   stateclient.StateClient
	}
	type args struct {
		q    *StateQueue
		req  *common.Request
		resp *common.Response
	}
	var f fields
	var a args
	f.StateClient = stateClient
	q := &StateQueue{}
	metadata := &metadata.Metadata{}
	q.metadata = metadata
	req := &common.Request{}
	resp := &common.Response{}
	a.q = q
	a.req = req
	a.resp = resp
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &Map{
				mutex:         tt.fields.mutex,
				config:        tt.fields.config,
				stateQueueMap: tt.fields.stateQueueMap,
				Invoker:       tt.fields.Invoker,
				StateClient:   tt.fields.StateClient,
			}
			if got := g.syncStateToExternalStorage(tt.args.q, tt.args.req, tt.args.resp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("syncStateToExternalStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMap_syncStateToExternalStorage_3(t *testing.T) {
	var stateClient MockStateStore
	type fields struct {
		mutex         *sync.RWMutex
		config        *QueueConfig
		stateQueueMap map[string]*StateQueue
		Invoker       common.Invoker
		StateClient   stateclient.StateClient
	}
	type args struct {
		q    *StateQueue
		req  *common.Request
		resp *common.Response
	}
	var f fields
	var a args
	f.StateClient = stateClient
	q := &StateQueue{}
	metaData := &metadata.Metadata{}
	q.metadata = metaData
	q.state = "false"
	req := &common.Request{}
	resp := &common.Response{}
	a.q = q
	a.req = req
	a.resp = resp
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, common.NewTaskError(1, "aaa")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &Map{
				mutex:         tt.fields.mutex,
				config:        tt.fields.config,
				stateQueueMap: tt.fields.stateQueueMap,
				Invoker:       tt.fields.Invoker,
				StateClient:   tt.fields.StateClient,
			}
			if got := g.syncStateToExternalStorage(tt.args.q, tt.args.req, tt.args.resp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("syncStateToExternalStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMap_DelState(t *testing.T) {
	Convey("TestMap_DelStateIfIdle", t, func() {
		config.Conf.PersistentState = false
		config.Conf.PersistentFuture = false
		g := newQueueMap()
		result := g.DelStateIfIdle("xxx1")
		So(result, ShouldBeFalse)

		md2 := &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "xxx2",
		}
		err := g.CreateStateQueue(md2)
		So(err, ShouldBeNil)
		deleted := g.DelState(md2.Key())
		So(deleted, ShouldBeTrue)
		deleted = g.DelState(md2.Key())
		So(deleted, ShouldBeFalse)

		md3 := &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "xxx3",
		}
		_ = g.CreateStateQueue(md3)
		queue := g.GetQueue(md3)
		queue.addRequest(&common.Request{})
		deleted = g.DelState(md3.Key())
		So(deleted, ShouldBeTrue)
		deleted = g.DelState(md3.Key())
		So(deleted, ShouldBeFalse)
	})
}

func TestGetQueueWithRecover(t *testing.T) {
	Convey("", t, func() {
		config.Conf.PersistentState = false
		config.Conf.PersistentFuture = false
		g := newQueueMap()
		meta := &metadata.Metadata{}
		g.GetQueueWithRecover(meta)
	})
}

func TestRequestTerminate(t *testing.T) {
	head := &reqnode.Node{}
	head2 := &reqnode.Node{}

	head.Next = head2
	head2.Next = head

	type fields struct {
		gMap        *Map
		sQueue      *StateQueue
		node        *reqnode.Node
		concurrency bool
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
	}{
		{"case1， deleteState error", fields{&Map{mutex: new(sync.RWMutex)}, &StateQueue{mutex: new(sync.RWMutex), head: head, metadata: &metadata.Metadata{}}, &reqnode.Node{Req: common.NewRequest(common.RequestOptions{
			FuncName:  "funcName",
			ReqIDList: []string{"reqIDList"},
		})}, false}, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*Map).deleteState, func(_ *Map, q *StateQueue) common.TaskError {
					return common.NewTaskError(int32(500), "error")
				}),
				gomonkey.ApplyFunc((*common.Request).SetResponse, func(_ *common.Request, _ *common.Response) {
				}),
			})
			return patches
		},
		},
		{"case2, deleteState success", fields{&Map{mutex: new(sync.RWMutex)}, &StateQueue{mutex: new(sync.RWMutex), head: head, curReq: &reqnode.Node{Req: common.NewRequest(common.RequestOptions{
			FuncName:  "funcName",
			ReqIDList: []string{"reqIDList"},
		})},
			metadata: &metadata.Metadata{}}, &reqnode.Node{Req: common.NewRequest(common.RequestOptions{
			FuncName:  "funcName",
			ReqIDList: []string{"reqIDList"},
		}), Next: &reqnode.Node{}}, false}, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*Map).deleteState, func(_ *Map, q *StateQueue) common.TaskError {
					return nil
				}),
				gomonkey.ApplyFunc((*common.Request).SetResponse, func(_ *common.Request, _ *common.Response) {
				}),
			})
			return patches
		},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.fields.gMap.requestTerminate(tt.fields.sQueue, tt.fields.node, tt.fields.concurrency)
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_IfNotPersistentState(t *testing.T) {
	Convey("IfNotPersistentState, create:false", t, func() {
		g := &Map{stateQueueMap: make(map[string]*StateQueue)}
		meta := &metadata.Metadata{}
		if _, err := g.ifNotPersistentState(meta, false); err != nil {
			t.Errorf("this is nil")
		}
	})

	Convey("TestMap_DelStateIfIdle, create:false, util.HasExceedNumsError err != nil", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(util.HasExceedNumsError, func(client stateclient.StateClient, tenantID, serviceID string) error {
				return errors.New("error")
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{}
			}),
		})
		defer patches.ResetAll()
		g := &Map{stateQueueMap: make(map[string]*StateQueue)}
		meta := &metadata.Metadata{}
		_, err := g.ifNotPersistentState(meta, true)
		if err == nil {
			t.Errorf("this should not be nil")
		}
	})

	Convey("TestMap_DelStateIfIdle, create:false, util.HasExceedNumsError err == nil", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(util.HasExceedNumsError, func(client stateclient.StateClient, tenantID, serviceID string) error {
				return nil
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{}
			}),
		})
		defer patches.ResetAll()
		g := &Map{stateQueueMap: make(map[string]*StateQueue)}
		meta := &metadata.Metadata{}
		_, err := g.ifNotPersistentState(meta, true)
		if err != nil {
			t.Errorf("this should not be nil")
		}
	})
}

func Test_checkStateQueue(t *testing.T) {
	Convey("checkStateQueue, queue != nil ", t, func() {
		sm := make(map[string]*StateQueue)
		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		k := metadata.Gen(meta.TenantID, meta.ServiceID, "stateID", meta.StateID)
		sm[k] = &StateQueue{}
		g := &Map{stateQueueMap: sm}
		if _, err := g.checkStateQueue(meta, false); err != nil {
			t.Errorf("this is nil")
		}
	})

	Convey("checkStateQueue, queue == nil, PersistentState false ", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(config.DefaultConfig, func() *config.Config {
				return &config.Config{
					MaxSingleWaitFutureCnt: config.DefaultMaxSingleWaitFutureCnt,
					CallTimeout:            int64(config.DefaultCallTimeout),
					ResolveDeadlock:        true,
					FutureTTL:              config.DefaultFutureTTL,
					ExternalFutureTTL:      config.DefaultExternalFutureTTL,
					MaxNumsOfStateID:       config.DefaultMaxNumsOfStateID,
					PersistentState:        false,
				}
			}),
			gomonkey.ApplyFunc((*Map).ifNotPersistentState, func(_ *Map, meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
		})
		defer patches.ResetAll()

		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		g := &Map{stateQueueMap: make(map[string]*StateQueue)}
		if _, err := g.checkStateQueue(meta, false); err != nil {
			t.Errorf("this is nil")
		}
	})

	Convey("checkStateQueue, queue == nil,PersistentState true, stateClient get ture", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(config.DefaultConfig, func() *config.Config {
				return &config.Config{
					MaxSingleWaitFutureCnt: config.DefaultMaxSingleWaitFutureCnt,
					CallTimeout:            int64(config.DefaultCallTimeout),
					ResolveDeadlock:        true,
					FutureTTL:              config.DefaultFutureTTL,
					ExternalFutureTTL:      config.DefaultExternalFutureTTL,
					MaxNumsOfStateID:       config.DefaultMaxNumsOfStateID,
					PersistentState:        true,
				}
			}),
			gomonkey.ApplyFunc((*Map).ifNotPersistentState, func(_ *Map, meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
			gomonkey.ApplyFunc((*MockStateStore).Get, func(s *MockStateStore, key string) (string, common.TaskError) {
				return "test", nil
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{}
			}),
		})
		defer patches.ResetAll()
		var sc stateclient.StateClient
		sc = &MockStateStore{}
		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		g := &Map{StateClient: sc, stateQueueMap: make(map[string]*StateQueue)}
		if _, err := g.checkStateQueue(meta, false); err != nil {
			t.Errorf("this is nil")
		}
	})

	Convey("checkStateQueue, queue == nil,PersistentState true, stateClient get error, create true", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(config.DefaultConfig, func() *config.Config {
				return &config.Config{
					MaxSingleWaitFutureCnt: config.DefaultMaxSingleWaitFutureCnt,
					CallTimeout:            int64(config.DefaultCallTimeout),
					ResolveDeadlock:        true,
					FutureTTL:              config.DefaultFutureTTL,
					ExternalFutureTTL:      config.DefaultExternalFutureTTL,
					MaxNumsOfStateID:       config.DefaultMaxNumsOfStateID,
					PersistentState:        true,
				}
			}),
			gomonkey.ApplyFunc((*Map).ifNotPersistentState, func(_ *Map, meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
			gomonkey.ApplyFunc((*MockStateStore).Get, func(s *MockStateStore, key string) (string, common.TaskError) {
				return "", common.NewTaskError(500, "error")
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{}
			}),
			gomonkey.ApplyFunc((*Map).createStateQueue, func(_ *Map, meta *metadata.Metadata, state string) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
		})
		defer patches.ResetAll()
		var sc stateclient.StateClient
		sc = &MockStateStore{}
		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		g := &Map{StateClient: sc, stateQueueMap: make(map[string]*StateQueue)}
		if _, err := g.checkStateQueue(meta, true); err != nil {
			t.Errorf("this is nil")
		}
	})

	Convey("checkStateQueue, queue == nil,PersistentState true, stateClient get error, create false, core.HExists true", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(config.DefaultConfig, func() *config.Config {
				return &config.Config{
					MaxSingleWaitFutureCnt: config.DefaultMaxSingleWaitFutureCnt,
					CallTimeout:            int64(config.DefaultCallTimeout),
					ResolveDeadlock:        true,
					FutureTTL:              config.DefaultFutureTTL,
					ExternalFutureTTL:      config.DefaultExternalFutureTTL,
					MaxNumsOfStateID:       config.DefaultMaxNumsOfStateID,
					PersistentState:        true,
				}
			}),
			gomonkey.ApplyFunc((*Map).ifNotPersistentState, func(_ *Map, meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
			gomonkey.ApplyFunc((*MockStateStore).Get, func(s *MockStateStore, key string) (string, common.TaskError) {
				return "", common.NewTaskError(500, "error")
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{}
			}),
			gomonkey.ApplyFunc((*Map).createStateQueue, func(_ *Map, meta *metadata.Metadata, state string) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
			gomonkey.ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
				return true, nil
			}),
		})
		defer patches.ResetAll()
		var sc stateclient.StateClient
		sc = &MockStateStore{}
		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		g := &Map{StateClient: sc, stateQueueMap: make(map[string]*StateQueue)}
		if _, err := g.checkStateQueue(meta, false); err != nil {
			t.Errorf("this is nil")
		}
	})

	Convey("checkStateQueue, queue == nil,PersistentState true, stateClient get error, create false, core.HExists false, IsStateInsExist true", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(config.DefaultConfig, func() *config.Config {
				return &config.Config{
					MaxSingleWaitFutureCnt: config.DefaultMaxSingleWaitFutureCnt,
					CallTimeout:            int64(config.DefaultCallTimeout),
					ResolveDeadlock:        true,
					FutureTTL:              config.DefaultFutureTTL,
					ExternalFutureTTL:      config.DefaultExternalFutureTTL,
					MaxNumsOfStateID:       config.DefaultMaxNumsOfStateID,
					PersistentState:        true,
				}
			}),
			gomonkey.ApplyFunc((*Map).ifNotPersistentState, func(_ *Map, meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
				return &StateQueue{
					g:                 &Map{},
					metadata:          meta,
					state:             "stateID",
					mutex:             new(sync.RWMutex),
					head:              reqnode.NewHead(),
					deadlockResolvers: make(map[string]*deadlockResolver, defaultDeadlockResolversSize),
					terminate:         false,
					ttl:               defaultStateTTL,
				}, nil
			}),
			gomonkey.ApplyFunc((*MockStateStore).Get, func(s *MockStateStore, key string) (string, common.TaskError) {
				return "", common.NewTaskError(500, "error")
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{
					g:                 &Map{},
					metadata:          metadata,
					state:             "stateID",
					mutex:             new(sync.RWMutex),
					head:              reqnode.NewHead(),
					deadlockResolvers: make(map[string]*deadlockResolver, defaultDeadlockResolversSize),
					terminate:         false,
					ttl:               defaultStateTTL,
				}
			}),
			gomonkey.ApplyFunc((*Map).createStateQueue, func(_ *Map, meta *metadata.Metadata, state string) (*StateQueue, common.TaskError) {
				return &StateQueue{
					g:                 &Map{},
					metadata:          meta,
					state:             "stateID",
					mutex:             new(sync.RWMutex),
					head:              reqnode.NewHead(),
					deadlockResolvers: make(map[string]*deadlockResolver, defaultDeadlockResolversSize),
					terminate:         false,
					ttl:               defaultStateTTL,
				}, nil
			}),
			gomonkey.ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
				return true, nil
			}),
			gomonkey.ApplyFunc((*servicediscovery.Registry).IsStateInsExist, func(_ *servicediscovery.Registry, stateID string) bool {
				return true
			}),
		})
		defer patches.ResetAll()
		var sc stateclient.StateClient
		sc = &MockStateStore{}
		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		g := &Map{StateClient: sc, stateQueueMap: make(map[string]*StateQueue)}
		q, err := g.checkStateQueue(meta, false)
		if err != nil {
			t.Errorf("this is nil")
		}
		if q.state != "stateID" {
			t.Errorf("this is error, q.state: %s", q.state)
		}
	})

	Convey("checkStateQueue, queue == nil,PersistentState true, stateClient get error, create false, core.HExists false, IsStateInsExist false", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(config.DefaultConfig, func() *config.Config {
				return &config.Config{
					MaxSingleWaitFutureCnt: config.DefaultMaxSingleWaitFutureCnt,
					CallTimeout:            int64(config.DefaultCallTimeout),
					ResolveDeadlock:        true,
					FutureTTL:              config.DefaultFutureTTL,
					ExternalFutureTTL:      config.DefaultExternalFutureTTL,
					MaxNumsOfStateID:       config.DefaultMaxNumsOfStateID,
					PersistentState:        true,
				}
			}),
			gomonkey.ApplyFunc((*Map).ifNotPersistentState, func(_ *Map, meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
			gomonkey.ApplyFunc((*MockStateStore).Get, func(s *MockStateStore, key string) (string, common.TaskError) {
				return "", common.NewTaskError(500, "error")
			}),
			gomonkey.ApplyFunc(newStateQueue, func(g *Map, metadata *metadata.Metadata, state string) *StateQueue {
				return &StateQueue{}
			}),
			gomonkey.ApplyFunc((*Map).createStateQueue, func(_ *Map, meta *metadata.Metadata, state string) (*StateQueue, common.TaskError) {
				return &StateQueue{}, nil
			}),
			gomonkey.ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
				return true, nil
			}),
			gomonkey.ApplyFunc((*servicediscovery.Registry).IsStateInsExist, func(_ *servicediscovery.Registry, stateID string) bool {
				return false
			}),
		})
		defer patches.ResetAll()
		var sc stateclient.StateClient
		sc = &MockStateStore{}
		meta := &metadata.Metadata{TenantID: "tenant", ServiceID: "service", StateID: "stateID"}
		g := &Map{StateClient: sc, stateQueueMap: make(map[string]*StateQueue)}
		if _, err := g.checkStateQueue(meta, false); err != nil {
			t.Errorf("this is nil")
		}
	})
}
