package custom

import (
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/stream"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
)

var (
	eventManager = &EventManager{
		waitMap: struct {
			sync.RWMutex
			waiters       map[string]chan *runtime.InvokeResponse
			timers        map[string]*time.Timer
			streamOptions map[string]*stream.ClientOption
		}{
			waiters: make(map[string]chan *runtime.InvokeResponse, 10),
		},
		requestChan: make(chan *runtime.InvokeRequest, 10),
	}
)

func TestPutRequest(t *testing.T) {
	rspChan := make(chan *runtime.InvokeResponse, 1)
	convey.Convey("Test Put Request", t, func() {
		defer gomonkey.ApplyFunc(invokectx.GetTimeTraceContext,
			func(ic invokectx.InvokeCtx) *invokectx.TimeTraceContext {
				return invokectx.NewTimeTraceContext()
			}).Reset()
		request := &runtime.InvokeRequest{
			TraceID: "1",
		}
		err := eventManager.PutRequest(invokectx.InvokeCtx{ID: 123}, request, rspChan)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test nil request", t, func() {
		defer gomonkey.ApplyFunc(invokectx.GetTimeTraceContext,
			func(ic invokectx.InvokeCtx) *invokectx.TimeTraceContext {
				return invokectx.NewTimeTraceContext()
			}).Reset()
		err := eventManager.PutRequest(invokectx.InvokeCtx{ID: 123}, nil, rspChan)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestPutResponse(t *testing.T) {
	convey.Convey("Test Put Response", t, func() {
		response := &runtime.InvokeResponse{}
		rspChan := make(chan *runtime.InvokeResponse, 10)
		eventManager.waitMap.waiters["1"] = rspChan
		err := eventManager.PutResponse("1", response)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test nil response", t, func() {
		err := eventManager.PutResponse("", nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test nil waiter", t, func() {
		response := &runtime.InvokeResponse{}
		err := eventManager.PutResponse("", response)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestGetResponse(t *testing.T) {
	convey.Convey("Test Get Response Fail", t, func() {
		rspChan := make(chan *runtime.InvokeResponse, 10)
		eventManager.waitMap.waiters["1"] = rspChan
		go close(rspChan)
		response := eventManager.GetResponse("1")
		convey.So(response, convey.ShouldBeNil)
	})
}

func TestIsAvailable(t *testing.T) {
	convey.Convey("Test Is Available", t, func() {
		rspChan := make(chan *runtime.InvokeResponse, 10)
		eventManager.waitMap.waiters["1"] = rspChan
		available := eventManager.isAvailable("1")
		convey.So(available, convey.ShouldEqual, true)
	})
}

func TestEventManager(t *testing.T) {
	convey.Convey("TestEventManager", t, func() {
		defer gomonkey.ApplyFunc(invokectx.GetTimeTraceContext,
			func(ic invokectx.InvokeCtx) *invokectx.TimeTraceContext {
				return invokectx.NewTimeTraceContext()
			}).Reset()

		rspChan := make(chan *runtime.InvokeResponse, 1)

		manager := NewEventManager()
		traceID := "traceID001"
		manager.PutRequest(invokectx.InvokeCtx{ID: 123}, &runtime.InvokeRequest{TraceID: traceID}, rspChan)

		request := manager.NextRequest()
		assert.Equal(t, traceID, request.TraceID)

		manager.PutResponse(traceID, &runtime.InvokeResponse{})
		alive := manager.isAvailable(traceID)
		assert.Equal(t, true, alive)

		invokeResponse := manager.GetResponse(traceID)
		assert.NotNil(t, invokeResponse)

		invokeResponse = manager.GetResponse(traceID)
		assert.Nil(t, invokeResponse)

		alive = manager.isAvailable(traceID)
		assert.Equal(t, false, alive)
	})
}

func Test_NextRequest(t *testing.T) {
	convey.Convey("Test NextRequest", t, func() {
		convey.Convey("Test 1", func() {
			e := &EventManager{
				requestChan: nil,
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
					timerC := make(chan time.Time)
					close(timerC)
					timer := &time.Timer{
						C: timerC,
					}
					return timer
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := e.NextRequest()
			convey.So(res, convey.ShouldBeNil)
		})
		convey.Convey("Test 2", func() {
			requestChan := make(chan *runtime.InvokeRequest)
			e := &EventManager{
				requestChan: requestChan,
			}
			close(e.requestChan)
			res := e.NextRequest()
			convey.So(res, convey.ShouldBeNil)
		})
	})
}

func Test_PutResponse(t *testing.T) {
	convey.Convey("Test PutResponse", t, func() {
		convey.Convey("Test 1", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			waiters := make(map[string]chan *runtime.InvokeResponse)
			response := &runtime.InvokeResponse{}
			e := &EventManager{
				requestChan: requestChan,
				waitMap: struct {
					sync.RWMutex
					waiters       map[string]chan *runtime.InvokeResponse
					timers        map[string]*time.Timer
					streamOptions map[string]*stream.ClientOption
				}{
					waiters: waiters,
				},
			}
			e.waitMap.waiters["Test 1"] = make(chan *runtime.InvokeResponse, 1)
			e.waitMap.waiters["Test 1"] <- response
			err := e.PutResponse("Test 1", response)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
