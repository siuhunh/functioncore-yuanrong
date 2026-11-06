package adaptor

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/snerror"
	commutils "functioncore/pkg/common/utils"
	"functioncore/pkg/core-sdk/communication"
	core "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/adaptor"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/filter"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/getinstance"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/readstate"
	"functioncore/pkg/functiontask/task/api/savestate"
	"functioncore/pkg/functiontask/task/api/settimeout"
	"functioncore/pkg/functiontask/task/api/terminate"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/api/waitfuture"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

type scaleTestPara struct {
	statusCode int
	body       []byte
	doRsp      error
}

var scaleTestParaInstance scaleTestPara

func TestAdaptor_CommonAdaptor(t *testing.T) {
	Convey("TestAdaptor_CommonAdaptor", t, func() {
		a := &adaptor.Adaptor{}
		a.InitMethods()
		servicediscovery.GetRegistry().FuncMeta["/0@service1@funcname1/latest"] = &servicediscovery.FunctionMetaInfo{
			FuncMetaData: servicediscovery.FuncMetaData{
				IsStatefulFunction: true,
			},
		}
		servicediscovery.GetRegistry().FuncMeta["/0@service2@funcname2/latest"] = &servicediscovery.FunctionMetaInfo{
			FuncMetaData: servicediscovery.FuncMetaData{
				IsStatefulFunction: false,
			},
		}
		core.FunctionCheck.IsFunctionExist = servicediscovery.GetRegistry().IsFunctionExist
		core.FunctionCheck.IsStatefulFunction = servicediscovery.GetRegistry().IsStatefulFunction
		core.FunctionCheck.IsStateExist = servicediscovery.GetRegistry().IsStateInsExist
		core.FunctionCheck.StateRecovery = servicediscovery.GetRegistry().StateRecovery
		Convey("test: invoke", func() {
			Convey("invoke: invoke", func() {
				patch := ApplyFunc(invoke.Invoke, func(taskCtx taskcontext.Context, req *invoke.Request) *invoke.Response {
					return &invoke.Response{}
				})
				taskCtx := testcontext.New(common.MethodInvoke)
				req := &adaptor.FuncArgs{
					Method: common.MethodInvoke,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_InvokeReq{
						InvokeReq: &invoke.RequestPb{},
					},
				}
				args, err := proto.Marshal(req)
				So(err, ShouldBeNil)
				err = a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("invoke: caller", func() {
				patch := ApplyMethod(reflect.TypeOf(&invoke.Request{}), "Caller",
					func(_ *invoke.Request, taskCtx taskcontext.Context) taskcontext.Serializable {
						return nil
					})
				taskCtx := testcontext.New(common.MethodInvoke)
				req := &adaptor.FuncArgs{
					Method: common.MethodInvoke,
					Source: common.SourceRuntimeToLocalFunctionTask,
					Data: &adaptor.CallRequestPb_InvokeReq{
						InvokeReq: &invoke.RequestPb{},
					},
				}
				args, err := proto.Marshal(req)
				So(err, ShouldBeNil)
				err = a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
		})
		Convey("test: calleeInvoke", func() {
			patch := ApplyMethod(reflect.TypeOf(&invoke.TaskRequest{}), "Callee",
				func(_ *invoke.TaskRequest, taskCtx taskcontext.Context) (resp taskcontext.Serializable) {
					return nil
				})
			taskCtx := testcontext.New(common.MethodTaskInvoke)
			req := &adaptor.FuncArgs{
				Method: common.MethodTaskInvoke,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_InvokeReq{
					InvokeReq: &invoke.RequestPb{},
				},
			}
			args, err := proto.Marshal(req)
			So(err, ShouldBeNil)
			err = a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: getFuture, SourceRuntimeToItsFunctionTask", func() {
			patch := ApplyMethod(reflect.TypeOf(&getfuture.Request{}), "Caller",
				func(_ *getfuture.RequestPb, taskCtx taskcontext.Context, deadlockChecker deadlock.Checker) (*getfuture.Response, string) {
					return &getfuture.Response{}, ""
				})
			taskCtx := testcontext.New(common.MethodGetFuture)
			req := &adaptor.FuncArgs{
				Method: common.MethodGetFuture,
				Source: common.SourceRuntimeToLocalFunctionTask,
				Data: &adaptor.CallRequestPb_GetReq{
					GetReq: &getfuture.RequestPb{StateID: taskcontext.NewState},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: getFuture,SourceRuntimeToCentralFunctionTask", func() {
			patch := ApplyMethod(reflect.TypeOf(&getfuture.Request{}), "Caller",
				func(_ *getfuture.RequestPb, taskCtx taskcontext.Context, deadlockChecker deadlock.Checker) (*getfuture.Response, string) {
					return &getfuture.Response{}, ""
				})
			taskCtx := testcontext.New(common.MethodGetFuture)
			req := &adaptor.FuncArgs{
				Method: common.MethodGetFuture,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_GetReq{
					GetReq: &getfuture.RequestPb{StateID: taskcontext.NewState},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: calleeGetFuture", func() {
			patch := ApplyMethod(reflect.TypeOf(&getfuture.TaskRequest{}), "Callee",
				func(_ *getfuture.TaskRequestPb, taskCtx taskcontext.Context) *getfuture.TaskResponsePb {
					return nil
				})
			taskCtx := testcontext.New(common.MethodTaskGetFuture)
			req := &adaptor.FuncArgs{
				Method: common.MethodTaskGetFuture,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_GetTaskReq{
					GetTaskReq: &getfuture.TaskRequestPb{},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: checkerStart", func() {
			patch := ApplyMethod(reflect.TypeOf(&deadlock.LocalChecker{}), "Start",
				func(_ *deadlock.LocalChecker, req *deadlock.StartRequest) *deadlock.StartResponse {
					return nil
				})
			taskCtx := testcontext.New(common.MethodTaskStartCheckAndResolveDeadlock)
			req := &adaptor.FuncArgs{
				Method: common.MethodTaskStartCheckAndResolveDeadlock,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_DeadlockStartReq{
					DeadlockStartReq: &deadlock.StartRequestPb{},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: checkerEnd", func() {
			patch := ApplyMethod(reflect.TypeOf(&deadlock.LocalChecker{}), "End",
				func(_ *deadlock.LocalChecker, req *deadlock.EndRequest) *deadlock.EndResponse {
					return nil
				})
			taskCtx := testcontext.New(common.MethodTaskEndCheckAndResolveDeadlock)
			req := &adaptor.FuncArgs{
				Method: common.MethodTaskEndCheckAndResolveDeadlock,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_DeadlockEndReq{
					DeadlockEndReq: &deadlock.EndRequestPb{},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: setCallback", func() {
			Convey("setCallback: future is nil", func() {
				taskCtx := testcontext.New(common.MethodOnComplete)
				req := &adaptor.FuncArgs{
					Method: common.MethodOnComplete,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_OnCompleteReq{
						OnCompleteReq: &invoke.OnCompleteRequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
			})
			Convey("setCallback: onComplete", func() {
				patch := ApplyFunc(invoke.OnComplete,
					func(taskcontext.Context, *invoke.OnCompleteRequest, *snfuture.Future) *invoke.ResponsePb {
						return nil
					})
				taskCtx := testcontext.New(common.MethodOnComplete)
				future := &snfuture.Future{}
				future.ID = "test"
				taskCtx.FutureStore().SetIfNotExist(future)
				req := &adaptor.FuncArgs{
					Method: common.MethodOnComplete,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_OnCompleteReq{
						OnCompleteReq: &invoke.OnCompleteRequestPb{
							FutureID: "test",
						},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("setCallback: caller", func() {
				patch := ApplyMethod(reflect.TypeOf(&invoke.OnCompleteRequest{}), "Caller",
					func(_ *invoke.OnCompleteRequest, taskCtx taskcontext.Context) taskcontext.Serializable {
						return nil
					})
				taskCtx := testcontext.New(common.MethodOnComplete)
				future := &snfuture.Future{}
				future.ID = "test"
				taskCtx.FutureStore().SetIfNotExist(future)
				req := &adaptor.FuncArgs{
					Method: common.MethodOnComplete,
					Source: common.SourceRuntimeToLocalFunctionTask,
					Data: &adaptor.CallRequestPb_OnCompleteReq{
						OnCompleteReq: &invoke.OnCompleteRequestPb{
							FutureID: "test",
						},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
		})
		Convey("test: terminate", func() {
			Convey("terminate: terminate", func() {
				patch := ApplyMethod(reflect.TypeOf(&terminate.RequestPb{}), "Terminate",
					func(_ *terminate.RequestPb, taskCtx taskcontext.Context) *terminate.Response {
						return nil
					})
				taskCtx := testcontext.New(common.MethodTerminate)
				req := &adaptor.FuncArgs{
					Method: common.MethodTerminate,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_TerminateReq{
						TerminateReq: &terminate.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("terminate: caller", func() {
				patch := ApplyMethod(reflect.TypeOf(&terminate.RequestPb{}), "Caller",
					func(_ *terminate.RequestPb, taskCtx taskcontext.Context) taskcontext.Serializable {
						return nil
					})
				taskCtx := testcontext.New(common.MethodTerminate)
				req := &adaptor.FuncArgs{
					Method: common.MethodTerminate,
					Source: common.SourceRuntimeToLocalFunctionTask,
					Data: &adaptor.CallRequestPb_TerminateReq{
						TerminateReq: &terminate.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
		})
		Convey("test: wait", func() {
			patch := ApplyFunc(waitfuture.Wait,
				func(taskCtx taskcontext.Context, req *waitfuture.TransformedRequest, checker deadlock.Checker) *waitfuture.Response {
					return nil
				})
			taskCtx := testcontext.New(common.MethodWait)
			req := &adaptor.FuncArgs{
				Method: common.MethodWait,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_WaitReq{
					WaitReq: &waitfuture.RequestPb{},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			req = &adaptor.FuncArgs{
				Method: common.MethodWait,
				Source: common.SourceRuntimeToLocalFunctionTask,
				Data: &adaptor.CallRequestPb_WaitReq{
					WaitReq: &waitfuture.RequestPb{},
				},
			}
			err = a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("test: setTimeout", func() {
			Convey("setTimeout: setTimeout", func() {
				patch := ApplyMethod(reflect.TypeOf(&settimeout.RequestPb{}), "SetTimeout",
					func(req *settimeout.Request, taskCtx taskcontext.Context) *settimeout.Response {
						return nil
					})
				taskCtx := testcontext.New(common.MethodSetTimeout)
				req := &adaptor.FuncArgs{
					Method: common.MethodSetTimeout,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_SetTimeoutReq{
						SetTimeoutReq: &settimeout.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("setTimeout: caller", func() {
				patch := ApplyMethod(reflect.TypeOf(&settimeout.RequestPb{}), "Caller",
					func(_ *settimeout.RequestPb, taskCtx taskcontext.Context) taskcontext.Serializable {
						return nil
					})
				taskCtx := testcontext.New(common.MethodSetTimeout)
				req := &adaptor.FuncArgs{
					Method: common.MethodSetTimeout,
					Source: common.SourceRuntimeToLocalFunctionTask,
					Data: &adaptor.CallRequestPb_SetTimeoutReq{
						SetTimeoutReq: &settimeout.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
		})
		Convey("test: readState", func() {
			Convey("readState: readState", func() {
				patch := ApplyMethod(reflect.TypeOf(&readstate.RequestPb{}), "ReadState",
					func(*readstate.RequestPb, taskcontext.Context) *readstate.ResponsePb {
						return nil
					})
				taskCtx := testcontext.New(common.MethodReadState)
				req := &adaptor.FuncArgs{
					Method: common.MethodReadState,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_ReadReq{
						ReadReq: &readstate.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("readState: caller", func() {
				patch := ApplyMethod(reflect.TypeOf(&readstate.RequestPb{}), "Caller",
					func(_ *readstate.RequestPb, taskCtx taskcontext.Context) *readstate.Response {
						return nil
					})
				taskCtx := testcontext.New(common.MethodReadState)
				req := &adaptor.FuncArgs{
					Method: common.MethodReadState,
					Source: common.SourceRuntimeToLocalFunctionTask,
					Data: &adaptor.CallRequestPb_ReadReq{
						ReadReq: &readstate.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
		})
		Convey("test: saveState", func() {
			Convey("saveState:localSaveState", func() {
				patch := ApplyMethod(reflect.TypeOf(&savestate.RequestPb{}), "SaveState",
					func(*savestate.RequestPb, taskcontext.Context) *savestate.ResponsePb {
						return nil
					})
				taskCtx := testcontext.New(common.MethodSaveState)
				req := &adaptor.FuncArgs{
					Method: common.MethodSaveState,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_SaveReq{
						SaveReq: &savestate.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("saveState:remoteSaveState", func() {
				patch := ApplyMethod(reflect.TypeOf(&savestate.RequestPb{}), "Caller",
					func(*savestate.RequestPb, taskcontext.Context) *savestate.ResponsePb {
						return nil
					})
				taskCtx := testcontext.New(common.MethodSaveState)
				req := &adaptor.FuncArgs{
					Method: common.MethodSaveState,
					Source: common.SourceRuntimeToLocalFunctionTask,
					Data: &adaptor.CallRequestPb_SaveReq{
						SaveReq: &savestate.RequestPb{},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToLocalFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
		})
		Convey("test: getInstance", func() {
			Convey("getInstance:stateExistCheckErr", func() {
				patch := ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
					return false, snerror.New(1, "err")
				})
				taskCtx := testcontext.New(common.MethodGetInstance)
				req := &adaptor.FuncArgs{
					Method: common.MethodGetInstance,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data:   &adaptor.CallRequestPb_GetInstanceReq{},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("getInstance:stateIsNotExist", func() {
				patch := ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
					return false, nil
				})
				taskCtx := testcontext.New(common.MethodGetInstance)
				req := &adaptor.FuncArgs{
					Method: common.MethodGetInstance,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data:   &adaptor.CallRequestPb_GetInstanceReq{},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("getInstance:stateExist", func() {
				stateKey, serviceID, funcName := "test", "0@service1", "funcname1"
				stateID := commutils.GenStateIDByKey("", serviceID, funcName, stateKey)
				servicediscovery.GetRegistry().States.Store(stateID, "")
				patches := []*Patches{
					ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
						return true, nil
					}),
					ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, opts ...clientv3.OpOption) ([]string, error) {
							return []string{"value"}, nil
						}),
					ApplyFunc(communication.CreateStateByHTTP,
						func(funcName string, stateID string, config types.CreateStateConfig) (string, string,
							snerror.SNError) {
							return "", "", nil
						}),
				}
				defer func() {
					for i := range patches {
						patches[i].Reset()
					}
				}()
				taskCtx := testcontext.New(common.MethodGetInstance)
				taskCtx.SetServiceID(serviceID)
				req := &adaptor.FuncArgs{
					Method: common.MethodGetInstance,
					Source: common.SourceRuntimeToCentralFunctionTask,
					Data: &adaptor.CallRequestPb_GetInstanceReq{
						GetInstanceReq: &getinstance.Request{
							Key: stateKey,
						},
					},
				}
				args, _ := proto.Marshal(req)
				err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
				So(err, ShouldBeNil)
				servicediscovery.GetRegistry().States.Delete(stateID)
			})
		})
		Convey("test: filter", func() {
			patch := ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("test err")
			})
			defer patch.Reset()
			Convey("filter: isStatefulFunctionCheck", func() {
				Convey("filter: isFunctionNotExist", func() {
					taskCtx := testcontext.New(common.MethodFilter)
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter: isNotStatefulFunction", func() {
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID("0@service2")
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: "funcname2",
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, "funcname2", args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter: isNotStatefulFunctionWithStateKey", func() {
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID("0@service2")
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: "funcname2",
								Key:      "aaa",
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, "funcname2", args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter: stateIDExist", func() {
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID("0@service1")
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: "funcname1",
								StateID:  "test",
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, "funcname1", args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter:redisException", func() {
					patches := [...]*Patches{
						ApplyFunc(adaptor.CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
							funcName, stateID string) bool {
							return false
						}),
						ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
							return false, snerror.New(1, "err")
						}),
					}
					defer func() {
						for _, p := range patches {
							p.Reset()
						}
					}()
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID("0@service1")
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: "funcname1",
								Key:      "test",
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, "funcname1", args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter:stateExist", func() {
					stateKey, serviceID, funcName := "test", "0@service1", "funcname1"
					stateID := commutils.GenStateIDByKey("", serviceID, funcName, stateKey)
					servicediscovery.GetRegistry().States.Store(stateID, "")
					patches := []*Patches{}
					{
						ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
							return true, nil
						})
						ApplyFunc(adaptor.CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
							funcName, stateID string) bool {
							return true
						})
					}
					defer func() {
						for i := range patches {
							patches[i].Reset()
						}
						servicediscovery.GetRegistry().States.Delete(stateID)
					}()
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID(serviceID)
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: funcName,
								Key:      stateKey,
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter:failedToCreateState", func() {
					stateKey, serviceID, funcName := "test", "0@service1", "funcname1"
					patches := []*Patches{}
					{
						ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
							return false, nil
						})
						ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName,
							stateID string) (string, common.TaskError) {
							return "", common.NewTaskError(1, "err")
						})
						ApplyFunc(adaptor.CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
							funcName, stateID string) bool {
							return false
						})
					}
					defer func() {
						for i := range patches {
							patches[i].Reset()
						}
					}()
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID(serviceID)
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: funcName,
								Key:      stateKey,
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter:isNotSameNode", func() {
					stateKey, serviceID, funcName := "test", "0@service1", "funcname1"
					client := &http.Client{}
					patches := []*Patches{}
					{
						ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
							return false, nil
						})
						ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {})
						ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
							return client
						})
						ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName,
							stateID string) (string, common.TaskError) {
							return "", nil
						})
						ApplyMethod(reflect.TypeOf(&filter.Request{}), "Caller", func(
							_ *filter.Request, taskCtx taskcontext.Context) *filter.Response {
							return nil
						})
						ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "IsSameNode", func(
							ctx *testcontext.Context, stateID string) bool {
							return false
						})
						ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo)
					}
					defer func() {
						for i := range patches {
							patches[i].Reset()
						}
					}()
					taskCtx := testcontext.New(common.MethodFilter)
					taskCtx.SetServiceID(serviceID)
					req := &adaptor.FuncArgs{
						Method: common.MethodFilter,
						Source: common.SourceRuntimeToCentralFunctionTask,
						Data: &adaptor.CallRequestPb_FilterReq{
							FilterReq: &filter.RequestPb{
								FuncName: funcName,
								Key:      stateKey,
							},
						},
					}
					args, _ := proto.Marshal(req)
					err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
					So(err, ShouldBeNil)
				})
				Convey("filter:isSameNode", func() {
					stateKey, serviceID, funcName := "test", "0@service1", "funcname1"
					client := &http.Client{}
					patches := []*Patches{}
					{
						ApplyFunc(core.HExists, func(key string, field string) (bool, snerror.SNError) {
							return false, nil
						})
						ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName,
							stateID string) (string, common.TaskError) {
							return "", nil
						})
						ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {})
						ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
							return client
						})
						ApplyMethod(reflect.TypeOf(&filter.Request{}), "Caller", func(
							_ *filter.Request, taskCtx taskcontext.Context) *filter.Response {
							return nil
						})
						ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "IsSameNode", func(
							ctx *testcontext.Context, stateID string) bool {
							return true
						})
						ApplyMethod(reflect.TypeOf(&statequeue.Map{}), "CheckAndClearOldStateQueue",
							func(_ *statequeue.Map, metadata *metadata.Metadata) {})
						ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo)
					}
					defer func() {
						for i := range patches {
							patches[i].Reset()
						}
					}()
					Convey("createStateErr", func() {
						patch := ApplyMethod(reflect.TypeOf(&statequeue.Map{}), "CreateStateQueue",
							func(_ *statequeue.Map, metadata *metadata.Metadata) common.TaskError {
								return common.NewTaskError(1, "")
							})
						taskCtx := testcontext.New(common.MethodFilter)
						taskCtx.SetServiceID(serviceID)
						req := &adaptor.FuncArgs{
							Method: common.MethodFilter,
							Source: common.SourceRuntimeToCentralFunctionTask,
							Data: &adaptor.CallRequestPb_FilterReq{
								FilterReq: &filter.RequestPb{
									FuncName: funcName,
									Key:      stateKey,
								},
							},
						}
						args, _ := proto.Marshal(req)
						err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
						So(err, ShouldBeNil)
						patch.Reset()
					})
					Convey("setRedisErr", func() {
						patches := []*Patches{
							ApplyMethod(reflect.TypeOf(&statequeue.Map{}), "CreateStateQueue",
								func(_ *statequeue.Map, metadata *metadata.Metadata) common.TaskError {
									return nil
								}),
							ApplyFunc(core.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
								return false, snerror.New(1, "")
							}),
						}
						defer func() {
							for idx := range patches {
								patches[idx].Reset()
							}
						}()
						taskCtx := testcontext.New(common.MethodFilter)
						taskCtx.SetServiceID(serviceID)
						req := &adaptor.FuncArgs{
							Method: common.MethodFilter,
							Source: common.SourceRuntimeToCentralFunctionTask,
							Data: &adaptor.CallRequestPb_FilterReq{
								FilterReq: &filter.RequestPb{
									FuncName: funcName,
									Key:      stateKey,
								},
							},
						}
						args, _ := proto.Marshal(req)
						err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
						So(err, ShouldBeNil)
					})
					Convey("testOk", func() {
						client := &http.Client{}
						patches := []*Patches{
							ApplyMethod(reflect.TypeOf(&statequeue.Map{}), "CreateStateQueue",
								func(_ *statequeue.Map, metadata *metadata.Metadata) common.TaskError {
									return nil
								}),
							ApplyFunc(core.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
								return true, nil
							}),
							ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {}),
							ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
								return client
							}),
							ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
						}
						defer func() {
							for idx := range patches {
								patches[idx].Reset()
							}
						}()
						taskCtx := testcontext.New(common.MethodFilter)
						taskCtx.SetServiceID(serviceID)
						req := &adaptor.FuncArgs{
							Method: common.MethodFilter,
							Source: common.SourceRuntimeToCentralFunctionTask,
							Data: &adaptor.CallRequestPb_FilterReq{
								FilterReq: &filter.RequestPb{
									FuncName: funcName,
									Key:      stateKey,
								},
							},
						}
						args, _ := proto.Marshal(req)
						err := a.CommonAdaptor(taskCtx, funcName, args, common.SourceRuntimeToCentralFunctionTask)
						So(err, ShouldBeNil)
					})
				})
			})
		})
		Convey("test: createStateQueue", func() {
			taskCtx := testcontext.New(common.MethodTaskCreateStateQueue)
			patch := ApplyMethod(reflect.TypeOf(&invoke.TaskCrateStateQueueRequest{}), "CreateStateQueue",
				func(*invoke.TaskCrateStateQueueRequest, taskcontext.Context) *invoke.ResponsePb {
					return nil
				})
			req := &adaptor.FuncArgs{
				Method: common.MethodTaskCreateStateQueue,
				Source: common.SourceRuntimeToCentralFunctionTask,
				Data: &adaptor.CallRequestPb_InvokeTaskCreateStateReq{
					InvokeTaskCreateStateReq: &invoke.TaskCrateStateQueueRequestPb{},
				},
			}
			args, _ := proto.Marshal(req)
			err := a.CommonAdaptor(taskCtx, "xx", args, common.SourceRuntimeToCentralFunctionTask)
			So(err, ShouldBeNil)
			patch.Reset()
		})
	})
}

func mockActionClientDo(_ *http.Client, _ *http.Request) (*http.Response, error) {
	body := scaleTestParaInstance.body
	return &http.Response{
		StatusCode: scaleTestParaInstance.statusCode,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, scaleTestParaInstance.doRsp
}
