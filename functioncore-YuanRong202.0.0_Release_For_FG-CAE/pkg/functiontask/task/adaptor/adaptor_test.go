package adaptor

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

func TestInitMethods(t *testing.T) {
	a := Adaptor{
		FutureStore:       futurestore.New(),
		CallerFutureStore: futurestore.New(),
	}
	a.InitMethods()
}

func TestCommonAdaptor(t *testing.T) {
	convey.Convey("method invalid", t, func() {
		a := Adaptor{
			FutureStore:       futurestore.New(),
			CallerFutureStore: futurestore.New(),
		}
		taskCtx := testcontext.New(common.MethodReadState)
		funcName := "test123"
		event := []byte("test1")
		source := 1
		patch := gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
			return nil
		})
		defer patch.Reset()
		err := a.CommonAdaptor(taskCtx, funcName, event, int32(source))
		assert.Equal(t, "invalid method", err.Error())
	})

	convey.Convey("method valid", t, func() {
		a := Adaptor{
			FutureStore:       futurestore.New(),
			CallerFutureStore: futurestore.New(),
		}
		taskCtx := testcontext.New(common.MethodReadState)
		funcName := "test123"
		source := 1
		a.InitMethods()
		taskCtx = testcontext.New(common.MethodInvoke)
		funcArgs := FuncArgs{Method: common.MethodInvoke}
		event, err := json.Marshal(funcArgs)
		if err != nil {
			return
		}
		patches := []*gomonkey.Patches{}
		patches = append(patches, gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
			return nil
		}))
		patches = append(patches, gomonkey.ApplyFunc(a.invoke, func(taskCtx taskcontext.Context, args *FuncArgs) {

		}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		err = a.CommonAdaptor(taskCtx, funcName, event, int32(source))
		assert.Equal(t, nil, err)
	})
}

func TestForwardInvoke(t *testing.T) {
	convey.Convey("success", t, func() {
		a := Adaptor{
			FutureStore:       futurestore.New(),
			CallerFutureStore: futurestore.New(),
		}
		a.InitMethods()
		taskCtx := testcontext.New(common.MethodInvoke)
		funcArgs := &FuncArgs{}
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.ClusterFrontends["cluster002"] = []string{"7.218.74.120"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(forwardReq,
			func(taskCtx taskcontext.Context, args *FuncArgs, req *invoke.Request, endpoint string) *invoke.Response {
				return nil
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		a.invoke(taskCtx, funcArgs)
	})
}

func TestForwardInvokeFailed(t *testing.T) {
	convey.Convey("need forward to other cluster, but no available frontend found", t, func() {
		a := Adaptor{
			FutureStore:       futurestore.New(),
			CallerFutureStore: futurestore.New(),
		}
		a.InitMethods()
		taskCtx := testcontext.New(common.MethodInvoke)
		funcArgs := &FuncArgs{}
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(forwardReq,
			func(taskCtx taskcontext.Context, args *FuncArgs, req *invoke.Request, endpoint string) *invoke.Response {
				return nil
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		a.invoke(taskCtx, funcArgs)
	})
}
