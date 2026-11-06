package frontend

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/trafficlimit"
)

func BenchmarkInvokeHandler(b *testing.B) {
	now := time.Now().String()
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", now
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
	}()

	trafficlimit.TenantBuf.ProcessUpdate(&etcd3.Event{
		Key:   "/sn/qos/business/yrk/tenant/01234567890123456789012345678901",
		Value: []byte(`{"quota":10000000}`),
	})

	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{
		MaxRequestBodySize: 100,
	}
	constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	constant.FConfig.RetryConfig = &constant.RetryConfig{}
	urnutils.Separator = "@"
	constant.NativeAz = constants.DefaultAZ
	constant.FConfig.InvokeMaxRetryTimes = 3

	stopCh := make(chan struct{})
	functionlb.FunctionLB.Start(stopCh)
	processAddEvent(&etcd3.Event{
		Key:   "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/testnode12345",
		Value: []byte(`{"ip":"","port":"8080","status":"ready","p2pPort":"32568","nodeIP":"127.0.0.1","nodePort":"22423"}`),
	})
	processAddEvent(&etcd3.Event{
		Key:   "/sn/workers/business/yrk/tenant/01234567890123456789012345678901/function/0@default@test_func/version/latest/defaultaz/defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk",
		Value: []byte(`{"InstanceId":"defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk","ip":"127.0.0.1","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"127.0.0.1","nodePort":"22423","nodeName":"testnode12345","applier":"functiontask-926cp","ownerIP":"127.0.0.1","cpu":300,"memory":128}`),
	})
	k := "/sn/functions/business/yrk/tenant/01234567890123456789012345678901/function/0@default@test_func/version/latest"
	value := []byte(`{"resourceMetaData":{"cpu":400,"memory":256,"enable_dynamic_memory":false,"customResources":""}}`)
	metadata.ProcessUpdate(k, value, etcd3.Meta)

	router := gin.New()
	router.GET("/serverless/v1/componentshealth", func(context *gin.Context) {
		ClusterHealth(context.Writer, context.Request)
	})
	router.GET("/healthz", func(context *gin.Context) {
		healthz(context.Writer, context.Request)
	})
	router.POST("/serverless/v1/functions/:function-name/invocations", InvokeHandler)

	constant.InitDefaultConfig()
	raw := []byte(`{"key": 2}`)
	r := bytes.NewReader(raw)
	data := ioutil.NopCloser(r)

	req, _ := http.NewRequest(http.MethodPost, "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations", data)
	req.Header.Set(constants.HeaderTraceID, uuid.New().String())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Reset(raw)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
	}

}
