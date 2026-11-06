package v1

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/worker-manager/api/v1/faasfunction"
)

func TestNewAPIV1(t *testing.T) {
	convey.Convey("NewAPIV1 test", t, func() {
		options := &apiServerOptions.APIServerOptions{
			EnableTenant: true,
		}
		stopCh := make(chan struct{})
		convey.Convey("NewAPIV1 test error", func() {
			APIV1, err := NewAPIV1(options, stopCh)
			convey.So(APIV1, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
	convey.Convey("register api test", t, func() {
		a := APIV1{
			FunctionManager: &faasfunction.FunctionAPI{},
		}
		router := gin.New()
		convey.So(a.RegisterAPI(router), convey.ShouldBeNil)
	})
	convey.Convey("register api test", t, func() {
		convey.So(GetVersionPath(), convey.ShouldNotBeNil)
	})
}
