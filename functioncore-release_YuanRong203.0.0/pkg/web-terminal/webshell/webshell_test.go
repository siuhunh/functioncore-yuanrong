package webshell

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/web-terminal/config"
	"functioncore/pkg/web-terminal/constant"
)

func TestParamCheck(t *testing.T) {
	Convey("Given a gin context and RequestInfo", t, func() {
		gin.SetMode(gin.TestMode)

		traceID := "test-trace-id"
		reqInfo := &RequestInfo{TraceID: traceID}

		Convey("When all parameters are provided", func() {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = gin.Params{
				{Key: constant.ClusterIDURNParam, Value: "test-cluster"},
				{Key: constant.NamespaceIDURNParam, Value: "test-namespace"},
				{Key: constant.PodNameURNParam, Value: "test-pod"},
			}

			err := paramCheck(c, reqInfo)
			So(err, ShouldBeNil)
		})

		Convey("When clusterID is missing", func() {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = gin.Params{
				{Key: constant.NamespaceIDURNParam, Value: "test-namespace"},
				{Key: constant.PodNameURNParam, Value: "test-pod"},
			}

			err := paramCheck(c, reqInfo)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "clusterID is empty, traceID: test-trace-id")
		})

		Convey("When namespaceID is missing", func() {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = gin.Params{
				{Key: constant.ClusterIDURNParam, Value: "test-cluster"},
				{Key: constant.PodNameURNParam, Value: "test-pod"},
			}

			err := paramCheck(c, reqInfo)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "namespace is empty, traceID: test-trace-id")
		})

		Convey("When podName is missing", func() {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Params = gin.Params{
				{Key: constant.ClusterIDURNParam, Value: "test-cluster"},
				{Key: constant.NamespaceIDURNParam, Value: "test-namespace"},
			}

			err := paramCheck(c, reqInfo)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "podName is empty, traceID: test-trace-id")
		})
	})
}

func TestAuthCheck(t *testing.T) {
	Convey("Test authCheck function", t, func() {
		Convey("When config is not available", func() {
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return nil, errors.New("failed to get config")
			})
			defer patches.Reset()

			r := &http.Request{
				Header: http.Header{
					constants.HeaderAuthorization: []string{"request_sign"},
					constants.HeaderAuthTimestamp: []string{"timestamp"},
				},
			}

			err := authCheck(r)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "failed to get config")
		})

		Convey("When authentication is not enabled", func() {
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					AuthenticationEnable: false,
				}, nil
			})
			defer patches.Reset()

			r := &http.Request{
				Header: http.Header{
					constants.HeaderAuthorization: []string{"request_sign"},
					constants.HeaderAuthTimestamp: []string{"timestamp"},
				},
			}

			err := authCheck(r)
			So(err, ShouldBeNil)
		})

		Convey("When local authorization check fails", func() {
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					AuthenticationEnable: true,
					LocalAuth: &localauth.AuthConfig{
						AKey:     "access_key",
						SKey:     "secret_key",
						Duration: 60,
					},
				}, nil
			})
			patches.ApplyFunc(localauth.AuthCheckLocally, func(akey, skey, requestSign, timestamp string, duration int) error {
				return errors.New("failed to check authorization locally")
			})
			defer patches.Reset()

			r := &http.Request{
				Header: http.Header{
					constants.HeaderAuthorization: []string{"request_sign"},
					constants.HeaderAuthTimestamp: []string{"timestamp"},
				},
			}

			err := authCheck(r)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "failed to check authorization locally")
		})

		Convey("When local authorization check succeeds", func() {
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					AuthenticationEnable: true,
					LocalAuth: &localauth.AuthConfig{
						AKey:     "access_key",
						SKey:     "secret_key",
						Duration: 60,
					},
				}, nil
			})
			patches.ApplyFunc(localauth.AuthCheckLocally, func(akey, skey, requestSign, timestamp string, duration int) error {
				return nil
			})
			defer patches.Reset()

			r := &http.Request{
				Header: http.Header{
					constants.HeaderAuthorization: []string{"request_sign"},
					constants.HeaderAuthTimestamp: []string{"timestamp"},
				},
			}

			err := authCheck(r)
			So(err, ShouldBeNil)
		})
	})
}
