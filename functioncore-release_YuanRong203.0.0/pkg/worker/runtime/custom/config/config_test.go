package config

import (
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"
)

func TestGetServerConfig(t *testing.T) {
	gPatch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
		f()
	})
	defer gPatch.Reset()
	convey.Convey("Test Get Server Config", t, func() {
		patches := gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
			return nil, nil
		})
		patches.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
			return true, nil
		})
		serverConfig, err := GetServerConfig()
		convey.So(serverConfig, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		patches.Reset()

	})
	convey.Convey("get server config unmarshal failed", t, func() {
		patches := gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
			return []byte("{"), nil
		})
		serverConfig, err := GetServerConfig()
		convey.So(serverConfig, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		patches.Reset()
	})
	convey.Convey("get server config valid failed", t, func() {
		patches := gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
			return nil, nil
		})
		patches.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
			return false, errors.New("failed to validate")
		})
		serverConfig, err := GetServerConfig()
		convey.So(serverConfig, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		patches.Reset()
	})
}

func TestGetRuntimeConfig(t *testing.T) {
	convey.Convey("Test Get Runtime Config error", t, func() {
		patches := gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
			return "", true
		})
		runtimeConfig, err := GetRuntimeConfig()
		convey.So(runtimeConfig, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		patches.Reset()
	})
}
