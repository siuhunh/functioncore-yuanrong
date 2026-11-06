package debug

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	wconfig "functioncore/pkg/worker/config"
)

func Test_extractDebugTag(t *testing.T) {
	runtime := wconfig.RuntimeConfig{
		EnvKey:  "",
		EnvInfo: "",
	}
	conf := &wconfig.Configuration{
		RuntimeConfig: runtime,
	}
	convey.Convey("ExtractDebugTag", t, func() {
		convey.Convey("case 1", func() {
			check1, check2 := ExtractDebugTag(conf, "")
			convey.So(check1, convey.ShouldEqual, "")
			convey.So(check2, convey.ShouldBeNil)
		})
		convey.Convey("case 2", func() {
			env := map[string]interface{}{}
			env["DEBUGGER_TAG"] = "tag"
			conf.TraceEnv = env
			patches := gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "ENABLE" })
			defer patches.Reset()
			check1, check2 := ExtractDebugTag(conf, "java")
			convey.So(check1, convey.ShouldEqual, "tag")
			convey.So(check2, convey.ShouldNotBeNil)
		})
		convey.Convey("case 3", func() {
			env := map[string]interface{}{}
			env["DEBUGGER_TAG"] = 0
			conf.TraceEnv = env
			patches := gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "ENABLE" })
			defer patches.Reset()
			check1, check2 := ExtractDebugTag(conf, "java")
			convey.So(check1, convey.ShouldEqual, "")
			convey.So(check2, convey.ShouldBeNil)
		})
		convey.Convey("case 4", func() {
			conf.EnvKey = "key"
			check1, check2 := ExtractDebugTag(conf, "")
			convey.So(check1, convey.ShouldEqual, "")
			convey.So(check2, convey.ShouldBeNil)
		})
	})
}

func Test_getDebugPara(t *testing.T) {
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil })
	patches.ApplyFunc(os.Setenv, func(key string, value string) error { return nil })
	debugger := "test"
	envMap := map[string]interface{}{"JAVA_TOOL_OPTIONS": "", "MARS_OUTPUT": ""}

	convey.Convey("GetDebugPara", t, func() {
		convey.Convey("case 1", func() {
			debug, err := GetDebugPara("", envMap)
			assert.Equal(t, []string{}, debug)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case 2", func() {
			debug, err := GetDebugPara(debugger, envMap)
			assert.Equal(t, []string{"JAVA_TOOL_OPTIONS=", "MARS_OUTPUT="}, debug)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case 3", func() {
			envMap = map[string]interface{}{"NODE_OPTIONS": "", "MARS_OUTPUT": ""}
			debug, err := GetDebugPara(debugger, envMap)
			assert.Equal(t, []string{"NODE_OPTIONS=", "MARS_OUTPUT="}, debug)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case 4", func() {
			patches := gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
				return "", errors.New("patch error")
			})
			defer patches.Reset()
			debug, err := GetDebugPara(debugger, envMap)
			assert.Equal(t, []string{}, debug)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("case 5", func() {
			patches := gomonkey.ApplyFunc(os.Setenv, func(key, value string) error {
				return errors.New("patch error")
			})
			defer patches.Reset()
			debug, err := GetDebugPara(debugger, envMap)
			assert.Equal(t, []string{}, debug)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("case 6", func() {
			cmd := &exec.Cmd{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(cmd), "Run", func(_ *exec.Cmd) error {
					return errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			debug, err := GetDebugPara(debugger, envMap)
			assert.Equal(t, []string{}, debug)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
