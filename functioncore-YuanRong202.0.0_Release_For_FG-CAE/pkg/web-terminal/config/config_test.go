package config

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/monitor"
	"functioncore/pkg/web-terminal/constant"
	mockutils "functioncore/test/utils"
)

func TestGetConfig(t *testing.T) {
	t.Run("Successful case", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(loadConfigFromEnv, func() (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, nil
			}),
		})
		configInstance = nil
		config, err := GetConfig()
		assert.NoError(t, err, "Expected no error")
		assert.NotNil(t, config, "Expected non-nil configInstance")
		patches.ResetAll()
	})

	t.Run("error loadConfig", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return &Config{}, errors.New("test error")
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, nil
			}),
		})
		configInstance = nil
		config, err := GetConfig()
		assert.Error(t, err, "Expected no error")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})

	t.Run("ValidateStruct error", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(loadConfigFromEnv, func() (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, errors.New("test error")
			}),
		})
		configInstance = nil
		config, err := GetConfig()
		assert.Error(t, err, "Expected no error")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})

	t.Run("configinstance nil", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(loadConfigFromEnv, func() (*Config, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, nil
			}),
		})
		configInstance = nil
		config, err := GetConfig()
		assert.Error(t, err, "Expected no error")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})
}

func TestLoadConfig(t *testing.T) {
	t.Run("Successful case", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(ioutil.ReadFile, func(fileName string) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return nil
			}),
		})
		config, err := loadConfig("test_config.json")
		assert.NoError(t, err, "Expected no error")
		assert.NotNil(t, config, "Expected non-nil config")
		patches.ResetAll()
	})

	t.Run("Error when opening file", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				return nil, errors.New("failed to open file")
			}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return nil
			}),
		})
		config, err := loadConfig("test_config.json")
		assert.Error(t, err, "Expected an error")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})
}

func TestWatchConfig(t *testing.T) {
	// Test case 1: Successful case
	t.Run("Successful case", func(t *testing.T) {
		stopCh := make(chan struct{})
		callback := func() {}
		err := WatchConfig(stopCh, callback)
		assert.NoError(t, err, "Expected no error when watching config")
	})

	// Test case 2: Error case when creating file watcher
	t.Run("Error case when creating file watcher", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(monitor.CreateFileWatcher, func(stopCh <-chan struct{}) (monitor.FileWatcher, error) {
				return nil, errors.New("failed to create file watcher")
			}),
		})
		stopCh := make(chan struct{})
		callback := func() {}
		err := WatchConfig(stopCh, callback)
		assert.Error(t, err, "Expected an error when creating file watcher fails")
		patches.ResetAll()
	})
}

func TestHotLoadConfig(t *testing.T) {
	Convey("Given a configuration file", t, func() {
		filename := "testconfig.yaml"
		opType := monitor.OpType(1)
		configInstance = &Config{}
		configInstance.ClusterConfig = map[string]*ClusterConfig{
			"a": {ClusterID: "a"},
		}
		configInstance.MaxSessions = 5
		newConfig := &Config{
			ClusterConfig: map[string]*ClusterConfig{"yuanrong-az101": {
				ClusterID: "yuanrong-az101",
			}},
			MaxSessions: 10,
		}

		Convey("When loadConfig fails", func() {
			patches := gomonkey.ApplyFunc(loadConfig, func(filename string) (*Config, error) {
				return nil, errors.New("failed to load config")
			})
			defer patches.Reset()
			hotLoadConfig(filename, opType)
			So(configInstance.ClusterConfig["a"].ClusterID, ShouldEqual, "a")
		})

		Convey("When loadConfig succeeds", func() {
			patches := gomonkey.ApplyFunc(loadConfig, func(filename string) (*Config, error) {
				return newConfig, nil
			})
			defer patches.Reset()

			hotLoadConfig(filename, opType)

			So(configInstance.ClusterConfig["yuanrong-az101"].ClusterID, ShouldEqual, "yuanrong-az101")
			So(configInstance.MaxSessions, ShouldEqual, 10)
		})
	})
}

func TestEnablePingPong(t *testing.T) {
	Convey("test enable ping pong", t, func() {
		Convey("When loadConfig fails", func() {
			patches := gomonkey.ApplyFunc(loadConfig, func(filename string) (*Config, error) {
				return nil, errors.New("failed to load config")
			})
			defer patches.Reset()
			enabled, interval := GetPingPongConfig()
			So(enabled, ShouldBeFalse)
			So(interval, ShouldEqual, constant.DefaultPingPongInterval)
		})
		Convey("When pingPongConfig is nil", func() {
			configInstance = &Config{}
			enabled, interval := GetPingPongConfig()
			So(enabled, ShouldBeFalse)
			So(interval, ShouldEqual, constant.DefaultPingPongInterval)
		})
		Convey("When pingPongConfig exist", func() {
			configInstance = &Config{
				PingPongConfig: &PingPongConfig{
					PingPongEnable:   true,
					PingPongInterval: 666,
				},
			}
			enabled, interval := GetPingPongConfig()
			So(enabled, ShouldBeTrue)
			So(interval, ShouldEqual, 666)
		})
	})
}
