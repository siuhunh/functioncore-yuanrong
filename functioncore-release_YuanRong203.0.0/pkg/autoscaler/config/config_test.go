/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package options define config
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
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/tls"
	mockutils "functioncore/test/utils"
)

func TestGetHTTPSConfig(t *testing.T) {
	// Test case 1: When HTTPS_CONFIG is set and is a valid JSON string
	os.Setenv("HTTPS_CONFIG", `{"key":"value"}`)
	httpsConfig := getHTTPSConfig()
	assert.NotNil(t, httpsConfig, "HTTPS config should not be nil")

	// Test case 2: When HTTPS_CONFIG is not a valid JSON string
	os.Setenv("HTTPS_CONFIG", "invalid_json")
	httpsConfig = getHTTPSConfig()
	assert.NotNil(t, httpsConfig, "HTTPS config should not be nil")

	// Test case 3: When HTTPS_CONFIG is empty
	os.Setenv("HTTPS_CONFIG", "")
	httpsConfig = getHTTPSConfig()
	assert.NotNil(t, httpsConfig, "HTTPS config should not be nil")

	// Test case 4: When HTTPS_CONFIG is set and validateStruct fail
	os.Setenv("HTTPS_CONFIG", `{"key":"value"}`)
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
		return true, errors.New("test error")
	})})
	httpsConfig = getHTTPSConfig()
	assert.NotNil(t, httpsConfig, "HTTPS config should not be nil")
	patches.ResetAll()
}

func TestGetConfig(t *testing.T) {
	t.Run("Successful case", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, nil
			}),
		})
		configPath = "test_config.json"
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
		configPath = "test_config.json"
		configInstance = nil
		config, err := GetConfig()
		assert.Error(t, err, "auto-scaler config is null")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})

	t.Run("ValidateStruct error", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, errors.New("test error")
			}),
		})
		configPath = "test_config.json"
		configInstance = nil
		config, err := GetConfig()
		assert.Error(t, err, "auto-scaler config is null")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})

	t.Run("configinstance nil", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(loadConfig, func(path string) (*Config, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, nil
			}),
		})
		configPath = "test_config.json"
		configInstance = nil
		config, err := GetConfig()
		assert.Error(t, err, "auto-scaler config is null")
		assert.Nil(t, config, "Expected config to be nil")
		patches.ResetAll()
	})
}

func TestLoadConfig(t *testing.T) {
	t.Run("Successful case", func(t *testing.T) {
		conf := Config{CAEMetaEtcdProxy: &etcd3.ClientConfig{ServerList: []string{"127.0.0.1"}}}
		bytes, _ := json.Marshal(conf)
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return bytes, nil
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

func TestGetLocalAuthConfig(t *testing.T) {
	// Test case 1: Valid JSON config in environment variable
	os.Setenv("LOCAL_AUTH", `{"username": "testuser", "password": "testpassword"}`)

	config := getLocalAuthConfig()
	assert.NotNil(t, config, "Expected non-nil localAuthConfig")

	// Test case 2: Invalid JSON config in environment variable
	os.Setenv("LOCAL_AUTH", "invalid_json")
	config = getLocalAuthConfig()
	assert.NotNil(t, config, "Expected non-nil localAuthConfig")
	// Add more assertions as needed for error handling in this case

	// Test case 3: Missing environment variable
	os.Unsetenv("LOCAL_AUTH")
	config = getLocalAuthConfig()
	assert.NotNil(t, config, "Expected non-nil localAuthConfig")

	// Test case 4: ValidateStruct error
	os.Setenv("LOCAL_AUTH", `{"username": "testuser", "password": "testpassword"}`)
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
		return true, errors.New("test error")
	})})
	config = getLocalAuthConfig()
	assert.NotNil(t, config, "HTTPS config should not be nil")
	patches.ResetAll()
}

func TestEnableHTTPS(t *testing.T) {
	t.Run("Successful case with HTTPS configuration provided", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetConfig, func() (*Config, error) {
				return &Config{
					HTTPSConfig: &tls.InternalHTTPSConfig{
						HTTPSEnable: true,
						TLSProtocol: "tls1.2",
						TLSCiphers:  "AES256-SHA",
					},
				}, nil
			}),
			gomonkey.ApplyFunc(tls.InitTLSConfig, func(tlsProtocols string, tlsCiphers string) error {
				return nil
			}),
		})
		enabled := EnableHTTPS()
		assert.True(t, enabled, "Expected HTTPS to be enabled")
		patches.ResetAll()
	})

	t.Run("HTTPS configuration not provided", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetConfig, func() (*Config, error) {
				return &Config{}, nil
			}),
			gomonkey.ApplyFunc(tls.InitTLSConfig, func(tlsProtocols string, tlsCiphers string) error {
				return nil
			}),
		})
		enabled := EnableHTTPS()
		assert.False(t, enabled, "Expected HTTPS to be disabled when HTTPSConfig is not provided")
		patches.ResetAll()
	})

	t.Run("Error when initializing HTTPS configuration", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetConfig, func() (*Config, error) {
				return &Config{
					HTTPSConfig: &tls.InternalHTTPSConfig{
						HTTPSEnable: true,
						TLSProtocol: "tls1.2",
						TLSCiphers:  "AES256-SHA",
					},
				}, nil
			}),
			gomonkey.ApplyFunc(tls.InitTLSConfig, func(tlsProtocols string, tlsCiphers string) error {
				return errors.New("failed to initialize TLS config")
			}),
		})
		enabled := EnableHTTPS()
		assert.False(t, enabled, "Expected HTTPS to be disabled when initialization fails")
		patches.ResetAll()
	})

	t.Run("Error when get config", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetConfig, func() (*Config, error) {
				return &Config{}, errors.New("test error")
			}),
		})
		enabled := EnableHTTPS()
		assert.False(t, enabled, "Expected HTTPS to be disabled when error get config")
		patches.ResetAll()
	})
}

func TestInitConfigPath(t *testing.T) {
	// Test case: Set a valid config path
	expectedPath := "/path/to/config"
	InitConfigPath(expectedPath)

	assert.Equal(t, expectedPath, configPath, "Expected configPath to be set to the provided path")
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
	// Test case 1: Successful hot load
	t.Run("Successful hot load", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(readHotLoadConfig, func(p string) (*HotLoadConfig, error) {
				return &HotLoadConfig{}, nil
			}),
		})
		filename := "config.json"
		opType := monitor.Create
		callbackInvoked := false
		configChangedCallback := func() { callbackInvoked = true }
		configChangedCallbacks = append(configChangedCallbacks, configChangedCallback)

		hotLoadConfig(filename, opType)

		assert.True(t, callbackInvoked, "Expected configChangedCallback to be invoked")
		patches.ResetAll()
	})

	// Test case 2: Error in reading config file
	t.Run("Error in reading config file", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(readHotLoadConfig, func(p string) (*HotLoadConfig, error) {
				return nil, errors.New("failed to read config file")
			}),
		})
		filename := "invalid_config.json"
		opType := monitor.Write
		callbackInvoked := false
		configChangedCallback := func() { callbackInvoked = true }
		configChangedCallbacks = append(configChangedCallbacks, configChangedCallback)

		hotLoadConfig(filename, opType)

		assert.False(t, callbackInvoked, "Expected configChangedCallback not to be invoked due to error")
		patches.ResetAll()
	})

	// Test case 2: Error ValidateStruct
	t.Run("Error ValidateStruct", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(readHotLoadConfig, func(p string) (*HotLoadConfig, error) {
				return nil, nil
			}),
		})
		filename := "invalid_config.json"
		opType := monitor.Write
		callbackInvoked := false
		configChangedCallback := func() { callbackInvoked = true }
		configChangedCallbacks = append(configChangedCallbacks, configChangedCallback)

		hotLoadConfig(filename, opType)

		assert.False(t, callbackInvoked, "Expected configChangedCallback not to be invoked due to error")
		patches.ResetAll()
	})
}

func TestReadHotLoadConfig(t *testing.T) {
	// Test case 1: Successful read and unmarshal
	fileContent := []byte(`{"key": "value"}`)
	mockFile := createTempFile(fileContent)
	defer os.Remove(mockFile.Name())

	hotLoadConfig, err := readHotLoadConfig(mockFile.Name())
	assert.NoError(t, err, "Expected no error when reading and unmarshaling config")
	assert.NotNil(t, hotLoadConfig, "Expected hotLoadConfig to be not nil")

	// Add more assertions for the content of hotLoadConfig if needed

	// Test case 2: Error when opening file
	_, err = readHotLoadConfig("non_existent_file.json")
	assert.Error(t, err, "Expected an error when opening non-existent file")

	// Test case 3: Error when reading file
	fileContent = []byte(`invalid json content`)
	mockFile = createTempFile(fileContent)
	defer os.Remove(mockFile.Name())

	_, err = readHotLoadConfig(mockFile.Name())
	assert.Error(t, err, "Expected an error when reading invalid JSON content")
}

// Helper function to create a temporary file with content
func createTempFile(content []byte) *os.File {
	tmpfile, _ := ioutil.TempFile("", "example")
	tmpfile.Write(content)
	tmpfile.Seek(0, 0)
	return tmpfile
}

func TestGetLocalAuth(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetConfig, func() (*Config, error) {
				return &Config{
					LocalAuth: &localauth.AuthConfig{
						AKey:     "akey",
						SKey:     "skey",
						Duration: 1,
					},
				}, nil
			}),
		})
		auth := GetLocalAuth()
		assert.Equal(t, "akey", auth.AKey)
		assert.Equal(t, "skey", auth.SKey)
		assert.Equal(t, 1, auth.Duration)
		patches.ResetAll()
	})
	t.Run("failed to get config", func(t *testing.T) {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetConfig, func() (*Config, error) {
				return nil, errors.New("error")
			}),
		})
		auth := GetLocalAuth()
		assert.Equal(t, "", auth.AKey)
		assert.Equal(t, "", auth.SKey)
		assert.Equal(t, 0, auth.Duration)
		patches.ResetAll()
	})
}
