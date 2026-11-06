/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

package functionenv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/trace"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

func TestFetchEnvFromConfig(t *testing.T) {
	convey.Convey("initLogger", t, func() {
		conf := &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				BucketID:    "appID",
				ObjectID:    "appSecret",
				LayerInfo:   "layerInfo",
				DeployDir:   "deployDir",
				StorageType: "local",
			},
			RuntimeConfig: config.RuntimeConfig{
				LoadTime:         int64(1),
				URN:              "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest",
				ResourceMetaData: config.ResourceMetaData{CPU: 300, Mem: 128},
				TraceService: trace.TraceService{
					TraceAK:     "ak",
					TraceSK:     "sk",
					ProjectName: "test",
					Enabled:     false,
				},
				RMConfig: rmconfig.Config{
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						"v1.0": rmconfig.RuntimeConfig{
							Addition: map[string]string{
								"enableDcCallStack": "true",
								"dcCallStackLayer":  "3",
							},
						},
					},
				},
			},
		}
		convey.Convey("test 1", func() {
			var err error = nil
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test 2", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return nil, errors.New("error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test 3", func() {
			tag := true
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						if tag {
							return []byte("secret"), nil
						}
						return nil, errors.New("error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test 4", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test 5", func() {
			patches := [...]*gomonkey.Patches{}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
	convey.Convey("initLogger err", t, func() {
		conf := &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				BucketID:    "appID",
				ObjectID:    "appSecret",
				LayerInfo:   "layerInfo",
				DeployDir:   "deployDir",
				StorageType: "local",
			},
			RuntimeConfig: config.RuntimeConfig{
				LoadTime:         int64(1),
				URN:              "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest",
				ResourceMetaData: config.ResourceMetaData{CPU: 300, Mem: 128},
				TraceService: trace.TraceService{
					TraceAK:     "ak",
					TraceSK:     "sk",
					ProjectName: "test",
					Enabled:     false,
				},
				EnvInfo:           "Test 1 EnvInfo",
				EncryptedUserData: "Test 2 EncryptedUserData",
				RMConfig: rmconfig.Config{
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						"v1.0": rmconfig.RuntimeConfig{
							Addition: map[string]string{
								"enableDcCallStack": "true",
								"dcCallStackLayer":  "3",
							},
						},
					},
				},
			},
		}
		convey.Convey("test err 1", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return nil, errors.New("error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test err 2", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte("decrypt test res"), nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test err 3", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte(`{"res":"decrypt test res"}`), nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldBeNil)
		})
	})
	convey.Convey("initLogger err 2", t, func() {
		conf := &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				BucketID:    "appID",
				ObjectID:    "appSecret",
				LayerInfo:   "layerInfo",
				DeployDir:   "deployDir",
				StorageType: "local",
			},
			RuntimeConfig: config.RuntimeConfig{
				LoadTime:         int64(1),
				URN:              "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest",
				ResourceMetaData: config.ResourceMetaData{CPU: 300, Mem: 128},
				TraceService: trace.TraceService{
					TraceAK:     "ak",
					TraceSK:     "sk",
					ProjectName: "test",
					Enabled:     false,
				},
				EnvInfo:           "",
				EncryptedUserData: "Test 2 EncryptedUserData",
				RMConfig: rmconfig.Config{
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						"v1.0": rmconfig.RuntimeConfig{
							Addition: map[string]string{
								"enableDcCallStack": "true",
								"dcCallStackLayer":  "3",
							},
						},
					},
				},
			},
		}
		convey.Convey("test err 1", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv,
					func(input string) string {
						return input
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte("decrypt test res"), nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
	convey.Convey("With cluster id", t, func() {
		conf := &config.Configuration{
			ClusterID: "clusterID111",
		}
		res, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
		convey.So(strings.Contains(strings.Join(res.FuncEnvs, ","), "func-FAAS_CLUSTER_ID=clusterID111"), convey.ShouldBeTrue)
		convey.So(err, convey.ShouldEqual, nil)

	})
	convey.Convey("With language DcCallStack", t, func() {
		conf := &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				BucketID:    "appID",
				ObjectID:    "appSecret",
				LayerInfo:   "layerInfo",
				DeployDir:   "deployDir",
				StorageType: "local",
			},
			RuntimeConfig: config.RuntimeConfig{
				LoadTime:         int64(1),
				URN:              "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest",
				ResourceMetaData: config.ResourceMetaData{CPU: 300, Mem: 128},
				TraceService: trace.TraceService{
					TraceAK:     "ak",
					TraceSK:     "sk",
					ProjectName: "test",
					Enabled:     false,
				},
				EnvInfo:           "",
				EncryptedUserData: "Test 2 EncryptedUserData",
				RMConfig: rmconfig.Config{
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						"Java8": rmconfig.RuntimeConfig{
							Addition: map[string]string{
								"enableDcCallStack": "true",
								"dcCallStackLayer":  "3",
							},
						},
					},
				},
				Language: "Java8",
			},
		}
		res, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
		convey.So(strings.Contains(strings.Join(res.RuntimeEnvs, ","), "func-EnableDcCallStack=true"), convey.ShouldBeTrue)
		convey.So(err, convey.ShouldEqual, nil)

	})
}

func Test_initLogger(t *testing.T) {
	if log.GetLogger() == nil {
		t.Errorf("failed to get logger")
	}
}

func Test_addRuntimeEnvFromEnvMap(t *testing.T) {
	result := &utils.Environment{}
	envMap := map[string]interface{}{
		"key": "value",
	}
	addRuntimeEnvFromEnvMap(result, envMap)
	if len(result.RuntimeEnvs) != 2 {
		t.Error("failed to add runtime env from env map")
	}
}

func Test_addFuncEnvFromEnvMap(t *testing.T) {
	currentSize := 0
	result := &utils.Environment{}
	envMap := map[string]interface{}{
		"no-func-key": "value",
	}
	addFuncEnvFromEnvMap(&currentSize, result, envMap)
	if len(result.FuncEnvs) != 1 {
		t.Error("failed to add func env from env map 1")
	}

	currentSize = 8192
	addFuncEnvFromEnvMap(&currentSize, result, envMap)
	if len(result.FuncEnvs) != 2 {
		t.Error("failed to add func env from env map 2")
	}
}

func Test_updateBranchFromEnvMap(t *testing.T) {
	result := &utils.Environment{}
	envMap := map[string]interface{}{
		"LD_LIBRARY_PATH": "value1",
	}
	userDataMap := map[string]interface{}{
		"LD_LIBRARY_PATH": "value2",
	}
	runtimeUserDataMap := map[string]interface{}{
		"LD_LIBRARY_PATH": "value3",
	}
	updateBranchFromEnvMap("", result, envMap, userDataMap, runtimeUserDataMap)
	if len(result.FuncEnvs) == 0 || len(result.RuntimeEnvs) == 0 {
		t.Error("failed to update envs")
	}
}

func Test_updateConfigFromEnv(t *testing.T) {
	configuration := &config.Configuration{}
	envMap := map[string]interface{}{
		"func-LOG_LEVEL": worker.FuncLogLevelDebug,
	}
	updateConfigFromEnv(configuration, envMap)
	if configuration.FuncLogLevel != worker.FuncLogLevelDebug {
		t.Error("failed to updateConfigFromEnv debug 1")
	}

	envMap["func-LOG_LEVEL"] = worker.FuncLogLevelInfo
	updateConfigFromEnv(configuration, envMap)
	if configuration.FuncLogLevel != worker.FuncLogLevelInfo {
		t.Error("failed to updateConfigFromEnv debug 2")
	}
}

func findTargetStr(target string, strList []string) bool {
	for _, str := range strList {
		if str == target {
			return true
		}
	}
	return false
}

func Test_initBranchEnvFromConfigWithHeartbeatEnableLanguage(t *testing.T) {
	config := &config.Configuration{}
	config.RuntimeConfig.Language = "java8"
	config.RuntimeConfig.Heartbeat.Handler = "index.heartbeat"
	config.RuntimeConfig.Heartbeat.Path = "index.path"
	config.RuntimeConfig.StartupHeartbeat.Path = "index.path"
	result := initBranchEnvFromConfig(config)

	env1 := "func-RUNTIME_HEARTBEAT_HANDLER=index.heartbeat"
	if !findTargetStr(env1, result) {
		t.Errorf("expect env %s not find in env result", env1)
	}

	env2 := "func-RUNTIME_HEARTBEAT_PATH=index.path"
	if !findTargetStr(env2, result) {
		t.Errorf("expect env %s not find in env result", env2)
	}

	env3 := "func-RUNTIME_STARTUP_HEARTBEAT_PATH=index.path"
	if !findTargetStr(env2, result) {
		t.Errorf("expect env %s not find in env result", env3)
	}
}

func Test_initBranchEnvFromConfigWithHeartbeatNotEnableLanguage(t *testing.T) {
	config := &config.Configuration{}
	config.RuntimeConfig.Language = "python3.6"
	config.RuntimeConfig.Heartbeat.Handler = "index.heartbeat"
	result := initBranchEnvFromConfig(config)

	env := "func-RUNTIME_HEARTBEAT_HANDLER=index.heartbeat"
	if findTargetStr(env, result) {
		t.Errorf("env expect empty, but get %s", env)
	}
}

func Test_initBranchEnvFromConfigWithReportCustomMetrics(t *testing.T) {
	config := &config.Configuration{}
	config.RuntimeConfig.CustomMetricsReport.Handler = "index.reportCustomMetrics"
	result := initBranchEnvFromConfig(config)

	env1 := "func-RUNTIME_CUSTOM_REPORT_METRICS_HANDLER=index.reportCustomMetrics"
	if !findTargetStr(env1, result) {
		t.Errorf("expect env %s not find in env result", env1)
	}
}

func Test_updateRuntimeUserData(t *testing.T) {
	convey.Convey("Test updateRuntimeUserData", t, func() {
		convey.Convey("Test 1", func() {
			result := &utils.Environment{}
			runtimeUserDataMap := make(map[string]interface{})
			runtimeUserDataMap["Test json marshel"] = "Json Marshel Fail"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("json marshel err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			updateRuntimeUserData(result, runtimeUserDataMap)
			convey.So(len(result.RuntimeEnvs), convey.ShouldEqual, 0)
			convey.So(len(result.FuncEnvs), convey.ShouldEqual, 0)
		})
	})
}

func Test_addUserDataFromEnvMap(t *testing.T) {
	convey.Convey("Test addUserDataFromEnvMap", t, func() {
		convey.Convey("Test 1", func() {
			var runtimeUserDataMap map[string]interface{}
			envMap := make(map[string]interface{}, 0)
			addUserDataFromEnvMap(&runtimeUserDataMap, envMap)
			convey.So(runtimeUserDataMap, convey.ShouldBeNil)
		})
		convey.Convey("Test 2", func() {
			var runtimeUserDataMap map[string]interface{}
			envMap := make(map[string]interface{}, 1)
			envMap["envMap test 1"] = "Test 1 res"
			addUserDataFromEnvMap(&runtimeUserDataMap, envMap)
			convey.So(runtimeUserDataMap["envMap test 1"], convey.ShouldEqual, "Test 1 res")
		})
	})
}

func Test_decryptEnv(t *testing.T) {
	convey.Convey("Test decryptEnv", t, func() {
		convey.Convey("Test 1", func() {
			c := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					EnvInfo:           "Test 1 EnvInfo",
					EncryptedUserData: "Test 2 EncryptedUserData",
				},
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return nil, errors.New("Decrypt a cypher text err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			result := &utils.Environment{}
			env, err := decryptEnv(c, result)
			convey.So(env.envJSON, convey.ShouldBeNil)
			convey.So(env.userData, convey.ShouldBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "decrypt user's env error: Decrypt a cypher text err")
		})
		convey.Convey("Test 2", func() {
			c := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					EnvInfo:           "",
					EncryptedUserData: "Test 2 EncryptedUserData",
				},
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			result := &utils.Environment{}
			env, err := decryptEnv(c, result)
			convey.So(env.envJSON, convey.ShouldBeNil)
			convey.So(env.userData, convey.ShouldBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("Test 3", func() {
			c := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					EnvInfo:           "",
					EncryptedUserData: "Test 3 EncryptedUserData",
				},
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return nil, errors.New("Decrypt a cypher text err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			result := &utils.Environment{}
			env, err := decryptEnv(c, result)
			convey.So(env.envJSON, convey.ShouldBeNil)
			convey.So(env.userData, convey.ShouldBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "decrypt user's env error: Decrypt a cypher text err")
		})
		convey.Convey("Test clear bytes memory", func() {
			expected := []byte("abc")
			c := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					EnvInfo:           "",
					EncryptedUserData: "Test EncryptedUserData",
				},
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return expected, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			result := &utils.Environment{}
			env, err := decryptEnv(c, result)
			convey.So(env.envJSON, convey.ShouldBeNil)
			assert.Equal(t, expected, env.userData)
			convey.So(err, convey.ShouldBeNil)
			env.cleanUp()
			assert.Equal(t, make([]byte, 3), env.userData)
		})
		convey.Convey("Test clear bytes memory 2", func() {
			data := []byte("abc")
			c := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Language:      "go",
					EnvInfo:       "",
					ExtraUserData: data,
				},
			}
			result := &utils.Environment{}
			env, err := decryptEnv(c, result)
			convey.So(env.envJSON, convey.ShouldBeNil)
			assert.Equal(t, data, env.userData)
			convey.So(err, convey.ShouldBeNil)
			env.cleanUp()
			assert.Equal(t, []byte("abc"), env.userData)
		})
	})
}

func Test_fetchTraceInfo(t *testing.T) {
	convey.Convey("Test fetchTraceInfo", t, func() {
		convey.Convey("Test 1", func() {
			c := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					EnvInfo:           "Test 1 EnvInfo",
					EncryptedUserData: "Test 2 EncryptedUserData",
					TraceService: trace.TraceService{
						TraceAK: "",
						TraceSK: "",
					},
				},
			}
			ts := fetchTraceInfo(c)
			convey.So(ts.traceAK, convey.ShouldEqual, "")
			convey.So(ts.traceSK, convey.ShouldEqual, "")
		})
	})
}

func TestFetchEnvFromConfigWithHandlerErr(t *testing.T) {
	type args struct {
		conf *config.Configuration
	}
	tests := []struct {
		name        string
		args        args
		want        *utils.Environment
		wantErr     assert.ErrorAssertionFunc
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "fetch_success",
			want: &utils.Environment{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					return false
				}
				return true
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(FetchEnvFromConfig, func(c *config.Configuration, runProcessParams *runtimemanagerrequest.
						RunProcessParams) (*utils.Environment, error) {
						return &utils.Environment{}, nil
					}),
				})
				return patches
			},
		},
		{
			name: "fetch_error",
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					return false
				}
				return true
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(FetchEnvFromConfig, func(c *config.Configuration, runProcessParams *runtimemanagerrequest.
						RunProcessParams) (*utils.Environment, error) {
						return nil, errors.New("fetch error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := FetchEnvFromConfigWithHandlerErr(tt.args.conf)
			if !tt.wantErr(t, err, fmt.Sprintf("FetchEnvFromConfigWithHandlerErr(%v)", tt.args.conf)) {
				return
			}
			assert.Equalf(t, tt.want, got, "FetchEnvFromConfigWithHandlerErr(%v)", tt.args.conf)
			patches.ResetAll()
		})
	}
}

func TestGetDcCallStackEnv(t *testing.T) {
	rmConfig := rmconfig.Config{
		RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
			"v1.0": rmconfig.RuntimeConfig{
				Addition: map[string]string{
					"enableDcCallStack": "true",
					"dcCallStackLayer":  "3",
				},
			},
			"v2.0": rmconfig.RuntimeConfig{},
		},
	}

	// Test case 1: version exists in runtime configs and has addition
	expected1 := []string{"func-EnableDcCallStack=true", "func-DcCallStackLayer=3"}
	result1 := getDcCallStackEnv("v1.0", rmConfig)
	assert.Equal(t, result1, expected1)

	// Test case 2: version exists in runtime configs but has no addition
	expected2 := []string{}
	result2 := getDcCallStackEnv("v2.0", rmConfig)
	assert.Equal(t, result2, expected2)

	// Test case 3: version does not exist in runtime configs
	expected3 := []string{}
	result3 := getDcCallStackEnv("v3.0", rmConfig)
	assert.Equal(t, result3, expected3)
}

func Test_addAGCCredentialEnv(t *testing.T) {
	tests := []struct {
		name        string
		runtimeName string
		userData    map[string]interface{}
		envs        []string

		expected []string
	}{
		{
			name:        "agc func, no user data",
			runtimeName: "agcnodejs20.15",
			envs:        []string{"a=b"},

			expected: []string{"a=b"},
		},
		{
			name:        "agc func, with user data",
			runtimeName: "agcnodejs20.15",
			userData: map[string]interface{}{
				constants.AGCCredentialEnv: "{'abcd':1234}",
			},
			envs: []string{"a=b"},

			expected: []string{"a=b", "func-AGCClientCredentialConfig={'abcd':1234}"},
		},
		{
			name:        "normal func, with user data",
			runtimeName: "nodejs20.15",
			userData: map[string]interface{}{
				constants.AGCCredentialEnv: "{'abcd':1234}",
			},
			envs: []string{"a=b"},

			expected: []string{"a=b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &config.Configuration{}
			c.RuntimeName = tt.runtimeName
			env := &utils.Environment{RuntimeEnvs: tt.envs}
			addAGCCredentialEnv(c, tt.userData, env)
			assert.Equal(t, tt.expected, env.RuntimeEnvs, tt.name)
		})
	}
}