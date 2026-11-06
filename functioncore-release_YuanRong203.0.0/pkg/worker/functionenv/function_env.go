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

// Package functionenv define env variable of worker
package functionenv

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/kms"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/utils"
)

var (
	nodeIP = os.Getenv("HOST_IP")
)

const (
	resultsCapacity             = 16
	envLengthLimit              = 20 * (1 << 10) // limit 20k
	extraReservedFieldsCapacity = 22

	netCore21 = "c#(.net core 2.1)"
	netCore31 = "c#(.net core 3.1)"
	netCore60 = "c#(.net core 6.0)"
	netCore80 = "c#(.net core 8.0)"
)

type traceService struct {
	enabled     bool
	traceAK     string
	traceSK     string
	projectName string
}

func initEnvFromConfig(c *config.Configuration) []string {
	result := make([]string, 0, resultsCapacity)
	funcURN := &urnutils.BaseURN{}
	if err := funcURN.ParseFrom(c.URN); err == nil {
		result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_NAME=%s", funcURN.Name))
		result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_VERSION=%s", funcURN.Version))
		result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_BUSINESS=%s", funcURN.BusinessID))
		result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_TENANTID=%s", funcURN.TenantID))
	}

	if c.ResourceMetaData.CPU != 0 {
		result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_CPU=%d", c.ResourceMetaData.CPU))
	}
	if c.ResourceMetaData.Mem != 0 {
		result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_MEM=%d", c.ResourceMetaData.Mem))
	}

	result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_USER_FILE_PATH=%s", "/tmp"))
	result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_DEPLOY_DIR=%s", c.DeployDir+"/layer/func"))
	result = append(result, fmt.Sprintf("func-FAAS_LAYER_DEPLOY_DIR=%s", c.DeployDir+"/layer"))
	result = append(result, fmt.Sprintf("func-FAAS_FUNCTION_TIMEOUT=%v", c.Timeout))
	result = append(result, fmt.Sprintf("func-FAAS_INITIALIZER_HANDLER=%s",
		c.RuntimeConfig.Initializer.InitializerHandler))
	result = append(result, fmt.Sprintf("func-FAAS_INITIALIZER_TIMEOUT=%v",
		c.RuntimeConfig.Initializer.InitializerTimeout))
	if c.ClusterID != "" {
		result = append(result, fmt.Sprintf("func-FAAS_CLUSTER_ID=%v", c.ClusterID))
	}
	result = append(result, fmt.Sprintf("func-FAAS_RUNTIME_NAME=%s", c.RuntimeConfig.RuntimeName))
	return result
}

func initBranchEnvFromConfig(c *config.Configuration) []string {
	result := make([]string, 0, extraReservedFieldsCapacity)
	funcURN := &urnutils.BaseURN{}
	if err := funcURN.ParseFrom(c.URN); err == nil {
		if packageID, name, err := urnutils.GetFunctionNameAndServiceName(funcURN.Name); err == nil {
			result = append(result, fmt.Sprintf("func-RUNTIME_FUNC_NAME=%s", name))
			result = append(result, fmt.Sprintf("func-RUNTIME_PACKAGE=%s", packageID))
		}
		result = append(result, fmt.Sprintf("func-RUNTIME_PROJECT_ID=%s", funcURN.TenantID))
		result = append(result, fmt.Sprintf("func-RUNTIME_FUNC_VERSION=%s", funcURN.Version))
	}
	result = append(result, fmt.Sprintf("func-RUNTIME_HANDLER=%s", c.RuntimeConfig.HandlerName))
	result = append(result, fmt.Sprintf("func-RUNTIME_TIMEOUT=%d", c.RuntimeConfig.Timeout))
	result = append(result, fmt.Sprintf("func-RUNTIME_CPU=%d", c.RuntimeConfig.ResourceMetaData.CPU))
	result = append(result, fmt.Sprintf("func-RUNTIME_MEMORY=%d", c.RuntimeConfig.ResourceMetaData.Mem))
	result = append(result, fmt.Sprintf("func-RUNTIME_INITIALIZER_HANDLER=%s",
		c.RuntimeConfig.Initializer.InitializerHandler))
	result = append(result, fmt.Sprintf("func-RUNTIME_INITIALIZER_TIMEOUT=%d",
		c.RuntimeConfig.Initializer.InitializerTimeout))
	result = append(result, fmt.Sprintf("func-RUNTIME_CODE_ROOT=%s", c.DeployDir))
	result = append(result, "func-RUNTIME_ROOT=/home/snuser/runtime")
	result = append(result, fmt.Sprintf("func-RUNTIME_LOG_DIR=%s", c.RuntimeLogDir))
	if c.MaxRequestBodySize != "" {
		result = append(result, fmt.Sprintf("func-MAX_REQUEST_BODY_SIZE=%s", c.MaxRequestBodySize))
	} else {
		result = append(result, fmt.Sprintf("func-MAX_REQUEST_BODY_SIZE=%s",
			os.Getenv("MAX_REQUEST_BODY_SIZE")))
	}
	if c.RuntimeConfig.PreStop.Type == "" || c.RuntimeConfig.PreStop.Type == "handler" {
		result = append(result, fmt.Sprintf("func-RUNTIME_PRESTOP_HANDLER=%s",
			c.RuntimeConfig.PreStop.Handler))
	}

	if utils.IsHeartbeatEnable(c.RuntimeConfig.Language) {
		result = append(result, fmt.Sprintf("func-RUNTIME_HEARTBEAT_HANDLER=%s",
			c.RuntimeConfig.Heartbeat.Handler))
		result = append(result, fmt.Sprintf("func-RUNTIME_HEARTBEAT_PATH=%s",
			c.RuntimeConfig.Heartbeat.Path))
		result = append(result, fmt.Sprintf("func-RUNTIME_STARTUP_HEARTBEAT_HANDLER=%s",
			c.RuntimeConfig.StartupHeartbeat.Handler))
		result = append(result, fmt.Sprintf("func-RUNTIME_STARTUP_HEARTBEAT_PATH=%s",
			c.RuntimeConfig.StartupHeartbeat.Path))
	}

	if len(c.RuntimeConfig.CustomMetricsReport.Handler) != 0 {
		result = append(result, fmt.Sprintf("func-RUNTIME_CUSTOM_REPORT_METRICS_HANDLER=%s",
			c.RuntimeConfig.CustomMetricsReport.Handler))
	}

	if len(c.RuntimeConfig.CustomMetricsReport.Path) != 0 {
		result = append(result, fmt.Sprintf("func-RUNTIME_CUSTOM_REPORT_METRICS_PATH=%s",
			c.RuntimeConfig.CustomMetricsReport.Path))
	}
	return result
}

type envData struct {
	envJSON  []byte
	userData []byte
	cleanUp  func()
}

func decryptEnv(c *config.Configuration, result *utils.Environment) (envData, error) {
	var envJSON, userData []byte
	var err error
	result.RuntimeEnvs = initBranchEnvFromConfig(c)
	result.FuncEnvs = append(result.FuncEnvs, result.RuntimeEnvs...)
	if strings.Contains(c.RuntimeConfig.Language, "go") || strings.Contains(c.RuntimeConfig.Language,
		"custom") || strings.Contains(c.RuntimeConfig.Language, "http") {
		return envData{
			envJSON:  c.RuntimeConfig.ExtraEnvJSON,
			userData: c.RuntimeConfig.ExtraUserData,
			cleanUp:  func() {},
		}, nil
	}
	if c.RuntimeConfig.EnvInfo != "" {
		if envJSON, err = decryptUserEnv(c.RuntimeConfig.EnvInfo, c.RuntimeConfig.EnvPlaintextKmsCipher); err != nil {
			return envData{}, fmt.Errorf("decrypt user's env error: %s", err.Error())
		}
	}
	if c.RuntimeConfig.EncryptedUserData != "" {
		if userData, err = decryptUserEnv(c.RuntimeConfig.EncryptedUserData, c.RuntimeConfig.EnvPlaintextKmsCipher); err != nil {
			if len(envJSON) != 0 {
				commonutils.ClearByteMemory(envJSON)
			}
			return envData{}, fmt.Errorf("decrypt user's env error: %s", err.Error())
		}
	}
	return envData{
		envJSON:  envJSON,
		userData: userData,
		cleanUp: func() {
			if len(envJSON) > 0 {
				commonutils.ClearByteMemory(envJSON)
			}
			if len(userData) > 0 {
				commonutils.ClearByteMemory(userData)
			}
		},
	}, nil
}

func decryptUserEnv(env, envPlaintextKmsCipher string) ([]byte, error) {
	if envPlaintextKmsCipher != "" {
		return kms.DecryptDataWithAESByPlainCipherToByte(env, envPlaintextKmsCipher)
	}
	return localauth.Decrypt(env)
}

// FetchEnvFromConfigWithHandlerErr _
func FetchEnvFromConfigWithHandlerErr(conf *config.Configuration) (*utils.Environment, error) {
	envs, err := FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{})
	if err != nil {
		// make a empty envs
		envs = &utils.Environment{
			FuncEnvs:    make([]string, 0, 0),
			RuntimeEnvs: make([]string, 0, 0),
		}
	}
	return envs, err
}

// FetchEnvFromConfig return the envs from configuration that will pass to runtime process
func FetchEnvFromConfig(c *config.Configuration, runProcessParams *runtimemanagerrequest.
	RunProcessParams) (*utils.Environment, error) {

	result := &utils.Environment{}
	result.FuncEnvs = initEnvFromConfig(c)
	env, err := decryptEnv(c, result)
	if err != nil {
		return nil, fmt.Errorf("decrypt env error: %s", err.Error())
	}
	defer env.cleanUp()
	traceInfo := fetchTraceInfo(c)

	// runtimeUserDataMap be used to serialize the env fields written by the user
	var envMap, runtimeUserDataMap map[string]interface{}
	if env.envJSON != nil {
		err = json.Unmarshal(env.envJSON, &envMap)
		if err != nil {
			return nil, fmt.Errorf("unmashal env error: %s", err.Error())
		}
		addUserDataFromEnvMap(&runtimeUserDataMap, envMap)
	}
	currentSize := 0
	addFuncEnvFromEnvMap(&currentSize, result, envMap)
	addRuntimeEnvFromEnvMap(result, envMap)
	addDataSystemConfig(result, c)

	addCheckpointEnv(c.RunningMode, result)
	if commonutils.IsCAEFunc(utils.GetBusinessType()) && !utils.IsCAERouterReady() {
		addWaitRouterReadyTimeoutEnv(int(c.RuntimeConfig.Initializer.InitializerTimeout), result)
	}

	var userDataMap map[string]interface{}
	if env.userData != nil {
		err = json.Unmarshal(env.userData, &userDataMap)
		if err != nil {
			return nil, fmt.Errorf("unmashal env error: %s", err.Error())
		}
		addUserDataFromEnvMap(&runtimeUserDataMap, userDataMap)
		addFuncEnvFromEnvMap(&currentSize, result, userDataMap)
	}

	log.GetLogger().Debugf("envMap is %+v, userDataMap is %+v", envMap, userDataMap)

	addStreamClientUserAkSk(userDataMap, result)
	addAGCCredentialEnv(c, userDataMap, result)
	updateBranchFromEnvMap(c, result, envMap, userDataMap, runtimeUserDataMap)
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-FAAS_FUNCTION_NAME=%s", c.URN))
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-FAAS_LOAD_TIME=%d", c.LoadTime))
	addFuncTraceEnvs(traceInfo, result)
	dcCallStackEnv := getDcCallStackEnv(c.Language, c.RMConfig)
	if len(dcCallStackEnv) != 0 {
		result.RuntimeEnvs = append(result.RuntimeEnvs, dcCallStackEnv...)
		log.GetLogger().Debugf("get dcCallStack of %+v", dcCallStackEnv)
	}

	result.RuntimeEnvs = append(result.RuntimeEnvs,
		fmt.Sprintf("func-RUNTIME_MAX_STATE_SIZE=%s", c.MaxStateSize))
	result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("func-RUNTIME_POD_NAME=%s", os.Getenv("PodName")))
	result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("func-BUSINESS_TYPE=%s", utils.GetBusinessType()))
	result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("func-RUNTIME_WEBSOCKET_ENABLE=%s",
		strconv.FormatBool(runtime.GetWebSocketEnable())))

	result.RuntimeEnvs = addMcpEnvs(result.RuntimeEnvs, runProcessParams, envMap, userDataMap)

	updateConfigFromEnv(c, envMap)
	return result, nil
}

func addMcpEnvs(runtimeEnvs []string, runProcessParams *runtimemanagerrequest.RunProcessParams,
	envMap, userDataMap map[string]interface{}) []string {

	canOverWritePath := true
	if runProcessParams.IsMcpFunction {
		if value, exist := envMap["PATH"]; exist {
			if _, ok := value.(string); ok {
				canOverWritePath = false
			}
		}
		if value, exist := userDataMap["PATH"]; exist {
			if _, ok := value.(string); ok {
				canOverWritePath = false
			}
		}

		// if user config PATH, use user config, if user not config, use nodejs20
		if canOverWritePath {
			runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-PATH=%s", worker.NodeJs20Path))
		}

		// npm config
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-NPM_CONFIG_CACHE=%s", worker.NpmConfigCache))
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-NPM_CONFIG_LOGS_DIR=%s", worker.NpmConfigLogsDir))
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-npm_config_registry=%s", runProcessParams.NPMConfigRegistry))
		// uv config
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-UV_CACHE_DIR=%s", worker.UVCacheDir))
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-UV_PYTHON_INSTALL_DIR=%s", worker.UVPythonInstallDir))
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-UV_TOOL_DIR=%s", worker.UVToolDir))
		runtimeEnvs = append(runtimeEnvs, fmt.Sprintf("func-UV_INDEX_URL=%s", runProcessParams.UVIndexURL))
	}
	return runtimeEnvs
}

func addFuncTraceEnvs(traceInfo *traceService, result *utils.Environment) {
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-FAAS_TRACE_AK=%s", traceInfo.traceAK))
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-FAAS_TRACE_SK=%s", traceInfo.traceSK))
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-FAAS_TRACE_ENABLE=%v", traceInfo.enabled))
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-FAAS_PROJECT_NAME=%s", traceInfo.projectName))
}

func addStreamClientUserAkSk(userDataMap map[string]interface{}, result *utils.Environment) {
	if userDataMap != nil {
		_, okStreamAk := userDataMap[constants.WorkerSystemAccessKey]
		_, okStreamSk := userDataMap[constants.WorkerSystemSecretKey]
		if okStreamAk && okStreamSk {
			log.GetLogger().Info("user already configured the stream client ak/sk")
			return
		}
	}

	if os.Getenv(constants.WorkerSystemAccessKey) != "" && os.Getenv(constants.WorkerSystemSecretKey) != "" {
		result.FuncEnvs = append(result.FuncEnvs,
			fmt.Sprintf("func-WORKER_SYSTEM_ACCESS_KEY=%s", os.Getenv(constants.WorkerSystemAccessKey)))
		result.FuncEnvs = append(result.FuncEnvs,
			fmt.Sprintf("func-WORKER_SYSTEM_SECRET_KEY=%s", os.Getenv(constants.WorkerSystemSecretKey)))
	}
}

func addAGCCredentialEnv(c *config.Configuration, userDataMap map[string]interface{}, result *utils.Environment) {
	if !strings.HasPrefix(c.RuntimeName, "agc") {
		return
	}
	val, ok := userDataMap[constants.AGCCredentialEnv]
	if !ok {
		log.GetLogger().Warnf("failed to get agc credential env")
		return
	}
	result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("func-%s=%s", constants.AGCCredentialEnv, val))
}

func addCheckpointEnv(runningMode string, result *utils.Environment) {
	if runningMode == constants.RunningModeCheckpoint {
		result.FuncEnvs = append(result.FuncEnvs, "func-isCheckpoint=true")
	}
}

func addWaitRouterReadyTimeoutEnv(initTimeout int, result *utils.Environment) {
	initTimeout *= int(time.Second / time.Millisecond)
	result.FuncEnvs = append(result.FuncEnvs, "func-waitRouterReadyTimeout="+strconv.Itoa(initTimeout))
}

func getDcCallStackEnv(version string, rmConfig rmconfig.Config) []string {
	log.GetLogger().Debugf("start to get DcCallStack env for %s", version)
	cfg, ok := rmConfig.RuntimeConfigs[version]
	if !ok {
		log.GetLogger().Errorf("no runtime config of %s", version)
		return []string{}
	}
	if cfg.Addition != nil {
		enable := fmt.Sprintf("func-EnableDcCallStack=%s", cfg.Addition["enableDcCallStack"])
		layerNumber := fmt.Sprintf("func-DcCallStackLayer=%s", cfg.Addition["dcCallStackLayer"])
		return []string{enable, layerNumber}
	}
	log.GetLogger().Debugf("runtime config of %s no addition ", version)
	return []string{}
}

func fetchTraceInfo(c *config.Configuration) *traceService {
	if c.TraceService.TraceAK != "" && c.TraceService.TraceSK != "" {
		traceSK, err := localauth.Decrypt(c.TraceService.TraceSK)
		if err != nil {
			log.GetLogger().Warnf("failed to decrypt trace SK: %s", err.Error())
			return &traceService{}
		}
		return &traceService{
			enabled:     c.TraceService.Enabled,
			traceAK:     c.TraceService.TraceAK,
			traceSK:     string(traceSK),
			projectName: c.TraceService.ProjectName,
		}
	}
	return &traceService{}
}

func updateBranchFromEnvMap(c *config.Configuration, result *utils.Environment,
	envMap, userDataMap, runtimeUserDataMap map[string]interface{}) {
	if c == nil {
		return
	}
	updateLibraryPath(c.DeployDir, c.Language, result, envMap, userDataMap)
	updateRuntimeUserData(result, runtimeUserDataMap)
}

func updateLibraryPath(deployDir string, version string, result *utils.Environment,
	envMap, userDataMap map[string]interface{}) {
	ldLibraryPath := fmt.Sprintf("/lib64:/usr/lib64:%s:%s/lib", deployDir, deployDir)
	if strings.Contains(version, netCore21) {
		ldLibraryPath = fmt.Sprintf("%s:%s", ldLibraryPath, constants.CSharp21LdLibraryPath)
	}
	if strings.Contains(version, netCore31) {
		ldLibraryPath = fmt.Sprintf("%s:%s", ldLibraryPath, constants.CSharp31LdLibraryPath)
	}
	if strings.Contains(version, netCore60) {
		ldLibraryPath = fmt.Sprintf("%s:%s", ldLibraryPath, constants.CSharp60LdLibraryPath)
	}
	if strings.Contains(version, netCore80) {
		ldLibraryPath = fmt.Sprintf("%s:%s", ldLibraryPath, constants.CSharp80LdLibraryPath)
	}

	if value, exist := envMap["LD_LIBRARY_PATH"]; exist {
		if valueStr, ok := value.(string); ok {
			ldLibraryPath = ldLibraryPath + ":" + valueStr
		}
	}
	if value, exist := userDataMap["LD_LIBRARY_PATH"]; exist {
		if valueStr, ok := value.(string); ok {
			ldLibraryPath = ldLibraryPath + ":" + valueStr
		}
	}
	result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("func-LD_LIBRARY_PATH=%s",
		ldLibraryPath))
	result.FuncEnvs = append(result.FuncEnvs, fmt.Sprintf("func-LD_LIBRARY_PATH=%s",
		ldLibraryPath))
}

func updateRuntimeUserData(result *utils.Environment, runtimeUserDataMap map[string]interface{}) {
	if runtimeUserDataMap == nil {
		return
	}
	runtimeUserData, err := json.Marshal(runtimeUserDataMap)
	if err != nil {
		log.GetLogger().Errorf("failed to convert runtimeUserDataMap to JSON, err:%s", err.Error())
		return
	}
	result.RuntimeEnvs = append(result.RuntimeEnvs,
		fmt.Sprintf("func-RUNTIME_USERDATA=%s", string(runtimeUserData)))
	result.FuncEnvs = append(result.FuncEnvs,
		fmt.Sprintf("func-RUNTIME_USERDATA=%s", string(runtimeUserData)))

}

func addUserDataFromEnvMap(runtimeUserDataMap *map[string]interface{}, envMap map[string]interface{}) {
	if len(envMap) == 0 {
		log.GetLogger().Debug("envMap has not meta")
		return
	}
	if *runtimeUserDataMap == nil {
		*runtimeUserDataMap = make(map[string]interface{}, len(envMap))
	}
	for key, value := range envMap {
		(*runtimeUserDataMap)[key] = value
	}
}

func addDataSystemConfig(result *utils.Environment, c *config.Configuration) {
	result.RuntimeEnvs = append(result.RuntimeEnvs,
		fmt.Sprintf("%s%s=%s", worker.RuntimeEnvPrefix, constants.DataSystemHost, nodeIP))
	result.RuntimeEnvs = append(result.RuntimeEnvs,
		fmt.Sprintf("%s%s=%s", worker.RuntimeEnvPrefix, constants.DataSystemPort, constants.DefaultDataSystemPort))
	result.RuntimeEnvs = append(result.RuntimeEnvs,
		fmt.Sprintf("%s%s=%s", worker.RuntimeEnvPrefix, constants.StreamContentTypeWhiteList, c.StreamContentType))
}

func addRuntimeEnvFromEnvMap(result *utils.Environment, envMap map[string]interface{}) {
	for key, value := range envMap {
		key = worker.RuntimeEnvPrefix + key
		result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("%s=%s", key, value))
	}
	result.RuntimeEnvs = append(result.RuntimeEnvs, fmt.Sprintf("%s%s=%s", worker.RuntimeEnvPrefix,
		constants.EnableLogPath, os.Getenv(constants.EnableLogPath)))
}

func addFuncEnvFromEnvMap(currentSize *int, result *utils.Environment, envMap map[string]interface{}) {
	for key, value := range envMap {
		valueStr := fmt.Sprintf("%s", value)
		if !strings.HasPrefix(key, worker.RuntimeEnvPrefix) {
			key = worker.RuntimeEnvPrefix + key
		}
		*currentSize += len(key) + len(valueStr)
		result.FuncEnvs = append(result.FuncEnvs, key+"="+valueStr)
	}
	if currentSize != nil {
		log.GetLogger().Infof("current env size %d bytes", *currentSize)
	}
}

func updateConfigFromEnv(c *config.Configuration, envMap map[string]interface{}) {
	logConfig, exist := envMap["func-LOG_LEVEL"]
	if exist {
		val := fmt.Sprintf("%s", logConfig)
		switch strings.ToUpper(val) {
		case worker.FuncLogLevelDebug, worker.FuncLogLevelWarn, worker.FuncLogLevelError:
			c.RuntimeConfig.FuncLogLevel = strings.ToUpper(val)
		default:
			c.RuntimeConfig.FuncLogLevel = worker.FuncLogLevelInfo
		}
	}
}
