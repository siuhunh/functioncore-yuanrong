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

// Package utils get function service from urnutils
package utils

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
)

var (
	// LocalFuncURN is URN of local function
	LocalFuncURN = &urnutils.BaseURN{}
	// ResourceLimitsMemory Memory limit, in bytes
	ResourceLimitsMemory int64 = math.MaxInt64

	// PodOriginDNSConf save the pod dns conf from the origin data
	PodOriginDNSConf []byte

	// WorkKeyConfig -
	WorkKeyConfig = "config-key"
	userDataPath  = "/runtime/tmp"

	resourceMetaData = &types.ResourceMetaData{}

	heartbeatEnableLanguages = map[string]bool{
		"java11": true,
		"java8":  true,
		"javax":  true,
		"java17": true,
		"java21": true,
		"http":   true,
	}

	userAgency = sync.Map{}

	businessType      = os.Getenv(constants.BusinessType)
	enableRemoteDebug = os.Getenv("EnableRemoteDebug") == "true"

	// IsDirectRuntime is direct runtime
	IsDirectRuntime = false

	enableVpa   = strings.TrimSpace(os.Getenv("ENABLE_VERTICAL_AUTOSCALING_ENV")) == "1"
	cpuLimit    int64
	memoryLimit int64

	enableLogPath = os.Getenv(constants.EnableLogPath) == "true"

	caeRouterReady = uint32(0)

	enableTenantAuth = strings.TrimSpace(os.Getenv(constants.EnableIAMTokenEnv)) == "true"

	podNames = os.Getenv("PodName")

	isMcpFunction     = false
	npmConfigRegistry = ""
	uvIndexURL        = ""
)

const (
	// CSharp define runtime type CSharp
	CSharp = "c#"

	podNameLength = 3
	cpuIndex      = 1
	memoryIndex   = 2
	base          = 10
	bitSize       = 64
	// PingPath define ping bin path
	PingPath = "/usr/bin/ping"
	// Ping6Path define ping6 bin path
	Ping6Path             = "/usr/bin/ping"
	gcLogPath             = "/home/sn/log/service/runtime/gc*"
	defaultDirectLanguage = "javax"
	memory512M            = 512
	// IptablesCmdSucceed iptable cmd success code
	IptablesCmdSucceed = 0
	// IptablesCmdFailed iptable cmd fail code
	IptablesCmdFailed = 2
	// IptablesExists iptable exists code
	IptablesExists = "exists"
	// MinPort is the lower limit of port
	MinPort = 0
	// MaxPort is the upper limit of port
	MaxPort = 65535

	sfs30 = "sfs3.0"
)

// FunctionInfo -
type FunctionInfo struct {
	TenantID  string
	FuncName  string
	Version   string
	ServiceID string
}

// Environment function evn
type Environment struct {
	FuncEnvs    []string
	RuntimeEnvs []string
}

// ExtractZipFile extracts zip file
func ExtractZipFile(zf *zip.File, dest string) error {
	permForExtractZipFile := os.FileMode(worker.DeployFileMode)
	if isMcpFunction {
		permForExtractZipFile = os.FileMode(worker.DeployFileModForMcp)
	}

	// Transfer Chinese simplified characters
	i := bytes.NewReader([]byte(zf.Name))
	decoder := transform.NewReader(i, simplifiedchinese.GB18030.NewDecoder())
	fileName, err := ioutil.ReadAll(decoder)
	if err != nil {
		return fmt.Errorf("read file content error: %s", err)
	}
	fPath := filepath.Join(dest, string(fileName))

	rc, err := zf.Open()
	if err != nil {
		return fmt.Errorf("open zip file error: %s", err)
	}
	defer rc.Close()
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)
	if zf.FileInfo().IsDir() {
		if err := os.Mkdir(fPath, permForExtractZipFile); err != nil {
			return fmt.Errorf("make dir all error: %s", err)
		}
		recoverModTime(fPath, zf.Modified)
		return nil
	}
	err = generateZipFile(rc, fPath)
	if err == nil {
		recoverModTime(fPath, zf.Modified)
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("generate zip file error: %s", err)
	}
	// Make parent dir first
	if err := os.MkdirAll(filepath.Dir(fPath), permForExtractZipFile); err != nil {
		return fmt.Errorf("make dir for dest file error: %s", err)
	}
	if err = generateZipFile(rc, fPath); err != nil {
		return fmt.Errorf("generate zip file error: %s", err)
	}
	recoverModTime(fPath, zf.Modified)
	return nil
}

func recoverModTime(fPath string, modTime time.Time) {
	if err := os.Chtimes(fPath, modTime, modTime); err != nil {
		log.GetLogger().Warnf("failed to recover file %s modTime %s", fPath, modTime)
	}
}

func generateZipFile(rc io.ReadCloser, fPath string) error {
	// Make file
	permForExtractZipFile := os.FileMode(worker.DeployFileMode)
	if isMcpFunction {
		permForExtractZipFile = os.FileMode(worker.DeployFileModForMcp)
	}
	oFile, err := os.OpenFile(fPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, permForExtractZipFile)
	if err != nil {
		return err
	}
	defer func() {
		err := oFile.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close output file: %s", err.Error())
		}
	}()

	if _, err = io.Copy(oFile, rc); err != nil {
		return fmt.Errorf("copy zip file error: %s", err)
	}
	return nil
}

// FunctionBus use uint64 as id of invocation, but Worker use uuid(string) instead,
// so conversion methods are required
const (
	uint64Base     = 10
	uint64Accuracy = 64
	// memory limit file in container
	memoryLimitFile = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	// cpuQuota related parameter
	perm                = 0666
	cpuQuotaMultiplyNum = 100
	cpuRestoreThreshold = 1024
	// cpu limit file in container
	cpuLimitFile = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
)

// BusID2InvokeID transfer bus id to invoke id
func BusID2InvokeID(busID uint64) string {
	return strconv.FormatInt(int64(busID), uint64Base)
}

// InvokeID2BusID transfer invoke id to bus id
func InvokeID2BusID(invokeID string) uint64 {
	// If invokeID is not initialized, return 0
	if invokeID == "initializer" {
		return 0
	}
	busID, err := strconv.ParseUint(invokeID, uint64Base, uint64Accuracy)
	if err != nil {
		log.GetLogger().Warnf("failed to Invoke ID transfer Bus ID, error: %s", err.Error())
	}
	return busID
}

// GetResourceLimitsMemory Memory limit, in bytes
func GetResourceLimitsMemory() {
	var envLimit string
	buf, err := ioutil.ReadFile(memoryLimitFile)
	if err != nil {
		log.GetLogger().Warnf("failed to read memory limit file")
		envLimit := os.Getenv(constants.ResourceLimitsMemory)
		if envLimit == "" {
			log.GetLogger().Warnf("failed to get env MEMORY_LIMIT_BYTES")
			return
		}
	} else {
		envLimit = strings.Replace(string(buf), "\n", "", 1)
	}
	envLimitInt, err := strconv.ParseInt(envLimit, base, bitSize)
	if err != nil {
		log.GetLogger().Warnf("invalid MEMORY_LIMIT_BYTES")
		return
	}
	ResourceLimitsMemory = envLimitInt
	log.GetLogger().Infof("succeed to set the ResourceLimitsMemory: %s", envLimit)
}

// SetCmdCredByID set cmd credential by uid and gid
func SetCmdCredByID(cmd *exec.Cmd, uid, gid int) error {
	cred := &syscall.Credential{
		Uid:    uint32(uid),
		Gid:    uint32(gid),
		Groups: []uint32{constants.DefaultWorkerGID, constants.DefaultRuntimeGID},
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
	return nil
}

// IsMultiRt returns true if MULTI_RUNTIME_MODE in env and its value is not empty string
func IsMultiRt() bool {
	r := os.Getenv("MULTI_RUNTIME_MODE")
	return r != ""
}

func genRandID(len int) (string, error) {
	const double = 2
	if len <= 0 {
		return "", nil
	}
	b := make([]byte, len)
	n, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	if n != len {
		return "", fmt.Errorf("generate random ID length %d, expect %d", n, len)
	}
	dst := make([]byte, len*double)
	_ = hex.Encode(dst, b)
	return string(dst[:len]), nil
}

const runtimeIDLength = 6

// GenRuntimeID gen a random string for RuntimeID
func GenRuntimeID() (string, error) {
	return genRandID(runtimeIDLength)
}

// IsRuntimeEnable is runtime preStart enable
func IsRuntimeEnable(config *rmconfig.RuntimeConfig) bool {
	if !config.Enable {
		return false
	}
	_, podMemory := GetCPUMemory()
	for index := range config.ExcludeSpecifications {
		if config.ExcludeSpecifications[index] == podMemory {
			return false
		}
	}
	return true
}

// GetCPUMemory get cpu and memory
func GetCPUMemory() (int64, int64) {
	if IsVpaFeatureEnabled() && (cpuLimit != 0 || memoryLimit != 0) {
		return cpuLimit, memoryLimit
	}
	if resourceMetaData.CPU == 0 && resourceMetaData.Mem == 0 {
		podName := os.Getenv("PodName")
		split := strings.Split(podName, "-")
		splitLen := len(split)
		if splitLen < podNameLength {
			log.GetLogger().Errorf("pod name length invalid")
		} else {
			cpu, err := strconv.ParseInt(split[cpuIndex], base, bitSize)
			if err != nil {
				cpu = 0
				log.GetLogger().Errorf("failed to parse the cpu string")
			}
			memory, err := strconv.ParseInt(split[memoryIndex], base, bitSize)
			if err != nil {
				memory = 0
				log.GetLogger().Errorf("failed to parse the memory string")
			}
			resourceMetaData.CPU = cpu
			resourceMetaData.Mem = memory
		}
	}
	return resourceMetaData.CPU, resourceMetaData.Mem
}

// RestoreCPULimit  Restore CPU Limit to the original specification
func RestoreCPULimit() error {
	if IsVpaFeatureEnabled() && utils.IsCAEFunc(GetBusinessType()) {
		log.GetLogger().Infof("vpa feature enabled and skip restore CPU limit")
		return nil
	}
	cpu, _ := GetCPUMemory()
	if cpu < cpuRestoreThreshold {
		cpuQuotaUs := strconv.FormatInt(cpu*cpuQuotaMultiplyNum, uint64Base)
		err := ioutil.WriteFile(cpuLimitFile, []byte(cpuQuotaUs), perm)
		if err != nil {
			return fmt.Errorf("restore CPU limit error : %s", err.Error())
		}
	}
	log.GetLogger().Infof("succeed to restore CPU limit ")
	return nil
}

// RemoveGCLog remove GC log
func RemoveGCLog() {
	needRemovePath := constants.GCPath + "/" + constants.GCFilePrefix
	files, err := filepath.Glob(needRemovePath)
	if err != nil {
		log.GetLogger().Errorf("remove gc log error")
		return
	}
	log.GetLogger().Infof("start to remove gc log, files num is %d", len(files))
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			log.GetLogger().Errorf("failed to remove GC log")
		} else {
			log.GetLogger().Infof("success to remove GC log: %s", f)
		}
	}
}

// RemoveDownloadPath remove download path
func RemoveDownloadPath(downloadFilePath, extractPath string) error {
	var err error
	needRemovePath := strings.TrimPrefix(downloadFilePath, extractPath+"/")
	pathArr := strings.Split(needRemovePath, "/")
	// needRemovePath format: xxx/xxx/xxx.zip. For 'xxx.zip', no tmp dir to clear.
	if pathArr == nil || len(pathArr) <= 1 {
		return err
	}
	// remove path of download file
	needRemovePath = extractPath + "/" + pathArr[0]
	if err = os.RemoveAll(needRemovePath); err != nil {
		log.GetLogger().Errorf("remove path %s error: %s", needRemovePath, err.Error())
	}
	return err
}

// IsHeartbeatEnable check whether runtime support heartbeat
func IsHeartbeatEnable(language string) bool {
	_, enable := heartbeatEnableLanguages[language]
	return enable
}

// IsM2SMode check whether function is severless microservice and checkpoint mode
func IsM2SMode(runningMode string, handler string) bool {
	return runningMode == constants.RunningModeCheckpoint &&
		strings.Contains(handler, constants.M2SFunctionPrefix)
}

// IsM2SHeartbeatEnable check whether function is microservice and enable heartbeat mode
func IsM2SHeartbeatEnable(language string, heartbeatEnable bool, handler string) bool {
	return IsHeartbeatEnable(language) && heartbeatEnable && strings.Contains(handler, constants.M2SFunctionPrefix)
}

// IsHealthCheckEnable check whether health enabled and handler is ok
func IsHealthCheckEnable(heartbeat types.Heartbeat) bool {
	return heartbeat.HealthEnable && strings.Contains(heartbeat.Handler, constants.M2SFunctionPrefix)
}

// GetHash get hash
func GetHash(path string) (hashValue string) {
	f, err := os.Open(path)
	if err != nil {
		log.GetLogger().Errorf("failed to open file %s", path)
		return ""
	}
	// close file when fail or succeed
	defer func() {
		err := f.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close file %s: %s", f, err.Error())
		}
	}()

	value := sha512.New()
	if _, err = io.Copy(value, f); err != nil {
		log.GetLogger().Errorf("calculate hash: %s", err.Error())
		return ""
	}
	return hex.EncodeToString(value.Sum(nil))
}

// GetUserAgency -
func GetUserAgency(key string) string {
	stringIf, ok := userAgency.Load(key)
	if ok {
		userAgencyValue, ok := stringIf.(string)
		if ok {
			return userAgencyValue
		}
	}
	return ""
}

// SetUserAgency -
func SetUserAgency(key string, value string) {
	userAgency.Store(key, value)
}

// NeedCustomContainerPreLoad -
func NeedCustomContainerPreLoad() bool {
	if commonUtils.IsCAEFunc(GetBusinessType()) {
		return false
	}
	if os.Getenv(constants.NATConfigures) != "" {
		return true
	}
	if os.Getenv(constants.EndpointVpcNATConfigure) != "" {
		return true
	}
	if os.Getenv(constants.FuncMountConfig) != "" {
		return true
	}
	return false
}

// GetBusinessType get business type
func GetBusinessType() string {
	return businessType
}

// IsDirectFunc -
func IsDirectFunc(language string, enable bool) bool {
	if strings.Contains(language, defaultDirectLanguage) {
		IsDirectRuntime = true
		return true
	}
	return enable
}

// GetEnableRemoteDebug -
func GetEnableRemoteDebug() bool {
	return enableRemoteDebug
}

// CleanTmpDisk -
func CleanTmpDisk() {
	if err := os.RemoveAll(userDataPath); err != nil {
		log.GetLogger().Warnf("failed to remove path: %s, %s", userDataPath, err.Error())
	}
}

// IsVpaFeatureEnabled return whether the switch is turned on
func IsVpaFeatureEnabled() bool {
	return enableVpa
}

// SetVpaResource -
func SetVpaResource(cpu, mem int64) {
	log.GetLogger().Infof("success receive and set resource, cpu: %d, mem: %d", cpu, mem)
	cpuLimit = cpu
	memoryLimit = mem
}

// EnableLogPath enable log path
func EnableLogPath() bool {
	return enableLogPath
}

// SetEnableLogPath set enable log path
func SetEnableLogPath(enable bool) {
	enableLogPath = enable
}

// DisableOOM -
func DisableOOM() {
	// linux system use -1000 to disable oom kill of a process
	setOOMScoreAdj("self", "-1000")
}

// ResetOOMScoreAdjToDefault -
func ResetOOMScoreAdjToDefault() {
	setOOMScoreAdj("self", "997")
}

func setOOMScoreAdj(pid, value string) {
	fileName := fmt.Sprintf("/proc/%s/oom_score_adj", pid)
	if err := ioutil.WriteFile(fileName, []byte(value), 0); err != nil {
		log.GetLogger().Warnf("failed to write file: %s, %s", fileName, err.Error())
	}
}

// IsCAERouterReady return whether current cae instance's router is ready
func IsCAERouterReady() bool {
	return atomic.LoadUint32(&caeRouterReady) != 0
}

// SetCAERouterReady set current cae instance's router ready
func SetCAERouterReady() {
	atomic.StoreUint32(&caeRouterReady, 1)
}

// GetExtendedInfo get extendedInfo
func GetExtendedInfo(funcMounts []types.FuncMount) string {
	for _, funcMount := range funcMounts {
		if funcMount.MountType == sfs30 && funcMount.ExtendedInfo != "" {
			return funcMount.ExtendedInfo
		}
	}
	return ""
}

// EnableTenantAuth enable tenant auth
func EnableTenantAuth() bool {
	return enableTenantAuth
}

// GetPodName -
func GetPodName() string {
	return podNames
}

// SetIsMcpFunction -
func SetIsMcpFunction(isMcpFunc bool) {
	isMcpFunction = isMcpFunc
	log.GetLogger().Infof("isMcpFunction is %t", isMcpFunction)
}

// GetIsMcpFunction -
func GetIsMcpFunction() bool {
	return isMcpFunction
}

// SetProxyURL -
func SetProxyURL(npmConfig, uvIndex string) {
	npmConfigRegistry = npmConfig
	uvIndexURL = uvIndex
}

// GetProxyURL -
func GetProxyURL() (string, string) {
	return npmConfigRegistry, uvIndexURL
}
