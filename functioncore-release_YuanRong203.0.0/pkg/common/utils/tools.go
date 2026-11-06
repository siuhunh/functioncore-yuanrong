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

// Package utils for common functions
package utils

import (
	"bufio"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
)

const (
	// OriginDefaultTimeout is 900
	OriginDefaultTimeout = 900
	// maxTimeout is 100 days
	maxTimeout        = 100 * 24 * 3600
	bytesToMb         = 1024 * 1024
	uint64ArrayLength = 8
	uint32Len         = 4
	// DirMode dir mode
	DirMode = 0700
	// FileMode file mode
	FileMode = 0600
	readSize = 32 * 1024
	// ObsMaxRetry obs max retry times 0
	ObsMaxRetry = 0
	// ObsDefaultTimeout 30 seconds
	ObsDefaultTimeout = 30
	// ObsDefaultConnectTimeout 10 seconds
	ObsDefaultConnectTimeout = 5
	// LayerListSep define the LayerList separation character
	LayerListSep        = "-#-"
	instanceIDLength    = 2
	dnsPairLength       = 2
	hostFilePath        = "/etc/hosts"
	resolveConfFilePath = "/etc/resolv.conf"

	labelDirect = "direct"

	ms = 1000

	roundBase = 10

	defaultFilePerm = 0644
)

var (
	caeJavaVersion = []string{"java8", "java17"}
)

const (
	minimumMemoryUnit      = 128
	minimumCPUUnit         = 100
	minimumReservedCPUUnit = 200
)

type hostFileInfo struct {
	Sha256  string
	Content []byte
	Mutex   sync.Mutex
}

// HostFile /etc/hosts file info
var HostFile hostFileInfo

type resolveFileInfo struct {
	Sha256  string
	Content []byte
	Mutex   sync.Mutex
}

// ResolveFile /etc/resolv.conf file info
var ResolveFile resolveFileInfo

var snapshotImagePathForWorker = constants.SnapshotImagePathForWorker

// CalculateCPUByMemory CPU and memory calculation methods presented by fg: cpu=memory/128*100+200
func CalculateCPUByMemory(memory int) int {
	return memory/minimumMemoryUnit*minimumCPUUnit + minimumReservedCPUUnit
}

var azEnv = parseAzEnv()

var enableHCS = os.Getenv("ENABLE_HCS") == "true"

func parseAzEnv() string {
	az := os.Getenv(constants.ZoneKey)
	if az == "" {
		az = constants.DefaultAZ
	}
	if len(az) > constants.ZoneNameLen {
		az = az[0 : constants.ZoneNameLen-1]
	}
	return az
}

// AzEnv set defaultaz env
func AzEnv() string {
	return azEnv
}

// GenerateInstanceID -
func GenerateInstanceID(podName string) string {
	return AzEnv() + "-#-" + podName
}

// GetPodNameByInstanceID -
func GetPodNameByInstanceID(instanceID string) string {
	elements := strings.Split(instanceID, LayerListSep)
	if len(elements) < instanceIDLength {
		return ""
	}
	return elements[1]
}

// Domain2IP convert domain to ip
func Domain2IP(endpoint string) (string, error) {
	var host, port string
	var err error
	host = endpoint
	if strings.Contains(endpoint, ":") {
		host, port, err = net.SplitHostPort(endpoint)
		if err != nil {
			return "", err
		}
	}
	if net.ParseIP(host) != nil {
		return endpoint, nil
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		return "", err
	}
	if port == "" {
		return ips[0], nil
	}
	return net.JoinHostPort(ips[0], port), nil
}

// DeepCopy will generate a new copy of original collection type
// currently this function is not recursive so elements will not be deep copied
func DeepCopy(origin interface{}) interface{} {
	oriTyp := reflect.TypeOf(origin)
	oriVal := reflect.ValueOf(origin)
	switch oriTyp.Kind() {
	case reflect.Slice:
		elemType := oriTyp.Elem()
		length := oriVal.Len()
		capacity := oriVal.Cap()
		newObj := reflect.MakeSlice(reflect.SliceOf(elemType), length, capacity)
		reflect.Copy(newObj, oriVal)
		return newObj.Interface()
	case reflect.Map:
		newObj := reflect.MakeMapWithSize(oriTyp, len(oriVal.MapKeys()))
		for _, key := range oriVal.MapKeys() {
			value := oriVal.MapIndex(key)
			newObj.SetMapIndex(key, value)
		}
		return newObj.Interface()
	default:
		return nil
	}
}

// ValidateTimeout check timeout
func ValidateTimeout(timeout *int64, defaultTimeout int64) {
	if *timeout <= 0 {
		*timeout = defaultTimeout
		return
	}
	if *timeout > maxTimeout {
		*timeout = maxTimeout
	}
}

// ClearStringMemory -
func ClearStringMemory(s string) {
	if len(s) <= 1 {
		return
	}
	bs := *(*[]byte)(unsafe.Pointer(&s))
	for i := 0; i < len(bs); i++ {
		bs[i] = 0
	}
}

// ClearByteMemory -
func ClearByteMemory(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// Float64ToByte -
func Float64ToByte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

// ByteToFloat64 -
func ByteToFloat64(bytes []byte) float64 {
	// bounds check to guarantee safety of function Uint64
	if len(bytes) != uint64ArrayLength {
		return 0
	}
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

// ExistPath whether path exists
func ExistPath(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

// GetFileSize -
func GetFileSize(path string) (int64, error) {
	realPath, err := filepath.Abs(path)
	if err != nil {
		return 0, err
	}
	file, err := os.Open(realPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// GetFileContent -
func GetFileContent(path string, start int64, end int64) ([]byte, error) {
	if end-start < 0 {
		return nil, errors.New("the end position is less than the start position")
	}
	realPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(realPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buffer := make([]byte, end-start)
	_, err = file.Seek(start, io.SeekStart)
	if err != nil {
		return nil, err
	}
	bytesRead, err := file.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer[:bytesRead], nil
}

// IsInputParameterValid check if input parameter is valid
func IsInputParameterValid(cmdName string) bool {
	if strings.Contains(cmdName, "&") ||
		strings.Contains(cmdName, "|") ||
		strings.Contains(cmdName, "$") ||
		strings.Contains(cmdName, "'") ||
		strings.Contains(cmdName, "`") ||
		strings.Contains(cmdName, "(") ||
		strings.Contains(cmdName, ")") ||
		strings.Contains(cmdName, "\"") {
		return false
	}
	return true
}

// UniqueID get unique ID
func UniqueID() string {
	return uuid.New().String()
}

// ShortUUID return short uuid encode by base64
func ShortUUID() string {
	id := uuid.New()
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(id)))
	base64.StdEncoding.Encode(buf, id[:])
	for i := range buf {
		if buf[i] == '=' || buf[i] == '+' || buf[i] == '/' {
			buf[i] = '-'
		}
	}
	return strings.ToLower(strings.Trim(string(buf), "-"))
}

// WriteFileToPath write file to path
func WriteFileToPath(writePath string, buffer []byte) error {
	return WriteFileToPathWithMode(writePath, buffer, DirMode, FileMode)
}

// WriteFileToPathWithMode write file to path with mode
func WriteFileToPathWithMode(writePath string, buffer []byte, dirMode fs.FileMode, fileMode fs.FileMode) error {
	baseDir := path.Dir(writePath)
	err := os.MkdirAll(baseDir, dirMode)
	if err != nil {
		return err
	}
	if err = ioutil.WriteFile(writePath, buffer, fileMode); err != nil {
		return err
	}
	return nil
}

// IsConnRefusedErr  -
func IsConnRefusedErr(err error) bool {
	netErr, ok := err.(net.Error)
	if !ok {
		return false
	}
	opErr, ok := netErr.(*net.OpError)
	if !ok {
		return false
	}
	syscallErr, ok := opErr.Err.(*os.SyscallError)
	if !ok {
		return false
	}
	if errno, ok := syscallErr.Err.(syscall.Errno); ok {
		if errno == syscall.ECONNREFUSED {
			return true
		}
	}
	return false
}

// ContainsConnRefusedErr -
func ContainsConnRefusedErr(err error) bool {
	const connRefusedStr = "connection refused"
	return strings.Contains(err.Error(), connRefusedStr)
}

// DefaultStringEnv return environment variable named by key and return val when not exist
func DefaultStringEnv(key string, val string) string {
	if env := os.Getenv(key); env != "" {
		return env
	}
	return val
}

// ReplaceByDNS update /etc/hosts
func ReplaceByDNS(filePath string, domainNames map[string]string) error {
	lines, err := ReadLines(filePath)
	if err != nil {
		return err
	}
	checkedDNSNames := make(map[string]bool, len(domainNames))
	var hasChange bool
	for i := range lines {
		arr := strings.Fields(lines[i])
		if len(arr) != dnsPairLength {
			continue
		}
		for name, ipAddress := range domainNames {
			if arr[0] == name || arr[1] == name {
				originLine := lines[i]
				lines[i] = ipAddress + " " + name
				checkedDNSNames[name] = true
				if lines[i] != originLine {
					hasChange = true
				}
				break
			}
		}
	}
	for name, ipAddress := range domainNames {
		// domain name is not in hosts file will append to hosts file
		if !checkedDNSNames[name] {
			lines = append(lines, ipAddress+" "+name)
			hasChange = true
		}
	}
	if !hasChange {
		return nil
	}
	HostFile.Mutex.Lock()
	defer HostFile.Mutex.Unlock()
	if err := WriteLines(filePath, lines); err != nil {
		return err
	}
	if err := HostFile.SaveHostFileInfo(); err != nil {
		return err
	}
	return nil
}

// SaveDNSConfigBeforeReset -
func SaveDNSConfigBeforeReset() ([]byte, error) {
	ResolveFile.Mutex.Lock()
	defer ResolveFile.Mutex.Unlock()
	if err := ResolveFile.SaveResolveFileInfo(); err != nil {
		return nil, err
	}

	return ResolveFile.Content, nil
}

// RecoverDNSConfig -
func RecoverDNSConfig(content []byte) error {
	ResolveFile.Mutex.Lock()
	defer ResolveFile.Mutex.Unlock()
	if err := os.WriteFile(resolveConfFilePath, content, defaultFilePerm); err != nil {
		return err
	}
	return nil
}

// ResetDNSConfig
func ResetDNSConfig(dnsConfig types.DNSConfig) error {
	if len(dnsConfig.DNS) == 0 && len(dnsConfig.DNSSearch) == 0 && len(dnsConfig.DNSOptions) == 0 {
		return nil
	}

	var newLines []string

	newLines = append(newLines, extractDNSConfig(dnsConfig.DNS, "nameserver")...)
	newLines = append(newLines, extractDNSConfig(dnsConfig.DNSSearch, "search")...)
	newLines = append(newLines, extractDNSConfig(dnsConfig.DNSOptions, "options")...)

	ResolveFile.Mutex.Lock()
	defer ResolveFile.Mutex.Unlock()
	if err := WriteLines(resolveConfFilePath, newLines); err != nil {
		return err
	}

	if err := ResolveFile.SaveResolveFileInfo(); err != nil {
		return err
	}

	return nil
}

func extractDNSConfig(newConfigItems []string, configType string) []string {
	if len(newConfigItems) == 0 {
		return []string{}
	}

	var newLines []string

	if configType == "nameserver" {
		for _, configItem := range newConfigItems {
			newLines = append(newLines, "nameserver"+" "+configItem)
		}
	} else {
		var newConfigLine string
		newConfigLine = configType
		for _, configItem := range newConfigItems {
			newConfigLine += " " + configItem
		}
		newLines = append(newLines, newConfigLine)
	}

	return newLines
}

func (hostFileInfo) SaveHostFileInfo() error {
	_, sha, err := GetFileHashInfo(hostFilePath)
	if err != nil {
		return err
	}
	HostFile.Sha256 = sha
	content, err := ioutil.ReadFile(hostFilePath)
	if err != nil {
		return err
	}
	HostFile.Content = content
	return nil
}

func (resolveFileInfo) SaveResolveFileInfo() error {
	_, sha, err := GetFileHashInfo(resolveConfFilePath)
	if err != nil {
		return err
	}
	ResolveFile.Sha256 = sha
	content, err := ioutil.ReadFile(resolveConfFilePath)
	if err != nil {
		return err
	}
	ResolveFile.Content = content
	return nil
}

// ReadLines read the lines of the given file.
func ReadLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// WriteLines  writes the lines to the given file.
func WriteLines(path string, lines []string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

// GenStateIDByKey returns stateID by serviceID, functionName and key
func GenStateIDByKey(tenantID, serviceID, funcName, key string) string {
	// if stateKey is empty, stateID is generated by default.
	if len(key) == 0 {
		return uuid.New().String()
	}
	preAllocationSlice := make([]byte, 0, len(tenantID)+len(serviceID)+len(funcName)+len(key))
	preAllocationSlice = append(preAllocationSlice, tenantID...)
	preAllocationSlice = append(preAllocationSlice, serviceID...)
	preAllocationSlice = append(preAllocationSlice, funcName...)
	preAllocationSlice = append(preAllocationSlice, key...)
	stateID := uuid.NewSHA1(uuid.NameSpaceURL, preAllocationSlice)
	return stateID.String()
}

// GetFileHashInfo get file hash info
func GetFileHashInfo(path string) (int64, string, error) {
	var fileSize int64
	realPath, err := filepath.Abs(path)
	if err != nil {
		return 0, "", err
	}
	file, err := os.Open(realPath)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0, "", err
	}
	fileSize = stat.Size()
	fileHash := sha256.New()
	if _, err := io.Copy(fileHash, file); err != nil {
		return 0, "", err
	}
	hashValue := hex.EncodeToString(fileHash.Sum(nil))
	return fileSize, hashValue, nil
}

// IsNetworkError judge whether it is a network error
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(net.Error)
	if !ok {
		return false
	}
	return true
}

// IsUserError -
func IsUserError(err error) bool {
	newErr, ok := err.(snerror.SNError)
	if !ok {
		return false
	}
	return snerror.IsUserError(newErr)
}

// FnvHashInt a hash function
func FnvHashInt(s string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}

	// for 2 <= base <= 36. The result uses the lower-case letters 'a' to 'z'
	return int(h.Sum32())
}

// FileMD5 calculate the md5 of file
func FileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return "", err
	}
	hash := md5.New()
	_, _ = io.Copy(hash, file)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ShuffleOneArray -
func ShuffleOneArray(arr []string) []string {
	arrLength := len(arr)
	if arrLength <= 1 {
		return arr
	}
	copyArr := make([]string, arrLength)
	copy(copyArr, arr)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(arrLength, func(i, j int) { copyArr[i], copyArr[j] = copyArr[j], copyArr[i] })
	return copyArr
}

// IsCAEFunc judge whether it is a CAE function
func IsCAEFunc(businessType string) bool {
	return businessType == constants.BusinessTypeCAE
}

// IsWebSocketFunc return true if the business type is websocket or cae with enable remote debug
func IsWebSocketFunc(businessType string, enableRemoteDebug, enableWebsocket bool) bool {
	return businessType == constants.BusinessTypeWebSocket ||
		(businessType == constants.BusinessTypeCAE && enableRemoteDebug) || enableWebsocket
}

// IsWebSocketCae return true if the business type is cae and enabled websocket
func IsWebSocketCae(businessType string, enableWebsocket bool) bool {
	return businessType == constants.BusinessTypeCAE && enableWebsocket
}

var directFunctions = map[string]struct{}{
	"javax": {},
}

// IsDirectFunc check whether it if a direct function (runtime connect to bus directly)
func IsDirectFunc(pod *corev1.Pod, language string) bool {
	_, ok := directFunctions[language]
	return ok || (pod != nil && pod.Labels != nil && pod.Labels[labelDirect] == "true")
}

// Ordered types that are ordered
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 |
		~string
}

// Min -
func Min[T Ordered](x, y T) T {
	if x < y {
		return x
	}
	return y
}

// Max -
func Max[T Ordered](x, y T) T {
	if x > y {
		return x
	}
	return y
}

// CheckJavaVersion check if java function
func CheckJavaVersion(version string) bool {
	switch version {
	case "java8", "java11", "java17":
		return true
	default:
		return false
	}
}

// InitTraceID init traceID
func InitTraceID(r *http.Request) string {
	traceID := r.Header.Get(constants.HeaderRequestID)
	if traceID == "" {
		traceID = r.Header.Get(constants.HeaderTraceID)
	}
	switch {
	case len(traceID) > constants.MaxTraceIDLength:
		traceID = traceID[:constants.MaxTraceIDLength]
	case traceID == "":
		traceID = uuid.New().String()
	default:
	}
	r.Header.Set(constants.HeaderTraceID, traceID)
	return traceID
}

// SetSnapshotImagePathForWorker -
func SetSnapshotImagePathForWorker(snapshotImagePath string) {
	snapshotImagePathForWorker = snapshotImagePath
}

// GetSnapshotImagePathForWorker -
func GetSnapshotImagePathForWorker() string {
	return snapshotImagePathForWorker
}

// ConvertSecondToMillisecond convert time value from second to millisecond
func ConvertSecondToMillisecond(second float64) float64 {
	return second * ms
}

// ExtractErrMsg extract error message
func ExtractErrMsg(errMsg string) string {
	failureFee := snerror.FailureFee{}
	if err := json.Unmarshal([]byte(errMsg), &failureFee); err != nil {
		return errMsg
	}
	return failureFee.Message
}

// CheckIAMTokenFunc check if the function language is iam token available
func CheckIAMTokenFunc(language string, businessType string) bool {
	return language == "java8" || language == "custom-runtime-rpc" || IsCAEFunc(businessType)
}

func GetCAEJavaVersion() []string {
	return caeJavaVersion
}

// IsInSlice -
func IsInSlice(s string, slice []string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func GetPreloadFunctionVersionName(serviceName, functionName, version string) string {
	return serviceName + "-" + functionName + "-" + version
}

func GetPreloadLayerVersionName(layerName, version string) string {
	return layerName + "-" + version
}

func Round(num float64, precision int) float64 {
	factor := math.Pow(roundBase, float64(precision))
	return math.Round(num*factor) / factor
}

// ContainsKey -
func ContainsKey(slice []int, element int) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}
	return false
}

// GetEnableHCS get enable hcs
func GetEnableHCS() bool {
	return enableHCS
}
