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

package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
	mockUtils "functioncore/test/utils"
)

// TestDomain2IP convert domain to ip
func TestDomain2IP(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(net.LookupHost, func(_ string) ([]string, error) {
			return []string{"1.1.1.1"}, nil
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()

	type args struct {
		endpoint string
	}
	tests := []struct {
		args    args
		want    string
		wantErr bool
	}{
		{
			args{endpoint: "1.1.1.1:9000"},
			"1.1.1.1:9000",
			false,
		},
		{
			args{endpoint: "1.1.1.1"},
			"1.1.1.1",
			false,
		},
		{
			args{endpoint: "test:9000"},
			"1.1.1.1:9000",
			false,
		},
		{
			args{endpoint: "test"},
			"1.1.1.1",
			false,
		},
	}
	for _, tt := range tests {
		got, err := Domain2IP(tt.args.endpoint)
		if (err != nil) != tt.wantErr {
			t.Errorf("Domain2IP() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		if got != tt.want {
			t.Errorf("Domain2IP() got = %v, want %v", got, tt.want)
		}
	}
}

func TestGenStateIDByKey(t *testing.T) {
	convey.Convey("Test gen stateID by UUID", t, func() {
		stateID := GenStateIDByKey("tenantID", "serviceID", "funcName", "")
		convey.So(stateID, convey.ShouldNotBeNil)
	})
	convey.Convey("Test gen stateID by params", t, func() {
		stateID := GenStateIDByKey("tenantID", "serviceID", "funcName", "key")
		convey.So(stateID, convey.ShouldEqual, "993e96b4-0550-523f-a412-a4b58682cb2e")
	})
}

func TestGetFileHashInfo(t *testing.T) {
	convey.Convey("Test get file hashInfo failed", t, func() {
		_, _, err := GetFileHashInfo("/xyz")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestFloat64ToByte(t *testing.T) {
	value := 123.45
	bytesValue := Float64ToByte(value)
	if ByteToFloat64(bytesValue) != value {
		t.Errorf("Float64ToByte and ByteToFloat64 failed")
	}

	// test empty bytes
	if ByteToFloat64([]byte{}) != 0 {
		t.Errorf("empty ByteToFloat64 failed")
	}
}

func TestExistPath(t *testing.T) {
	path := os.Args[0]
	if !ExistPath(path) {
		t.Errorf("test path exist true failed, path: %s", path)
	}
	if ExistPath(path + "abc") {
		t.Errorf("test path exist false failed, path: %s", path+"abc")
	}
}

func TestUniqueID(t *testing.T) {
	uuid1 := UniqueID()
	uuid2 := UniqueID()
	assert.NotEqual(t, uuid1, uuid2)
}

func Test_parseAzEnv(t *testing.T) {
	assert.Equal(t, constants.DefaultAZ, AzEnv())
	tests := []struct {
		name      string
		zoneValue string
		want      string
	}{
		{
			name:      "empty zoneValue",
			zoneValue: "",
			want:      constants.DefaultAZ,
		},
		{
			name: fmt.Sprintf("ZoneName > %d", constants.ZoneNameLen),
			zoneValue: "12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"123456",
			want: "12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"12345678901234567890123456789012345678901234567890" +
				"1234",
		},
		{
			name:      "Normal",
			zoneValue: "1234567890",
			want:      "1234567890",
		},
	}
	for _, tt := range tests {
		if err := os.Setenv(constants.ZoneKey, tt.zoneValue); err != nil {
			t.Errorf("failed to set Zone env, %s", err)
		}
		actual := parseAzEnv()
		assert.Equal(t, tt.want, actual)
	}
}

func TestIsConnRefusedErr(t *testing.T) {
	err := errors.New("abc")
	assert.False(t, IsConnRefusedErr(err))
	err = syscall.EADDRINUSE
	assert.False(t, IsConnRefusedErr(err))
	_, err = net.Dial("tcp", "127.0.0.1:33334")
	assert.True(t, IsConnRefusedErr(err))
}

func TestContainsConnRefusedErr(t *testing.T) {
	err := errors.New("dial tcp 10.249.0.54:22668: connect: connection refused")
	assert.True(t, ContainsConnRefusedErr(err))
}

// TestWriteFileToPath is used to test the function of writing a file to a specified path.
func TestWriteFileToPath(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Equal(t, err, nil)
	addFile, err := ioutil.TempFile(dir, "test")
	err = WriteFileToPath(addFile.Name(), []byte("test"))
	assert.Equal(t, err, nil)
}

// TestIsHexString is used to test whether the character string meets the requirements.
func TestIsHexString(t *testing.T) {
	flag := IsHexString("2345")
	assert.True(t, flag)
	flag = IsHexString("test")
	assert.False(t, flag)
}

// TestValidateTimeout: indicates whether the timeout interval exceeds the maximum value or is the default value.
func TestValidateTimeout(t *testing.T) {
	var timeout int64 = -1
	var defaultTimeout int64 = 1
	ValidateTimeout(&timeout, defaultTimeout)
	assert.Equal(t, timeout, int64(1))
	timeout = 100*24*3600 + 1
	ValidateTimeout(&timeout, defaultTimeout)
	assert.Equal(t, timeout, int64(100*24*3600))
}

// TestDeepCopy is used to test the deep copy of maps and slices.
func TestDeepCopy(t *testing.T) {
	str := []string{"test1", "test2"}
	cpyStr := DeepCopy(str)
	curStr, ok := cpyStr.([]string)
	assert.True(t, ok)
	assert.Equal(t, len(curStr), 2)
	assert.Equal(t, curStr[0], "test1")
	assert.Equal(t, curStr[1], "test2")

	tmpMap := make(map[string]string)
	tmpMap["test1"] = "test1"
	tmpMap["test2"] = "test2"
	cpyMap := DeepCopy(tmpMap)
	curMap, ok := cpyMap.(map[string]string)
	assert.True(t, ok)
	assert.Equal(t, len(curMap), 2)
	assert.Equal(t, curMap["test1"], "test1")
	assert.Equal(t, curMap["test2"], "test2")

	integer := 0
	cpyinteger := DeepCopy(integer)
	assert.Equal(t, nil, cpyinteger)
}

func TestIsInputParameterValid(t *testing.T) {
	res1 := IsInputParameterValid("|")
	assert.Equal(t, res1, false)
	res2 := IsInputParameterValid("ddd")
	assert.Equal(t, res2, true)
	res3 := IsInputParameterValid("ab(d)e")
	assert.Equal(t, res3, false)
	res4 := IsInputParameterValid("abde`")
	assert.Equal(t, res4, false)
	res5 := IsInputParameterValid("&abde")
	assert.Equal(t, res5, false)
	res6 := IsInputParameterValid("-DMQS_SERVER=7.189.152.212:9776;7.198.155.231:9776")
	assert.Equal(t, res6, true)
}

func TestDefaultString(t *testing.T) {
	convey.Convey("TestDefaultString", t, func() {
		convey.So(DefaultStringEnv("abc", "def"), convey.ShouldEqual, "def")
	})
}

func Test_replaceByDNS(t *testing.T) {
	convey.Convey("Test_replaceByDNSError", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ReadLines, func(path string) ([]string, error) {
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := ReplaceByDNS("", nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test_replaceByDNS", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ReadLines, func(path string) ([]string, error) {
				return []string{"192.168.1.1 www.example.com"}, nil
			}),
			gomonkey.ApplyFunc(WriteLines, func(path string, lines []string) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := ReplaceByDNS("", map[string]string{"www.example.com": "192.168.1.2"})
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test_replaceByDNSFileReWrite", t, func() {
		testLine := []string{"192.168.1.1 www.example.com"}
		err1 := WriteLines("/tmp/dnsTestFile", testLine)
		convey.So(err1, convey.ShouldBeNil)

		// 能够将第一次的文件内容覆盖
		testLine = []string{"192.168.1.1 www.example.com",
			"192.168.1.2 www.example2.com",
			"192.168.1.3 www.example3.com",
			"192.168.1.4 www.example4.com",
			"192.168.1.5 www.example5.com",
			"192.168.1.6 www.example6.com",
			"192.168.1.7 www.example7.com",
			"192.168.1.8 www.example8.com",
			"192.168.1.9 www.example9.com",
			"192.168.1.10 www.example10.com",
			"192.168.1.11 www.example11.com",
			"192.168.1.12 www.example12.com",
			"192.168.1.13 www.example13.com test-array-len-3-exception"}
		err1 = WriteLines("/tmp/dnsTestFile", testLine)
		lineContext, err1 := ReadLines("/tmp/dnsTestFile")
		convey.So(err1, convey.ShouldBeNil)
		convey.So(len(lineContext), convey.ShouldEqual, 13)
		var rst = 1
		for i := range lineContext {
			if strings.Contains(lineContext[i], "www.example.com") {
				if strings.Contains(lineContext[i], "192.168.1.1") {
					rst = 0
				}
			}
		}
		convey.So(rst, convey.ShouldEqual, 0)

		// 修改其中的一个条，其他内容不变
		err := ReplaceByDNS("/tmp/dnsTestFile", map[string]string{"www.example.com": "192.168.1.4"})
		err = ReplaceByDNS("/tmp/dnsTestFile", map[string]string{"www.example.com": "192.168.1.4"})
		convey.So(err, convey.ShouldBeNil)
		lineContext, err1 = ReadLines("/tmp/dnsTestFile")
		fmt.Println(lineContext)
		convey.So(err1, convey.ShouldBeNil)
		convey.So(len(lineContext), convey.ShouldEqual, 13)
		lineContext, err = ReadLines("/tmp/dnsTestFile")
		rst = 1
		for i := range lineContext {
			if strings.Contains(lineContext[i], "www.example.com") {
				if strings.Contains(lineContext[i], "192.168.1.4") {
					rst = 0
				}
			}
		}
		convey.So(rst, convey.ShouldEqual, 0)
		rst = 1
		for i := range lineContext {
			if strings.Contains(lineContext[i], "www.example2.com") {
				if strings.Contains(lineContext[i], "192.168.1.2") {
					rst = 0
				}
			}
		}
		convey.So(rst, convey.ShouldEqual, 0)

		// 新增一条,修改一条，一条不变，能够保持成功
		testLine = []string{"192.168.1.1 www.example.com",
			"192.168.1.2 www.example2.com",
			"192.168.1.14 www.example14.com"}
		err = ReplaceByDNS("/tmp/dnsTestFile", map[string]string{"www.example.com": "192.168.1.1",
			"www.example14.com": "192.168.1.14",
			"www.example2.com":  "192.168.1.2"})
		convey.So(err, convey.ShouldBeNil)
		lineContext, err1 = ReadLines("/tmp/dnsTestFile")
		convey.So(err1, convey.ShouldBeNil)
		convey.So(len(lineContext), convey.ShouldEqual, 14)
		lineContext, err = ReadLines("/tmp/dnsTestFile")
		rst = 1
		for i := range lineContext {
			if strings.Contains(lineContext[i], "www.example.com") {
				if strings.Contains(lineContext[i], "192.168.1.1") {
					rst = 0
				}
			}
		}
		convey.So(rst, convey.ShouldEqual, 0)
		rst = 1
		for i := range lineContext {
			if strings.Contains(lineContext[i], "www.example2.com") {
				if strings.Contains(lineContext[i], "192.168.1.2") {
					rst = 0
				}
			}
		}
		convey.So(rst, convey.ShouldEqual, 0)
		rst = 1
		for i := range lineContext {
			if strings.Contains(lineContext[i], "www.example14.com") {
				if strings.Contains(lineContext[i], "192.168.1.14") {
					rst = 0
				}
			}
		}
		convey.So(rst, convey.ShouldEqual, 0)
	})
}

func TestShortUUID(t *testing.T) {
	assert.Greater(t, 30, len(ShortUUID()))
}

func TestResetDNSConfig(t *testing.T) {
	convey.Convey("Test_resetDNSConfig", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(WriteLines, func(path string, lines []string) error {
				return nil
			}),
			gomonkey.ApplyFunc(GetFileHashInfo, func(path string) (int64, string, error) {
				return 1, "test diff", nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
				return nil, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		DNSOptions := []string{"timeout:2", "attempts:3"}
		DNSSearch := []string{"example.com", "huawei.example.com"}
		DNSConfig := types.DNSConfig{DNS: DNS, DNSOptions: DNSOptions, DNSSearch: DNSSearch}

		err := ResetDNSConfig(DNSConfig)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestCombineOriginAndNewLine(t *testing.T) {
	convey.Convey("Test_newConfigItemsZero", t, func() {
		var newConfigItems []string
		extractDNSConfig(newConfigItems, "")
	})
	convey.Convey("Test_nameserverCombine", t, func() {
		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		var newLines []string
		newLines = extractDNSConfig(DNS, "nameserver")
		convey.So(newLines, convey.ShouldContain, "nameserver 8.8.8.8")
		convey.So(newLines, convey.ShouldContain, "nameserver 8.8.8.1")
		convey.So(newLines, convey.ShouldContain, "nameserver 8.8.8.2")
	})
	convey.Convey("Test_optionsCombine", t, func() {
		DNSOptions := []string{"timeout:2", "attempts:3"}
		var newLines []string
		newLines = extractDNSConfig(DNSOptions, "options")
		convey.So(newLines, convey.ShouldContain, "options timeout:2 attempts:3")
	})
	convey.Convey("Test_searchCombine", t, func() {
		DNSSearch := []string{"example.com", "huawei.example.com"}
		var newLines []string
		newLines = extractDNSConfig(DNSSearch, "search")
		convey.So(newLines, convey.ShouldContain, "search example.com huawei.example.com")
	})
}

func TestIsNetworkError(t *testing.T) {
	assert.Equal(t, false, IsNetworkError(nil))
	assert.Equal(t, true, IsNetworkError(syscall.EHOSTUNREACH))
	assert.Equal(t, true, IsNetworkError(os.ErrDeadlineExceeded))
	assert.Equal(t, false, IsNetworkError(errors.New("test error")))
}

func Test_IsUserError(t *testing.T) {
	flag := IsUserError(errors.New("test"))
	assert.Equal(t, false, flag)

	snErr := snerror.New(100, "test")
	flag = IsUserError(snErr)
	assert.Equal(t, true, flag)

	snErr = snerror.New(10500, "test")
	flag = IsUserError(snErr)
	assert.Equal(t, false, flag)
}

func TestCalculateCPUByMemory(t *testing.T) {
	cpuInfo := CalculateCPUByMemory(10)
	assert.Equal(t, cpuInfo, 200)
}

func TestGenerateInstanceID(t *testing.T) {
	instanceID := GenerateInstanceID("podName")
	assert.Equal(t, instanceID, "defaultaz-#-podName")
}

func TestGetPodNameByInstanceID(t *testing.T) {
	podName := GetPodNameByInstanceID("defaultaz-#-podName")
	assert.Equal(t, podName, "podName")
}

func TestShuffleOneArray(t *testing.T) {
	arr1 := []string{"1"}
	arr2 := ShuffleOneArray(arr1)
	assert.Equal(t, arr1, arr2)

	arr3 := []string{"1", "2"}
	arr4 := ShuffleOneArray(arr1)
	assert.NotEqual(t, arr3, arr4)

	arr5 := make([]string, 0)
	arr6 := ShuffleOneArray(arr5)
	assert.Equal(t, len(arr6), 0)
}

func TestIsCAEFunc(t *testing.T) {
	assert.Equal(t, true, IsCAEFunc(constants.BusinessTypeCAE))
	assert.Equal(t, false, IsCAEFunc(constants.WorkerManagerApplier))
}

func TestIsDirectFunc(t *testing.T) {
	directPod := &corev1.Pod{}
	directPod.Labels = make(map[string]string)
	directPod.Labels[labelDirect] = "true"

	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name:     "python3.6",
			expected: false,
		},
		{
			name:     "java8",
			expected: false,
		},
		{
			name:     "javax",
			expected: true,
		},
		{
			name:     "custom-runtime-rpc",
			pod:      directPod,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsDirectFunc(tt.pod, tt.name), tt.name)
		})
	}
}

func TestMinMax(t *testing.T) {
	assert.Equal(t, int(5), Min(int(5), int(10)))
	assert.Equal(t, int(10), Max(int(5), int(10)))

	assert.Equal(t, float32(5), Min(float32(5), float32(10)))
	assert.Equal(t, float32(10), Max(float32(5), float32(10)))

	assert.Equal(t, uint32(5), Min(uint32(5), uint32(10)))
	assert.Equal(t, uint32(10), Max(uint32(5), uint32(10)))
}

func Test_initTraceID(t *testing.T) {
	uuidLen := len(uuid.New().String())

	tests := []struct {
		name         string
		inputTraceID string

		exceptedLen int
	}{
		{
			name: "empty traceID",

			exceptedLen: uuidLen,
		},
		{
			name:         "traceID length 20",
			inputTraceID: string(make([]byte, 20, 20)),
			exceptedLen:  20,
		},
		{
			name:         "reach max length",
			inputTraceID: string(make([]byte, constants.MaxTraceIDLength, constants.MaxTraceIDLength)),
			exceptedLen:  constants.MaxTraceIDLength,
		},
		{
			name:         "large than max length",
			inputTraceID: string(make([]byte, constants.MaxTraceIDLength+1, constants.MaxTraceIDLength+1)),
			exceptedLen:  constants.MaxTraceIDLength,
		},
	}

	for _, test := range tests {
		req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:8080", nil)
		req.Header.Set(constants.HeaderTraceID, test.inputTraceID)
		assert.Equal(t, test.exceptedLen, len(InitTraceID(req)), test.name)
	}
}

func TestSetSnapshotImagePathForWorker(t *testing.T) {
	snapshotImagePath := "test"
	SetSnapshotImagePathForWorker(snapshotImagePath)
	result := GetSnapshotImagePathForWorker()
	assert.Equal(t, snapshotImagePath, result)
}

func TestConvertSecondToMillisecond(t *testing.T) {
	valueMs := ConvertSecondToMillisecond(0.123456)
	assert.Equal(t, 123.456, valueMs)
}

func TestExtractErrMsg(t *testing.T) {
	convey.Convey("test ExtractErrMsg", t, func() {
		convey.Convey("case1: success", func() {
			msg := "error message"
			failureFee := snerror.FailureFee{
				Log:     "log",
				Summary: snerror.SummaryInfo{},
				Message: msg,
			}
			bytes, _ := json.Marshal(failureFee)
			res := ExtractErrMsg(string(bytes))
			convey.So(res, convey.ShouldEqual, msg)
		})
		convey.Convey("case2: unmarshal error", func() {
			msg := "aaa"
			res := ExtractErrMsg(msg)
			convey.So(res, convey.ShouldEqual, msg)
		})
	})
}

func Test_IsInSlice(t *testing.T) {
	convey.Convey("IsInSlice", t, func() {
		convey.Convey("test key in slice", func() {
			isExist := IsInSlice("key1", []string{"key1", "xx", "test"})
			convey.So(isExist, convey.ShouldBeTrue)
		})
		convey.Convey("test key not in slice", func() {
			isExist := IsInSlice("key1", []string{"key2", "xx", "test"})
			convey.So(isExist, convey.ShouldBeFalse)
		})
	})
}

func TestGetPreloadFunctionVersionName(t *testing.T) {
	versionName := GetPreloadFunctionVersionName("test", "func", "latest")
	assert.Equal(t, "test-func-latest", versionName)
}

func TestGetPreloadLayerVersionName(t *testing.T) {
	versionName := GetPreloadLayerVersionName("layer", "1")
	assert.Equal(t, "layer-1", versionName)
}

func TestRound(t *testing.T) {
	testNumber := 3.14156734

	keepOne := Round(testNumber, 1)
	keepTwo := Round(testNumber, 2)
	keepThree := Round(testNumber, 3)

	assert.Equal(t, 3.14, keepTwo)
	assert.Equal(t, 3.1, keepOne)
	assert.Equal(t, 3.142, keepThree)
}

func TestSaveDNSConfigBeforeReset(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     error
	}{
		{
			name: "Test SaveDNSConfigBeforeReset failed",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(GetFileHashInfo, func(path string) (int64, string, error) {
						return 0, "", errors.New("get hash failed")
					}),
				})
				return patches
			},
			wantErr: errors.New("get hash failed"),
		},
		{
			name: "Test SaveDNSConfigBeforeReset success",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(GetFileHashInfo, func(path string) (int64, string, error) {
						return 0, "", nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(path string) (content []byte, err error) {
						return []byte("nameserver 127.0.0.1"), nil
					}),
				})
				return patches
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ResolveFile = resolveFileInfo{
				Sha256:  "",
				Content: []byte("nameserver 127.0.0.1"),
				Mutex:   sync.Mutex{},
			}

			_, err := SaveDNSConfigBeforeReset()
			assert.Equal(t, tt.wantErr, err)
			patches.ResetAll()
		})
	}
}

func TestRecoverDNSConfig(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     error
	}{
		{
			name: "Test RecoverDNSConfig failed",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.WriteFile, func(name string, data []byte, perm os.FileMode) error {
						return errors.New("write file failed")
					}),
				})
				return patches
			},
			wantErr: errors.New("write file failed"),
		},
		{
			name: "Test RecoverDNSConfig success",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.WriteFile, func(name string, data []byte, perm os.FileMode) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ResolveFile = resolveFileInfo{
				Sha256:  "",
				Content: []byte("nameserver 127.0.0.1"),
				Mutex:   sync.Mutex{},
			}

			err := RecoverDNSConfig(ResolveFile.Content)
			assert.Equal(t, tt.wantErr, err)
			patches.ResetAll()
		})
	}
}

func TestGetFileSize(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "testfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	writeData := "test data"
	_, err = tmpfile.Write([]byte(writeData))
	if err != nil {
		t.Fatal(err)
	}

	err = tmpfile.Close()
	if err != nil {
		t.Fatal(err)
	}

	size, err := GetFileSize(tmpfile.Name())
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if size != int64(len(writeData)) {
		t.Errorf("Expected size %d, got %v", len(writeData), size)
	}

	_, err = GetFileSize("nonexistentfile")
	if err == nil {
		t.Errorf("Expected error, got none")
	}
}

func TestGetFileContent(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "testfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name()) // Ensure the temporary file is deleted after testing

	// Write test data to the temporary file
	_, err = tmpFile.Write([]byte("Hello, world!"))
	if err != nil {
		t.Fatal(err)
	}
	err = tmpFile.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Test the normal case where we read a portion of the file
	data, err := GetFileContent(tmpFile.Name(), 0, 5)
	if err != nil {
		t.Error(err)
	}
	if string(data) != "Hello" {
		t.Errorf("Expected 'Hello', got '%s'", data)
	}

	// Test the case where the file does not exist
	_, err = GetFileContent("nonexistentfile", 0, 5)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	// Test the case where the start position is greater than the file length
	data, err = GetFileContent(tmpFile.Name(), 20, 25)
	if err == nil {
		t.Error("Expected error EOF, got nil")
	}

	// Test the case where the end position is less than the start position
	data, err = GetFileContent(tmpFile.Name(), 5, 0)
	if err == nil {
		t.Error("Expected error, got nil")
	}
	if len(data) != 0 {
		t.Errorf("Expected empty data, got '%s'", data)
	}
}
