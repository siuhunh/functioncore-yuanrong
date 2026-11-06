/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package tokenrotation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"functioncore/pkg/common/alarm"
	testutils "functioncore/test/utils"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/localauth"
)

func setReadRotatedIDTokenSuccPatches() *gomonkey.Patches {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	patches.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
		return []byte(""), nil
	})
	return patches
}

func setNewRequestErrPatches() *gomonkey.Patches {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
		return nil, fmt.Errorf("error")
	})
	return patches
}

func setHTTPDoErrPatches() *gomonkey.Patches {
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(getHTTPClient()), "Do", func(client *http.Client, req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("error")
	})
	return patches
}

func setHTTPRequestStatusErrPatcher() *gomonkey.Patches {
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(getHTTPClient()), "Do", func(client *http.Client, req *http.Request) (*http.Response, error) {
		res := &http.Response{
			Body:       ioutil.NopCloser(bytes.NewReader([]byte(""))),
			StatusCode: http.StatusBadRequest,
		}
		return res, nil
	})
	return patches
}

func setRequestSuccessPatches() *gomonkey.Patches {
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(getHTTPClient()), "Do", func(client *http.Client, req *http.Request) (*http.Response, error) {
		cre := CredentialInfo{
			Token: TempToken{
				Access:        "1",
				Secret:        "2",
				SecurityToken: "3",
			},
		}
		content, _ := json.Marshal(cre)
		res := &http.Response{
			Body:       ioutil.NopCloser(bytes.NewReader(content)),
			StatusCode: http.StatusCreated,
		}
		return res, nil
	})
	return patches
}

func TestStartTokenRotation(t *testing.T) {
	{
		stopCh := make(chan struct{})
		err := StartTokenRotation(stopCh)
		assert.Equal(t, err, nil)
		close(stopCh)
	}
	{
		os.Setenv("TOKEN_ROTATION_CONFIG", "invalid")
		regionID = "1"
		stopCh := make(chan struct{})
		err := StartTokenRotation(stopCh)
		assert.NotNil(t, err)
		close(stopCh)
	}
	{
		regionID = "1"
		os.Setenv("TOKEN_ROTATION_CONFIG", `{"projectID": "1", "identityProvider": "1", "iamHostName": "1",
"tokenCronSpec": "1"}`)
		stopCh := make(chan struct{})
		config.CronSpec = "0 0 9,21 * * ?"
		err := StartTokenRotation(stopCh)
		assert.NotNil(t, err)
		close(stopCh)
	}
	{
		// return at once
		err := instance.cronGetTokenFromIAM(nil)
		assert.NotNil(t, err)

		stopCh := make(chan struct{})
		config.CronSpec = "abc"
		err = instance.cronGetTokenFromIAM(stopCh)
		assert.NotNil(t, err)

		// manually check cron stopped
		config.CronSpec = "0 0 9,21 * * ?"
		err = instance.cronGetTokenFromIAM(stopCh)
		assert.Nil(t, err)
		close(stopCh)
	}
	{
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			return
		})
		// cron function without ret code
		instance.getTokenFromIAMWithRetry()
	}
}

func TestGetTempToken(t *testing.T) {
	{
		ak, sk, token := GetTempToken()
		assert.Equal(t, ak, "")
		assert.Equal(t, sk, "")
		assert.Equal(t, token, "")
	}
	{
		err := instance.storeTempToken(TempToken{})
		assert.NotNil(t, err)
	}
	{
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyFunc(localauth.Encrypt, func(_ string) (string, error) {
			return "a", nil
		})
		err := instance.storeTempToken(TempToken{})
		assert.Nil(t, err)
		ak, sk, token := GetTempToken()
		assert.Equal(t, ak, "")
		assert.Equal(t, sk, "a")
		assert.Equal(t, token, "a")
		instance.securityToken = ""
		instance.secret = ""
	}
}

func Test_getIAMToken(t *testing.T) {
	{
		patches := setNewRequestErrPatches()
		_, err := getIAMToken([]byte(""))
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setHTTPDoErrPatches()
		_, err := getIAMToken([]byte(""))
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setHTTPRequestStatusErrPatcher()
		_, err := getIAMToken([]byte(""))
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setRequestSuccessPatches()
		_, err := getIAMToken([]byte(""))
		assert.Nil(t, err)
		patches.Reset()
	}
}

func Test_getTempTokenByIAMToken(t *testing.T) {
	{
		patches := setNewRequestErrPatches()
		_, err := getTempTokenByIAMToken("")
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setHTTPDoErrPatches()
		_, err := getTempTokenByIAMToken("")
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setHTTPRequestStatusErrPatcher()
		_, err := getTempTokenByIAMToken("")
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setRequestSuccessPatches()
		_, err := getTempTokenByIAMToken("")
		assert.Nil(t, err)
		patches.Reset()
	}
}

func Test_getIAMTokenByAgency(t *testing.T) {
	{
		patches := setNewRequestErrPatches()
		_, err := getRealIAMTokenByAgency("")
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setHTTPDoErrPatches()
		_, err := getRealIAMTokenByAgency("")
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setHTTPRequestStatusErrPatcher()
		_, err := getRealIAMTokenByAgency("")
		assert.NotNil(t, err)
		patches.Reset()
	}
	{
		patches := setRequestSuccessPatches()
		_, err := getRealIAMTokenByAgency("")
		assert.Nil(t, err)
		patches.Reset()
	}
}

func Test_getTokenFromIAM(t *testing.T) {
	patches1 := setReadRotatedIDTokenSuccPatches()
	defer patches1.Reset()
	patches2 := setRequestSuccessPatches()
	defer patches2.Reset()
	_, err := getTokenFromIAM()
	assert.NotNil(t, err)
}

func Test_tokenRotation_start(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc testutils.PatchesFunc
	}{
		{
			name:    "getTokenFromIAM error",
			wantErr: true,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(getTokenFromIAM, func() (TempToken, error) {
						return TempToken{}, fmt.Errorf("mockError")
					}),
				})
				return p
			},
		},
		{
			name:    "storeTempToken error",
			wantErr: true,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(getTokenFromIAM, func() (TempToken, error) {
						return TempToken{}, nil
					}),
					gomonkey.ApplyFunc((*tokenRotation).storeTempToken, func(_ *tokenRotation, token TempToken) error {
						return fmt.Errorf("mockError")
					}),
				})
				return p
			},
		},
		{
			name:    "cronGetTokenFromIAM error",
			wantErr: true,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(getTokenFromIAM, func() (TempToken, error) {
						return TempToken{}, nil
					}),
					gomonkey.ApplyFunc((*tokenRotation).storeTempToken, func(_ *tokenRotation, token TempToken) error {
						return nil
					}),
					gomonkey.ApplyFunc((*tokenRotation).cronGetTokenFromIAM, func(_ *tokenRotation, stopCh <-chan struct{}) error {
						return fmt.Errorf("mockError")
					}),
				})
				return p
			},
		},
		{
			name:    "happy path",
			wantErr: false,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(getTokenFromIAM, func() (TempToken, error) {
						return TempToken{}, nil
					}),
					gomonkey.ApplyFunc((*tokenRotation).storeTempToken, func(_ *tokenRotation, token TempToken) error {
						return nil
					}),
					gomonkey.ApplyFunc((*tokenRotation).cronGetTokenFromIAM, func(_ *tokenRotation, stopCh <-chan struct{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(alarm.GetClient, func() *alarm.Client {
						return &alarm.Client{}
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType, func(_ *alarm.Client, _ alarm.Type) {
						return
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			r := &tokenRotation{}
			err := r.start(nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("tokenRotation start actual %v, want %v", err, tt.wantErr)
				return
			}
			p.ResetAll()
		})
	}
}
