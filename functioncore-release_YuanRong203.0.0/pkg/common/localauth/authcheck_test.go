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
package localauth

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestAuthCheckLocally(t *testing.T) {
	type args struct {
		ak          string
		sk          string
		requestSign string
		timestamp   string
		duration    int
	}
	var a args
	var b args
	b.requestSign = "aaa"
	var c args
	c.requestSign = "aaa"
	c.timestamp = strconv.FormatInt(time.Now().AddDate(1, 0, 0).Unix(), 10)
	var d args
	d.requestSign = "aaa"
	d.timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	var e args
	e.requestSign = "aaa,appid=aaa"
	e.timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
		{"case2", b, true},
		{"case3", c, true},
		{"case4", d, true},
		{"case5", e, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AuthCheckLocally(tt.args.ak, tt.args.sk, tt.args.requestSign, tt.args.timestamp, tt.args.duration); (err != nil) != tt.wantErr {
				t.Errorf("AuthCheckLocally() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_GetTimestampDiffLimit(t *testing.T) {
	tsDiffLimit := getTimestampDiffLimit()
	assert.Equal(t, 5, int(tsDiffLimit))
	patches := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
		return "100"
	})
	tsDiffLimit = getTimestampDiffLimit()
	assert.Equal(t, 100, int(tsDiffLimit))
	defer patches.Reset()
}

func TestCreateAuthorization(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(DecryptKeys,
			func(_ string, _ string) ([]byte, []byte, error) {
				return []byte{}, []byte{}, errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		ak    string
		sk    string
		url   string
		appID string
		data  []byte
	}
	var a args
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{"case1", a, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := CreateAuthorization(tt.args.ak, tt.args.sk, tt.args.url, tt.args.appID, tt.args.data)
			if got != tt.want {
				t.Errorf("CreateAuthorization() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("CreateAuthorization() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSignOMSVC(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(DecryptKeys,
			func(_ string, _ string) ([]byte, []byte, error) {
				return []byte{}, []byte{}, errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		ak   string
		sk   string
		url  string
		data []byte
	}
	var a args
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{"case1", a, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := SignOMSVC(tt.args.ak, tt.args.sk, tt.args.url, tt.args.data)
			if got != tt.want {
				t.Errorf("SignOMSVC() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("SignOMSVC() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSigner_buildCanonicalHeaders(t *testing.T) {
	type fields struct {
		signTime    time.Time
		serviceName string
		region      string
	}
	type args struct {
		request *http.Request
	}
	var f fields
	var a args
	request := &http.Request{
		Method:           "",
		URL:              nil,
		Proto:            "",
		ProtoMajor:       0,
		ProtoMinor:       0,
		Header:           nil,
		Body:             nil,
		GetBody:          nil,
		ContentLength:    0,
		TransferEncoding: nil,
		Close:            false,
		Host:             "",
		Form:             nil,
		PostForm:         nil,
		MultipartForm:    nil,
		Trailer:          nil,
		RemoteAddr:       "",
		RequestURI:       "",
		TLS:              nil,
		Response:         nil,
	}
	a.request = request
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"case1", f, a, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig := &Signer{
				signTime:    tt.fields.signTime,
				serviceName: tt.fields.serviceName,
				region:      tt.fields.region,
			}
			if got := sig.buildCanonicalHeaders(tt.args.request); got != tt.want {
				t.Errorf("buildCanonicalHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSigner(t *testing.T) {
	type args struct {
		mode        string
		serviceName string
		region      string
	}
	var a args
	a.mode = "aaa"
	signer := &Signer{}
	tests := []struct {
		name string
		args args
		want *Signer
	}{
		{"case1", a, signer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSigner(tt.args.mode, tt.args.serviceName, tt.args.region); !reflect.DeepEqual(got.serviceName, tt.want.serviceName) {
				t.Errorf("getSigner() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_signLocalAuthRequest(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(url.Parse,
			func(_ string) (*url.URL, error) {
				return nil, errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		rawURL    string
		timeStamp string
		appID     string
		key       *Authentication
		data      []byte
	}
	var a args
	var b args
	b.timeStamp = strconv.FormatInt(time.Now().Unix(), 10)
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, ""},
		{"case2", b, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := signLocalAuthRequest(tt.args.rawURL, tt.args.timeStamp, tt.args.appID, tt.args.key, tt.args.data); got != tt.want {
				t.Errorf("signLocalAuthRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSignLocally(t *testing.T) {
	convey.Convey("TestSignLocally", t, func() {
		auth, time := SignLocally("ak", "sk", "appID", 0)
		convey.So(auth, convey.ShouldNotBeEmpty)
		convey.So(time, convey.ShouldNotBeEmpty)
	})
}
