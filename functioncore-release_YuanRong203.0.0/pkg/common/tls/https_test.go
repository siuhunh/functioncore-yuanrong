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

package tls

import (
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

type mockListener struct{}

func (m *mockListener) Accept() (net.Conn, error) {
	return nil, fmt.Errorf("failed to accept")
}

func (m *mockListener) Close() error {
	return nil
}
func (m *mockListener) Addr() net.Addr {
	return nil
}

func TestGetURLScheme(t *testing.T) {
	if "https" != GetURLScheme(true) {
		t.Error("GetURLScheme failed")
	}
	if "http" != GetURLScheme(false) {
		t.Error("GetURLScheme failed")
	}
}

func TestInitTLSConfig(t *testing.T) {
	p := gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
		return nil, nil
	})
	p.ApplyFunc(containPassPhase, func(keyContent []byte, passPhase string, isHttps bool) (Content []byte, err error) {
		return nil, nil
	})
	p.ApplyFunc(tls.X509KeyPair, func(certPEMBlock, keyPEMBlock []byte) (tls.Certificate, error) {
		var cert tls.Certificate
		return cert, nil
	})
	defer p.Reset()

	err := InitTLSConfig("TLSv1.2", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
	assert.Equal(t, nil, err)
}

func TestGetClientTLSConfig(t *testing.T) {
	actual := GetClientTLSConfig()
	assert.Equal(t, tlsConfig, actual)
}

func TestContainPassPhase(t *testing.T) {
	convey.Convey("ContainPassPhase", t, func() {
		convey.Convey("http error case 1", func() {
			keyContent := []byte{}
			passPhase := ""
			isHttps := false
			content, err := containPassPhase(keyContent, passPhase, isHttps)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(content, convey.ShouldBeNil)
		})

		convey.Convey("https error case 1", func() {
			keyContent := []byte{}
			passPhase := ""
			isHttps := true
			content, err := containPassPhase(keyContent, passPhase, isHttps)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to decode key file")
			convey.So(content, convey.ShouldBeNil)
		})
		convey.Convey("Decrypt error", func() {
			keyContent := pem.EncodeToMemory(&pem.Block{
				Type:    "MESSAGE",
				Headers: map[string]string{"DEK-Info": "test"},
				Bytes:   []byte("test containPassPhase")})
			passPhase := "abc"
			isHttps := true
			content, err := containPassPhase(keyContent, passPhase, isHttps)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(content, convey.ShouldBeNil)
		})
	})
}

func TestNewHttpsServer(t *testing.T) {
	handler := gin.New()
	err := HTTPListenAndServeTLS("123", handler)
	assert.NotEqual(t, err, nil)

	server := fasthttp.Server{}
	err = FastHTTPListenAndServeTLS("123", &server)
	assert.NotEqual(t, err, nil)
	convey.Convey("FastHTTPListenAndServeTLS", t, func() {
		patch := gomonkey.ApplyFunc(net.Listen, func(network, address string) (net.Listener, error) {
			return &mockListener{}, nil
		})
		defer patch.Reset()
		err = FastHTTPListenAndServeTLS("123", &server)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestLoadCerts(t *testing.T) {
	convey.Convey("https Load Certs 1", t, func() {
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			return "aaa"
		})
		defer patch.Reset()
		cert := loadCerts("./test")
		convey.So(cert, convey.ShouldNotBeNil)
	})
	convey.Convey("https Load Certs 2", t, func() {
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			return "aaa"
		})
		patch2 := gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
			return "a", errors.New("bbb")
		})
		defer patch.Reset()
		defer patch2.Reset()
		cert := loadCerts("1")
		convey.So(cert, convey.ShouldNotBeNil)
	})

}

func Test_parseSSLProtocol(t *testing.T) {
	convey.Convey("Test_parseSSLProtocol", t, func() {
		convey.So(parseSSLProtocol("TLSv1.2"), convey.ShouldEqual, tls.VersionTLS12)
		convey.So(parseSSLProtocol("abc"), convey.ShouldEqual, 0)
	})
}

func Test_parseURL(t *testing.T) {
	url := ParseURL("http://test.com")
	assert.Equal(t, url, "test.com")
	url1 := ParseURL("test.com")
	assert.Equal(t, url1, "test.com")
}

func TestGetClientTLSConfig_Multi(t *testing.T) {
	old := tlsConfig

	tlsConfig = &tls.Config{}

	defer func() {
		tlsConfig = old
	}()

	a := GetClientTLSConfig()
	a.CipherSuites = append(a.CipherSuites, 10)

	b := GetClientTLSConfig()

	assert.NotEqual(t, a, b)
	assert.NotSame(t, a, b)
	assert.Equal(t, 1, len(a.CipherSuites))
	assert.Equal(t, 0, len(b.CipherSuites))
}
