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

package tls

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"crypto/x509"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

// TestLoadRootCAs is used to test the root certificate loading error.
func TestLoadRootCAs(t *testing.T) {
	convey.Convey("LoadRootCAs", t, func() {
		convey.Convey("error case 1", func() {
			caFiles := ""
			_, err := LoadRootCAs(caFiles)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("error case 2", func() {
			dir, err := ioutil.TempDir("", "*")
			require.NoError(t, err)
			cryptoFile, err := ioutil.TempFile(dir, "crypto")
			require.NoError(t, err)
			_, err = LoadRootCAs(cryptoFile.Name())
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("error case 3", func() {
			dir, err := ioutil.TempDir("", "*")
			require.NoError(t, err)
			cryptoFile, err := ioutil.TempFile(dir, "test")
			require.NoError(t, err)
			err = ioutil.WriteFile(filepath.Join(dir, "test"), []byte("a"), os.ModePerm)
			require.NoError(t, err)
			_, err = LoadRootCAs(cryptoFile.Name())
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

// TestLoadRootCAs is used to test certificate modification errors.
func TestVerifyCert(t *testing.T) {
	convey.Convey("VerifyCert", t, func() {
		convey.Convey("error case 1", func() {
			rawCerts := [][]byte{}
			verifiedChains := [][]*x509.Certificate{}
			err := VerifyCert(rawCerts, verifiedChains)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "cert number is 0")
		})

		convey.Convey("error case 2", func() {
			rawCerts := [][]byte{[]byte("test1"), []byte("test2")}
			verifiedChains := [][]*x509.Certificate{}
			err := VerifyCert(rawCerts, verifiedChains)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
