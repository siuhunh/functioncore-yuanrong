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

// Package obs is the obs client
package obs

import (
	"crypto/tls"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"

	"functioncore/pkg/common/logger/log"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/pkg/common/utils"
)

const (
	// caFile default Certificate authority file path
	caFile                     = "/home/sn/resource/ca/ca.pem"
	httpPrefix                 = "http://"
	httpsPrefix                = "https://"
	uint64Base                 = 10
	readSize                   = 32 * 1024
	defaultTLSHandshakeTimeout = 5 * time.Second
	defaultReadHeaderTimeout   = 5 * time.Second

	// PartSize -
	PartSize = 15 * 1024 * 1024
)

var tlsConfig = struct {
	sync.Once
	config *tls.Config
}{}

var (
	secretKeyRecord string
	// from token rotation
	skRecord            []byte
	securityTokenRecord []byte
)

// Option contains the options to connect the OBS
type Option struct {
	Secure         bool
	AccessKey      string
	SecretKey      string
	Endpoint       string
	CaFile         string
	TrustedCA      bool
	MaxRetryCount  int
	Timeout        int
	ConnectTimeout int
}

// NewObsClient instantiate obs client, adds automatic verification of signature.
func NewObsClient(o Option) (*Client, error) {
	var endpoint string
	var urlPrefix string
	var err error

	var transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: time.Second * time.Duration(o.ConnectTimeout),
		}).DialContext,
		ResponseHeaderTimeout: defaultReadHeaderTimeout,
		DisableCompression:    true,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
	}

	endpoint, urlPrefix = getEndpoint(o.Endpoint)
	// used to clean up the secret key in the memory, even if the obs client initialized incorrectly
	secretKeyRecord = o.SecretKey
	if o.Secure {
		// load ca certificate
		if o.CaFile == "" {
			o.CaFile = caFile
		}
		transport.TLSClientConfig = getTLSConfig(o.CaFile, o.TrustedCA)
		urlPrefix = httpsPrefix
	} else {
		endpoint, err = utils.Domain2IP(endpoint)
		if err != nil {
			return nil, err
		}
	}

	obsClient, err := obs.New(o.AccessKey, o.SecretKey, urlPrefix+endpoint,
		obs.WithSslVerify(o.Secure), obs.WithHttpTransport(transport), obs.WithMaxRetryCount(o.MaxRetryCount),
		obs.WithPathStyle(false))

	if err != nil {
		return nil, err
	}
	return &Client{cli: obsClient}, nil
}

// getTLSConfig get tls config
func getTLSConfig(ca string, trustedCA bool) *tls.Config {
	tlsConfig.Do(func() {
		if trustedCA {
			// No ca is required.
			tlsConfig.config = commonTLS.NewTLSConfig()
			tlsConfig.config.InsecureSkipVerify = true
		} else {
			tlsConfig.config = commonTLS.NewTLSConfig(commonTLS.WithRootCAs(ca))
		}
	})
	return tlsConfig.config
}

func getEndpoint(endpoint string) (string, string) {
	var urlPrefix string
	urlPrefix = httpPrefix
	if strings.HasPrefix(endpoint, httpPrefix) {
		endpoint = strings.TrimPrefix(endpoint, httpPrefix)
		urlPrefix = httpPrefix
	}
	if strings.HasPrefix(endpoint, httpsPrefix) {
		endpoint = strings.TrimPrefix(endpoint, httpsPrefix)
		urlPrefix = httpsPrefix
	}
	return endpoint, urlPrefix
}

// DownloadFileToPath download file to path with obs output
func DownloadFileToPath(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
	baseDir := path.Dir(downloadPath)
	log.GetLogger().Debugf("downloadPath: %s, baseDir: %s", downloadPath, baseDir)
	err := os.MkdirAll(baseDir, utils.DirMode)
	if err != nil {
		log.GetLogger().Error("failed to make dir")
		return 0, errors.New("failed to make dir")
	}
	file, err := os.Create(downloadPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	err = file.Chmod(utils.FileMode)
	if err != nil {
		return 0, err
	}
	defer objectOutput.Body.Close()
	p := make([]byte, readSize)
	var readErr error
	var readCount int
	var fileSize int64
	for {
		readCount, readErr = objectOutput.Body.Read(p)
		if readCount > 0 {
			write, err := file.Write(p[:readCount])
			if write > 0 {
				fileSize += int64(write)
			}
			if err != nil {
				return 0, err
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				log.GetLogger().Warnf("failed to get obj from obs, err: %s", readErr.Error())
			}
			break
		}
	}
	if fileSize != objectOutput.ContentLength && !isLayer {
		return 0, errors.New("download size does not match the origin size" +
			". download size: " + strconv.FormatInt(fileSize, uint64Base) +
			". origin size: " + strconv.FormatInt(objectOutput.ContentLength, uint64Base))
	}
	return fileSize, nil
}

// DownloadAllFileToPath download All File To Path
func DownloadAllFileToPath(downloadPath string, body []byte) error {
	baseDir := path.Dir(downloadPath)
	log.GetLogger().Debugf("downloadPath: %s, baseDir: %s", downloadPath, baseDir)
	err := os.MkdirAll(baseDir, utils.DirMode)
	if err != nil {
		log.GetLogger().Error("failed to make dir")
		return errors.New("failed to make dir")
	}
	file, err := os.Create(downloadPath)
	if err != nil {
		return err
	}
	defer file.Close()
	err = file.Chmod(utils.FileMode)
	if err != nil {
		return err
	}
	var fileSize int64
	write, err := file.Write(body)
	if write > 0 {
		fileSize += int64(write)
	}
	return err
}

// DownloadFileToPathBySlice download file to path by slice with obs client
func DownloadFileToPathBySlice(bucketID string, objectID string, objPath string, objSize int64, client Client) error {
	start := time.Now()
	tackNum := int(math.Ceil(float64(objSize) / PartSize))
	input := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: bucketID,
			Key:    objectID,
		},
		DownloadFile:     objPath,
		PartSize:         PartSize,
		TaskNum:          tackNum,
		EnableCheckpoint: true,
	}
	err := DownloadFile(input, client)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			log.GetLogger().Errorf("fail to downloadFile. error code: %s, error message: %s",
				obsError.Code, obsError.Message)
			return obsError
		}
		return err
	}
	log.GetLogger().Infof("success to download file to path by slice, cost:%s", time.Since(start))
	return nil
}

// DownloadFile download file to path by slice with obs client
func DownloadFile(input *obs.DownloadFileInput, client Client) error {
	_, err := client.cli.DownloadFile(input)
	return err
}

// GetOutputObjectBuffer get output object buffer
func GetOutputObjectBuffer(objectOutput *obs.GetObjectOutput) ([]byte, error) {
	buffer, err := ioutil.ReadAll(objectOutput.Body)
	if err != nil {
		return nil, errors.New("filed read buffer error: " + err.Error())
	}
	return buffer, nil
}

// ClearSecretKeyMemory clear secretKey memory
func ClearSecretKeyMemory() {
	if len(skRecord) != 0 {
		utils.ClearByteMemory(skRecord)
	}
	if len(securityTokenRecord) != 0 {
		utils.ClearByteMemory(securityTokenRecord)
	}
	utils.ClearStringMemory(secretKeyRecord)
}
