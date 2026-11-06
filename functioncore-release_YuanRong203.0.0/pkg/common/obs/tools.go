/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package obs

import (
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
)

const (
	defaultRetryTimes    = 3
	defaultRetryDuration = 1 * time.Second
	maxFileSize          = 16 * 1024 * 1024
)

// S3Deployer OBS Deployer
type S3Deployer struct {
	cli           *Client
	ioReadTimeout int
	fileLimitSize int
}

// NewDeployer -
func NewDeployer(s3Config *types.S3Config) (*S3Deployer, error) {
	var authString = ""
	secretKey := []byte("")
	if s3Config.SecretKey != "" {
		var err error
		secretKey, err = localauth.Decrypt(s3Config.SecretKey)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt S3 SecretKey")
			return nil, err
		}
	}
	opt := Option{
		Endpoint:       s3Config.Endpoint,
		CaFile:         s3Config.CaFile,
		Secure:         s3Config.UseSSL,
		AccessKey:      s3Config.AccessKey,
		SecretKey:      *(*string)(unsafe.Pointer(&secretKey)),
		TrustedCA:      s3Config.TrustedCA,
		MaxRetryCount:  utils.ObsMaxRetry,
		Timeout:        utils.ObsDefaultTimeout,
		ConnectTimeout: utils.ObsDefaultConnectTimeout,
	}
	s3Client, err := NewObsClient(opt)
	if err != nil {
		utils.ClearStringMemory(authString)
		ClearSecretKeyMemory()
		log.GetLogger().Errorf("failed to init S3 client: %s", err.Error())
		return nil, err
	}
	return &S3Deployer{
		cli: s3Client,
	}, nil
}

// DownLoadFromObs download customContainer from obs
func (s *S3Deployer) DownLoadFromObs(bucketID string, objectID string) ([]byte, error) {
	if bucketID == "" || objectID == "" {
		return nil, errors.New("file mount config error")
	}
	var objectOutput *obs.GetObjectOutput
	var err error
	for i := 0; i < defaultRetryTimes; i++ {
		objectOutput, err = s.cli.GetObject(&obs.GetObjectInput{
			GetObjectMetadataInput: obs.GetObjectMetadataInput{
				Bucket: bucketID,
				Key:    objectID,
			},
		})
		if err != nil {
			log.GetLogger().Warnf("failed to get object %s: %s, retry times is: %d", objectID, err.Error(), i)
			time.Sleep(defaultRetryDuration)
			continue
		}
		break
	}

	if objectOutput == nil {
		if err != nil {
			alarm.GetClient().ReportAlarm(alarm.OBSConnection, fmt.Sprintf("error msg=%s,", err.Error()))
		}
		log.GetLogger().Errorf("failed to get object %s for %d times", objectID, defaultRetryTimes)
		return nil, err
	}
	alarm.GetClient().ClearAlarm(alarm.OBSConnection)

	buffer, err := GetOutputObjectBuffer(objectOutput)
	if err != nil {
		log.GetLogger().Errorf("failed to read object %s: %s", objectID, err.Error())
		return nil, err
	}
	if len(buffer) >= maxFileSize {
		return nil, errors.New("failed to download the file because the file size exceeds 16 MB")
	}
	return buffer, nil
}
