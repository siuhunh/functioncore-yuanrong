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

package obs

import (
	"unsafe"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"

	"functioncore/pkg/common/logger/log"
)

// Client -
type Client struct {
	cli *obs.ObsClient
}

// GetObject -
func (c *Client) GetObject(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
	return c.cli.GetObject(input)
}

// GetObjectMetadata -
func (c *Client) GetObjectMetadata(input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
	return c.cli.GetObjectMetadata(input)
}

// Refresh refresh token
func (c *Client) Refresh(ak string, sk, securityToken []byte) {
	skRecord, securityTokenRecord = sk, securityToken
	c.cli.Refresh(ak, *(*string)(unsafe.Pointer(&sk)), *(*string)(unsafe.Pointer(&securityToken)))
}

// UploadFile upload file to obs
func (c *Client) UploadFile(input *obs.PutFileInput) error {
	_, err := c.cli.HeadBucket(input.Bucket)
	if err != nil {
		log.GetLogger().Errorf("bucket is not exists %s", err.Error())
		return err
	}
	_, err = c.cli.PutFile(input)
	return err
}

// DeleteObject -
func (c *Client) DeleteObject(input *obs.DeleteObjectInput) (*obs.DeleteObjectOutput, error) {
	return c.cli.DeleteObject(input)
}

// InitMultipart -
func (c *Client) InitMultipart(input *obs.InitiateMultipartUploadInput) (string, error) {
	initResp, err := c.cli.InitiateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	return initResp.UploadId, nil
}

// UploadMultipart -
func (c *Client) UploadMultipart(input *obs.UploadPartInput) (output *obs.UploadPartOutput, err error) {
	return c.cli.UploadPart(input)
}

// CompleteMultipartUpload -
func (c *Client) CompleteMultipartUpload(input *obs.CompleteMultipartUploadInput) error {
	_, err := c.cli.CompleteMultipartUpload(input)
	return err
}
