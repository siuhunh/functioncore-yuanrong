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

// Package client -
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	fhttp "github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/pkg/cert"
	"functioncore/pkg/cli/pkg/http/fast"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/signals"
)

const (
	defaultMaxRetryTimes = 3

	// MaxClientConcurrency is the max concurrency of fast http client
	MaxClientConcurrency = 1000

	// DialTimeOut -
	DialTimeOut = 10

	// TCPKeepAlivePeriod -
	TCPKeepAlivePeriod = 10
	// expires 3600s
	expires = 3600 * time.Second
)

var tcpDialer = fhttp.TCPDialer{Concurrency: MaxClientConcurrency}

// Client -
type Client interface {
	Post(url string, body []byte, headers map[string]string) (*fhttp.Response, error)
	PutKeyWithValue(key, value string) error
	GetValuesByKey(key string) ([]string, error)
	MatchKeysByPrefixKey(prefixKey string) ([]string, error)
	DeleteByKey(key string) error
	DeleteByPrefixKey(prefixKey string) error
	UploadObject(file *os.File, fileSize int64, objectName string) error
	CopyObject(dtsObjectKey, srcObjectKey string) error
	DeleteObject(objectKey string) error
	GetObjectDownloadURL(objectKey string) (u *url.URL, err error)
}

// SutureClient -
type SutureClient struct {
	HTTPClient  *fast.Fast
	MinioClient *minio.Client
	EtcdClient  *etcd3.EtcdClient
}

func newEtcdClient() (*etcd3.EtcdClient, error) {
	etcd := config.FConfig.CLIConfig.ETCD
	etcdConfig := &etcd3.ClientConfig{
		ServerList: etcd.URL,
		User:       etcd.UserName,
		Password:   etcd.Password,
		SslEnable:  etcd.SSLEnable,
		CaFile:     etcd.CaFile,
		CertFile:   etcd.CertFile,
		KeyFile:    etcd.KeyFile,
	}
	stopCh := signals.WaitForSignal()
	if err := etcd3.InitParam().WithMetaEtcdConfig(etcdConfig).WithStopCh(stopCh).InitClient(); err != nil {
		return nil, err
	}
	return etcd3.GetMetaEtcdClient(), nil
}

func newMinioClient() (*minio.Client, error) {
	minioConfig := config.FConfig.CLIConfig.Minio
	sk, err := localauth.Decrypt(minioConfig.SecretAccessKey)
	if err != nil {
		return nil, err
	}
	minioClient := &minio.Client{}
	if !minioConfig.SSLEnable {
		minioClient, err = minio.New(minioConfig.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(minioConfig.AccessKeyID, string(sk), ""),
			Secure: false,
		})
	} else {
		certPool := x509.NewCertPool()
		caCrt, err := ioutil.ReadFile(minioConfig.CertFile)
		if err != nil {
			return nil, err
		}
		certPool.AppendCertsFromPEM(caCrt)
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:            certPool,
				InsecureSkipVerify: true,
			},
		}
		minioClient, err = minio.New(minioConfig.Endpoint, &minio.Options{
			Creds:     credentials.NewStaticV4(minioConfig.AccessKeyID, string(sk), ""),
			Secure:    true,
			Transport: transport,
		})
	}
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}

func newHTTPClient() *fast.Fast {
	tlsConf := &tls.Config{
		RootCAs:    cert.Pool,
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
		Renegotiation:      tls.RenegotiateNever,
		InsecureSkipVerify: true,
	}
	cli := &fast.Fast{
		Client: &fhttp.Client{
			TLSConfig:                 tlsConf,
			MaxIdemponentCallAttempts: defaultMaxRetryTimes,
			ReadBufferSize:            http.DefaultMaxHeaderBytes,
			Dial: func(addr string) (net.Conn, error) {
				conn, err := tcpDialer.DialTimeout(addr, DialTimeOut*time.Second)
				if err != nil {
					fmt.Printf("failed to dial %s, error %s\n", addr, err.Error())
					return nil, err
				}

				tcpConn, ok := conn.(*net.TCPConn)
				if !ok {
					fmt.Printf("failed to dial %s\n", addr)
					return nil, nil
				}
				err = tcpConn.SetKeepAlive(true)
				if err != nil {
					fmt.Printf("failed to set connection keepalive %s, error: %s",
						addr, err.Error())
					return nil, err
				}

				err = tcpConn.SetKeepAlivePeriod(TCPKeepAlivePeriod * time.Second)
				if err != nil {
					fmt.Printf("failed to set connection keepalive period %s, error: %s",
						addr, err.Error())
					return nil, err
				}

				return tcpConn, nil
			},
		}}
	return cli
}

// Init init client
func Init() (Client, error) {
	httpClient := newHTTPClient()
	minioClient, err := newMinioClient()
	if err != nil {
		return nil, err
	}
	etcdClient, err := newEtcdClient()
	if err != nil {
		return nil, err
	}
	client := &SutureClient{
		HTTPClient:  httpClient,
		MinioClient: minioClient,
		EtcdClient:  etcdClient,
	}
	return client, nil
}

// Post http post
func (c *SutureClient) Post(url string, body []byte, headers map[string]string) (*fhttp.Response, error) {
	return c.HTTPClient.Post(url, body, headers)
}

// GetValuesByKey get values by key from etcd
func (c *SutureClient) GetValuesByKey(key string) ([]string, error) {
	localContext := etcd3.CreateEtcdCtxInfo(context.Background())
	return c.EtcdClient.GetValues(localContext, key)
}

// MatchKeysByPrefixKey match keys from etcd
func (c *SutureClient) MatchKeysByPrefixKey(prefixKey string) ([]string, error) {
	localContext := etcd3.CreateEtcdCtxInfo(context.Background())
	response, err := c.EtcdClient.GetResponse(localContext, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	values := make([]string, len(response.Kvs), len(response.Kvs))
	for index, v := range response.Kvs {
		values[index] = string(v.Key)
	}
	return values, err
}

// DeleteByKey delete key from etcd
func (c *SutureClient) DeleteByKey(key string) error {
	localContext := etcd3.CreateEtcdCtxInfo(context.Background())
	return c.EtcdClient.Delete(localContext, key)
}

// DeleteByPrefixKey delete key by prefixKey from etcd
func (c *SutureClient) DeleteByPrefixKey(prefixKey string) error {
	localContext := etcd3.CreateEtcdCtxInfo(context.Background())
	return c.EtcdClient.Delete(localContext, prefixKey, clientv3.WithPrefix())
}

// PutKeyWithValue put key value to etcd
func (c *SutureClient) PutKeyWithValue(key, value string) error {
	localContext := etcd3.CreateEtcdCtxInfo(context.Background())
	return c.EtcdClient.Put(localContext, key, value)
}

// UploadObject upload object to minio
func (c *SutureClient) UploadObject(file *os.File, fileSize int64, objectName string) error {
	bucket := config.FConfig.CLIConfig.Minio.Bucket
	exists, err := c.MinioClient.BucketExists(context.Background(), bucket)
	if err != nil {
		return err
	}
	if !exists {
		err := c.MinioClient.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}
	_, err = c.MinioClient.PutObject(context.Background(),
		config.FConfig.CLIConfig.Minio.Bucket, objectName, file, fileSize,
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

// CopyObject copy object in minio
func (c *SutureClient) CopyObject(dtsObjectKey, srcObjectKey string) error {
	dts := minio.CopyDestOptions{
		Bucket: config.FConfig.CLIConfig.Minio.Bucket,
		Object: dtsObjectKey,
	}
	src := minio.CopySrcOptions{
		Bucket: config.FConfig.CLIConfig.Minio.Bucket,
		Object: srcObjectKey,
	}
	_, err := c.MinioClient.CopyObject(context.Background(), dts, src)
	return err
}

// DeleteObject delete object in minio
func (c *SutureClient) DeleteObject(objectKey string) error {
	return c.MinioClient.RemoveObject(context.Background(), config.FConfig.CLIConfig.Minio.Bucket, objectKey,
		minio.RemoveObjectOptions{})
}

// GetObjectDownloadURL -
func (c *SutureClient) GetObjectDownloadURL(objectKey string) (u *url.URL, err error) {
	return c.MinioClient.PresignedGetObject(context.Background(), config.FConfig.CLIConfig.Minio.Bucket,
		objectKey, expires, url.Values{})
}
