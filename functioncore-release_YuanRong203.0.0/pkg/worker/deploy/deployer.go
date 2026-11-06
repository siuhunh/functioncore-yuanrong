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

// Package deploy deploys Layers and functions
package deploy

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/kms"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/storage"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	utilities "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

const (
	perm             = 0750
	mbShift          = 20
	readAllLimitSize = 4194304
	cacheLimitSize   = 100 * 1024
	// set default Request Cache Timeout 300s as timeout of function task is 300s
	defaultRequestCacheTimeout   = 300 * time.Second
	defaultRequestCacheHeartBeat = 10 * time.Second

	int64Base     = 10
	int64BitSize  = 64
	retryTime     = 3
	retryInterval = 1 * time.Second

	partSize         = 10 * 1024 * 1024 // 10MB
	maxWorkers       = 10
	versionSeparator = ".VERSION"
)

var limitCodeSize = parseLimitCodeSize()

func parseLimitCodeSize() int64 {
	if e := os.Getenv("LIMIT_CODE_SIZE"); e != "" {
		if res, err := strconv.ParseInt(e, int64Base, int64BitSize); err == nil {
			if res > readAllLimitSize {
				return readAllLimitSize
			}
			return res
		}
	}
	return cacheLimitSize
}

// Deployer deploy function and layer
type Deployer interface {
	Deploy(conf *config.Configuration, getCodeFinishCh chan struct{}, language string) (*DurationInfo, error)
	DeploySnapshot(conf *config.Configuration, getCodeFinishCh chan struct{}, language string) (*DurationInfo, error)
	UploadSnapshot(conf *config.Configuration, bucketID, ObjectID string) (int64, string, error)
	DownloadSnapshotWithRetry(conf *config.Configuration, snapshotConfig config.SnapshotConfig,
		language string, traceID string) (bool, error)
	DeleteSnapshot(conf *config.Configuration, bucketID, ObjectID string, needRemoveObjectIDs []string) error
	SetCacheManagerServiceIP(conf *config.Configuration)
	UploadDumpFile(conf *config.Configuration, dumpFileDir string) error
}

// Decompressor decompress package
type Decompressor interface {
	Decompress(objectSize int64, downloadFilePath string, buffer []byte, extractPath string, isLayer bool) error
}

// DurationInfo is info of duration
type DurationInfo struct {
	// Deploy function time
	DeployFuncTime time.Duration
	// Decompress time
	Decompress time.Duration
	// Deploy layer time
	DeployLayerTime time.Duration
}

// Message define message content
type Message struct {
	Duration *DurationInfo
	Err      error
}

type deployDetail struct {
	BucketID               string
	ObjectID               string
	CodePlaintextKmsCipher string
	Path                   string
	Checksum               string
	CodeSize               int64
	DeployLayer            bool
	Etag                   string
	URN                    string
	Language               string
	FastStartEnable        bool
}

// PartResult define multipart upload result
type PartResult struct {
	PartNumber int
	ETag       string
	Err        error
}

// NewDeployer implements a new deploy
func NewDeployer(c *config.Configuration) (Deployer, error) {
	switch c.StorageType {
	case "s3":
		secretKey := []byte("")
		// support token rotation
		if c.S3Config.SecretKey != "" {
			var err error
			secretKey, err = localauth.Decrypt(c.S3Config.SecretKey)
			if err != nil {
				log.GetLogger().Errorf("failed to decrypt S3 SecretKey")
				return nil, err
			}
		}
		conf := commonObs.Option{
			Secure:         c.S3Config.UseSSL,
			AccessKey:      c.S3Config.AccessKey,
			SecretKey:      *(*string)(unsafe.Pointer(&secretKey)),
			Endpoint:       c.S3Config.Endpoint,
			CaFile:         c.S3Config.CaFile,
			TrustedCA:      c.S3Config.TrustedCA,
			MaxRetryCount:  utilities.ObsMaxRetry,
			Timeout:        utilities.ObsDefaultTimeout,
			ConnectTimeout: utilities.ObsDefaultConnectTimeout,
		}
		s3Client, err := commonObs.NewObsClient(conf)
		if err != nil {
			log.GetLogger().Errorf("failed to init S3 client: %s", err.Error())
			commonObs.ClearSecretKeyMemory()
			return nil, err
		}
		httpClient, err := preconnectCache(c)
		if err != nil {
			commonObs.ClearSecretKeyMemory()
			return nil, err
		}
		return &s3Deployer{
			cli:              s3Client,
			httpClient:       httpClient,
			ioReadTimeout:    c.ServerConfig.IOReadTimeout,
			cacheHTTPSEnable: c.InternalHTTPSConfig.HTTPSEnable,
		}, nil
	default:
		return nil, errors.New("unknown storage type")
	}
}

// NewDumpDeployer -
func NewDumpDeployer(c *config.Configuration, customS3Config commontypes.CustomUploadConfig) (Deployer, error) {
	secretKey := []byte("")
	// support token rotation
	if customS3Config.CustomSK != "" {
		var err error
		secretKey, err = localauth.Decrypt(customS3Config.CustomSK)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt S3 SecretKey")
			return nil, err
		}
	}
	conf := commonObs.Option{
		Secure:          c.S3Config.UseSSL,
		AccessKey:       customS3Config.CustomAK,
		CustomSecretKey: string(secretKey),
		UseCustomSK:     true,
		Endpoint:        customS3Config.Endpoint,
		CaFile:          c.S3Config.CaFile,
		TrustedCA:       c.S3Config.TrustedCA,
		MaxRetryCount:   utilities.ObsMaxRetry,
		Timeout:         utilities.ObsDefaultTimeout,
		ConnectTimeout:  utilities.ObsDefaultConnectTimeout,
	}
	s3Client, err := commonObs.NewObsClient(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to init S3 client: %s", err.Error())
		commonObs.ClearCustomSecretKeyMemory()
		return nil, err
	}

	return &s3Deployer{
		cli:              s3Client,
		ioReadTimeout:    c.ServerConfig.IOReadTimeout,
		cacheHTTPSEnable: c.InternalHTTPSConfig.HTTPSEnable,
	}, nil
}

type s3Deployer struct {
	cli                    *commonObs.Client
	httpClient             *http.Client
	ioReadTimeout          int
	cacheHTTPSEnable       bool
	cacheManagerServiceIP  string
	isSafety               bool
	codePlaintextKmsCipher string
}

// RefreshToken refresh CCMS token when download from obs or upload to obs
func (d *s3Deployer) RefreshToken(conf *config.Configuration) error {
	secretKey, err := localauth.Decrypt(conf.Secret)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt S3 SecretKey")
		return err
	}
	securityToken, err := localauth.Decrypt(conf.SecurityToken)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt S3 SecurityToken")
		utilities.ClearByteMemory(secretKey)
		return err
	}
	d.cli.Refresh(conf.Access, secretKey, securityToken)
	return nil
}

func (d *s3Deployer) SetCacheManagerServiceIP(conf *config.Configuration) {
	if conf == nil {
		log.GetLogger().Warnf("worker deployer set cache-manager service ip failed, conf is nil")
		return
	}
	d.cacheManagerServiceIP = conf.CacheManagerClusterID
	d.isSafety = conf.IsSafety
	log.GetLogger().Infof("worker deployer set cache-manager service ip: %s instead of cache-manager domain",
		conf.CacheManagerClusterID)
}

// DeploySnapshot implements snapshot deploy
func (d *s3Deployer) DeploySnapshot(conf *config.Configuration,
	getCodeFinishCh chan struct{}, language string) (*DurationInfo, error) {
	start := time.Now()
	duration := &DurationInfo{
		DeployFuncTime:  constants.ErrorDuration,
		DeployLayerTime: constants.ErrorDuration,
	}
	snapshotFilePath := utilities.GetSnapshotImagePathForWorker()
	snapshotBucketID := conf.SnapshotConfig.SnapshotBucketID
	snapshotObjectID := conf.SnapshotConfig.SnapshotObjectID
	snapshotCheckSum := conf.SnapshotConfig.SnapshotSha256
	log.GetLogger().Infof("restore snapshot with bucketID: %s, objectID: %s, language: %s, storage type: %s",
		snapshotBucketID, snapshotObjectID, language, storage.GetStorageType())
	if conf.Access != "" {
		if err := d.RefreshToken(conf); err != nil {
			log.GetLogger().Errorf("failed to refresh ccms token when deploy snapshot with error %s", err.Error())
			return nil, err
		}
	}
	if !conf.UsePoolPod {
		defer commonObs.ClearSecretKeyMemory()
	}

	err := checkDeployPath(snapshotFilePath, conf)
	if err != nil {
		return duration, err
	}
	// Deploy snapshot
	deployInfo := &deployDetail{
		BucketID:        snapshotBucketID,
		ObjectID:        snapshotObjectID,
		Path:            snapshotFilePath,
		Checksum:        snapshotCheckSum,
		Language:        language,
		FastStartEnable: conf.SnapshotConfig.FastStartEnable,
	}
	if err := d.deployFunction(duration, deployInfo, getCodeFinishCh, true); err != nil {
		return duration, err
	}

	duration.DeployFuncTime = time.Since(start)

	log.GetLogger().Infof("the snapshot is downloaded and unpacked successfully with "+
		"bucketID %s, objectID: %s, language: %s. cost time %s. storage type %s", snapshotBucketID, snapshotObjectID,
		language, duration.DeployFuncTime, storage.GetStorageType())
	return duration, nil
}

// Deploy implements Deployer.Deploy
func (d *s3Deployer) Deploy(conf *config.Configuration,
	getCodeFinishCh chan struct{}, language string) (*DurationInfo, error) {
	if conf.Access != "" {
		if err := d.RefreshToken(conf); err != nil {
			log.GetLogger().Errorf("failed to refresh ccms token when deploy function with error %s", err.Error())
			return nil, err
		}
	}
	duration := &DurationInfo{
		DeployFuncTime:  constants.ErrorDuration,
		DeployLayerTime: constants.ErrorDuration,
	}

	deployPath := conf.DeploymentConfig.DeployDir
	layerENV := conf.DeploymentConfig.LayerInfo
	checksum := conf.RuntimeConfig.CodeSha512
	if checksum == "" {
		checksum = conf.RuntimeConfig.CodeSha256
	}

	if err := checkDeployPath(deployPath, conf); err != nil {
		return duration, err
	}
	// Deploy function
	start := time.Now()
	deployInfo := &deployDetail{
		BucketID:               conf.DeploymentConfig.BucketID,
		ObjectID:               conf.DeploymentConfig.ObjectID,
		CodePlaintextKmsCipher: conf.CodePlaintextKmsCipher,
		Path:                   deployPath,
		Checksum:               checksum,
		CodeSize:               conf.DeploymentConfig.CodeSize,
		URN:                    conf.RuntimeConfig.URN,
		Language:               language,
	}
	if err := d.deployFunction(duration, deployInfo, getCodeFinishCh, false); err != nil {
		return duration, err
	}
	duration.DeployFuncTime = time.Since(start)

	// Deploy layer
	start = time.Now()
	if layerENV != "" {
		if err := d.deployLayers(layerENV, deployPath, conf.RuntimeConfig.URN, conf.Language); err != nil {
			return duration, err
		}
	}
	duration.DeployLayerTime = time.Since(start)
	if conf.RunningMode != constants.RunningModeCheckpoint &&
		// If running mode is restore and use pool pod, we need to clear secret key
		// while both code and snapshot download task are finished, or 403 err will happened
		!(conf.RunningMode == constants.RunningModeRestore && conf.UsePoolPod) {
		commonObs.ClearSecretKeyMemory()
	} else {
		// checkpoint needs to upload, still needs the SecretKeyMemory
		log.GetLogger().Infof("checkpoint mode skip ClearSecretKeyMemory")
	}
	return duration, nil
}

// UploadSnapshot upload snapshot to s3
func (d *s3Deployer) UploadSnapshot(conf *config.Configuration, bucketID, ObjectID string) (int64, string, error) {
	tmpZipFileName := "/tmp/upload_snapshot.zip"
	if conf.FastStartEnable {
		tmpZipFileName = "/runtime/var-tmp/snapshot/upload_snapshot.tar.zst"
	}

	log.GetLogger().Info("start to pack snapshot")
	if !conf.FastStartEnable && storage.GetStorageType() == "data-system" {
		_, _, err := UploadSnapshotToDS(bucketID, ObjectID)
		if err != nil {
			return 0, "", err
		}
	}
	// go on uploading snapshot to obs when succeed to upload to data system
	packStart := time.Now()
	if conf.Access != "" {
		if err := d.RefreshToken(conf); err != nil {
			log.GetLogger().Errorf("failed to refresh ccms token when upload snapshot with error %s", err.Error())
			return 0, "", err
		}
	}
	err := d.compressFileBySnapshotType(conf.FastStartEnable, tmpZipFileName)
	if err != nil {
		log.GetLogger().Errorf("failed to compress file %s snapshot: %s", tmpZipFileName, err.Error())
		return 0, "", err
	}

	uploadStart := time.Now()
	fileSize, fileHash, err := utilities.GetFileHashInfo(tmpZipFileName)
	if err != nil {
		log.GetLogger().Errorf("snapshot file size is empty: %s", err.Error())
		return 0, "", err
	}
	if err = checkPodDNSConfig(); err != nil {
		return 0, "", err
	}

	log.GetLogger().Infof("start to upload snapshot to obs with bucketID: %s, ObjectID: %s filePath: %s",
		bucketID, ObjectID, tmpZipFileName)

	fileInput := &obs.PutFileInput{}
	fileInput.Bucket = bucketID
	fileInput.Key = ObjectID
	fileInput.SourceFile = tmpZipFileName
	fileInput.ContentLength = fileSize

	// retry 3 times
	if err := d.UploadFileWithRetry(fileInput); err != nil {
		log.GetLogger().Errorf("Failed to upload snapshot to obs 3 times, s3 bucketID: %s, objectID: %s,"+
			" file path: %s, err: %s.", fileInput.Bucket, fileInput.Key, fileInput.SourceFile, err.Error())
		err = snerror.New(snerror.UploadSnapshotErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.UploadSnapshotErrorCode), err.Error(), bucketID, ObjectID, tmpZipFileName))
		return 0, "", err
	}

	log.GetLogger().Infof("finish uploading snapshot to obs, pack cost: %s, upload cost: %s",
		uploadStart.Sub(packStart), time.Since(uploadStart))

	return fileSize, fileHash, nil
}

func checkPodDNSConfig() error {
	if len(utils.PodOriginDNSConf) > 0 {
		err := utilities.RecoverDNSConfig(utils.PodOriginDNSConf)
		if err != nil {
			log.GetLogger().Errorf("failed to recover dns config, error: %s", err.Error())
			return err
		}
	}
	return nil
}

func (d *s3Deployer) compressFileBySnapshotType(fastStartEnable bool, tmpZipFileName string) error {
	if fastStartEnable {
		zstdHandler := newSnapshotZstdHandler()
		return zstdHandler.Compress(tmpZipFileName, utilities.GetSnapshotImagePathForWorker())
	}
	zipHandler := newSnapshotZipHandler()
	return zipHandler.Compress(tmpZipFileName, utilities.GetSnapshotImagePathForWorker())
}

func (d *s3Deployer) UploadFileWithRetry(fileInput *obs.PutFileInput) error {
	var err error
	for i := 0; i < retryTime; i++ {
		err = d.cli.UploadFile(fileInput)
		if err != nil {
			log.GetLogger().Errorf("failed to write to s3 bucketID: %s, objectID: %s, file path: %s, err: %s",
				fileInput.Bucket, fileInput.Key, fileInput.SourceFile, err.Error())
		} else {
			// upload succeed
			break
		}
		if i != retryTime-1 {
			time.Sleep(retryInterval)
		}
	}
	return err
}

func (d *s3Deployer) UploadDumpFile(conf *config.Configuration, dumpFileDir string) error {
	fileInput := &obs.PutFileInput{}
	var filePath string
	if conf.IsCAECustomS3Config {
		fileInput.Bucket = conf.CAECustomS3ConfigInfo.BucketName
		filePath = conf.CAECustomS3ConfigInfo.FilePath
	} else {
		fileInput.Bucket = conf.CustomUploadConfig.BucketName
		filePath = conf.CustomUploadConfig.ObsKey
	}

	fileInput.Key = renameKeyByReplacingSlash(filePath, dumpFileDir)
	fileInput.SourceFile = dumpFileDir

	log.GetLogger().Infof("prepare upload info finished, bucket name: %s, upload obs key: %s, source file: %s",
		fileInput.Bucket, fileInput.Key, fileInput.SourceFile)
	fileSize, err := utilities.GetFileSize(dumpFileDir)
	if err != nil {
		log.GetLogger().Errorf("dump file: %s size is empty: %s", dumpFileDir, err.Error())
		return err
	}
	if fileSize <= 0 {
		return errors.New("dump file size is empty")
	}
	uploadStart := time.Now()
	uploadID, err := d.cli.InitMultipart(&obs.InitiateMultipartUploadInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: fileInput.Bucket,
			Key:    fileInput.Key,
		},
	})
	if err != nil {
		return err
	}
	partResults := d.uploadMultipart(fileSize, dumpFileDir, fileInput, uploadID)
	for _, result := range partResults {
		if result.Err != nil {
			errMsg := fmt.Sprintf("failed to upload multipart due to part %d error: %s",
				result.PartNumber, result.Err.Error())
			return errors.New(errMsg)
		}
	}
	err = d.completeMultipartUpload(fileSize, partResults, fileInput, uploadID)
	if err != nil {
		return err
	}
	log.GetLogger().Infof("finish to upload dump file: %s to obs, upload cost: %s, s3 bucketID: %s,"+
		" objectID: %s, file path: %s, fileSize: %d,", dumpFileDir, time.Since(uploadStart), fileInput.Bucket,
		fileInput.Key, fileInput.SourceFile, fileSize)
	return nil
}

func (d *s3Deployer) completeMultipartUpload(fileSize int64, partResults []PartResult,
	fileInput *obs.PutFileInput, uploadID string) error {
	totalParts := (fileSize + int64(partSize) - 1) / int64(partSize)
	if int(totalParts) != len(partResults) {
		return fmt.Errorf("total parts %d not equals to partResult length %d", totalParts, len(partResults))
	}
	completeParts := make([]obs.Part, totalParts)
	for i := 0; i < int(totalParts); i++ {
		completeParts[i] = obs.Part{
			PartNumber: partResults[i].PartNumber,
			ETag:       partResults[i].ETag,
		}
	}
	err := d.cli.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
		Bucket:   fileInput.Bucket,
		Key:      fileInput.Key,
		UploadId: uploadID,
		Parts:    completeParts,
	})
	if err != nil {
		errMsg := fmt.Sprintf("failed to complete multipart upload error: %s",
			err.Error())
		return errors.New(errMsg)
	}
	return nil
}

func (d *s3Deployer) uploadMultipart(fileSize int64, filePath string,
	fileInput *obs.PutFileInput, uploadID string) []PartResult {
	var wg sync.WaitGroup
	totalParts := (fileSize + int64(partSize) - 1) / int64(partSize)
	partResults := make([]PartResult, totalParts)
	partChan := make(chan int, maxWorkers)
	defer close(partChan)
	for i := 0; i < int(totalParts); i++ {
		partChan <- i
		wg.Add(1)
		go func(partNumber int) {
			defer func() {
				defer wg.Done()
				<-partChan
			}()
			start := int64(partNumber) * int64(partSize)
			end := start + int64(partSize)
			if end > fileSize {
				end = fileSize
			}
			fileBytes, errRead := utilities.GetFileContent(filePath, start, end)
			if errRead != nil {
				log.GetLogger().Errorf("failed to read file for part %d, err: %s",
					partNumber+1, errRead.Error())
				partResults[partNumber] = PartResult{PartNumber: partNumber + 1, Err: errRead}
				return
			}
			d.uploadParts(partNumber, fileInput, uploadID, fileBytes, partResults)
		}(i)
	}
	wg.Wait()
	return partResults
}

func (d *s3Deployer) uploadParts(partNumber int, fileInput *obs.PutFileInput,
	uploadID string, fileBytes []byte, partResults []PartResult) {
	var uploadPartResp = &obs.UploadPartOutput{}
	var err error
	for attempt := 0; attempt < retryTime; attempt++ {
		uploadPartResp, err = d.cli.UploadMultipart(&obs.UploadPartInput{
			Bucket:     fileInput.Bucket,
			Key:        fileInput.Key,
			PartNumber: partNumber + 1,
			UploadId:   uploadID,
			Body:       bytes.NewReader(fileBytes),
		})
		if err == nil {
			break
		}
		log.GetLogger().Warnf("failed to upload part %d, attempt %d, err: %s",
			partNumber+1, attempt+1, err.Error())
		time.Sleep(retryInterval)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to upload part %d after %d retries: %s",
			partNumber+1, retryTime, err.Error())
		partResults[partNumber] = PartResult{PartNumber: partNumber + 1, Err: err}
	} else {
		partResults[partNumber] = PartResult{PartNumber: partNumber + 1, ETag: uploadPartResp.ETag}
	}
}

func renameKeyByReplacingSlash(filePath, dumpFileDir string) string {
	filePath = strings.TrimLeft(filePath, "/")
	dumpFileNameList := strings.Split(dumpFileDir, "/")
	dumpFileName := dumpFileNameList[len(dumpFileNameList)-1]
	obsKey := strings.Replace(filePath+"/"+dumpFileName, "//", "", -1)
	log.GetLogger().Infof("re-format the obs key success, key: %s", obsKey)
	return obsKey
}

func checkDeployPath(deployPath string, conf *config.Configuration) error {
	err := utilities.ValidateFilePath(deployPath)
	if err != nil {
		return fmt.Errorf("invalid deploy path: %s", deployPath)
	}

	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)

	deployFilePath := deployPath
	if err := os.MkdirAll(deployFilePath, worker.DeployFileMode); err != nil {
		return fmt.Errorf("create function deploy path error")
	}
	return nil
}

func isDir(dir string) bool {
	s, err := os.Stat(dir)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func exists(dir string) bool {
	_, err := os.Stat(dir)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func (d *s3Deployer) deployLayers(deployENV, path, URN string, language string) error {
	rawData, err := base64.StdEncoding.DecodeString(deployENV)
	if err != nil {
		log.GetLogger().Errorf("failed to decode layer env: %s", err.Error())
		return err
	}

	var layers []*commontypes.Layer
	if err = json.Unmarshal(rawData, &layers); err != nil {
		log.GetLogger().Errorf("failed to parse layer info: %s", err.Error())
		return err
	}

	for _, layer := range layers {
		log.GetLogger().Infof("layer name is %s, layer type is %v, BucketURL is %s, BucketID=%s, ObjectID=%s",
			layer.Name, layer.DependencyType, layer.BucketURL, layer.BucketID, layer.ObjectID)
		if layer.DependencyType == "public" {
			ok, err := linkPublicDependencies(language, layer, path)
			if err != nil {
				return errors.New("failed to link public dependencies")
			}
			if ok {
				continue
			}
		}
		if err = deployLayerByPreload(layer, language, path); err == nil {
			continue
		}
		deployInfo := &deployDetail{
			BucketID:    layer.BucketID,
			ObjectID:    layer.ObjectID,
			Path:        path,
			Checksum:    layer.Sha256,
			Etag:        layer.ETag,
			DeployLayer: true,
			URN:         URN,
			Language:    language,
		}
		if err = d.deploy(&DurationInfo{}, deployInfo, layer.ObjectID, nil, false); err != nil {
			log.GetLogger().Errorf("failed to deploy layer from S3 BucketID=%s, ObjectID=%s: %s",
				layer.BucketID, layer.ObjectID, err.Error())
			return err
		}
	}
	return nil
}

func linkPublicDependencies(language string, layer *commontypes.Layer, path string) (bool, error) {
	mountPath := constants.PublicDependenciesPath + language + "/" + layer.Name
	if layer.Version != "" {
		mountPath += "@" + layer.Version
	}
	if strings.Contains(language, "nodejs") {
		nodejsDependenciesPath := path + "/node_modules/"
		mountPath += "/node_modules"
		return linkDependencies(nodejsDependenciesPath, mountPath)
	} else if strings.Contains(language, "java") {
		javaDependenciesPath := path + "/lib/"
		return linkDependencies(javaDependenciesPath, mountPath)
	} else if strings.Contains(language, "python") {
		pythonDependenciesPath := path + "/"
		return linkDependencies(pythonDependenciesPath, mountPath)
	} else if strings.Contains(language, "cangjie") {
		cangjieDependenciesPath := path + "/"
		return linkDependencies(cangjieDependenciesPath, mountPath)
	}
	return false, nil
}

func linkDependencies(dependenciesPath, mountPath string) (bool, error) {
	if ok := exists(mountPath); !ok {
		log.GetLogger().Warnf("mount path (%s) does not exist", mountPath)
		return false, nil
	}
	if err := createIfNotExist(dependenciesPath); err != nil {
		return false, err
	}
	dir, err := ioutil.ReadDir(mountPath)
	if err != nil {
		log.GetLogger().Errorf("failed to read mountPath : %s", mountPath)
		return false, err
	}
	for _, fi := range dir {
		dependencies := dependenciesPath + fi.Name()
		ok := exists(dependencies)
		if ok {
			log.GetLogger().Warnf("dependency already exist")
			continue
		}
		err = os.Symlink(mountPath+"/"+fi.Name(), dependencies)
		if err != nil {
			log.GetLogger().Errorf("failed to link public dependencies")
			return false, err
		}
	}
	return true, nil
}

func createIfNotExist(dependenciesPath string) error {
	ok := isDir(dependenciesPath)
	if !ok {
		err := os.Mkdir(dependenciesPath, perm)
		if err != nil {
			log.GetLogger().Errorf("failed to create file: %s", dependenciesPath)
			return err
		}
		err = os.Chown(dependenciesPath, constants.DefaultWorkerUID, constants.DefaultWorkerGID)
		if err != nil {
			log.GetLogger().Errorf("failed to chown dependencies path: %s", err.Error())
			return err
		}
	}
	return nil
}

func (d *s3Deployer) deployFunction(duration *DurationInfo, deployInfo *deployDetail,
	getCodeFinishCh chan struct{}, isSnapshot bool) error {
	if err := deployFunctionByPreload(deployInfo.Path, deployInfo.Checksum, deployInfo.Language); err == nil {
		return nil
	}
	log.GetLogger().Debugf("deploy the function: BucketID=%s, ObjectID=%s", deployInfo.BucketID, deployInfo.ObjectID)
	if err := d.deploy(duration, deployInfo, "", getCodeFinishCh, isSnapshot); err != nil {
		log.GetLogger().Warnf("failed to deploy function from S3: %s", err.Error())
		return err
	}
	return nil
}

type deployOBSFunc func(bucketID string, objectID string, path string,
	layerName string, language string) (buf []byte, size int64, err error)

func (d *s3Deployer) deployUnzippedObject(bucketID string, objectID string, path string,
	layerName string, _ string) ([]byte, int64, error) {
	log.GetLogger().Info("start to get unzipped function object from S3")
	filePath := path
	// parse file name
	arr := strings.Split(objectID, "/")
	if len(arr) == 0 {
		filePath += "/" + objectID
	} else {
		filePath += "/" + arr[len(arr)-1]
	}

	log.GetLogger().Debugf("get object filePath: %s", filePath)

	objectOutput, err := d.cli.GetObject(&obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: bucketID,
			Key:    objectID,
		},
	})
	if err != nil {
		alarmErr := snerror.New(snerror.DownLoadCodeErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.DownLoadCodeErrorCode), err.Error(), bucketID, objectID, layerName))
		log.GetLogger().Errorf("failed to get object: %s", err.Error())
		return nil, 0, alarmErr
	}
	isLayer := len(layerName) != 0
	err = d.decodeAndSaveObject(objectOutput, filePath, isLayer)
	if err != nil {
		log.GetLogger().Errorf("download file to path error: %s", err.Error())
		return nil, 0, err
	}

	if err = os.Chmod(filePath, worker.DeployFileMode); err != nil {
		log.GetLogger().Errorf("failed to chmod object: %s, error: %s", filePath, err.Error())
		return nil, 0, err
	}
	log.GetLogger().Info("succeed to get unzipped function object from S3")
	return nil, 0, nil
}

func (d *s3Deployer) getCodeFromObs(bucketID string, objectID string, path string,
	layerName string, language string) (buffer []byte, objSize int64, err error) {
	log.GetLogger().Infof("start to get function object from S3, bucketID: %s, objectID: %s",
		bucketID, objectID)
	isLayer := len(layerName) != 0
	configuration, err := config.GetConfig()
	if err != nil {
		return nil, 0, err
	}
	if (strings.Contains(language, "java") && d.codePlaintextKmsCipher == "") || isLayer {
		objSize, flag, err := d.tryToDownloadFileBySlice(configuration, bucketID, objectID, path, layerName)
		if err != nil || flag {
			return nil, objSize, err
		}
	}
	objectOutput, err := d.cli.GetObject(&obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: bucketID,
			Key:    objectID,
		},
	})
	if err != nil {
		err = snerror.New(snerror.DownLoadCodeErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.DownLoadCodeErrorCode), err.Error(), bucketID, objectID, layerName))
		return
	}
	if objectOutput.ContentLength > int64(configuration.ZipFileSizeMaxMB<<mbShift) {
		return nil, 0, fmt.Errorf("zip file size is out of range")
	}
	objSize = objectOutput.ContentLength
	if objSize > readAllLimitSize || isLayer {
		err = d.processLayerOrBigObsObj(objectOutput, path, objectID, isLayer)
	} else {
		buffer, err = d.processNormalObsObj(objectOutput)
	}
	log.GetLogger().Info("succeed to get function object from S3")
	return
}

func (d *s3Deployer) processLayerOrBigObsObj(objectOutput *obs.GetObjectOutput, path, objectID string,
	isLayer bool) error {
	objPath := filepath.Join(path, objectID)
	err := d.decodeAndSaveObject(objectOutput, objPath, isLayer)
	if err != nil {
		log.GetLogger().Errorf("download file to path error: %s", err.Error())
		return err
	}
	if err = os.Chmod(objPath, worker.DeployFileMode); err != nil {
		log.GetLogger().Errorf("failed to chmod object: %s, error: %s", objPath, err.Error())
	}
	return err
}

func (d *s3Deployer) processNormalObsObj(objectOutput *obs.GetObjectOutput) ([]byte, error) {
	buffer, err := commonObs.GetOutputObjectBuffer(objectOutput)
	if err != nil {
		log.GetLogger().Errorf("failed to read object: %s", err.Error())
		return buffer, err
	}
	if d.codePlaintextKmsCipher != "" {
		buffer, err = kms.DecryptDataWithAESByPlainCipherToByte(string(buffer), d.codePlaintextKmsCipher)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt data with kms cipher")
			return buffer, err
		}
	}
	return buffer, nil
}

func (d *s3Deployer) decodeAndSaveObject(objectOutput *obs.GetObjectOutput, filePath string, isLayer bool) error {
	var saveErr error
	if d.codePlaintextKmsCipher != "" && !isLayer {
		buffer, err := commonObs.GetOutputObjectBuffer(objectOutput)
		if err != nil {
			log.GetLogger().Errorf("failed to get user code: %s", err.Error())
			return err
		}
		fileBuffer, err := kms.DecryptDataWithAESByPlainCipherToByte(string(buffer), d.codePlaintextKmsCipher)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt user code: %s", err.Error())
			return err
		}
		saveErr = commonObs.DownloadAllFileToPath(filePath, fileBuffer)
	} else {
		_, saveErr = commonObs.DownloadFileToPath(filePath, objectOutput, isLayer)
	}
	return saveErr
}

func (d *s3Deployer) tryToDownloadFileBySlice(configuration *config.Configuration, bucketID string,
	objectID string, path string, layerName string) (objSize int64, flag bool, err error) {
	metadataOutput, err := d.cli.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: bucketID,
		Key:    objectID,
	})
	if err != nil {
		err = snerror.New(snerror.DownLoadCodeErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.DownLoadCodeErrorCode), err.Error(), bucketID, objectID, layerName))
		return
	}
	if metadataOutput == nil {
		return 0, false, nil
	}
	objSize = metadataOutput.ContentLength
	if objSize > int64(configuration.ZipFileSizeMaxMB<<mbShift) {
		return 0, false, fmt.Errorf("zip file size is out of range")
	}
	if objSize > commonObs.PartSize {
		log.GetLogger().Infof("object size: %d, download file to path by slice", objSize)
		objPath := filepath.Join(path, objectID)
		err = commonObs.DownloadFileToPathBySlice(bucketID, objectID, objPath, objSize, *d.cli)
		if err != nil {
			log.GetLogger().Errorf("download file to path error: %s", err.Error())
			return 0, false, err
		}

		if err = os.Chmod(objPath, worker.DeployFileMode); err != nil {
			log.GetLogger().Errorf("failed to chmod object: %s, error: %s", objPath, err.Error())
			return 0, false, err
		}
		return objSize, true, nil
	}
	log.GetLogger().Infof("object size: %d, download file to path directly", objSize)
	return 0, false, nil
}

func (d *s3Deployer) getCodeFromCacheManager(dep *deployDetail) ([]byte, int64, error) {
	codeSize := dep.CodeSize
	start := time.Now()
	var cacheBuffer []byte
	checksum := dep.Checksum
	if dep.DeployLayer {
		checksum = dep.Etag
	}
	req, err := d.makeRequest(dep.ObjectID, checksum, dep.URN, dep.DeployLayer)
	if err != nil {
		log.GetLogger().Errorf("failed to make request: %s", err.Error())
		return nil, 0, err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		log.GetLogger().Warnf("failed to request to cache: %s", err.Error())
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Warnf("failed to get code from cache, error code：%d", resp.StatusCode)
		err = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		return nil, 0, err
	}
	if codeSize > readAllLimitSize || dep.DeployLayer {
		objPath := filepath.Join(dep.Path, dep.ObjectID)
		codeLength, err := commonObs.DownloadFileToPath(objPath, &obs.GetObjectOutput{
			Body:                    resp.Body,
			GetObjectMetadataOutput: obs.GetObjectMetadataOutput{ContentLength: codeSize},
		}, dep.DeployLayer)
		if err != nil {
			log.GetLogger().Errorf("download file to path error: %s", err.Error())
			return nil, 0, err
		}
		if err = os.Chmod(objPath, worker.DeployFileMode); err != nil {
			log.GetLogger().Errorf("failed to chmod object: %s, error: %s", objPath, err.Error())
		}
		if dep.DeployLayer {
			codeSize = codeLength
		}
		log.GetLogger().Infof("object %s is downloaded from the cache-manager", dep.ObjectID)
	} else {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.GetLogger().Errorf("failed to get cache response body: %s", err.Error())
			return nil, 0, err
		}
		cacheBuffer = respBody
	}
	log.GetLogger().Infof("get code from cache-manager cost: %v, length: %d", time.Since(start), codeSize)
	return cacheBuffer, codeSize, nil
}

func (d *s3Deployer) makeRequest(objectID, checksum, URN string, isLayer bool) (*http.Request, error) {
	cacheManagerNameSpace := os.Getenv(constants.SystemNameSpaceEnvKey)
	if cacheManagerNameSpace == "" {
		cacheManagerNameSpace = constants.SystemDefaultNameSpace
	}
	requestURI, err := d.determineCacheManagerSafetyVPC(objectID)
	if err != nil {
		log.GetLogger().Errorf("failed to get cache-manager request url, err: %s", err.Error())
		return nil, err
	}
	request, err := http.NewRequest("GET", requestURI, bytes.NewBuffer([]byte{}))
	if err != nil {
		log.GetLogger().Errorf("action failed when make scaleUp request, err %s", err.Error())
		return nil, err
	}
	request.Header.Set(constants.HeaderFuncURN, URN)
	request.Header.Set(constants.HeaderServiceID, fmt.Sprintf(constants.CacheServiceName, cacheManagerNameSpace))
	request.Header.Set(constants.HeaderSha512, checksum)
	if isLayer {
		request.Header.Set(constants.HeaderIsLayer, "true")
	} else {
		request.Header.Set(constants.HeaderIsLayer, "false")
	}
	return request, nil
}

func (d *s3Deployer) determineCacheManagerSafetyVPC(objectID string) (string, error) {
	cacheManagerNameSpace := os.Getenv(constants.SystemNameSpaceEnvKey)
	if cacheManagerNameSpace == "" {
		cacheManagerNameSpace = constants.SystemDefaultNameSpace
	}
	scheme := tls.GetURLScheme(d.cacheHTTPSEnable)
	var requestURI string
	if d.isSafety {
		if d.cacheManagerServiceIP == "" || net.ParseIP(d.cacheManagerServiceIP) == nil {
			return "", fmt.Errorf("safety vpc set cache-manager service ip: %s is not available",
				d.cacheManagerServiceIP)
		}
		requestURI = scheme + "://" + d.cacheManagerServiceIP + ":" + constants.DefaultCachePort +
			"/v1/load?id=" + objectID
		return requestURI, nil

	}
	requestURI = scheme + "://" + fmt.Sprintf(constants.CacheServiceName, cacheManagerNameSpace) + ":" +
		constants.DefaultCachePort + "/v1/load?id=" + objectID
	return requestURI, nil
}

func preconnectCache(c *config.Configuration) (*http.Client, error) {
	deploy := &s3Deployer{
		cacheHTTPSEnable: c.InternalHTTPSConfig.HTTPSEnable,
	}
	req, err := deploy.makeRequest("pre", c.RuntimeConfig.CodeSha512, c.RuntimeConfig.URN, false)
	if err != nil {
		log.GetLogger().Errorf("failed to make pre request: %s", err.Error())
		return nil, err
	}
	client := getHTTPClient(c.InternalHTTPSConfig.HTTPSEnable)
	go func() {
		buf := make([]byte, cacheLimitSize)
		for {
			resp, err := client.Do(req)
			if err != nil {
				log.GetLogger().Warnf("failed to preconnect cache: %s", err.Error())
				return
			}
			// should discard and close body, otherwise the connection will not be reused
			_, err = io.CopyBuffer(io.Discard, resp.Body, buf)
			if err != nil {
				log.GetLogger().Warnf("failed to copy cache buffer, err: %s", err.Error())
			}
			err = resp.Body.Close()
			if err != nil {
				log.GetLogger().Warnf("failed to close body, err: %s", err.Error())
			}
			time.Sleep(defaultRequestCacheHeartBeat)
		}
	}()
	return client, nil
}

func getHTTPClient(httpsEnable bool) *http.Client {
	timeout := defaultRequestCacheTimeout
	httpClient := &http.Client{Timeout: timeout}
	if httpsEnable {
		tr, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			log.GetLogger().Errorf("failed to assert https default transport")
			return nil
		}
		tr.TLSClientConfig = tls.GetClientTLSConfig()
		httpClient.Transport = tr
	}
	return httpClient
}

func closeConnectionToCacheMgr() {
	go func() {
		cmdLine := fmt.Sprintf("iptables -D OUTPUT -d 'cache-manager.default.svc.cluster.local'/32 " +
			"-p tcp -m tcp --dport " + constants.DefaultCachePort + " -j ACCEPT")
		cmd := exec.Command("/bin/bash", "-c", cmdLine)
		err := cmd.Run()
		if err != nil {
			log.GetLogger().Warnf("failed to drop cache-manager port: %s", err.Error())
		}
	}()
}

type deployFunc func() (buf []byte, size int64, err error)

type deployTask struct {
	name string
	f    deployFunc
}

// concurrentControl controls multi-concurrent deploy tasks
type concurrentControl struct {
	wg           sync.WaitGroup // wg waits all task finish
	tasks        []deployTask
	totalTask    uint32
	successCount uint32        // count of succeed tasks
	errorCount   uint32        // count of failed tasks
	done         chan struct{} // signal that one of tasks is success, or all tasks failed

	buffer   []byte
	objSize  int64
	depError []error
}

func (cc *concurrentControl) run() {
	cc.totalTask = uint32(len(cc.tasks))
	cc.wg.Add(len(cc.tasks))
	cc.depError = make([]error, len(cc.tasks), len(cc.tasks))
	for i := range cc.tasks {
		go cc.runDeployTask(i, cc.tasks[i].name, cc.tasks[i].f)
	}
	go func() {
		cc.wg.Wait()
		select {
		case cc.done <- struct{}{}:
		default:
		}
		log.GetLogger().Infof("all task done")
	}()
}

func (cc *concurrentControl) runDeployTask(index int, name string, depFunc deployFunc) {
	defer cc.wg.Done()
	tempBuffer, tempSize, err := depFunc()
	if err != nil {
		log.GetLogger().Warnf("failed to get obj from %s: %s", name, err.Error())
		if index >= len(cc.depError) || index < 0 {
			log.GetLogger().Errorf("index out of bounds, index: %d, len: %d", index, len(cc.depError))
			return
		}
		cc.depError[index] = err
		return
	}
	// use the first success task's result
	if atomic.AddUint32(&cc.successCount, 1) == 1 {
		log.GetLogger().Infof("use buffer from %s", name)
		cc.buffer, cc.objSize = tempBuffer, tempSize
		select {
		case cc.done <- struct{}{}:
		default:
		}
	}
	return
}

func (d *s3Deployer) generateCacheDeployFunc(dep *deployDetail, getCodeFinishCh chan struct{}) deployFunc {
	return func() ([]byte, int64, error) {
		buf, size, err := d.getCodeFromCacheManager(dep)
		closeConnectionToCacheMgr()
		if getCodeFinishCh != nil {
			close(getCodeFinishCh)
		}
		return buf, size, err
	}
}

func (d *s3Deployer) generateLargeCodeDeployFunc(dep *deployDetail, obsFunc deployOBSFunc, layerName string,
	getCodeFinishCh chan struct{}) deployFunc {
	return func() ([]byte, int64, error) {
		defer func() {
			closeConnectionToCacheMgr()
			if getCodeFinishCh != nil {
				close(getCodeFinishCh)
			}
		}()
		buf, size, err := obsFunc(dep.BucketID, dep.ObjectID, dep.Path, layerName, dep.Language)
		if err == nil {
			return buf, size, nil
		}
		log.GetLogger().Errorf("failed to get from obs: %s", err.Error())
		buf, size, err = d.getCodeFromCacheManager(dep)
		if err != nil {
			log.GetLogger().Errorf("failed to get from cache: %s", err.Error())
			return nil, 0, errors.New("failed to download code from OBS and cache-manager")
		}
		return buf, size, nil
	}
}

func (d *s3Deployer) deploy(duration *DurationInfo, dep *deployDetail, layerName string,
	getCodeFinishCh chan struct{}, isSnapshot bool) error {
	if isSnapshot && storage.GetStorageType() == "data-system" {
		if err := DownloadSnapshotFromDS(dep.BucketID, dep.ObjectID); err != nil {
			// go on downloading from obs when failed to download from data system
			log.GetLogger().Warnf("it will go on downloading from obs when it failed to download snapshot from "+
				"data system with %s, %s, %s", dep.BucketID, dep.ObjectID, err)
		} else {
			// return directly when succeed to download snapshot from data system
			return nil
		}
	}
	if dep.CodePlaintextKmsCipher != "" && !isSnapshot {
		d.codePlaintextKmsCipher = dep.CodePlaintextKmsCipher
	}
	var (
		obsDeployFunc deployOBSFunc = d.getCodeFromObs
		handler       Decompressor  = newZipHandler()
		needUnzip                   = true
	)
	cc := &concurrentControl{
		done: make(chan struct{}, 1),
	}

	if strings.HasSuffix(dep.ObjectID, ".tar.zst") {
		handler = newZstdHandler()
	}
	// Deploy snapshot process do not mark unzip false
	if !strings.HasSuffix(dep.ObjectID, ".zip") && !strings.HasSuffix(dep.ObjectID, ".tar.zst") && !isSnapshot {
		needUnzip = false
		obsDeployFunc = d.deployUnzippedObject
	}
	if dep.CodeSize <= limitCodeSize && !dep.DeployLayer {
		// deploy code from cache and obs concurrently
		cc.tasks = append(cc.tasks,
			deployTask{name: "obs", f: func() ([]byte, int64, error) {
				return obsDeployFunc(dep.BucketID, dep.ObjectID, dep.Path, layerName, dep.Language)
			}},
			deployTask{name: "cache-manager", f: d.generateCacheDeployFunc(dep, getCodeFinishCh)},
		)
	} else {
		cc.tasks = append(cc.tasks, deployTask{name: "largeCode", f: d.generateLargeCodeDeployFunc(dep,
			obsDeployFunc, layerName, getCodeFinishCh)})
	}

	cc.run()

	select {
	// cc.done will be notified when one of tasks succeed, or tasks all failed
	case <-cc.done:
		// if all tasks failed, return error directly
		if atomic.LoadUint32(&cc.successCount) == 0 {
			// we only care about errors of obs and orchestrate obs as the first task
			return cc.depError[0]
		}
	case <-time.After(time.Duration(d.ioReadTimeout) * time.Millisecond):
		err := snerror.New(snerror.WorkerInternalErrorCode, "deploy code timed out")
		log.GetLogger().Errorf("deploy code timed out, objID: %s, layerName: %s", dep.ObjectID, layerName)
		return err
	}

	if !needUnzip {
		return nil
	}
	if isSnapshot {
		if dep.FastStartEnable {
			handler = newSnapshotZstdHandler()
		} else {
			handler = newSnapshotZipHandler()
		}
	}

	return d.decompress(duration, handler, cc.objSize, dep, cc.buffer)
}

func (d *s3Deployer) decompress(duration *DurationInfo, handler Decompressor, objSize int64,
	dep *deployDetail, buffer []byte) error {
	if err := checkHash(objSize, filepath.Join(dep.Path, dep.ObjectID), buffer, dep.Checksum); err != nil {
		return err
	}
	start := time.Now()
	log.GetLogger().Infof("current decompress, dep.Path %s, dep.ObjectID %s, len buffer %d",
		dep.Path, dep.ObjectID, len(buffer))
	err := handler.Decompress(objSize, filepath.Join(dep.Path, dep.ObjectID), buffer, dep.Path, dep.DeployLayer)
	if err != nil {
		return err
	}
	cost := time.Since(start)
	duration.Decompress = cost
	log.GetLogger().Infof("succeed to decompress with path %s, objectID %s, len buffer %d, cost %s",
		dep.Path, dep.ObjectID, len(buffer), cost)
	return nil
}

func checkHash(objectSize int64, downloadFilePath string, buffer []byte, checksum string) error {
	if checksum == "" {
		log.GetLogger().Warnf("skip checking because checksum is empty")
		return nil // skip checking if no checksum generated
	}

	var err error
	if objectSize > readAllLimitSize {
		err = checkFileHash(downloadFilePath, checksum)
	} else {
		err = checkBufferHash(buffer, checksum)
	}
	return err
}

func checkFileHash(filename string, checksum string) error {
	f, err := os.Open(filename)
	if err != nil {
		log.GetLogger().Errorf("failed to open file %s", filename)
		return err
	}

	// close file when fail or succeed
	defer func() {
		err := f.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close file %s: %s", f, err.Error())
		}
	}()

	hasher := sha512.New()
	if len(checksum) <= sha256.BlockSize {
		hasher = sha256.New()
	}

	if _, err = io.Copy(hasher, f); err != nil {
		log.GetLogger().Errorf("calculate hash: %s", err.Error())
		return err
	}

	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != checksum {
		log.GetLogger().Errorf("checksum in etcd: %s, checksum of the package: %s", checksum, actual)
		return snerror.New(snerror.CheckSumErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.CheckSumErrorCode), "check file sum error"))
	}
	return nil
}

func checkBufferHash(buffer []byte, checksum string) error {
	hasher := sha512.New()
	if len(checksum) <= sha256.BlockSize {
		hasher = sha256.New()
	}

	n, err := hasher.Write(buffer)
	if n != len(buffer) || err != nil {
		log.GetLogger().Errorf("hasher write num %d, buffer len %d, return %v", n, len(buffer), err)
		return snerror.New(snerror.CheckSumErrorCode, fmt.Sprintf(snerror.ErrText(
			snerror.CheckSumErrorCode), "write SHA buffer error when checking file sum"))
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != checksum {
		log.GetLogger().Errorf("expect checksum: %s, actual checksum: %s", checksum, actual)
		return snerror.New(snerror.CheckSumErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.CheckSumErrorCode), "check file sum error"))
	}
	return nil
}

// DownloadSnapshot download snapshot from s3
func (d *s3Deployer) DownloadSnapshot(conf *config.Configuration, snapshotConfig config.SnapshotConfig,
	language string) error {
	startTime := time.Now()

	if conf.Access != "" {
		if err := d.RefreshToken(conf); err != nil {
			return err
		}
	}
	defer commonObs.ClearSecretKeyMemory()

	utilities.SetSnapshotImagePathForWorker(constants.SnapshotImageDownloadPath)
	bucketID := snapshotConfig.SnapshotBucketID
	objectID := snapshotConfig.SnapshotObjectID
	snapshotFilePath := utilities.GetSnapshotImagePathForWorker()
	err := checkDeployPath(snapshotFilePath, conf)
	if err != nil {
		return err
	}

	log.GetLogger().Infof("start to download snapshot with bucketID: %s, objectID: %s", bucketID, objectID)
	buf, size, err := d.getCodeFromObs(bucketID, objectID, snapshotFilePath, "", language)
	if err != nil {
		log.GetLogger().Errorf("failed to get from obs: %s with bucketID: %s, objectID: %s", err.Error(),
			bucketID, objectID)
		return err
	}

	var handler Decompressor
	if snapshotConfig.FastStartEnable {
		handler = newSnapshotZstdHandler()
	} else {
		handler = newSnapshotZipHandler()
	}

	// Deploy snapshot
	deployInfo := &deployDetail{
		BucketID: bucketID,
		ObjectID: objectID,
		Path:     snapshotFilePath,
		Checksum: snapshotConfig.SnapshotSha256,
	}
	if err = d.decompress(&DurationInfo{}, handler, size, deployInfo, buf); err != nil {
		log.GetLogger().Errorf("failed to decompress snapshot with bucketID: %s, objectID: %s", bucketID, objectID)
		return err
	}

	log.GetLogger().Infof("succeed to download snapshot with bucketID: %s, objectID: %s, costTime: %s",
		bucketID, objectID, time.Now().Sub(startTime))
	return nil
}

// DownloadSnapshotWithRetry -
func (d *s3Deployer) DownloadSnapshotWithRetry(conf *config.Configuration, snapshotConfig config.SnapshotConfig,
	language string, traceID string) (bool, error) {
	var err error
	for i := 0; i < retryTime; i++ {
		err = d.DownloadSnapshot(conf, snapshotConfig, language)
		if err != nil {
			log.GetLogger().Warnf("err: %s, traceID: %s", err.Error(), traceID)
		} else {
			break
		}

		if i != retryTime-1 {
			time.Sleep(retryInterval)
		}

		if i >= retryTime-1 {
			return true, err
		}
	}
	return false, err
}

func (d *s3Deployer) DeleteSnapshot(conf *config.Configuration, bucketID, objectID string,
	needRemoveObjectIDs []string) error {
	if len(needRemoveObjectIDs) == 0 {
		return nil
	}

	if conf.Access != "" {
		if err := d.RefreshToken(conf); err != nil {
			log.GetLogger().Errorf("failed to refresh ccms token when deploy function with error %s", err.Error())
			return err
		}
	}
	log.GetLogger().Info("start to delete snapshot data")
	var dataSystemErr, obsErr error
	if storage.GetStorageType() == constants.DataSystemStorageType {
		dataSystemErr = DeleteSnapshotFromDS(bucketID, objectID, needRemoveObjectIDs)
		if dataSystemErr != nil {
			log.GetLogger().Errorf("delete snapshot data from data-system with bucketID: %s, objectID: %s, "+
				"error: %s", bucketID, objectID, dataSystemErr.Error())
		}
	}

	obsErr = d.deleteSnapshotFromObs(bucketID, needRemoveObjectIDs)
	if obsErr != nil {
		log.GetLogger().Errorf("failed to delete snapshot data from obs with bucketID: %s, objectID: %s, err: %s",
			bucketID, objectID, obsErr.Error())
	}

	if dataSystemErr == nil && obsErr == nil {
		log.GetLogger().Infof("success to delete snapshot data from obs with bucketID: %s, objectID: %s,"+
			" needRemoveObjectIDs: %s", bucketID, objectID, needRemoveObjectIDs)
		return nil
	}

	return fmt.Errorf("delete snapshot data error, data-system error: %v | obs errror: %v", dataSystemErr, obsErr)
}

func (d *s3Deployer) deleteSnapshotFromObs(bucketID string, needRemoveObjectIDs []string) error {
	if len(needRemoveObjectIDs) == 0 {
		return nil
	}
	log.GetLogger().Infof("begin to delete snapshot data from obs with bucketID: %s, needRemoveObjectIDs: %s",
		bucketID, needRemoveObjectIDs)

	for i := 0; i < len(needRemoveObjectIDs); i++ {
		_, err := d.cli.DeleteObject(&obs.DeleteObjectInput{
			Bucket: bucketID,
			Key:    needRemoveObjectIDs[i],
		})
		if err != nil {
			log.GetLogger().Errorf("failed to delete snapshot data from obs with bucketID: %s, objectID: %s, err: %s",
				bucketID, needRemoveObjectIDs[i], err.Error())
			return err
		}
		log.GetLogger().Infof("success to delete snapshot data from obs with bucketID: %s, history objectID: %s",
			bucketID, needRemoveObjectIDs[i])
	}
	return nil
}
