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
package server

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/cache-manager/pkg/healthcheck"
	"functioncore/pkg/cache-manager/pkg/status"
	"functioncore/pkg/cache-manager/pkg/utils"
	"functioncore/pkg/cache-manager/render"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/strutil"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/tokenrotation"
	commonutils "functioncore/pkg/common/utils"
)

const (
	defaultRetryTimes      = 3
	defaultRetryDuration   = 1 * time.Second
	defaultHttpHeaderLimit = 1048576
	perm                   = 0600
)

var (
	// EvsEnable is used to check whether EVS is enabled.
	EvsEnable = os.Getenv("EVS_ENABLE") == "true"
)

// Deployer deploy function and layer
type Deployer interface {
	Deploy(info *config.FunctionMetaInfo) error
}

type s3Deployer struct {
	cli           *commonObs.Client
	smallCodePool *ants.Pool
	largeCodePool *ants.Pool
	limitCodeSize int
	evsEnable     bool
}

// CodeCache contains the code buffer and the checksum
type CodeCache struct {
	// CodeBuffer is the code package buffer
	CodeBuffer []byte
	// Checksum is the code package checksum
	Checksum string
}

// CacheMap stores code buffer in cache-manager
var CacheMap sync.Map

// LruCacheMap is the LRU cache structure of CacheMap.
var LruCacheMap *utils.Lru

// CachePath defines the persistent path
var CachePath string

// HTTPServerCreate create and run cache-manager http api server
func HTTPServerCreate(conf *config.CacheManagerConfig) error {
	// build HTTP server
	podIP := os.Getenv(constants.PodIPEnvKey)
	address := fmt.Sprintf(podIP+":%v", constants.DefaultCachePort)
	server := &http.Server{
		Addr:           address,
		Handler:        buildHandler(),
		MaxHeaderBytes: defaultHttpHeaderLimit,
	}
	log.GetLogger().Infof("server is running at address: %s", address)

	if !conf.HTTPSConfig.HTTPSEnable {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.GetLogger().Errorf("http server error: %s", err.Error())
			return err
		}
		return errors.New("http server error")
	}

	err := tls.HTTPListenAndServeTLS(address, server.Handler)
	if err != nil {
		log.GetLogger().Errorf("error when executes FastHTTPListenAndServeTLS: %s", err.Error())
		return err
	}
	return errors.New("https server error")
}

func buildHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	healthcheck.RegisterHandlers(router)
	rg := router.Group("/v1")
	RegisterHandlers(rg.Group(""))
	return router
}

// NewDeployer -
func NewDeployer(c *config.CacheManagerConfig) (Deployer, error) {
	var authString = ""
	if ak, _, _ := tokenrotation.GetTempToken(); ak == "" {
		// attempt to use fixed key
		auth, err := localauth.Decrypt(c.S3Config.SecretKey)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt obs secret: %s", err.Error())
			return nil, err
		}
		authString = string(auth)
		commonutils.ClearByteMemory(auth)
	}
	opt := commonObs.Option{
		Endpoint:       c.S3Config.Endpoint,
		CaFile:         c.S3Config.CaFile,
		Secure:         c.S3Config.UseSSL,
		AccessKey:      c.S3Config.AccessKey,
		SecretKey:      authString,
		TrustedCA:      c.S3Config.TrustedCA,
		MaxRetryCount:  commonutils.ObsMaxRetry,
		Timeout:        commonutils.ObsDefaultTimeout,
		ConnectTimeout: commonutils.ObsDefaultConnectTimeout,
	}
	s3Client, err := commonObs.NewObsClient(opt)
	if err != nil {
		commonutils.ClearStringMemory(authString)
		commonObs.ClearSecretKeyMemory()
		log.GetLogger().Errorf("failed to init S3 client: %s", err.Error())
		return nil, err
	}
	sp, err := ants.NewPool(c.SmallCodePoolSize, ants.WithPreAlloc(true))
	if err != nil {
		log.GetLogger().Errorf("failed to init small code pool %s", err.Error())
		return nil, err
	}
	lp, err := ants.NewPool(c.LargeCodePoolSize, ants.WithPreAlloc(true))
	if err != nil {
		log.GetLogger().Errorf("failed to init large code pool %s", err.Error())
		return nil, err
	}

	return &s3Deployer{
		cli:           s3Client,
		smallCodePool: sp,
		largeCodePool: lp,
		limitCodeSize: c.LimitCodeSize,
		evsEnable:     EvsEnable,
	}, nil
}

// RegisterHandlers sets up the routing of the HTTP handlers.
func RegisterHandlers(r *gin.RouterGroup) {
	r.GET("/load", loadFunction)
}

func (s *s3Deployer) refreshToken(ak, sk, token string) error {
	secretKey, err := localauth.Decrypt(sk)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt S3 SecretKey")
		return err
	}
	securityToken, err := localauth.Decrypt(token)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt S3 SecurityToken")
		commonutils.ClearByteMemory(secretKey)
		return err
	}
	s.cli.Refresh(ak, secretKey, securityToken)
	return nil
}

func (s *s3Deployer) deployFunction(info *config.FunctionMetaInfo) {
	bucketID := info.S3MetaData.BucketID
	objectID := info.S3MetaData.ObjectID

	task := func() {
		if err := s.DownloadObject(bucketID, objectID, info, false, ""); err != nil {
			log.GetLogger().Warnf("failed to download object %s %s, err: %s",
				info.FuncMetaData.FunctionURN, objectID, err.Error())
			return
		}
		log.GetLogger().Infof("deploy function successfully %s", info.FuncMetaData.FunctionURN)
	}

	if info.FuncMetaData.CodeSize <= s.limitCodeSize {
		s.smallCodePool.Submit(task)
	} else {
		s.largeCodePool.Submit(task)
	}
}

func (s *s3Deployer) deployLayers(info *config.FunctionMetaInfo) {
	layers := info.FuncMetaData.Layers
	if len(layers) == 0 {
		return
	}
	for i := 0; i < len(layers); i++ {
		if layers[i] == nil {
			continue
		}
		if layers[i].DependencyType == "public" {
			continue
		}
		bucketID := layers[i].BucketID
		objectID := layers[i].ObjectID
		// The layer does not contain SHA256. The Etag is used to determine whether the file is unique.
		checksum := layers[i].ETag
		layerTask := func() {
			if err := s.DownloadObject(bucketID, objectID, info, true, checksum); err != nil {
				log.GetLogger().Warnf("failed to deploy func %s %s, err: %s",
					info.FuncMetaData.FunctionURN, objectID, err.Error())
				return
			}
			log.GetLogger().Infof("deploy layer successfully %s", info.FuncMetaData.FunctionURN)
		}
		s.largeCodePool.Submit(layerTask)
	}
}

// Deploy implements Deployer.Deploy
func (s *s3Deployer) Deploy(info *config.FunctionMetaInfo) error {
	if ak, sk, token := tokenrotation.GetTempToken(); ak != "" {
		if err := s.refreshToken(ak, sk, token); err != nil {
			return err
		}
	}
	s.deployFunction(info)
	if s.evsEnable {
		s.deployLayers(info)
	}
	return nil
}

// DownloadObject indicates that the file is downloaded from OBS
func (s *s3Deployer) DownloadObject(bucketID string, objectID string, info *config.FunctionMetaInfo,
	isLayer bool, checksum string) error {
	log.GetLogger().Infof("deploy the function: BucketID=%s, ObjectID=%s", bucketID, objectID)
	if info.FuncMetaData.VPCTriggerImage != "" || info.ExtendedMetaData.CustomContainerConfig.Image != "" {
		return nil
	}
	if err := s.DetermineFile(bucketID, objectID, info, isLayer, checksum); err != nil {
		return err
	}
	return nil
}

// DetermineFile determine file type
func (s *s3Deployer) DetermineFile(bucketID string, objectID string, info *config.FunctionMetaInfo,
	isLayer bool, checksum string) error {
	if strings.Contains(objectID, ".") && !strings.HasSuffix(objectID, ".zip") &&
		!strings.HasSuffix(objectID, ".tar.zst") {
		return errors.New("unsupported unzip file")
	} else {
		key := info.FuncMetaData.FunctionVersionURN + "-" + objectID
		if GetObjSizeCacheInstance().IsObjInDisk(info, checksum, key, isLayer) {
			log.GetLogger().Infof("obj %s is in disk, not need be persistent", key)
			return nil
		}
		err := s.GetObject(bucketID, objectID, info, isLayer, checksum)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetObject get object
func (s *s3Deployer) GetObject(bucketID string, objectID string, info *config.FunctionMetaInfo,
	isLayer bool, checksum string) error {
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
		return err
	}
	alarm.GetClient().ClearAlarm(alarm.OBSConnection)
	buffer, err := commonObs.GetOutputObjectBuffer(objectOutput)
	if err != nil {
		log.GetLogger().Errorf("failed to read object %s: %s", objectID, err.Error())
		return err
	}
	curChecksum := checksum
	if !isLayer {
		curChecksum = utils.GenerateChecksum(buffer, info)
	}
	key := info.FuncMetaData.FunctionVersionURN + "-" + objectID
	GetObjSizeCacheInstance().UpdateObjSizeAndCheckSum(key, (int64)(len(buffer)), curChecksum)
	if LruCacheMap != nil {
		LruCacheMap.AddCache(objectID, CodeCache{
			CodeBuffer: buffer,
			Checksum:   curChecksum,
		}, int64(len(buffer)))
	}
	if status.Get() == status.Persistence {
		persistence(objectID, buffer, curChecksum, info.FuncMetaData.FunctionVersionURN, isLayer)
	}
	return nil
}

func persistence(objectID string, buffer []byte, checksum string, urn string, isLayer bool) {
	results := utils.ParseObjectID(objectID, isLayer)
	name := utils.GenerateMD5(results)
	fw, err := os.OpenFile(CachePath+"/"+name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		log.GetLogger().Errorf("failed to open cache file %s", objectID)
		return
	}
	defer fw.Close()
	w := bufio.NewWriter(fw)
	codes, err := render.Marshal(objectID, buffer, checksum)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal cache code %s: %s", objectID, err.Error())
		return
	}
	result, err := localauth.Encrypt(strutil.BytesToString(codes))
	if err != nil {
		log.GetLogger().Errorf("failed to encrypt cache code %s: %s", objectID, err.Error())
		return
	}
	_, err = w.Write(strutil.StringToBytes(result))
	if err != nil {
		log.GetLogger().Errorf("failed to write cache code file %s: %s", objectID, err.Error())
		return
	}
	err = w.Flush()
	if err != nil {
		log.GetLogger().Errorf("failed to flush cache code file %s: %s", objectID, err.Error())
		return
	}
	key := urn + "-" + objectID
	GetObjSizeCacheInstance().UpdateObjPersistName(key, name)
}

func loadFunction(c *gin.Context) {
	time1 := time.Now()
	objectID := c.Query("id")
	sha512 := c.GetHeader(constants.HeaderSha512)
	service := c.GetHeader(constants.HeaderServiceID)
	urn := c.GetHeader(constants.HeaderFuncURN)
	isLayer := c.GetHeader(constants.HeaderIsLayer)
	if service != fmt.Sprintf(constants.CacheServiceName, commonutils.PodNameSpace()) {
		c.JSON(http.StatusUnauthorized, nil)
		return
	}
	if objectID == "pre" {
		c.JSON(http.StatusBadRequest, nil)
		return
	}
	key := urn + "-" + objectID
	if EvsEnable && !monitor.GetMemInstance().AllowWithCondition(func() bool {
		// the lower threshold is reached, which is greater than the 100k code package limit
		if GetObjSizeCacheInstance().CheckObjInMemThreshold(key) {
			log.GetLogger().Warnf("The memory reaches a lowerThreshold. " +
				"Only code packages smaller than 100 KB are supported.")
			return true
		}
		return false
	}) {
		c.JSON(http.StatusBadRequest, nil)
		return
	}
	codeIF, ok := LruCacheMap.GetCache(objectID, isLayer == "true")
	if !ok {
		log.GetLogger().Errorf("load function error, objectID: %s", objectID)
		c.JSON(http.StatusBadRequest, nil)
		return
	}
	codeCache, transOK := codeIF.(CodeCache)
	if !transOK {
		log.GetLogger().Errorf("transform codeCache error, objectID: %s", objectID)
		c.JSON(http.StatusBadRequest, nil)
		return
	}
	if sha512 != codeCache.Checksum {
		log.GetLogger().Errorf("check checksum error, objectID: %s, received checksum: %s, stored checksum:"+
			" %s", objectID, sha512, codeCache.Checksum)
		c.JSON(http.StatusBadRequest, nil)
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", codeCache.CodeBuffer)
	log.GetLogger().Infof("load function %s cost: %v, checksum: %s", objectID, time.Since(time1), sha512)
	return
}

func getCacheMapNum() int {
	mapNum := 0
	CacheMap.Range(func(k, v interface{}) bool {
		mapNum++
		return true
	})
	return mapNum
}

// PrintInputLog prints health log
func PrintInputLog() {
	log.GetLogger().Infof("cache-manager is alive. The number of function caches is %d", getCacheMapNum())
	return
}
