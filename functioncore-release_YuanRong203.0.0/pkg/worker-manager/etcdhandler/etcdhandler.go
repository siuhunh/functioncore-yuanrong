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

// Package etcdhandler handle etcd
package etcdhandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

const (
	// PutOperation do worker cache check while OperateETCDWithRetry
	PutOperation = "PUT"
	// UpdateOperation skip worker cache check while OperateETCDWithRetry
	UpdateOperation = "UPDATE"
	// DelOperation -
	DelOperation   = "DEL"
	refreshSeconds = 15

	notifyWorkerTimeout = 3 * time.Second
)

var (
	leaseID         clientv3.LeaseID
	refreshInterval       = refreshSeconds * time.Second
	leaseTTL        int64 = 30
	leaseTicker     *time.Ticker
	localCluster    = os.Getenv("CLUSTER_ID")
)

// OperateETCD -
func OperateETCD(ctx context.Context, path, value, operate string) error {
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, etcd3.DurationContextTimeout)
	if operate == PutOperation || operate == UpdateOperation {
		return etcd3.GetRouterEtcdClient().Put(defaultEtcdCtx, path, value)
	} else if operate == DelOperation {
		return etcd3.GetRouterEtcdClient().Delete(defaultEtcdCtx, path, clientv3.WithPrefix())
	} else {
		return fmt.Errorf("unknown etcd operation with %s", operate)
	}
}

// OperateCaeRouterETCD -
func OperateCaeRouterETCD(ctx context.Context, path, value, operate string) error {
	if etcd3.GetCaeRouterEtcdClient() == nil || etcd3.GetCaeRouterEtcdClient().Client == nil {
		log.GetLogger().Errorf("failed to get global router etcd client")
		return errors.New("failed to get global router etcd client")
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, etcd3.DurationContextTimeout)
	if operate == PutOperation || operate == UpdateOperation {
		return etcd3.GetCaeRouterEtcdClient().Put(defaultEtcdCtx, path, value)
	} else if operate == DelOperation {
		return etcd3.GetCaeRouterEtcdClient().Delete(defaultEtcdCtx, path, clientv3.WithPrefix())
	} else {
		return fmt.Errorf("unknown cae router etcd operation with %s", operate)
	}
}

// OperateETCDWithRetry -
func OperateETCDWithRetry(ctx context.Context, path, value, operate string) error {
	gCtx, cancel := context.WithTimeout(ctx, etcd3.DurationContextTimeout)
	defer cancel()
	return wait.ExponentialBackoffWithContext(gCtx, wait.Backoff{
		Duration: utils.PutToRouterETCDInterval,
		Steps:    utils.MaxPutToRouterETCDRetryTimes,
	}, func(context.Context) (done bool, err error) {
		if operate == PutOperation {
			if !checkWorkerCache(path) {
				return true, nil
			}
		}
		if err = OperateETCD(gCtx, path, value, operate); err != nil {
			log.GetLogger().Errorf("failed to operate etcd, operation: %s, path: %s, err: %s",
				operate, path, err.Error())
			// operate etcd will retry when return false, nil
			// if operating router etcd fails, no need to write to global router etcd
			return false, nil
		}
		// operate etcd successfully
		return true, nil
	})
}

// QueryETCDWithRetry -
func QueryETCDWithRetry(ctx context.Context, path string) (string, error) {
	var res string
	return res, wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
		Duration: utils.PutToRouterETCDInterval,
		Steps:    utils.MaxPutToRouterETCDRetryTimes,
	}, func(context.Context) (done bool, err error) {
		etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(ctx, etcd3.DurationContextTimeout)
		results, err := etcd3.GetRouterEtcdClient().GetValues(etcdCtx, path)
		if err != nil {
			log.GetLogger().Errorf("failed to query etcd with path: %s, err: %s", path, err.Error())
			return false, nil
		}
		if len(results) > 0 {
			res = results[0]
		}
		return true, nil
	})
}

// OperateCAEETCDWithRetry -
func OperateCAEETCDWithRetry(ctx context.Context, path, value, operate string) error {
	return wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
		Duration: utils.PutToRouterETCDInterval,
		Steps:    utils.MaxPutToRouterETCDRetryTimes,
	}, func(context.Context) (done bool, err error) {
		if operate == PutOperation {
			exists := checkWorkerCache(path)
			if !exists {
				return true, nil
			}
		}
		if err = OperateCaeRouterETCD(ctx, path, value, operate); err != nil {
			log.GetLogger().Errorf("failed to operate cae router etcd, operation: %s, path: %s, err: %s",
				operate, path, err.Error())
			// operate cae router etcd will retry when return false, nil
			// if operating router etcd fails, no need to write to global router etcd
			return false, nil
		}
		// operate car router etcd successfully
		return true, nil
	})
}

func checkWorkerCache(path string) bool {
	instanceKey := etcdkey.WorkerInstanceKey{}
	if err := instanceKey.ParseFrom(path); err != nil {
		log.GetLogger().Errorf("invalid worker instance instanceKey: %s, err: %s", path, err.Error())
		return false
	}
	podName := utils.GetPodNameFromInstanceID(instanceKey.Instance)
	funcSvcID := utils.CrName(instanceKey.BusinessID, instanceKey.TenantID,
		instanceKey.Function, instanceKey.Version)
	if _, ok := cache.WorkerCache.GetWorkerInstance(funcSvcID, podName); !ok {
		// stop retry and remove this key if worker cache has no this instance when put to etcd
		log.GetLogger().Errorf("worker instance is not exists for: %s with: %s", funcSvcID, podName)
		return false
	}
	return true
}

// RegisterToCAEMetaETCD -
func RegisterToCAEMetaETCD(stopCh <-chan struct{}) {
	instanceKey, err := getWorkerManagerInstanceKey()
	err = putInstanceInfoToEtcd(instanceKey)
	if err != nil {
		log.GetLogger().Errorf("failed to register instance to etcd when start, error:%s", err.Error())
		return
	}
	go startRefreshLeaseJob(instanceKey, stopCh)
	return
}

func startRefreshLeaseJob(instanceKey string, stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil, will not refresh lease")
		return
	}
	leaseTicker = time.NewTicker(refreshInterval)
	for {
		select {
		case <-leaseTicker.C:
			refreshLease(instanceKey)
		case <-stopCh:
			leaseTicker.Stop()
			caeMetaClient := etcd3.GetCAEMetaEtcdClient()
			if caeMetaClient == nil || caeMetaClient.Client == nil {
				log.GetLogger().Warnf("failed to init cae meta ETCD client")
				return
			}
			revokeCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err := caeMetaClient.Revoke(revokeCtx, leaseID)
			if err != nil {
				log.GetLogger().Warnf("revoke lease failed, err:%s", err.Error())
			}

			ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err = caeMetaClient.Delete(ctx, instanceKey)
			if err != nil {
				log.GetLogger().Warnf("delete key:%s failed, err:%s", instanceKey, err.Error())
			}
			return
		}
	}
}

func refreshLease(instanceKey string) {
	if !isKeyExist(instanceKey) {
		if err := putInstanceInfoToEtcd(instanceKey); err != nil {
			return
		}
	}
	caeMetaEtcd := etcd3.GetCAEMetaEtcdClient()
	if caeMetaEtcd == nil || caeMetaEtcd.Client == nil {
		log.GetLogger().Warnf("failed to init cae meta ETCD client")
		return
	}
	keepAliveOnceCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err := caeMetaEtcd.KeepAliveOnce(keepAliveOnceCtx, leaseID)
	if err != nil {
		log.GetLogger().Warnf("unable to refresh lease:%s", err.Error())
	}
}

func isKeyExist(instanceKey string) bool {
	metaCaeEtcd := etcd3.GetCAEMetaEtcdClient()
	if metaCaeEtcd == nil || metaCaeEtcd.Client == nil {
		log.GetLogger().Warnf("failed to init cae meta ETCD client")
		return false
	}
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := metaCaeEtcd.GetResponseWithSerializable(ctx, instanceKey,
		clientv3.WithKeysOnly())
	if err != nil {
		log.GetLogger().Errorf("failed to get etcd key:%s, err:%s", instanceKey, err)
		return false
	}
	return len(resp.Kvs) > 0
}

func putInstanceInfoToEtcd(instanceKey string) error {
	caeMetaClient := etcd3.GetCAEMetaEtcdClient()
	if caeMetaClient == nil || caeMetaClient.Client == nil {
		log.GetLogger().Warnf("failed to get cae meta ETCD client")
		return errors.New("failed to get cae meta ETCD client")
	}
	grantCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	id, err := caeMetaClient.Grant(grantCtx, leaseTTL)
	if err != nil {
		log.GetLogger().Errorf("failed to grant worker-manager instance lease: %s", err.Error())
		return err
	}
	leaseID = id
	if leaseTicker != nil {
		leaseTicker.Reset(refreshInterval)
	}

	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = caeMetaClient.Put(ctx, instanceKey, "active", clientv3.WithLease(leaseID))
	if err != nil {
		log.GetLogger().Errorf("failed to put key:%s to cae meta etcd, err:%s", instanceKey, err.Error())
		return err
	}
	log.GetLogger().Infof("register instance:%s to cae meta etcd", instanceKey)
	return nil
}

func getWorkerManagerInstanceKey() (string, error) {
	clusterID := os.Getenv("CLUSTER_ID")
	nodeIP := os.Getenv("NODE_IP")
	podName := os.Getenv("POD_NAME")
	err := validateEnvs(clusterID, nodeIP, podName)
	if err != nil {
		return "", err
	}

	key := fmt.Sprintf("/sn/worker-manager/instances/%s/%s/%s", clusterID, nodeIP, podName)
	return key, nil
}

func validateEnvs(clusterID, nodeIP, podName string) error {
	if clusterID == "" {
		log.GetLogger().Errorf("can not find CLUSTER_ID env, can not register worker-manager info to etcd")
		return fmt.Errorf("CLUSTER_ID env not found")
	}

	if nodeIP == "" {
		log.GetLogger().Errorf("can not find NODE_IP env, can not register worker-manager info to etcd")
		return fmt.Errorf("NODE_IP env not found")
	}

	if podName == "" {
		log.GetLogger().Errorf("can not find POD_NAME env, can not register worker-manager info to etcd")
		return fmt.Errorf("POD_NAME env not found")
	}

	return nil
}

// PutInitInstanceToCAERouterEtcd put init instance info to cae router etcd
func PutInitInstanceToCAERouterEtcd(ctx context.Context, pod *corev1.Pod, spec *faasfunction.FunctionServiceSpec) {
	instance := utils.MakeInitWorkerIns(pod, spec)
	instance.CreationTime = time.Now().Unix()
	path := utils.GenerateInitInstanceKey(instance)
	val, err := json.Marshal(instance)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal init instance: %s for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
		return
	}
	if err = OperateCaeRouterETCD(ctx, path, string(val), PutOperation); err != nil {
		log.GetLogger().Warnf("failed to put init instance %s to cae router etcd for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	} else {
		log.GetLogger().Infof("succeed to put init instance %s to cae router etcd for: %s", instance.InstanceID,
			instance.FuncName)
	}
}

func getNeedDeleteObjectIDs(meta *types.SnapshotMeta, caeMetaEtcd *etcd3.EtcdClient) ([]string, error) {
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	values, err := caeMetaEtcd.GetValues(ctx, etcd3.HandleCellPrefix(meta.ETCDKey))
	if err != nil {
		log.GetLogger().Errorf("failed to get key: %s to cae meta etcd, err:%s", meta.ETCDKey, err.Error())
		return nil, err
	}
	if len(values) == 0 {
		log.GetLogger().Errorf("failed to get value from etcd, key: %s", meta.ETCDKey)
		return nil, errors.New("failed to get values")
	}
	var snapshotResult types.SnapshotDeploymentSpec
	err = json.Unmarshal([]byte(values[0]), &snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal snapshot result: %s for: %s", meta.ETCDKey, err.Error())
		return nil, err
	}

	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	curImageTags := utils.SplitImageTagByColon(cfg.PoolMgrConfig.PoolContainerTags.RuntimeImageTag)
	var needDeleteObjectIDs []string
	for i := 0; i < len(snapshotResult.SnapshotVersions); i++ {
		if snapshotResult.SnapshotVersions[i].SnapshotImageTag == "" {
			log.GetLogger().Warnf("get empty snapshot image tag from snapshot version %v", snapshotResult.SnapshotVersions)
			continue
		}
		if snapshotResult.SnapshotVersions[i].SnapshotImageTag == curImageTags {
			continue
		}
		needDeleteObjectIDs = append(needDeleteObjectIDs, snapshotResult.SnapshotVersions[i].ObjectID)
	}
	return needDeleteObjectIDs, nil
}

// DeleteInitInstanceFromCAERouterEtcd delete init instance info from cae router etcd
func DeleteInitInstanceFromCAERouterEtcd(ctx context.Context, pod *corev1.Pod, spec *faasfunction.FunctionServiceSpec) {
	instance := utils.MakeInitWorkerIns(pod, spec)
	path := utils.GenerateInitInstanceKey(instance)
	if err := OperateCAEETCDWithRetry(ctx, path, "", DelOperation); err != nil {
		log.GetLogger().Warnf("failed to delete init instance %s from cae router etcd for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	} else {
		log.GetLogger().Infof("succeed to delete init instance %s in cae router etcd for: %s", instance.InstanceID,
			instance.FuncName)
	}
}

// PutInstanceToETCD -
func PutInstanceToETCD(ctx context.Context, instance v1alpha1.WorkerInstance) {
	err := PutInstanceToETCDWithErr(ctx, instance, PutOperation)
	if err != nil {
		log.GetLogger().Errorf("failed to put instance %s to etcd for: %s", instance.InstanceID, instance.FuncName)
	}
}

// PutInstanceToETCDWithErr -
func PutInstanceToETCDWithErr(ctx context.Context, instance v1alpha1.WorkerInstance, operation string) error {
	path := utils.GetWorkerKeyByInstance(instance)
	value, err := json.Marshal(instance)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal instance: %s for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
		return err
	}
	if err = OperateETCDWithRetry(ctx, path, string(value), operation); err != nil {
		etcd3.SaveFailedOperation(ctx, path, string(value), operation)
		log.GetLogger().Errorf("failed to put instance %s to etcd for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	} else {
		etcd3.RemoveFailedOperation(path)
		log.GetLogger().Infof("putIns | %s | %s", urnutils.ExtractPodName(instance.InstanceID),
			urnutils.ExtractFuncName(instance.FuncName))
	}

	if !commutils.IsCAEFunc(instance.BusinessType) {
		return nil
	}
	initPath := utils.GenerateInitInstanceKey(instance)
	if err = OperateCAEETCDWithRetry(context.Background(), initPath, "", DelOperation); err != nil {
		log.GetLogger().Errorf("failed to delete init instance %s from cae router etcd for %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	}

	if err = OperateCAEETCDWithRetry(ctx, path, string(value), operation); err != nil {
		log.GetLogger().Errorf("failed to put instance %s to cae router etcd for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	} else {
		log.GetLogger().Infof("succeed to put instance %s to cae router etcd for: %s", instance.InstanceID,
			instance.FuncName)
	}
	if operation == PutOperation {
		e := notifyRouterReadyToWorker(utils.GetWorkerClient(), instance)
		if e != nil {
			log.GetLogger().Warnf("failed to notify router ready %s, err: %s", instance.InstanceID, e.Error())
		}
	}
	return err
}

func notifyRouterReadyToWorker(client *fasthttp.Client, instance v1alpha1.WorkerInstance) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	host := instance.IP + ":8081"
	url := "//" + host + constants.NotifyRouterReady
	req.URI().Update(url)

	req.Header.SetHost(host)

	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}

	if cfg.HTTPSConfig.HTTPSEnable {
		req.URI().SetScheme("https")
	} else {
		req.URI().SetScheme("http")
	}

	auth, timestamp := localauth.SignLocally(cfg.LocalAuth.AKey, cfg.LocalAuth.SKey, utils.AppID,
		cfg.LocalAuth.Duration)
	req.Header.Set(constants.Authorization, auth)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetConnectionClose()

	err = client.DoTimeout(req, resp, notifyWorkerTimeout)
	if err != nil {
		log.GetLogger().Errorf("failed to notify router ready %s, err: %s", instance.InstanceID, err.Error())
		return err
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		log.GetLogger().Warnf("failed to notify router ready to instance %s, code: %d, resp: %s",
			instance.InstanceID, resp.StatusCode(), string(resp.Body()))
	}
	return nil
}

// DeleteInstanceFromETCD -
func DeleteInstanceFromETCD(ctx context.Context, instance v1alpha1.WorkerInstance) {
	path := utils.GetWorkerKeyByInstance(instance)
	funcURN := "sn:cn:yrk:" + instance.TenantID + ":function:" + instance.FuncName + ":" + instance.Version
	if err := utils.UpdateClusterBridgeInstance(instance.IsBridgeFunc, funcURN, instance.PodName, ""); err != nil {
		log.GetLogger().Warnf("failed to updateClusterBridgeInstance for %s %s:%s",
			funcURN, instance.PodName, err.Error())
	}
	if err := OperateETCDWithRetry(ctx, path, "", DelOperation); err != nil {
		etcd3.SaveFailedOperation(ctx, path, "", DelOperation)
		log.GetLogger().Errorf("failed to delete instance %s from etcd for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	} else {
		etcd3.RemoveFailedOperation(path)
		log.GetLogger().Infof("delIns | %s | %s", urnutils.ExtractPodName(instance.InstanceID),
			urnutils.ExtractFuncName(instance.FuncName))
	}

	if !commutils.IsCAEFunc(instance.BusinessType) {
		return
	}
	if err := OperateCaeRouterETCD(ctx, path, "", DelOperation); err != nil {
		log.GetLogger().Errorf("failed to delete instance %s from cae router etcd for: %s with error: %s",
			instance.InstanceID, instance.FuncName, err.Error())
	} else {
		log.GetLogger().Infof("succeed to delete instance %s in cae router etcd for: %s", instance.InstanceID,
			instance.FuncName)
	}
}

// DeleteInstanceByPrefixFromETCD -
func DeleteInstanceByPrefixFromETCD(ctx context.Context, service *faasfunction.FunctionService) {
	path := utils.WorkerKeyByCr(service, "")
	if err := OperateETCD(ctx, path, "", DelOperation); err != nil {
		log.GetLogger().Errorf("failed to delete instance by prefix %s from etcd for: %s with error: %s",
			etcdkey.AnonymizeTenantCommonEtcdKey(path), service.Spec.Configuration.FunctionName, err.Error())
	}
	log.GetLogger().Infof("succeed to delete instance by prefix %s in etcd for: %s",
		etcdkey.AnonymizeTenantCommonEtcdKey(path), service.Spec.Configuration.FunctionName)
	if !commutils.IsCAEFunc(service.Spec.Configuration.BusinessType) {
		return
	}
	if err := OperateCaeRouterETCD(ctx, path, "", DelOperation); err != nil {
		log.GetLogger().Errorf("failed to delete instance by prefix %s from cae router etcd for: %s with error: %s",
			etcdkey.AnonymizeTenantCommonEtcdKey(path), service.Spec.Configuration.FunctionName, err.Error())
	}
	log.GetLogger().Infof("succeed to delete instance by prefix %s in cae router etcd for: %s",
		etcdkey.AnonymizeTenantCommonEtcdKey(path), service.Spec.Configuration.FunctionName)
}

// PutSNStateWorkerFromETCD -
func PutSNStateWorkerFromETCD(requestInfo *config.RequestInfo, instance v1alpha1.WorkerInstance) {
	funcNamePrefix, serviceID, shortFuncName := utils.SplitFuncName(instance.FuncName)
	valueMap := make(map[string]string, 11)
	valueMap[requestInfo.StateID] = instance.NodeName
	valueMap[constants.CreatorKey+requestInfo.StateID] = instance.Creator
	valueMap[constants.TenantIDKey+requestInfo.StateID] = instance.TenantID
	valueMap[constants.BusinessIDKey+requestInfo.StateID] = instance.BusinessID
	valueMap[constants.ServiceIDKey+requestInfo.StateID] = funcNamePrefix + utils.FuncNameSeparator + serviceID
	valueMap[constants.VersionKey+requestInfo.StateID] = instance.Version
	valueMap[constants.FuncNameKey+requestInfo.StateID] = shortFuncName
	valueMap[constants.CPUKey+requestInfo.StateID] = strconv.Itoa(int(requestInfo.CPU))
	valueMap[constants.MemoryKey+requestInfo.StateID] = strconv.Itoa(int(requestInfo.Memory))
	valueMap[constants.ClusterKey] = localCluster
	valueMap[constants.ApplierKey] = constants.WorkerManagerApplier
	value, err := json.Marshal(valueMap)
	if err != nil {
		log.GetLogger().Errorf("json marshal value map error: %s", err.Error())
	}
	path := utils.GetSNStateWorkerPath(instance)

	putValueIntoStateETCD(path, string(value), instance)
	return
}

func putValueIntoStateETCD(path, value string, instance v1alpha1.WorkerInstance) {
	putEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Errorf("state client is nil")
		return
	}
	if err := stateClient.Put(putEtcdCtx, path, value); err != nil {
		log.GetLogger().Errorf("updating function-task worker: %s, stateID: %s from etcd failed: %s",
			instance.InstanceID, instance.StateID, err.Error())
		return
	}
	log.GetLogger().Infof("updating function-task worker: %s, stateID: %s, value: %+v succeed",
		instance.InstanceID, instance.StateID, value)
	return
}
