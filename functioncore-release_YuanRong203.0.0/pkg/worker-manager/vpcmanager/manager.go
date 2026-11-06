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

// Package vpcmanager is vpc manager module
package vpcmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"plugin"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
)

// ChangedPatPodInfo -
type ChangedPatPodInfo struct {
	TenantID string             `json:"tenantID"`
	VpcID    string             `json:"vpcID"`
	PatPod   types.NATConfigure `json:"patPod"`
}

// VpcHandler define struct of vpc manager
type VpcHandler struct {
	instanceList map[string]map[string]*PatInstance
	patKeyList   map[string]map[string]struct{}
	vpcMutex     sync.Mutex
	KubeClient   kubernetes.Interface
	VpcPlugin    PluginVPC
	VpcChannel   chan string
	stopCh       <-chan struct{}
}

const (
	k8sType   = "k8s"
	putMethod = "put"
	delMethod = "delete"
)

type operationRecord struct {
	operateType string
	method      string
	operateKey  string
	Value       string
}

const (
	// PluginFile is the path of vpccontroller.so
	PluginFile = "/home/sn/vpccontroller.so"
	// DefaultVpcChannelLen is the default length of vpc channel
	DefaultVpcChannelLen     = 10
	vpcInitError             = "realpath failed"
	poolSubStr               = "-#-"
	triggerSubStr            = "@"
	baseInteger              = 10
	defaultKeyLen            = 6
	patPodIndex              = 5
	defaultTriggerLen        = 3
	functionNameIndex        = 2
	instanceNameIndex        = 1
	funcVpc                  = 1
	endpointTenantVpc        = 2
	publicVPC                = 3
	maxPatPodLength          = 2
	createPatRetryTimes      = 3
	vpcResourceTimeout       = time.Second * 60
	workerInstanceType       = "worker-instance"
	pullTriggerPodType       = "pull-trigger-pod"
	pullTriggerEnvKey        = "SERVERLESS_VPCNATCONF"
	pullTriggerContainerName = "vpcpulltrigger"
)

// PatInstance -
type PatInstance struct {
	instanceType string
	funcSvcID    string
	podName      string
}

// SendPatPodIPToWorkerByRetry define function as a type
type SendPatPodIPToWorkerByRetry func(ctx context.Context, retryPeriod time.Duration, param *config.PatPodRequestParam)

var (
	vpcHandler        *VpcHandler
	vpcControllerOnce sync.Once
	vpcControllerErr  error
)
var (
	sendPatPodIPToWorkerByRetry SendPatPodIPToWorkerByRetry
	errInvalidPatType           = errors.New("invalid pat type")
)

// RegisterSendPatPodIPFunc -
func RegisterSendPatPodIPFunc(sendFunc SendPatPodIPToWorkerByRetry) {
	sendPatPodIPToWorkerByRetry = sendFunc
}

// InitVPCManager init vpc manager
func InitVPCManager(instancePods, pullTriggerPods []corev1.Pod) error {
	vpcHandler, err := GetVPCController()
	if err != nil {
		log.GetLogger().Errorf("failed to init vpc controller with error: %s", err.Error())
		return err
	}
	// notify vpc plugin that worker-manager has become to leader
	if err = vpcHandler.BecomeLeader(); err != nil {
		return err
	}
	// recover pat pods and build map of pat pods and instance pods
	go vpcHandler.RecoverPatPodsCache(instancePods)
	// recover pat pods and build map of pat pods and pull trigger pods
	go vpcHandler.RecoverPatPodsCache(pullTriggerPods)
	// receive terminated pat pod and second pat pod from vpc channel
	go vpcHandler.handleVpcChannelReceive()

	return nil
}

// NotifyVpcControllerExit notify vpc controller exit
func NotifyVpcControllerExit() {
	vpcHandler, err := GetVPCController()
	if err != nil {
		log.GetLogger().Errorf("failed to init vpc controller with error: %s", err.Error())
		return
	}
	if vpcHandler != nil {
		vpcHandler.handleNotifyVpcControllerExit()
	}
}

// DeleteRemainedVpcPullTrigger Delete the VPC Pull Trigger that does not exist in ETCD.
func DeleteRemainedVpcPullTrigger() {
	deployNameMap := map[string]bool{}
	deployList, err := tools.ListDeployByK8S(tools.GetKubernetesClient(), metav1.ListOptions{
		LabelSelector: "cff-type=serverless",
	})
	if err != nil {
		log.GetLogger().Errorf("failed to list all deployment with error: %s", err.Error())
		return
	}
	for _, poolDeploy := range deployList.Items {
		deployNameMap[poolDeploy.Name] = false
	}
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient.Client == nil {
		log.GetLogger().Warnf("failed to init meta etcd client")
		return
	}
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := metaClient.GetResponse(defaultCtx, etcd3.HandleCellPrefix(tools.FunctionMetadataPrefix),
		clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		log.GetLogger().Errorf("failed to connect to etcd, err: %s", err.Error())
		return
	}
	for name := range deployNameMap {
		deleteStatus := true
		for _, kv := range resp.Kvs {
			if strings.Contains(string(kv.Key), name) {
				deleteStatus = false
				break
			}
		}
		if deleteStatus {
			deployNameMap[name] = true
			err = tools.DeleteDeployByK8S(tools.GetKubernetesClient(), name)
			if err != nil && !k8serror.IsNotFound(err) {
				log.GetLogger().Warnf("failed to delete from k8s with deploy: %s and error: %s",
					name, err.Error())
				continue
			}
			log.GetLogger().Infof("succeed to delete from k8s with deploy: %s", name)
		}
	}
}

// GetVPCController -
func GetVPCController() (*VpcHandler, error) {
	vpcControllerOnce.Do(func() {
		log.GetLogger().Infof("start to init vpc controller")
		start := time.Now()
		vpcEnable := true
		vpcHandler = &VpcHandler{
			patKeyList:   make(map[string]map[string]struct{}, defaultKeyLen),
			instanceList: make(map[string]map[string]*PatInstance, defaultKeyLen),
		}
		vpcHandler.VpcChannel = make(chan string, DefaultVpcChannelLen)
		pluginFile, err := plugin.Open(PluginFile)
		if err != nil {
			if strings.Contains(err.Error(), vpcInitError) {
				vpcEnable = false
			} else {
				fmt.Printf("failed to open vpc plugin file: %s\n", err.Error())
				vpcControllerErr = err
				return
			}
		}
		kubeClient := tools.GetKubernetesClient()
		if kubeClient == nil {
			log.GetLogger().Errorf("failed to get kubeClient")
			vpcControllerErr = errors.New("failed to get kubeClient")
			return
		}
		vpcHandler.KubeClient = kubeClient
		vpcHandler.VpcPlugin.Plugin = pluginFile
		if vpcEnable && vpcHandler != nil {
			err = vpcHandler.VpcPlugin.initVpcPlugin()
			if err != nil {
				vpcControllerErr = err
				log.GetLogger().Errorf("failed to init vpc plugin, error: %s", err.Error())
				return
			}
			err = cleanPatKey()
			if err != nil {
				vpcControllerErr = err
				log.GetLogger().Errorf("failed to exec cleanPatKey, error: %s", err.Error())
				return
			}
			alarm.GetClient().RegisterAlarmType(alarm.PatPodCreateFailed)
		}
		log.GetLogger().Infof("succeed to init vpc controller with cost time %s", time.Since(start).String())
	})
	return vpcHandler, vpcControllerErr
}

func cleanPatKey() error {
	client := etcd3.GetRouterEtcdClient()
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	getResp, err := client.GetResponse(defaultEtcdCtx, "/net/tenant/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range getResp.Kvs {
		delEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		patServiceID := string(kv.Key)
		splits := strings.Split(patServiceID, "/")
		if len(splits) != defaultKeyLen {
			continue
		}
		err = client.Delete(delEtcdCtx, patServiceID)
		if err != nil {
			log.GetLogger().Warnf("failed to pre-delete patServiceID: %s", err.Error())
		}
	}
	return nil
}

func (v *VpcHandler) checkExists(patKey string) bool {
	v.vpcMutex.Lock()
	if v.instanceList == nil {
		v.vpcMutex.Unlock()
		return false
	}
	_, ok := v.instanceList[patKey]
	v.vpcMutex.Unlock()
	return ok
}

func (v *VpcHandler) loadInstancesByKey(patKey string) (map[string]*PatInstance, bool) {
	v.vpcMutex.Lock()
	if v.instanceList == nil {
		v.vpcMutex.Unlock()
		return nil, false
	}
	instances, ok := v.instanceList[patKey]
	if !ok {
		v.vpcMutex.Unlock()
		return nil, false
	}
	copyInstances := make(map[string]*PatInstance, len(instances))
	for podName, patInstanceInfo := range instances {
		copyInstances[podName] = patInstanceInfo
	}
	v.vpcMutex.Unlock()

	return copyInstances, true
}

func (v *VpcHandler) loadAndDeleteIns(deletedPatKey string) (map[string]*PatInstance, bool) {
	v.vpcMutex.Lock()
	if v.instanceList == nil {
		v.vpcMutex.Unlock()
		return nil, false
	}
	instances, ok := v.instanceList[deletedPatKey]
	if !ok {
		v.vpcMutex.Unlock()
		return nil, false
	}
	copyInstances := make(map[string]*PatInstance, len(instances))
	for podName, patInstanceInfo := range instances {
		copyInstances[podName] = patInstanceInfo
	}
	// delete pat key in instanceList map
	delete(v.instanceList, deletedPatKey)
	// delete pat key in patKeyList map
	for ins := range instances {
		if v.patKeyList == nil {
			continue
		}
		if _, ok = v.patKeyList[ins]; !ok {
			continue
		}
		delete(v.patKeyList[ins], deletedPatKey)
	}
	v.vpcMutex.Unlock()

	return copyInstances, true
}

func (v *VpcHandler) storeTwoWay(podName, patKey string, patInstance *PatInstance) {
	log.GetLogger().Debugf("store two way with pod name: %s and pat key: %s", podName, patKey)
	v.vpcMutex.Lock()
	if v.patKeyList[podName] == nil {
		v.patKeyList[podName] = make(map[string]struct{}, defaultKeyLen)
	}
	if v.instanceList[patKey] == nil {
		v.instanceList[patKey] = make(map[string]*PatInstance, defaultKeyLen)
	}
	v.patKeyList[podName][patKey] = struct{}{}
	v.instanceList[patKey][podName] = patInstance
	v.vpcMutex.Unlock()
}

func (v *VpcHandler) loadOrStoreByPodName(patKey, podName string, patInstance *PatInstance) bool {
	log.GetLogger().Debugf("loadOrStoreByPodName with pat key: %s and pod name: %s", patKey, podName)
	v.vpcMutex.Lock()
	if v.instanceList[patKey] == nil {
		v.instanceList[patKey] = make(map[string]*PatInstance, defaultKeyLen)
	}
	if v.patKeyList[podName] == nil {
		v.patKeyList[podName] = make(map[string]struct{}, defaultKeyLen)
	}
	_, insListOk := v.instanceList[patKey][podName]
	_, patListOk := v.patKeyList[podName][patKey]
	v.instanceList[patKey][podName] = patInstance
	v.patKeyList[podName][patKey] = struct{}{}
	log.GetLogger().Debugf("loadOrStoreByPodName with instanceList len: %d and patKeyList len: %d",
		len(v.instanceList), len(v.patKeyList))
	v.vpcMutex.Unlock()
	return insListOk && patListOk
}

func getPatType(funcMeta *faasfunction.FunctionService, vpcID string) int {
	if funcMeta == nil {
		return 0
	}
	patType := 0
	funcVpcID := funcMeta.Spec.Configuration.FuncVpc.VpcID
	endpointVpcID := funcMeta.Spec.Configuration.EndpointTenantVpc.VpcID
	disablePublicNetwork := funcMeta.Spec.Configuration.NetworkController.DisablePublicNetwork

	if vpcID == funcVpcID {
		patType = funcVpc
	} else if vpcID == funcMeta.Spec.Configuration.EndpointTenantVpc.VpcID {
		patType = endpointTenantVpc
	} else if !disablePublicNetwork {
		patType = publicVPC
	} else {
		log.GetLogger().Warnf("vpc id: %s is not matched func vpc id: %s or endpoint vpc id: %s",
			vpcID, funcVpcID, endpointVpcID)
	}
	return patType
}

func (v *VpcHandler) sendPatPodIPToWorker(funcMeta *faasfunction.FunctionService,
	changedPatPodInfo *ChangedPatPodInfo, instance *PatInstance) {
	if funcMeta == nil {
		log.GetLogger().Warnf("funcMeta is nil with func: %s", instance.funcSvcID)
		return
	}
	traceID := "pat-ip-" + uuid.New().String()
	patType := getPatType(funcMeta, changedPatPodInfo.VpcID)
	publicBandWidth := cache.WorkerCache.GetPublicNetworkConf(funcMeta.Spec.Configuration.TenantID)
	if patType == publicVPC && len(publicBandWidth) == 0 {
		log.GetLogger().Warnf("func %s no need create public network pat, publicBandWidth is empty",
			funcMeta.Spec.Configuration.FunctionName)
		return
	}
	patPods, err := v.VpcPlugin.createVpcResource(funcMeta, traceID, patType, true, true)
	if err != nil {
		log.GetLogger().Errorf("failed to create vpc resource for %s with traceID %s", funcMeta.ID, traceID)
		return
	}
	if len(patPods) != maxPatPodLength {
		log.GetLogger().Errorf("pat pods length is not 2 with pat type: %d for func: %s", patType, funcMeta.ID)
		return
	}
	patPodRequestParam := &config.PatPodRequestParam{
		FuncSvcID: instance.funcSvcID,
		PodName:   instance.podName,
		Timeout:   tools.SendPatIPTimeout,
		TraceID:   traceID,
	}
	if patType == funcVpc {
		patPodRequestParam.PatPodInfo = patPods
	} else if patType == endpointTenantVpc {
		patPodRequestParam.EndpointPatPodInfo = patPods
	} else if patType == publicVPC {
		patPodRequestParam.PublicPatPodInfo = patPods
	} else {
		log.GetLogger().Errorf("unknown pat type: %d for func: %s", patType, funcMeta.ID)
		return
	}
	// update cache
	for _, patPod := range patPods {
		patKey := buildPatKey(changedPatPodInfo.TenantID, patPod.PatPodName)
		v.storeTwoWay(instance.podName, patKey, instance)
	}

	go sendPatPodIPToWorkerByRetry(context.Background(), tools.SendPatIPInterval, patPodRequestParam)
}

func (v *VpcHandler) sendPatPodIPToAllWorkers(changedPatPodInfo *ChangedPatPodInfo,
	instances map[string]*PatInstance) {
	for i := range instances {
		if instances[i] == nil {
			continue
		}
		if instances[i].instanceType != workerInstanceType {
			continue
		}
		funcMeta := cache.WorkerCache.GetFuncMeta(instances[i].funcSvcID)
		v.sendPatPodIPToWorker(funcMeta, changedPatPodInfo, instances[i])
	}
}

func (v *VpcHandler) handlePatPodInfo(changedPatPodInfo *ChangedPatPodInfo) error {
	var notifiedPatPod types.NATConfigure
	notifiedPatPod = changedPatPodInfo.PatPod
	notifiedPatKey := buildPatKey(changedPatPodInfo.TenantID, notifiedPatPod.PatPodName)

	var ok bool
	var needNotifiedInstances map[string]*PatInstance
	if notifiedPatPod.IsDeleted {
		needNotifiedInstances, ok = v.loadAndDeleteIns(notifiedPatKey)
	} else {
		needNotifiedInstances, ok = v.loadInstancesByKey(notifiedPatKey)
	}
	if ok {
		log.GetLogger().Infof("pat service pod: %s is received, deleted flag is %t, we will notify %d instances",
			notifiedPatPod.PatPodName, notifiedPatPod.IsDeleted, len(needNotifiedInstances))
		// send request to workers at first and then update cache
		go v.sendPatPodIPToAllWorkers(changedPatPodInfo, needNotifiedInstances)
	}

	return nil
}

func (v *VpcHandler) handleVpcChannelReceive() {
	if v.VpcChannel == nil {
		fmt.Println("HandleVpcChannel receive nil vpcChannel")
		return
	}
	for {
		select {
		case value, ok := <-v.VpcChannel:
			if !ok {
				log.GetLogger().Error("vpc channel has been closed")
				return
			}
			patPodInfo := &ChangedPatPodInfo{}
			if err := json.Unmarshal([]byte(value), patPodInfo); err != nil {
				log.GetLogger().Warnf("failed to unmarshal pat pod info with error: %s", err.Error())
				continue
			}
			log.GetLogger().Warnf("attention a pat pod is received from vpc channel with info %+v", patPodInfo)
			if err := v.handlePatPodInfo(patPodInfo); err != nil {
				log.GetLogger().Warnf("failed to send pat pod ip to workers with error: %s", err.Error())
				continue
			}
		}
	}
}

func (v *VpcHandler) handleNotifyVpcControllerExit() {
	if err := v.VpcPlugin.notifyVpcControllerExit(); err != nil {
		log.GetLogger().Errorf("failed to notify exit: %s", err.Error())
	} else {
		log.GetLogger().Infof("succeed to notify exit")
	}
}

func (v *VpcHandler) getOrCreatePatPods(funcMeta *faasfunction.FunctionService,
	vpcType int, traceID string) ([]types.NATConfigure, error) {
	var vpcPats []types.NATConfigure
	switch vpcType {
	case funcVpc:
		vpcPats = cache.WorkerCache.GetFuncVpcPats(funcMeta.ID)
	case endpointTenantVpc:
		vpcPats = cache.WorkerCache.GetEndpointVpcPats(funcMeta.ID)
	case publicVPC:
		vpcPats = cache.WorkerCache.GetPublicVpcPats(funcMeta.ID)
	default:
		log.GetLogger().Errorf("failed to get pat pods because of invalid pat type")
		return nil, errInvalidPatType
	}
	if len(vpcPats) == maxPatPodLength {
		return vpcPats, nil
	}
	var err error
	// create resource
	vpcPats, err = v.createVpcResourceByRetry(traceID, vpcType, funcMeta, createPatRetryTimes)
	if err != nil {
		log.GetLogger().Errorf("failed to create func vpc pats for %s with traceID %s and error %s",
			funcMeta.ID, traceID, err.Error())
		return nil, err
	}
	log.GetLogger().Infof("succeed to create vpc resource for %s with vpc type %d and traceID %s, pats info: %v",
		funcMeta.ID, vpcType, traceID, vpcPats)

	return vpcPats, nil
}

func (v *VpcHandler) recoverPatPodsCache(funcMeta *faasfunction.FunctionService,
	pod *corev1.Pod, vpcType int, traceID string) ([]types.NATConfigure, error) {
	vpcPats, err := v.getOrCreatePatPods(funcMeta, vpcType, traceID)
	if err != nil {
		return nil, err
	}
	// set func vpc pat pods by id
	switch vpcType {
	case funcVpc:
		cache.WorkerCache.SetFuncVpcPats(funcMeta.ID, vpcPats)
	case endpointTenantVpc:
		cache.WorkerCache.SetEndpointVpcPats(funcMeta.ID, vpcPats)
	case publicVPC:
		cache.WorkerCache.SetPublicVpcPats(funcMeta.ID, vpcPats)
	default:
		log.GetLogger().Errorf("failed to set func vpc pat pods because of invalid pat type")
		return nil, errInvalidPatType
	}
	if len(vpcPats) == maxPatPodLength {
		v.storeTwoWay(pod.Name, buildPatKey(funcMeta.Spec.Configuration.TenantID, vpcPats[0].PatPodName),
			&PatInstance{instanceType: workerInstanceType, funcSvcID: funcMeta.ID, podName: pod.Name})
		v.storeTwoWay(pod.Name, buildPatKey(funcMeta.Spec.Configuration.TenantID, vpcPats[1].PatPodName),
			&PatInstance{instanceType: workerInstanceType, funcSvcID: funcMeta.ID, podName: pod.Name})
		return vpcPats, nil
	}
	log.GetLogger().Errorf("vpc pats len expect 2 but found %d for %s, traceID %s", len(vpcPats), funcMeta.ID, traceID)
	return nil, fmt.Errorf("vpc pats len expect 2 but found %d", len(vpcPats))
}

func (v *VpcHandler) recoverPatPods(funcMeta *faasfunction.FunctionService, pod corev1.Pod,
	traceID string) ([]types.NATConfigure, []types.NATConfigure, []types.NATConfigure, error) {
	var err error
	var isFuncVpc, isEndpointVpc bool
	var funcPats, endpointPats, publicPats []types.NATConfigure
	if funcMeta.Spec.Configuration.FuncVpc.VpcID != "" {
		isFuncVpc = true
		if funcPats, err = v.recoverPatPodsCache(funcMeta, &pod, funcVpc, traceID); err != nil {
			return []types.NATConfigure{}, []types.NATConfigure{}, []types.NATConfigure{}, err
		}
	}
	if funcMeta.Spec.Configuration.EndpointTenantVpc.VpcID != "" {
		isEndpointVpc = true
		if endpointPats, err = v.recoverPatPodsCache(funcMeta, &pod, endpointTenantVpc, traceID); err != nil {
			return []types.NATConfigure{}, []types.NATConfigure{}, []types.NATConfigure{}, err
		}
	}
	if !isFuncVpc && !isEndpointVpc && needPublicPat(funcMeta) {
		if publicPats, err = v.recoverPatPodsCache(funcMeta, &pod, publicVPC, traceID); err != nil {
			return []types.NATConfigure{}, []types.NATConfigure{}, []types.NATConfigure{}, err
		}
	}
	return funcPats, endpointPats, publicPats, nil
}

// RecoverPatPodsCache -
func (v *VpcHandler) RecoverPatPodsCache(pods []corev1.Pod) {
	log.GetLogger().Infof("recover pat pods cache from %d instance pods or pull trigger pods", len(pods))
	for _, pod := range pods {
		funcSvcID, ok := tools.OwnedByFunctionService(&pod)
		if !ok {
			// pod is not a instance
			continue
		}
		funcMeta := cache.WorkerCache.GetFuncMeta(funcSvcID)
		if funcMeta == nil {
			continue
		}
		var err error
		var funcPats, endpointPats, publicPats []types.NATConfigure
		traceID := "recover-" + uuid.New().String()
		patPodRequestParam := &config.PatPodRequestParam{
			FuncSvcID: funcMeta.ID,
			PodName:   pod.Name,
			Timeout:   tools.SendPatIPTimeout,
			TraceID:   traceID,
		}
		if !needPat(funcMeta) {
			continue
		}
		log.GetLogger().Infof("recover pats use func vpc id %s, endpoint vpc id %s for %s with pod %s and traceID %s",
			funcMeta.Spec.Configuration.FuncVpc.VpcID, funcMeta.Spec.Configuration.EndpointTenantVpc.VpcID,
			funcSvcID, pod.Name, traceID)
		funcPats, endpointPats, publicPats, err = v.recoverPatPods(funcMeta, pod, traceID)
		if err != nil {
			continue
		}
		// it is no need to send pat pod ip to pull trigger pod
		if tools.IsPullTriggerFunction(funcMeta) {
			continue
		}
		if len(funcPats) == maxPatPodLength && (funcPats[0].IsNewCreated || funcPats[1].IsNewCreated) {
			patPodRequestParam.PatPodInfo = funcPats
		}
		if len(endpointPats) == maxPatPodLength && (endpointPats[0].IsNewCreated || endpointPats[1].IsNewCreated) {
			patPodRequestParam.EndpointPatPodInfo = endpointPats
		}
		if len(publicPats) == maxPatPodLength && (publicPats[0].IsNewCreated || publicPats[1].IsNewCreated) {
			patPodRequestParam.PublicPatPodInfo = publicPats
		}
		// one of vpc pats is new, it will send request to worker
		if patPodRequestParam.PatPodInfo != nil || patPodRequestParam.EndpointPatPodInfo != nil ||
			patPodRequestParam.PublicPatPodInfo != nil {
			go sendPatPodIPToWorkerByRetry(context.Background(), tools.SendPatIPInterval, patPodRequestParam)
		}
	}
}

func needPat(service *faasfunction.FunctionService) bool {
	if service.Spec.Configuration.FuncVpc.VpcID == "" && service.Spec.Configuration.EndpointTenantVpc.VpcID == "" &&
		!needPublicPat(service) {
		return false
	}
	return true
}

func needPublicPat(service *faasfunction.FunctionService) bool {
	bandwidth := cache.WorkerCache.GetPublicNetworkConf(service.Spec.Configuration.TenantID)
	if service.Spec.Configuration.NetworkController.DisablePublicNetwork || len(bandwidth) == 0 {
		return false
	}
	return true
}

// HandleVpcFunctionInfo get pat-service from vpc controller for worker
func (v *VpcHandler) HandleVpcFunctionInfo(service *faasfunction.FunctionService,
	requestInfo *config.RequestInfo) error {
	// handle vpc when resource vpc or final vpc is exist
	if !needPat(service) {
		return nil
	}
	log.GetLogger().Infof("handle vpc function info with name: %s, service: %s, vpcID: %s, traceID: %s, "+
		"disablePublicNetwork %t", requestInfo.FunctionName, service.ID, service.Spec.Configuration.FuncVpc.VpcID,
		requestInfo.TraceID, service.Spec.Configuration.NetworkController.DisablePublicNetwork)
	start := time.Now()
	funcVpcErrCh, endpointVpcErrCh, publicVpcErrCh, finishCh := make(chan error), make(chan error),
		make(chan error), make(chan struct{})
	// get the count of vpc need change
	finishCount := getFinishCount(service)
	// create func vpc pat
	if service.Spec.Configuration.FuncVpc.VpcID != "" {
		go v.handleVpcResource(requestInfo, &handleVpcParam{finishCount: &finishCount,
			errCh: funcVpcErrCh, finishCh: finishCh, vpcType: funcVpc, needAllInit: false,
			needNewVersion: true}, service)
	}
	// create endpoint vpc pat
	if service.Spec.Configuration.EndpointTenantVpc.VpcID != "" {
		go v.handleVpcResource(requestInfo, &handleVpcParam{finishCount: &finishCount,
			errCh: endpointVpcErrCh, finishCh: finishCh, vpcType: endpointTenantVpc, needAllInit: false,
			needNewVersion: true}, service)
	}
	// create public vpc pat
	if service.Spec.Configuration.FuncVpc.VpcID == "" && service.Spec.Configuration.EndpointTenantVpc.VpcID == "" &&
		needPublicPat(service) {
		go v.handleVpcResource(requestInfo, &handleVpcParam{finishCount: &finishCount,
			errCh: publicVpcErrCh, finishCh: finishCh, vpcType: publicVPC, needAllInit: false,
			needNewVersion: true}, service)
	}
	return handleVpcFunctionInfoResult(requestInfo, vpcResultCh{
		funcVpcErrCh:     funcVpcErrCh,
		endpointVpcErrCh: endpointVpcErrCh,
		publicVpcErrCh:   publicVpcErrCh,
		finishCh:         finishCh,
	}, service, start)
}

type vpcResultCh struct {
	funcVpcErrCh     chan error
	endpointVpcErrCh chan error
	publicVpcErrCh   chan error
	finishCh         chan struct{}
}

func handleVpcFunctionInfoResult(requestInfo *config.RequestInfo, param vpcResultCh,
	service *faasfunction.FunctionService, start time.Time) error {
	var err error
	defer func() {
		if err != nil {
			cache.WorkerCache.HandlePatPodCreateFailAlarm(requestInfo, service, err)
		} else {
			cache.WorkerCache.HandlePatPodCreateSuccessAlarm(requestInfo, service)
		}
	}()
	select {
	case err = <-param.funcVpcErrCh:
		return err
	case err = <-param.endpointVpcErrCh:
		return err
	case err = <-param.publicVpcErrCh:
		return err
	case <-time.After(vpcResourceTimeout):
		err = errors.New("create vpc resource timeout")
		return err
	case _, ok := <-param.finishCh:
		if !ok {
			err = errors.New("channel closed")
			return err
		}
		log.GetLogger().Infof("finished to create vpc resource for %s, cost %v, traceID %s, %v, %v, %v",
			service.ID, time.Since(start), requestInfo.TraceID,
			requestInfo.PatPodInfo, requestInfo.EndpointTalentPatPodInfo, requestInfo.PublicPatPodInfo)
	}
	return nil
}

func getFinishCount(service *faasfunction.FunctionService) (finishCount int32) {
	if service.Spec.Configuration.FuncVpc.VpcID != "" {
		finishCount++
	}
	if service.Spec.Configuration.EndpointTenantVpc.VpcID != "" {
		finishCount++
	}
	if service.Spec.Configuration.FuncVpc.VpcID == "" && service.Spec.Configuration.EndpointTenantVpc.VpcID == "" &&
		needPublicPat(service) {
		finishCount++
	}
	return finishCount
}

type handleVpcParam struct {
	finishCount    *int32
	errCh          chan error
	finishCh       chan struct{}
	vpcType        int
	needAllInit    bool
	needNewVersion bool
}

func (v *VpcHandler) handleVpcResource(info *config.RequestInfo, param *handleVpcParam,
	service *faasfunction.FunctionService) {
	patPod, err := v.VpcPlugin.createVpcResource(service, info.TraceID, param.vpcType,
		param.needAllInit, param.needNewVersion)
	if err != nil {
		log.GetLogger().Errorf("create func vpc resource with error: %s, traceID: %s", err.Error(), info.TraceID)
		param.errCh <- err
		return
	}
	if param.vpcType == funcVpc {
		info.PatPodInfo = patPod
	} else if param.vpcType == endpointTenantVpc {
		info.EndpointTalentPatPodInfo = patPod
	} else if param.vpcType == publicVPC {
		info.PublicPatPodInfo = patPod
	} else {
		log.GetLogger().Errorf("unknown vpc type %d with traceID %s", param.vpcType, info.TraceID)
		param.errCh <- fmt.Errorf("unknown vpc type %d", param.vpcType)
		return
	}
	curFinishCount := atomic.AddInt32(param.finishCount, -1)
	log.GetLogger().Infof("succeed to create vpc resource for %s with count %d, traceID %s and pat info %v, vpcType %d",
		service.ID, curFinishCount, info.TraceID, patPod, param.vpcType)
	if curFinishCount == 0 {
		param.finishCh <- struct{}{}
	}
}

func (v *VpcHandler) createVpcResourceByRetry(traceID string, vpcType int,
	service *faasfunction.FunctionService, retryTimes int) ([]types.NATConfigure, error) {
	var err error
	var patPods []types.NATConfigure
	for i := 0; i < retryTimes; i++ {
		patPods, err = v.VpcPlugin.createVpcResource(service, traceID, vpcType, true, false)
		if err == nil {
			// stop to retry when succeed to create pat pods
			break
		}
		// retry interval is 1 second
		time.Sleep(1 * time.Second)
	}
	return patPods, err
}

// describeRequest get different requestInfo by vpcType
func describeRequest(traceID string, service *faasfunction.FunctionService, vpcType int,
	needInit, needNewVersion bool) config.VpcControllerRequester {
	vpcInfo := config.Vpc{
		Namespace: service.Spec.Configuration.TenantID,
		DomainID:  service.Spec.Configuration.DomainID,
	}
	delegate := config.Delegate{
		Xrole:    service.Spec.Configuration.Role.XRole,
		AppXrole: service.Spec.Configuration.Role.AppXRole,
	}
	var needPublicPat bool
	var publicBandWidth string
	switch vpcType {
	case funcVpc:
		vpcInfo = describeFuncVpc(vpcInfo, service)
	case endpointTenantVpc:
		delegate.Xrole = service.Spec.Configuration.EndpointTenantVpc.Xrole
		vpcInfo = describeEndpointTenantVpc(vpcInfo, service)
	case publicVPC:
		needPublicPat = true
		publicBandWidth = cache.WorkerCache.GetPublicNetworkConf(service.Spec.Configuration.TenantID)
		log.GetLogger().Infof("publicBandWidth %s, funcName %s", publicBandWidth, service.Spec.Configuration.FunctionName)
	default:
		// nothing to do
	}
	request := config.VpcControllerRequester{
		TraceID:         traceID,
		Delegate:        delegate,
		Vpc:             vpcInfo,
		NeedInit:        needInit,
		NeedNewVersion:  needNewVersion,
		NeedPublicPat:   needPublicPat,
		PublicBandWidth: publicBandWidth,
	}
	return request
}

func describeFuncVpc(vpcInfo config.Vpc, service *faasfunction.FunctionService) config.Vpc {
	vpcInfo.SubnetName = service.Spec.Configuration.FuncVpc.SubnetName
	vpcInfo.VpcID = service.Spec.Configuration.FuncVpc.VpcID
	vpcInfo.SubnetID = service.Spec.Configuration.FuncVpc.SubnetID
	vpcInfo.Gateway = service.Spec.Configuration.FuncVpc.Gateway
	vpcInfo.ID = service.Spec.Configuration.FuncVpc.ID
	vpcInfo.TenantCidr = service.Spec.Configuration.FuncVpc.TenantCidr
	vpcInfo.HostVMCidr = service.Spec.Configuration.FuncVpc.HostVMCidr
	vpcInfo.VpcName = service.Spec.Configuration.FuncVpc.VpcName
	vpcInfo.SecurityGroups = service.Spec.Configuration.FuncVpc.SecurityGroups
	vpcInfo.IPV6Enable = service.Spec.Configuration.FuncVpc.IPV6Enable
	vpcInfo.IPV6Gateway = service.Spec.Configuration.FuncVpc.IPV6Gateway
	vpcInfo.IPV6TenantCidr = service.Spec.Configuration.FuncVpc.IPV6TenantCidr
	vpcInfo.IPV6HostVMCidr = service.Spec.Configuration.FuncVpc.IPV6HostVMCidr
	vpcInfo.NeutronSubnetID = service.Spec.Configuration.FuncVpc.NeutronSubnetID
	vpcInfo.NeutronSubnetIDV6 = service.Spec.Configuration.FuncVpc.NeutronSubnetIDV6
	vpcInfo.IsSafety = service.Spec.Configuration.FuncVpc.IsSafety
	vpcInfo.ExtendedInfo = tools.GetExtendedInfo(service.Spec.Configuration.FuncMountConfig.FuncMounts)
	return vpcInfo
}

func describeEndpointTenantVpc(vpcInfo config.Vpc, service *faasfunction.FunctionService) config.Vpc {
	vpcInfo.SubnetName = service.Spec.Configuration.EndpointTenantVpc.SubnetName
	vpcInfo.VpcID = service.Spec.Configuration.EndpointTenantVpc.VpcID
	vpcInfo.SubnetID = service.Spec.Configuration.EndpointTenantVpc.SubnetID
	vpcInfo.Gateway = service.Spec.Configuration.EndpointTenantVpc.Gateway
	vpcInfo.ID = service.Spec.Configuration.EndpointTenantVpc.ID
	vpcInfo.TenantCidr = service.Spec.Configuration.EndpointTenantVpc.TenantCidr
	vpcInfo.HostVMCidr = service.Spec.Configuration.EndpointTenantVpc.HostVMCidr
	vpcInfo.VpcName = service.Spec.Configuration.EndpointTenantVpc.VpcName
	vpcInfo.Xrole = service.Spec.Configuration.EndpointTenantVpc.Xrole
	vpcInfo.DomainID = service.Spec.Configuration.EndpointTenantVpc.DomainID
	vpcInfo.Namespace = service.Spec.Configuration.EndpointTenantVpc.Namespace
	vpcInfo.SecurityGroups = service.Spec.Configuration.EndpointTenantVpc.SecurityGroups
	vpcInfo.IPV6Enable = service.Spec.Configuration.EndpointTenantVpc.IPV6Enable
	vpcInfo.IPV6Gateway = service.Spec.Configuration.EndpointTenantVpc.IPV6Gateway
	vpcInfo.IPV6TenantCidr = service.Spec.Configuration.EndpointTenantVpc.IPV6TenantCidr
	vpcInfo.IPV6HostVMCidr = service.Spec.Configuration.EndpointTenantVpc.IPV6HostVMCidr
	vpcInfo.NeutronSubnetID = service.Spec.Configuration.EndpointTenantVpc.NeutronSubnetID
	vpcInfo.NeutronSubnetIDV6 = service.Spec.Configuration.EndpointTenantVpc.NeutronSubnetIDV6
	vpcInfo.IsSafety = service.Spec.Configuration.EndpointTenantVpc.IsSafety
	return vpcInfo
}

func buildPatKey(tenantID, patPodName string) string {
	return "/net/tenant/" + tenantID + "/vpc/" + patPodName
}

// HandlePatServiceList handle pat service list info
func (v *VpcHandler) HandlePatServiceList(tenantID, funcSvcID string,
	patPodInfo []types.NATConfigure, podName string) {
	log.GetLogger().Debugf("receive handle patServiceList request: %+v, podName: %s", patPodInfo, podName)
	for _, pat := range patPodInfo {
		patKey := buildPatKey(tenantID, pat.PatPodName)
		// store pod name and pat key to patKeyList map and instanceList map
		v.storeTwoWay(podName, patKey, &PatInstance{
			instanceType: workerInstanceType,
			funcSvcID:    funcSvcID,
			podName:      podName,
		})
	}
}

// DeletePatPods -
func (v *VpcHandler) DeletePatPods(podName, funcSvcID, traceID string) {
	if err := v.IdleDeletePatServiceList(podName); err != nil {
		log.GetLogger().Errorf("failed to idle delete pats for %s and %s with traceID %s and %s",
			funcSvcID, podName, traceID, err.Error())
	}
}

// IdleDeletePatServiceList delete pat service list
func (v *VpcHandler) IdleDeletePatServiceList(instanceName string) error {
	if v.VpcPlugin.Plugin == nil {
		return nil
	}
	log.GetLogger().Debugf("receive delete patServiceList request: %s", instanceName)
	if strings.Contains(instanceName, poolSubStr) {
		splits := strings.Split(instanceName, poolSubStr)
		if len(splits) == functionNameIndex {
			instanceName = splits[instanceNameIndex]
		}
	}
	start := time.Now()
	v.vpcMutex.Lock()
	// find pat key list by pod name
	for patKey := range v.patKeyList[instanceName] {
		log.GetLogger().Debugf("instance list length: %d for pat key: %s", len(v.instanceList[patKey]), patKey)
		// load and delete instanceList map by pat key and pod name
		patInstance, ok := v.instanceList[patKey][instanceName]
		if ok {
			delete(v.instanceList[patKey], instanceName)
			// clean instance list map
			if len(v.instanceList[patKey]) == 0 {
				log.GetLogger().Warnf("record delete pat in cache with pat key %s and %s", patKey, instanceName)
				delete(v.instanceList, patKey)
			}
			err := v.recordDeleteRequest(len(v.instanceList[patKey]), patKey, patInstance.funcSvcID)
			if err != nil {
				v.vpcMutex.Unlock()
				log.GetLogger().Errorf("failed to delete PatService with key: %s and error: %s", patKey, err.Error())
				return err
			}
		}
	}
	delete(v.patKeyList, instanceName)
	v.vpcMutex.Unlock()
	log.GetLogger().Debugf("finished to delete patServiceList request: %s, cost: %v", instanceName, time.Since(start))

	return nil
}

// BecomeLeader -
func (v *VpcHandler) BecomeLeader() error {
	if v.VpcPlugin.Plugin == nil {
		log.GetLogger().Infof("vpc plugin is nil and no need to notify become leader")
		return nil
	}
	return v.VpcPlugin.becomeVpcLeader()
}

func (v *VpcHandler) recordDeleteRequest(lengthAfterDelete int, patKey, funcSvcID string) error {
	if lengthAfterDelete == 0 {
		log.GetLogger().Infof("record delete resource with pat key %s for %s", patKey, funcSvcID)
		err := v.VpcPlugin.deleteVpcResource(patKey)
		if err != nil {
			log.GetLogger().Errorf("failed to delete vpc resource: %s", err.Error())
			return err
		}
	}
	return nil
}

// CheckPullTriggerInfo Check PullTrigger Info
func (v *VpcHandler) CheckPullTriggerInfo(funcSvcID, patServiceID, podName string) bool {
	exists := v.loadOrStoreByPodName(patServiceID, podName, &PatInstance{
		instanceType: pullTriggerPodType,
		funcSvcID:    funcSvcID,
		podName:      podName,
	})
	if exists {
		return true
	}
	return false
}

// CleanPullTrigger Clean PullTrigger
func (v *VpcHandler) CleanPullTrigger(FuncName string) {
	if v.VpcPlugin.Plugin == nil {
		return
	}
	log.GetLogger().Infof("receive CleanPullTrigger request, FuncName is: %s", FuncName)
	splits := strings.Split(FuncName, triggerSubStr)
	if len(splits) != defaultTriggerLen {
		return
	}
	deployName := splits[functionNameIndex]
	operationList := make([]operationRecord, 0, defaultKeyLen)
	operationList = append(operationList,
		operationRecord{operateType: k8sType, method: delMethod, operateKey: deployName})
	v.doOperation(operationList, deployName)
	start := time.Now()
	v.vpcMutex.Lock()
	log.GetLogger().Infof("patKeyList len: %d, instanceList len: %d for %s",
		len(v.patKeyList), len(v.instanceList), FuncName)
	// find pat key list by pod name
	for patKey := range v.patKeyList[deployName] {
		log.GetLogger().Debugf("instance list length: %d for pat key: %s", len(v.instanceList[patKey]), patKey)
		// load and delete instanceList map by pat key and pod name
		patInstance, ok := v.instanceList[patKey][deployName]
		if ok {
			delete(v.instanceList[patKey], deployName)
			// clean instance list map
			if len(v.instanceList[patKey]) == 0 {
				log.GetLogger().Warnf("record delete pat in cache with pat key %s and %s", patKey, deployName)
				delete(v.instanceList, patKey)
			}
			err := v.recordDeleteRequest(len(v.instanceList[patKey]), patKey, patInstance.funcSvcID)
			if err != nil {
				v.vpcMutex.Unlock()
				log.GetLogger().Errorf("failed to delete PatService with key: %s and error: %s", patKey, err.Error())
				return
			}
		}
	}
	delete(v.patKeyList, deployName)
	v.vpcMutex.Unlock()
	log.GetLogger().Infof("finished to CleanPullTrigger, FuncName is: %s, cost: %v", FuncName, time.Since(start))
}

func (v *VpcHandler) doOperation(operationList []operationRecord, instanceName string) {
	log.GetLogger().Infof("do k8s operation with length: %d for: %s", len(operationList), instanceName)
	for i := range operationList {
		if operationList[i].operateType == k8sType && operationList[i].method == delMethod {
			podName := operationList[i].operateKey
			err := v.KubeClient.AppsV1().Deployments("default").Delete(context.TODO(), podName, metav1.DeleteOptions{})
			if err != nil && !k8serror.IsNotFound(err) {
				log.GetLogger().Warnf("failed to delete from k8s with pod: %s and error: %s", podName, err.Error())
				continue
			}
			log.GetLogger().Infof("succeed to delete from k8s with pod: %s for: %s", podName, instanceName)
		} else {
			log.GetLogger().Warnf("unKnown operation type: %s for: %s", operationList[i].operateType, instanceName)
			continue
		}
	}
}

// ParseFunctionMeta parse FunctionMeta to CreateVpcTriggerInstruct
func ParseFunctionMeta(meta *faasfunction.FunctionService) *CreateVpcTriggerInstruct {
	name := meta.Spec.Configuration.FunctionName
	splits := strings.Split(name, triggerSubStr)
	if len(splits) != defaultTriggerLen {
		log.GetLogger().Errorf("unsupported type of Parse functionMeta functionName %s", name)
		return nil
	}
	return &CreateVpcTriggerInstruct{
		PulltriggerPort: "28937",
		PodName:         splits[functionNameIndex],
		MemoryOption:    strconv.FormatInt(meta.Spec.Configuration.Resource.Mem, baseInteger),
		CpuOption:       strconv.FormatInt(meta.Spec.Configuration.Resource.CPU, baseInteger),
		Image:           meta.Spec.Configuration.VPCTriggerImage,
		DomainId:        meta.Spec.Configuration.DomainID,
		Namespace:       meta.Spec.Configuration.TenantID,
		VpcName:         meta.Spec.Configuration.FuncVpc.VpcName,
		VpcId:           meta.Spec.Configuration.FuncVpc.VpcID,
		SubnetName:      meta.Spec.Configuration.FuncVpc.SubnetName,
		SubnetId:        meta.Spec.Configuration.FuncVpc.SubnetID,
		TenantCidr:      meta.Spec.Configuration.FuncVpc.TenantCidr,
		HostVmCidr:      meta.Spec.Configuration.FuncVpc.HostVMCidr,
		Gateway:         meta.Spec.Configuration.FuncVpc.Gateway,
		AppXrole:        meta.Spec.Configuration.Role.AppXRole,
		Xrole:           meta.Spec.Configuration.Role.XRole,
	}
}

func checkPatIsUsing(funcSvcID string) bool {
	minScale := cache.WorkerCache.GetMinScale(funcSvcID)
	return minScale > 0
}

func (v *VpcHandler) acquirePatPods(functionMeta *faasfunction.FunctionService) []types.NATConfigure {
	requestInfo := &config.RequestInfo{TraceID: "pull-trigger-" + uuid.New().String()}
	err := v.HandleVpcFunctionInfo(functionMeta, requestInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to handle vpc function info for %s with error %s", functionMeta.ID, err.Error())
		return nil
	}
	return requestInfo.PatPodInfo
}

func (v *VpcHandler) updatePullTriggerCache(funcSvcID string, patPods []types.NATConfigure,
	triggerInfo *CreateVpcTriggerInstruct) {
	for _, pat := range patPods {
		patKey := buildPatKey(triggerInfo.Namespace, pat.PatPodName)
		check := v.CheckPullTriggerInfo(funcSvcID, patKey, triggerInfo.PodName)
		if check {
			log.GetLogger().Infof("%s pullTrigger pod already exist", triggerInfo.PodName)
			continue
		}
	}
}

// ProvisionPullTriggerPod provision pull trigger pod
func (v *VpcHandler) ProvisionPullTriggerPod(functionMeta *faasfunction.FunctionService, replicas int32) error {
	// acquire pat pod
	newPatPods := v.acquirePatPods(functionMeta)
	vpcNatConfByte, err := json.Marshal(newPatPods)
	if err != nil || len(newPatPods) == 0 {
		log.GetLogger().Errorf("failed to handle VpcFunction info for %s", functionMeta.ID)
		return nil
	}
	triggerInfo := ParseFunctionMeta(functionMeta)
	if triggerInfo == nil {
		// trigger name is invalid, no need to retry
		return nil
	}
	// update cache
	v.updatePullTriggerCache(functionMeta.ID, newPatPods, triggerInfo)
	// get pull trigger deploy
	triggerDeploy, err := tools.GetDeployByK8S(tools.GetKubernetesClient(), triggerInfo.PodName)
	if k8serror.IsNotFound(err) {
		// if trigger deploy is not found, it will create pull trigger deploy
		triggerDeploy = MakePullTriggerDeploy(triggerInfo, vpcNatConfByte, replicas)
		if err = tools.CreateDeployByK8S(tools.GetKubernetesClient(), triggerDeploy); err != nil {
			return err
		}
		log.GetLogger().Infof("succeed to create trigger deploy %s for %s with %s, tenantID: %s, replicas: %d",
			triggerDeploy.Name, functionMeta.ID, string(vpcNatConfByte), triggerInfo.Namespace, replicas)
		return nil
	}
	if err != nil {
		return err
	}
	// check if pull trigger deploy record pat pods info is same as pat pod info from create resource method
	// if it is same, it will do noting
	// else, it will update pull trigger deploy
	var curPatPods []types.NATConfigure
	curPatPodsStr := getPullTriggerEnvValueInDeploy(triggerDeploy, pullTriggerEnvKey)
	if err = json.Unmarshal([]byte(curPatPodsStr), &curPatPods); err != nil {
		log.GetLogger().Errorf("failed to unmarshal %s with error %s", curPatPodsStr, err.Error())
		return nil
	}
	triggerDeploy = setPullTriggerEnvInDeploy(triggerDeploy, pullTriggerEnvKey, string(vpcNatConfByte))
	if !isSamePatPods(curPatPods, newPatPods) {
		log.GetLogger().Infof("update trigger deploy %s for %s with diff pats", triggerDeploy.Name, functionMeta.ID)
		triggerDeploy.Spec.Replicas = &replicas
		return tools.UpdateDeployByK8S(tools.GetKubernetesClient(), triggerDeploy)
	}
	if *triggerDeploy.Spec.Replicas != replicas {
		log.GetLogger().Infof("update trigger deploy %s replicas from %d to %d for %s",
			triggerDeploy.Name, *triggerDeploy.Spec.Replicas, replicas, functionMeta.ID)
		triggerDeploy.Spec.Replicas = &replicas
		return tools.UpdateReplicasByK8s(triggerDeploy.Name, replicas)
	}
	log.GetLogger().Infof("no need to update trigger deploy with same pats and same replicas %d, %d for %s",
		*triggerDeploy.Spec.Replicas, replicas, functionMeta.ID)
	return nil
}

func isSamePatPods(curPatPods, newPatPods []types.NATConfigure) bool {
	if len(curPatPods) != len(newPatPods) {
		return false
	}
	if len(curPatPods) == 1 {
		return curPatPods[0].PatPodName == newPatPods[0].PatPodName
	}
	if len(curPatPods) == maxPatPodLength {
		return (curPatPods[0].PatPodName == newPatPods[0].PatPodName &&
			curPatPods[1].PatPodName == newPatPods[1].PatPodName) ||
			(curPatPods[0].PatPodName == newPatPods[1].PatPodName &&
				curPatPods[1].PatPodName == newPatPods[0].PatPodName)
	}
	return false
}

func getPullTriggerEnvs(deploy *v1.Deployment) []corev1.EnvVar {
	containers := deploy.Spec.Template.Spec.Containers
	for _, container := range containers {
		if container.Name == pullTriggerContainerName {
			return container.Env
		}
	}
	return nil
}

func getPullTriggerEnvValueInDeploy(deploy *v1.Deployment, envKey string) string {
	envs := getPullTriggerEnvs(deploy)
	for i := range envs {
		if envs[i].Name == envKey {
			return envs[i].Value
		}
	}
	return ""
}

func setPullTriggerEnvInDeploy(deploy *v1.Deployment, envKey, envValue string) *v1.Deployment {
	envs := getPullTriggerEnvs(deploy)
	for i := range envs {
		if envs[i].Name == envKey {
			envs[i].Value = envValue
		}
	}
	return deploy
}
