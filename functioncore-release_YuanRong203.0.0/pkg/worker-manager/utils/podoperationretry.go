/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package utils

import (
	"container/list"
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/logger/log"
)

// enum of failed pod operation type
const (
	DeleteType = "DELETE"
	PatchType  = "PATCH"
)

const (
	retryDeletePodInterval             = "RETRY_DELETE_POD_INTERVAL"
	defaultRetryDeletePodInterval      = 15
	defaultRetryTooManyRequestInterval = 3
)

func init() {
	RetryPodOperationList = NewPodOperationRetryList()
}

// RetryPodOperationList failed to delete pods before, need to retry
var RetryPodOperationList *PodOperationRetryList

// PodOperationEle retry pods operation element
type PodOperationEle struct {
	PodName                 string
	OperationType           string
	Ctx                     context.Context
	Namespace               string
	OperationOptions        interface{}
	StrategicMergePatchType types.PatchType
	PatchBytes              []byte
}

// PodOperationRetryList retry operation list
type PodOperationRetryList struct {
	podOpRetryList *list.List
	mutex          sync.Mutex
}

// NewPodOperationRetryList init retry pod list
func NewPodOperationRetryList() *PodOperationRetryList {
	return &PodOperationRetryList{
		podOpRetryList: list.New(),
		mutex:          sync.Mutex{},
	}
}

// AddPodOperation add failed pods operation to retry list
func (retryList *PodOperationRetryList) AddPodOperation(podOpEle *PodOperationEle) {
	retryList.mutex.Lock()
	defer retryList.mutex.Unlock()
	retryList.podOpRetryList.PushBack(podOpEle)
	return
}

// StartRetryFailedPodOperation start go routine to retry failed pods operation, e.g. Delete, Patch
func StartRetryFailedPodOperation(stopCh <-chan struct{}) {
	if RetryPodOperationList != nil {
		retryPodOperation(stopCh)
	}
}

func (retryList *PodOperationRetryList) copyPodOperationList() *list.List {
	// return original list and init a new one to avoid blocking add-operation in other routines
	retryList.mutex.Lock()
	defer retryList.mutex.Unlock()
	tmpList := retryList.podOpRetryList
	retryList.podOpRetryList = list.New()
	return tmpList
}

func retryPodOperationList() {
	kubeClient := GetKubernetesClient()
	if kubeClient == nil {
		log.GetLogger().Errorf("failed to get kubernetes client when retrying failed pods operations")
		return
	}
	if RetryPodOperationList == nil {
		log.GetLogger().Errorf("RetryPodOperationList is nil when retrying failed pods operations")
		return
	}
	// copy list and delete all pods in the copied list, if still failed then re-add to list again
	waitDeletePodList := RetryPodOperationList.copyPodOperationList()
	log.GetLogger().Infof("start to retry to failed operations of pods, len: %d", waitDeletePodList.Len())
	for ele := waitDeletePodList.Front(); ele != nil; ele = ele.Next() {
		podEle, ok := ele.Value.(*PodOperationEle)
		if !ok {
			log.GetLogger().Errorf("failed to covert retry list ele to PodOperationEle type, value: %v",
				ele.Value)
			continue
		}
		retryOperation(podEle, kubeClient)
	}
}

func retryOperation(podEle *PodOperationEle, kubeClient kubernetes.Interface) {
	if RetryPodOperationList == nil {
		log.GetLogger().Errorf("RetryPodOperationList is nil when retrying failed pods operations")
		return
	}
	switch podEle.OperationType {
	case PatchType:
		operationOptions, ok := podEle.OperationOptions.(metav1.PatchOptions)
		if !ok {
			log.GetLogger().Errorf("failed to covert value to PatchOptions in pod operation retry list "+
				"and will remove it from retry list, value: %v", podEle)
			return
		}
		ctx, cancel := context.WithTimeout(podEle.Ctx, K8sPatchTimeout)
		_, err := kubeClient.CoreV1().Pods(podEle.Namespace).Patch(ctx, podEle.PodName,
			podEle.StrategicMergePatchType, podEle.PatchBytes, operationOptions)
		cancel()
		if err != nil && !IsPodNotFound(err) {
			log.GetLogger().Errorf("failed to update pod %s label, error %s", podEle.PodName, err.Error())
			RetryPodOperationList.AddPodOperation(podEle)
		}
		if k8serror.IsTooManyRequests(err) {
			log.GetLogger().Infof("too many request for api server, sleep few seconds")
			time.Sleep(time.Second * defaultRetryTooManyRequestInterval)
		}
	case DeleteType:
		operationOptions, ok := podEle.OperationOptions.(metav1.DeleteOptions)
		if !ok {
			log.GetLogger().Errorf("failed to covert value to DeleteOptions in pod operation retry list "+
				"and will remove it from retry list, value: %v", podEle)
			return
		}
		err := kubeClient.CoreV1().Pods(podEle.Namespace).Delete(podEle.Ctx, podEle.PodName, operationOptions)
		isDeleted := err == nil || k8serror.IsNotFound(err)
		if !isDeleted {
			log.GetLogger().Errorf("failed to delete pod %s, error %s", podEle.PodName, err.Error())
			RetryPodOperationList.AddPodOperation(podEle)
		}
		if k8serror.IsTooManyRequests(err) {
			log.GetLogger().Infof("too many request for api server, sleep few seconds")
			time.Sleep(time.Second * defaultRetryTooManyRequestInterval)
		}
	default:
		log.GetLogger().Errorf("invalid operation type for retry, value: %s", podEle.OperationOptions)
		return
	}
}

func retryPodOperation(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	interval, err := strconv.Atoi(os.Getenv(retryDeletePodInterval))
	if err != nil {
		interval = defaultRetryDeletePodInterval
	}
	t := time.NewTicker(time.Duration(interval) * time.Second)
	for {
		select {
		case _, ok := <-t.C:
			if !ok {
				t.Stop()
				return
			}
			log.GetLogger().Infof("start to delete pods in retry delete list")
			retryPodOperationList()
		case <-stopCh:
			t.Stop()
			return
		}
	}
}
