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

// Package detector detect pod status
package detector

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/utils"
)

const (
	maxToleranceRestartingTimes          = 3
	monitorRestartingPodsIntervalEnv     = "MONITOR_RESTARTING_PODS_INTERVAL_MINUTE"
	defaultMonitorRestartingPodsInterval = 10
	maxPrintRestartingPodsNum            = 10

	terminatingPodTimeout              = 72 * time.Hour
	checkTerminatingPodIntervalEnv     = "CHECK_TERMINATING_POD_INTERVAL_HOUR"
	defaultCheckTerminatingPodInterval = 24
	checkTerminatingPodTimerPeriod     = time.Duration(1) * time.Hour
)

// PodDetector -
type PodDetector struct {
	// key is pod name, value is RestartingPodInfo
	RestartingPods sync.Map
	// terminatingPodMap key is pod name, value is terminatingTimestamp
	terminatingPodMap sync.Map
}

// RestartingPodInfo -
type RestartingPodInfo struct {
	RestartCount int32
	NodeIP       string
	Reason       string
}

// InitPodDetector -
func InitPodDetector() (*PodDetector, error) {
	alarm.GetClient().RegisterAlarmType(alarm.RestartingPod)
	return &PodDetector{}, nil
}

// Store -
func (pd *PodDetector) Store(pod *corev1.Pod) {
	if pod.DeletionTimestamp != nil {
		pd.terminatingPodMap.LoadOrStore(pod.Name, time.Now())
	}

	pd.statisticsRestartingPods(pod)
}

func (pd *PodDetector) statisticsRestartingPods(pod *corev1.Pod) {
	if utils.IsCustomContainerPod(pod.Name) {
		return
	}

	if utils.IsPodReady(pod) {
		return
	}

	if utils.IsPoolPod(pod) {
		return
	}

	info := RestartingPodInfo{
		RestartCount: 0,
		NodeIP:       pod.Status.HostIP,
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != utils.WorkerContainerName && status.Name != utils.RuntimeContainerName {
			continue
		}
		if status.RestartCount > info.RestartCount {
			info.RestartCount = status.RestartCount
		}
		if status.State.Waiting == nil {
			continue
		}
		info.Reason = status.State.Waiting.Reason
	}
	if info.RestartCount > 0 {
		pd.RestartingPods.Store(pod.Name, info)
		log.GetLogger().Warnf("restarting pod: %s has stored info: %+v in cache", pod.Name, info)
	}
}

// Delete -
func (pd *PodDetector) Delete(podName string) {
	pd.RestartingPods.Delete(podName)
	pd.terminatingPodMap.Delete(podName)
}

func (pd *PodDetector) getRestartingPods() []string {
	pods := make([]string, 0, maxPrintRestartingPodsNum)
	pd.RestartingPods.Range(func(k, v interface{}) bool {
		pod, ok := k.(string)
		if !ok {
			return true
		}
		podInfo, ok := v.(RestartingPodInfo)
		if !ok {
			return true
		}
		if podInfo.RestartCount > maxToleranceRestartingTimes {
			pods = append(pods, pod)
		}
		return true
	})
	return pods
}

func (pd *PodDetector) monitorRestartingPods(stopCh <-chan struct{}) {
	monitorInterval := defaultMonitorRestartingPodsInterval
	if monitorIntervalEnv := os.Getenv(monitorRestartingPodsIntervalEnv); monitorIntervalEnv != "" {
		monitorIntervalFromEnv, err := strconv.Atoi(monitorIntervalEnv)
		if err != nil {
			log.GetLogger().Warnf("can not convert monitorRestartingPodsIntervalEnv to int,"+
				" use default monitor interval: %v", monitorInterval)
		} else if monitorIntervalFromEnv == 0 {
			log.GetLogger().Warnf("find monitorRestartingPodsIntervalEnv == 0, will not monitor restarting pods")
			return
		} else {
			monitorInterval = monitorIntervalFromEnv
		}
	} else {
		log.GetLogger().Warnf("monitorRestartingPodsIntervalEnv is not set,"+
			" use default monitor interval: %v", monitorInterval)
	}

	log.GetLogger().Infof("begin to monitor pool restarting pods in cluster by interval: %v", monitorInterval)
	defer log.GetLogger().Warnf("finished to monitor pool restarting pods in cluster by interval: %v", monitorInterval)
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	isAlarmed := false
	ticker := time.NewTicker(time.Duration(monitorInterval) * time.Minute)
	for {
		select {
		case <-stopCh:
			log.GetLogger().Warnf("stop channel is closed")
			ticker.Stop()
			return
		case <-ticker.C:
			pods := pd.getRestartingPods()
			restartingPodsNum := len(pods)
			log.GetLogger().Infof("there are %d restarting pods and is alarmed: %t", restartingPodsNum, isAlarmed)

			if restartingPodsNum > 0 && !isAlarmed {
				isAlarmed = true
				alarm.GetClient().ReportAlarm(alarm.RestartingPod,
					fmt.Sprintf("error msg=restarting pods num is %d,", restartingPodsNum))
				printRestartingPodsLog(pods)
			} else if restartingPodsNum == 0 && isAlarmed {
				isAlarmed = false
				alarm.GetClient().ClearAlarm(alarm.RestartingPod)
			}
		}
	}
}

func (pd *PodDetector) deleteTimeoutTerminatingPods() {
	kubeClient := utils.GetKubernetesClient()
	pd.terminatingPodMap.Range(func(podName, timestamp interface{}) bool {
		if time.Since(timestamp.(time.Time)) > terminatingPodTimeout {
			log.GetLogger().Infof("delete timeout terminating pod, podName: %s", podName)
			pd.deleteK8sPodByForce(podName, kubeClient)
		}
		return true
	})
}

func (pd *PodDetector) deleteTerminatedPods() {
	kubeClient := utils.GetKubernetesClient()
	pd.terminatingPodMap.Range(func(podName, timestamp interface{}) bool {
		pod, err := utils.GetPodInformer().Lister().Pods(utils.FunctionNameSpace()).Get(podName.(string))
		if err != nil {
			log.GetLogger().Warnf("failed to get terminating pod: %s, %s", podName, err.Error())
			return true
		}
		if !checkPodAllContainerRunning(pod) {
			log.GetLogger().Infof("delete container terminated pod, podName: %s", podName)
			pd.deleteK8sPodByForce(podName, kubeClient)
		}
		return true
	})
}

func (pd *PodDetector) deleteK8sPodByForce(podName interface{}, kubeClient kubernetes.Interface) {
	gracePeriodSeconds := int64(0)
	propagationPolicy := metav1.DeletePropagationForeground
	err := kubeClient.CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), podName.(string),
		metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &propagationPolicy})
	if err != nil && !utils.IsPodNotFound(err) {
		log.GetLogger().Errorf("failed to force delete terminating pod: %s, %s", podName, err.Error())
	} else {
		pd.terminatingPodMap.Delete(podName)
		log.GetLogger().Infof("succeed to force delete terminating pod: %s", podName)
	}
}

func checkPodAllContainerRunning(pod *corev1.Pod) bool {
	allRunning := true
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Terminated != nil {
			allRunning = false
			break
		}
	}
	return allRunning
}

func (pd *PodDetector) checkTimeoutTerminatingPods(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	interval, err := strconv.Atoi(os.Getenv(checkTerminatingPodIntervalEnv))
	if err != nil {
		interval = defaultCheckTerminatingPodInterval
	}
	count := 0
	t := time.NewTicker(checkTerminatingPodTimerPeriod)
	for {
		select {
		case _, ok := <-t.C:
			if !ok {
				return
			}
			count++
			log.GetLogger().Infof("start to check terminated pods")
			// check terminated pods every 1h
			pd.deleteTerminatedPods()
			// check terminating timeout pod every 24h (default)
			if count >= interval {
				log.GetLogger().Infof("start to check timeout terminating pods")
				pd.deleteTimeoutTerminatingPods()
				count = 0
			}

		case <-stopCh:
			t.Stop()
			return
		}
	}
}

func printRestartingPodsLog(pods []string) {
	if len(pods) > maxPrintRestartingPodsNum {
		log.GetLogger().Warnf("report restarting pods alarm, there are %d restarting pods : %v",
			len(pods), pods[:maxPrintRestartingPodsNum])
	} else {
		log.GetLogger().Warnf("report restarting pods alarm, there are %d restarting pods : %v",
			len(pods), pods)
	}
}

// Run starts PodDetector
func (pd *PodDetector) Run(stopCh <-chan struct{}) {
	go pd.monitorRestartingPods(stopCh)
	go pd.checkTimeoutTerminatingPods(stopCh)
	go utils.StartRetryFailedPodOperation(stopCh)
}
