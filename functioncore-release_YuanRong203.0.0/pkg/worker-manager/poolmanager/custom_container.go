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

// Package poolmanager manages pool
package poolmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fhttp "github.com/valyala/fasthttp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/redisclient/podevent"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tools"
	"functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
)

const (
	minIDValue                                 = -1
	customContainerDefaultWaitReadyTimeout     = 5 * time.Minute
	customContainerDefaultUnschedulableTimeout = 30 * time.Second
	// 20G
	customContainerMaxImageSize = 20 << 30

	defaultLivenessProbePeriodSeconds          = 5
	defaultLivenessProbeFailureThreshold       = 3
	defaultCustomLivenessProbeFailureThreshold = 20
	defaultCustomLivenessProbeFailureSeconds   = 1

	customContainerDemandNodeAffinityWeight = 100

	waitWorkerReadyRequestTimeout = 1 * time.Second
	waitWorkerReadyBackoff        = 20 * time.Millisecond
	waitWorkerReadyRetryTimes     = 500
	notifyWorkerRetryTimes        = 10
	getWorkerRetryTimes           = 300

	// Image name example: swr.cn-north-1.myhuaweicloud.com/mynamespace/myrepo:mytag
	imageNameTokenMinSize = 3
	imageRepoAndTagSize   = 2

	// k8s pod log parameter settings
	maxTailLogSize                  = 2 * 1024 // 2*1024
	maxTailLogLine                  = 20
	customContainerGpuPodNamePrefix = "custom.image.gpu"
	minMountFileSize                = 1
	defaultMaxMountFileSize         = 30

	logConfigWorkerMountPath = "/home/sn/log/logconfig"

	defaultPreferredWeight = 60
)

const (
	fieldSelectOfPodName = "involvedObject.name=%s"
	podKind              = "Pod"
	noSpaceMessage       = "no space left on device"

	labelApp     = "app"
	labelCffType = "cff-type"
	// cae terminationGracePeriodSeconds
	caeTerminationGracePeriodSeconds = 920
)

var (
	components = []string{
		"worker-manager",
		"cache-manager",
		"state-manager",
		"frontend",
		"faas-scheduler",
	}
	labelValueOfVPCPullTrigger   = []string{"serverless"}
	maxMountFileSize             = defaultMaxMountFileSize
	enableSpecialImagePullPolicy = os.Getenv("ENABLE_SPECIAL_IMAGE_PULL_POLICY") != ""
	unmarshalErrReg              = regexp.MustCompile("(.*)cannot unmarshal (.*) into Go struct field(.*)")
)

type imageName struct {
	nameSpace  string
	repository string
	tag        string
}

type customContainerMGR struct {
	workerClient    *fhttp.Client
	flight          *imageVerifyFlight
	dataFlight      *swrDataFlight
	nameSpace       string
	kubeClient      kubernetes.Interface
	config          config.PoolMgrConfig
	onDemandWaitMap sync.Map // key: pod.Name, value: *customContainerWaiter
}

func init() {
	sizeFromEnv, err := strconv.Atoi(os.Getenv("MAX_MOUNT_FILE_SIZE"))
	if err == nil && sizeFromEnv >= minMountFileSize {
		maxMountFileSize = sizeFromEnv
	}
}

func newCustomContainerMGR(workerClient *fhttp.Client, kubeClient kubernetes.Interface, cfg config.PoolMgrConfig,
) *customContainerMGR {
	return &customContainerMGR{
		workerClient: workerClient,
		flight:       newImageVerifyFlight(),
		dataFlight:   newSwrDataFlight(),
		nameSpace:    utils.FunctionNameSpace(),
		kubeClient:   kubeClient,
		config:       cfg,
	}
}

func (c *customContainerMGR) makeCustomContainerFileMountSecret(ctx context.Context, podName string,
	spec *faasfunction.FunctionServiceSpec, req common.PodRequest, handler *swrHandler) (customContainerAddOn, error) {
	regcredName := podName + ".regcred"
	secret := newCustomContainerRegcred(c, regcredName, spec, handler, req.TraceID)
	if err := secret.create(ctx); err != nil {
		return nil, fmt.Errorf("failed to create custom container image pull secret: %s", err.Error())
	}
	return &simpleAddOn{
		preCreateFn: func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = append(p.Spec.ImagePullSecrets, corev1.LocalObjectReference{
				Name: regcredName,
			})
		},
		postCreateFn: func(p *corev1.Pod) {
			if err := secret.setOwner(context.TODO(), p); err != nil {
				log.GetLogger().Warnf(
					"failed to set custom container image pull secret owner to pod: %s, traceID: %s",
					err.Error(), req.TraceID)
			}
		},
		rollbackFn: func() {
			if err := secret.delete(context.TODO()); err != nil {
				log.GetLogger().Warnf(
					"failed to delete custom container image pull secret: %s, traceID: %s",
					err.Error(), req.TraceID)
			}
		},
	}, nil
}

func (c *customContainerMGR) prepareSWRAddOn(ctx context.Context, podName string,
	spec *faasfunction.FunctionServiceSpec, req common.PodRequest) (customContainerAddOn, error) {
	if spec.Configuration.CustomContainerConfig.UseUserAuth {
		return c.makeCustomContainerFileMountSecret(ctx, podName, spec, req, nil)
	}

	swrHandler := newSWRHandler(spec, utils.DefaultSWRErrRespHandlers, req.TraceID)
	if !spec.Configuration.CustomContainerConfig.IsPublic {
		imageVerifier := newImageVerifier(swrHandler, req.TraceID)
		verifyFlightKey := spec.Configuration.TenantID + spec.Configuration.DomainID +
			strings.TrimSpace(spec.Configuration.CustomContainerConfig.Image)
		err := c.flight.do(verifyFlightKey, func() error {
			err := imageVerifier.verify(ctx, spec.Configuration.CustomContainerConfig.Image)
			if err != nil && !strings.Contains(err.Error(), "The throttling threshold has been reached") {
				c.flight.remove(verifyFlightKey)
				return err
			}

			// The verification remains valid for the next minute.
			time.AfterFunc(1*time.Minute, func() { c.flight.remove(verifyFlightKey) })
			return err
		})
		if err != nil {
			log.GetLogger().Errorf("failed to verify image: %s", err.Error())
			return nil, err
		}
	}

	return c.makeCustomContainerFileMountSecret(ctx, podName, spec, req, swrHandler)
}

func generatePodName(req common.PodRequest, spec *faasfunction.FunctionServiceSpec) string {
	var podName string
	podName = strings.ToLower(fmt.Sprintf("custom-%d-%d-%s", req.CPU, req.Mem, commonutils.UniqueID()))
	if FilterCheckpointCustom(req) {
		podNameStr := fmt.Sprintf("checkpoint.%s", podName)
		if len(req.ClusterID) != 0 && req.ClusterID != utils.GetEnvClusterID() {
			podNameStr = fmt.Sprintf("upload.%s", podName)
		}
		if req.IsNeedDeleteSnapshot {
			podNameStr = fmt.Sprintf("delete.%s", podName)
			log.GetLogger().Infof("delete.custom pod image is: %s, traceId: %s",
				spec.Configuration.CustomContainerConfig.Image, req.TraceID)
		}
		if utils.IsSnapShotModel() {
			podNameStr = fmt.Sprintf("checkpoint.upgrade.%s", podName)
		}
		podName = strings.ToLower(podNameStr)
	}
	return podName
}

func (c *customContainerMGR) preparePodSpecAndAddOn(ctx context.Context, spec *faasfunction.FunctionServiceSpec,
	req common.PodRequest, pool *Pool, podName string) (*corev1.Pod, customContainerAddOn, error) {
	log.GetLogger().Infof("begin to generate pod by name: %s, traceId: %s", podName, req.TraceID)
	var addOns []customContainerAddOn
	if !c.config.CustomContainerConfig.DisableSWRAddOn {
		addOn, err := c.prepareSWRAddOn(ctx, podName, spec, req)
		if err != nil {
			log.GetLogger().Errorf("failed to prepare secret: %s", err.Error())
			return nil, nil, err
		}
		addOns = append(addOns, addOn)
	} else if spec.Configuration.CustomContainerConfig.UseUserAuth ||
		isNeedMountFile(spec.Configuration.FileMountConfig) ||
		len(spec.Configuration.FileMountConfig.SecretFiles) > 0 {
		addOn, err := c.makeCustomContainerFileMountSecret(ctx, podName, spec, req, nil)
		if err != nil {
			return nil, nil, err
		}
		addOns = append(addOns, addOn)
	}

	pod, err := c.createPodSpec(podName, spec, pool, req)
	if err != nil {
		log.GetLogger().Errorf("failed to create custom container pod spec: %s", err.Error())
		return nil, nil, err
	}

	return pod, combinedAddOn{addOns: addOns}, nil
}

type customContainerAddOn interface {
	preCreate(*corev1.Pod)
	postCreate(*corev1.Pod)
	rollback()
}

type simpleAddOn struct {
	preCreateFn  func(*corev1.Pod)
	postCreateFn func(*corev1.Pod)
	rollbackFn   func()
}

func (a simpleAddOn) preCreate(pod *corev1.Pod) {
	if a.preCreateFn != nil {
		a.preCreateFn(pod)
	}
}

func (a simpleAddOn) postCreate(pod *corev1.Pod) {
	if a.postCreateFn != nil {
		a.postCreateFn(pod)
	}
}

func (a simpleAddOn) rollback() {
	if a.rollbackFn != nil {
		a.rollbackFn()
	}
}

type combinedAddOn struct {
	addOns []customContainerAddOn
}

func (a combinedAddOn) forEach(fn func(customContainerAddOn)) {
	for _, addOn := range a.addOns {
		fn(addOn)
	}
}

func (a combinedAddOn) preCreate(pod *corev1.Pod) {
	a.forEach(func(addOn customContainerAddOn) { addOn.preCreate(pod) })
}

func (a combinedAddOn) postCreate(pod *corev1.Pod) {
	a.forEach(func(addOn customContainerAddOn) { addOn.postCreate(pod) })
}

func (a combinedAddOn) rollback() {
	a.forEach(func(addOn customContainerAddOn) { addOn.rollback() })
}

type createCustomContainerPodParam struct {
	pool           *Pool
	spec           *faasfunction.FunctionServiceSpec
	ctx            context.Context
	req            common.PodRequest
	customResource map[string]int64
	demandNodes    []string
}

func (c *customContainerMGR) retryCreatePodWithSpec(param createCustomContainerPodParam, pod *corev1.Pod,
	addOn customContainerAddOn) (*corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(param.ctx, getTimeoutDuration(
		c.config.CustomContainerConfig.WaitReadyTimeout, customContainerDefaultWaitReadyTimeout))
	defer cancel()
	param.ctx = ctx

	cnt := 0
	for {
		readyPod, err := c.createPodWithSpec(param, pod, addOn)
		// only retry when GPU pod get startError
		if err == nil || !utils.IsXGPUStartError(err) {
			return readyPod, err
		}
		cnt++
		// renew pod name
		pod.Name = strings.ToLower(fmt.Sprintf("custom.image.gpu-%d-%d-%d-%s",
			param.req.CPU, param.req.Mem, param.req.GpuMemory, commonutils.UniqueID()))
		time.Sleep(time.Second)
		log.GetLogger().Warnf("retry to create pod with start error, retry count %d, traceID %s", cnt,
			param.req.TraceID)
	}
}

func (c *customContainerMGR) createPodWithSpec(param createCustomContainerPodParam, pod *corev1.Pod,
	addOn customContainerAddOn) (*corev1.Pod, error) {
	start := time.Now()
	var err error
	var curPod *corev1.Pod
	var readyPod *corev1.Pod

	// the cache of instance need before create because if cache not exist, the status notify of pod will ignored
	strength.GetPodCache().SetState(pod.Name, "", strength.BeforeSpecialized)
	waiter := utils.NewPodWaiter()
	c.onDemandWaitMap.Store(pod.Name, waiter)

	// avoid modify the request CPU of the pod repeatedly
	var copyPod = pod.DeepCopy()
	if utils.IsVpaFeatureEnabled() && commonutils.IsCAEFunc(param.spec.Configuration.BusinessType) {
		for _, container := range copyPod.Spec.Containers {
			if container.Name == utils.RuntimeContainerName {
				container.Resources.Requests["cpu"] = resource.MustParse(fmt.Sprintf("%dm",
					int64(float64(container.Resources.Requests.Cpu().MilliValue())*utils.GetCAERequestFactor())))
			}
		}
	}
	defer func() {
		if err != nil {
			stageInfo := &podevent.EventStageInfo{
				EventStage:   podevent.PullingStage,
				EventSummary: podevent.PodPullFailed,
				EventType:    podevent.EventErrorType,
			}
			podevent.ReportCaePodEvent(pod, param.spec, stageInfo, "", err)
		}
	}()
	curPod, err = c.kubeClient.CoreV1().Pods(c.nameSpace).Create(param.ctx, copyPod, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create pod: %s, error: %s", copyPod.Name, err.Error())
		addOn.rollback()
		// reset the cache
		strength.GetPodCache().DelState(pod.Name)
		c.onDemandWaitMap.Delete(pod.Name)
		c.forceDeletePodAndAlarm(param, err, copyPod)
		return nil, err
	}

	if commonutils.IsCAEFunc(param.spec.Configuration.BusinessType) && !FilterCheckpointCustom(param.req) {
		etcdhandler.PutInitInstanceToCAERouterEtcd(param.ctx, curPod, param.spec)
	}
	log.GetLogger().Infof("wait for custom container pod to become ready, podName: %s, deployNode: %s, traceID: %s",
		curPod.Name, curPod.Spec.NodeName, param.req.TraceID)
	notifyCtx, notifyCancel := context.WithTimeout(param.ctx, customContainerDefaultWaitReadyTimeout)
	defer notifyCancel()
	if len(param.req.FuncMountConfig.FuncMounts) != 0 {
		log.GetLogger().Infof("try to notify custom worker: %s for func mount information, traceID: %s",
			curPod.Name, param.req.TraceID)
		go c.notifyWorker(notifyCtx, curPod)
	}
	readyPod, err = c.waitPodReady(param.ctx, curPod, addOn, waiter)
	if err != nil {
		if commonutils.IsCAEFunc(param.spec.Configuration.BusinessType) && !FilterCheckpointCustom(param.req) {
			etcdhandler.DeleteInitInstanceFromCAERouterEtcd(context.Background(), pod, param.spec)
		}
		log.GetLogger().Errorf("failed to wait pod ready: %s, traceID: %s", err.Error(), param.req.TraceID)
		return nil, err
	}
	podReady := time.Now()

	err = c.waitWorkerReady(param.ctx, readyPod)
	if err != nil {
		if commonutils.IsCAEFunc(param.spec.Configuration.BusinessType) && !FilterCheckpointCustom(param.req) {
			etcdhandler.DeleteInitInstanceFromCAERouterEtcd(context.Background(), pod, param.spec)
		}
		log.GetLogger().Errorf("failed to wait worker ready: %s, traceID: %s", err.Error(), param.req.TraceID)
		return nil, err
	}

	log.GetLogger().Infof("custom container pod is ready, podName: %s, deployNode: %s, traceID: %s, cost: %s|%s",
		readyPod.Name, readyPod.Spec.NodeName, param.req.TraceID, podReady.Sub(start), time.Now().Sub(podReady))
	return readyPod, nil
}

func (c *customContainerMGR) forceDeletePodAndAlarm(
	param createCustomContainerPodParam, err error, copyPod *corev1.Pod) {
	if unmarshalErrReg.MatchString(err.Error()) {
		deletionPolicy := metav1.DeletePropagationForeground
		gracePeriodSeconds := int64(0)
		var option = metav1.DeleteOptions{PropagationPolicy: &deletionPolicy, GracePeriodSeconds: &gracePeriodSeconds}
		if err := c.kubeClient.CoreV1().Pods(c.nameSpace).Delete(param.ctx, copyPod.Name, option); err != nil {
			log.GetLogger().Errorf("failed to force delete %s pod: %s, error: %s",
				param.spec.Configuration.FunctionURN, copyPod.Name, err.Error())
		}
		alarm.GetClient().ReportAlarm(alarm.CreatePodRespFormatErr,
			fmt.Sprintf("error msg=%s create pod %s resp format error %s",
				param.spec.Configuration.FunctionURN, copyPod.Name, err.Error()))
	}
}

func (c *customContainerMGR) createMigratePod(param createCustomContainerPodParam) (*corev1.Pod, error) {
	pod := param.req.PodTemplate
	var addOns []customContainerAddOn
	if !c.config.CustomContainerConfig.DisableSWRAddOn {
		addOn, err := c.prepareSWRAddOn(param.ctx, pod.Name, param.spec, param.req)
		if err != nil {
			log.GetLogger().Errorf("failed to prepare secret: %s", err.Error())
			return nil, err
		}
		addOns = append(addOns, addOn)
	} else if param.spec.Configuration.CustomContainerConfig.UseUserAuth ||
		isNeedMountFile(param.spec.Configuration.FileMountConfig) ||
		len(param.spec.Configuration.FileMountConfig.SecretFiles) > 0 {
		addOn, err := c.makeCustomContainerFileMountSecret(param.ctx, pod.Name, param.spec, param.req, nil)
		if err != nil {
			return nil, err
		}
		addOns = append(addOns, addOn)
	}
	addOn := combinedAddOn{addOns: addOns}
	addOn.preCreate(pod)

	return c.createPodWithSpec(param, pod, addOn)
}

func generateErrWithPodName(podName string, isCAE bool, err error) error {
	if !isCAE {
		return err
	}
	if err != nil {
		newErr, ok := err.(snerror.SNError)
		if !ok {
			return fmt.Errorf("%s "+utils.PodNameSplit+" %s", podName, err.Error())
		}
		return newErr
	}
	return nil
}

func (c *customContainerMGR) createPod(param createCustomContainerPodParam) (*corev1.Pod, error) {
	if isNeedMountFile(param.spec.Configuration.FileMountConfig) {
		err := checkMountFile(param.spec.Configuration.FileMountConfig)
		if err != nil {
			return nil, err
		}
	}
	podName := generatePodName(param.req, param.spec)
	isCAE := commonutils.IsCAEFunc(param.spec.Configuration.BusinessType)
	pod, addOn, err := c.preparePodSpecAndAddOn(param.ctx, param.spec, param.req, param.pool, podName)
	if err != nil {
		log.GetLogger().Errorf("failed to prepare pod spec and addOn: %s, traceID: %s", err.Error(), param.req.TraceID)
		return nil, generateErrWithPodName(podName, isCAE, err)
	}

	if !param.pool.CaeCustomizePoolEnable {
		utils.ConfigureResource(param.req.CPU, param.req.Mem, &pod.Spec, param.spec.Configuration.BusinessType)
	}

	addOn.preCreate(pod)

	configCustomResources(pod, param.customResource)
	configDemandNodes(pod, c.config.PoolList.NodeAffinity, param.demandNodes, param.req.ForceDeploy)
	configPodSchedulePolicyWithLabels(pod, param.req.FunSvcID)
	configCustomContainerLiftCycle(pod)
	if len(param.spec.Configuration.DNSDomainCfg) != 0 || len(param.spec.Configuration.EndpointDomainCfg) != 0 {
		err = configDNS(pod, utils.CrNameByURN(param.spec.Configuration.FunctionVersionURN))
		if err != nil {
			log.GetLogger().Errorf("failed to configDNS: %s", err.Error())
			return nil, generateErrWithPodName(podName, isCAE, err)
		}
	}
	if err = c.configGPU(param, pod); err != nil {
		return nil, generateErrWithPodName(podName, isCAE, err)
	}
	if err = c.configCAENodeAffinity(param.spec.Configuration.BusinessType, pod); err != nil {
		return nil, generateErrWithPodName(podName, isCAE, err)
	}
	c.configNodeAffinityOfImageCache(param.spec.Configuration.FunctionVersionURN, pod)
	if err = c.configNodeAffinityOfDisk(param.req.FunSvcID, pod); err != nil {
		return nil, generateErrWithPodName(podName, isCAE, err)
	}

	if param.req.Applier == constants.WorkerManagerApplier {
		configAntiAffinityForReservedIns(param, pod)
	}

	configAntiAffinityForSystemComponents(pod)

	pod, err = c.retryCreatePodWithSpec(param, pod, addOn)
	return pod, generateErrWithPodName(podName, isCAE, err)
}

func configAntiAffinityForReservedIns(param createCustomContainerPodParam, pod *corev1.Pod) {
	antiAffinityValue := utils.FnvHash(param.spec.Configuration.FunctionVersionURN)
	pod.Labels["urnHash"] = antiAffinityValue
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAntiAffinity == nil {
		pod.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}
	pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		corev1.WeightedPodAffinityTerm{
			Weight: 20,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"urnHash": antiAffinityValue,
					},
				},
				TopologyKey: k8sHostNameKey,
			},
		})
}

func configAntiAffinityForSystemComponents(pod *corev1.Pod) {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAntiAffinity == nil {
		pod.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}
	pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		getWeightedPodAffinityTermOfSystemComponents())
}

func (c *customContainerMGR) configCAENodeAffinity(businessType string, pod *corev1.Pod) error {
	if !commonutils.IsCAEFunc(businessType) {
		return nil
	}
	caePreferredSchedulingTerm := corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      utils.PoolNodeLabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{utils.CAENodeAffinityValue},
			}},
		},
	}
	nodeAffinity := pod.Spec.Affinity.NodeAffinity
	if nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution =
			[]corev1.PreferredSchedulingTerm{caePreferredSchedulingTerm}
	} else {
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution =
			append(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, caePreferredSchedulingTerm)
	}
	if utils.IsVpaFeatureEnabled() {
		terms := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		for i := range terms {
			terms[i].MatchExpressions = append(terms[i].MatchExpressions, corev1.NodeSelectorRequirement{
				Key:      constants.NodeVpaPausePodFaultsLabelKey,
				Operator: corev1.NodeSelectorOpDoesNotExist,
				Values:   []string{},
			})
		}
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = terms
	}
	return nil
}

func (c *customContainerMGR) configNodeAffinityOfDisk(funcSvcID string, pod *corev1.Pod) error {
	notSchedulerList, usdDefaultScheduler, nodeMirroredDiskCacheLen := c.calculateNotSchedulerList(funcSvcID)

	// means all nodes can't scheduler need return error immediately
	if len(notSchedulerList) == nodeMirroredDiskCacheLen && !usdDefaultScheduler {
		return utils.ErrNoNodesCanScheduler
	}

	if len(notSchedulerList) == 0 || usdDefaultScheduler {
		log.GetLogger().Warnf("use default scheduler, notSchedulerList len is: %d, useDefaultScheduler is %t",
			len(notSchedulerList), usdDefaultScheduler)
		return nil
	}

	nodeSelectorTerm := corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{
			{
				Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpNotIn, Values: notSchedulerList,
			},
		},
	}

	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{nodeSelectorTerm},
				},
			},
		}
		return nil
	}

	nodeAffinity := pod.Spec.Affinity.NodeAffinity
	if nodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{nodeSelectorTerm},
			},
		}

		return nil
	}

	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{nodeSelectorTerm},
		}
		return nil
	}

	nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms =
		append(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, nodeSelectorTerm)

	return nil
}

func (c *customContainerMGR) configNodeAffinityOfImageCache(funcURN string, pod *corev1.Pod) {
	cacheSchedulerList, ok := cache.ImageNodeCache.Load(funcURN)
	if !ok {
		log.GetLogger().Infof("function %s do not get configure affinity policies for image cache nodes", funcURN)
		return
	}

	schedulerList, ok := cacheSchedulerList.([]string)
	if !ok || len(schedulerList) == 0 {
		log.GetLogger().Infof("function %s do not need to configure affinity policies for image cache nodes", funcURN)
		return
	}

	schedulerNodeList := convertNodeIPToNodeName(schedulerList)
	if len(schedulerNodeList) == 0 {
		log.GetLogger().Infof("function %s do not get node name to configure affinity policies", funcURN)
		return
	}

	nodePreferredSchedulingTerm := corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      corev1.LabelHostname,
				Operator: corev1.NodeSelectorOpIn,
				Values:   schedulerNodeList,
			}},
		},
	}
	nodeAffinity := pod.Spec.Affinity.NodeAffinity
	if nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution =
			[]corev1.PreferredSchedulingTerm{nodePreferredSchedulingTerm}
	} else {
		nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution =
			append(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, nodePreferredSchedulingTerm)
	}
	return
}

func (c *customContainerMGR) configGPU(param createCustomContainerPodParam, pod *corev1.Pod) error {
	if param.req.GpuMemory > 0 {
		gpuMemoryCapacity := c.config.GPUPodConfig.GPUMemoryCapacity[param.req.GpuType]
		if gpuMemoryCapacity <= 0 {
			gpuMemoryCapacity = int64(utils.DefaultGPUMemoryCapacity)
		}
		configGPU(pod, param.req, gpuMemoryCapacity, c.config.PoolList.NodeAffinity.NodeAffinityKey)
	}
	return nil
}

func (c *customContainerMGR) waitPodReady(parent context.Context, curPod *corev1.Pod,
	addOn customContainerAddOn, waiter *utils.PodWaiter) (*corev1.Pod, error) {

	waitReadyCtx, waitReadyCancel := context.WithTimeout(parent, getTimeoutDuration(
		c.config.CustomContainerConfig.WaitReadyTimeout,
		customContainerDefaultWaitReadyTimeout))

	unschedulableCtx, unschedulableCancel := context.WithTimeout(waitReadyCtx, getTimeoutDuration(
		c.config.CustomContainerConfig.UnschedulableTimeout,
		customContainerDefaultUnschedulableTimeout))

	defer func() {
		c.onDemandWaitMap.Delete(curPod.Name)
		unschedulableCancel()
		waitReadyCancel()
	}()

	helper := func(ctx context.Context, ctxDone func() (*corev1.Pod, error)) (*corev1.Pod, error) {
		select {
		case <-ctx.Done():
			log.GetLogger().Infof("receive ctx.Done for custom container pod %s", curPod.Name)
			return ctxDone()
		case status := <-waiter.WaitStatus():
			log.GetLogger().Infof("receive notification for custom container pod %s", curPod.Name)
			if err := status.Err; err != nil {
				// The pod has entered a non-recoverable error state. Tell the user what was going on.
				c.waitPodReadyRollback(curPod, addOn, err, false)
				return nil, err
			}

			addOn.postCreate(curPod)
			return status.Pod, nil
		}
	}

	return helper(unschedulableCtx, func() (*corev1.Pod, error) {
		if waiter.IsUnschedulable() {
			// The cluster does not have enough resources to run the pod. Return early.
			c.waitPodReadyRollback(curPod, addOn, nil, false)
			return nil, fmt.Errorf("custom container pod %s is still unschedulable", curPod.Name)
		}
		return helper(waitReadyCtx, func() (*corev1.Pod, error) {
			c.waitPodReadyRollback(curPod, addOn, nil, true)
			return nil, fmt.Errorf("never receive notification for custom container pod %s", curPod.Name)
		})
	})
}

func printPodNotReadyStatus(originPodStatus corev1.PodStatus) []byte {
	var notReadyPodStatus = corev1.PodStatus{}
	notReadyPodStatus.Phase = originPodStatus.Phase
	notReadyPodStatus.Reason = originPodStatus.Reason
	notReadyPodStatus.Message = originPodStatus.Message
	var containerStatus corev1.ContainerStatus
	for _, status := range originPodStatus.InitContainerStatuses {
		if status.Ready == true && status.RestartCount == 0 {
			continue
		}
		containerStatus = corev1.ContainerStatus{}
		containerStatus.Name = status.Name
		containerStatus.State = status.State
		containerStatus.RestartCount = status.RestartCount
		notReadyPodStatus.InitContainerStatuses = append(notReadyPodStatus.InitContainerStatuses, containerStatus)
	}
	for _, status := range originPodStatus.ContainerStatuses {
		if status.Ready == true && status.RestartCount == 0 {
			continue
		}
		containerStatus = corev1.ContainerStatus{}
		containerStatus.Name = status.Name
		containerStatus.State = status.State
		containerStatus.RestartCount = status.RestartCount
		notReadyPodStatus.ContainerStatuses = append(notReadyPodStatus.ContainerStatuses, containerStatus)
	}
	notReadyPodStatus.Conditions = originPodStatus.Conditions
	if marshalBytes, err := json.Marshal(notReadyPodStatus); err == nil {
		return marshalBytes
	}
	return []byte{}
}

func (c *customContainerMGR) waitPodReadyRollback(pod *corev1.Pod, addOn customContainerAddOn, waitErr error,
	isForceDelete bool) {
	{
		curPod, err := c.kubeClient.CoreV1().Pods(c.nameSpace).Get(context.TODO(), pod.Name,
			metav1.GetOptions{})
		if err != nil {
			log.GetLogger().Warnf("failed to get custom container pod %s: %s", pod.Name, err.Error())
		} else {
			log.GetLogger().Warnf("custom container pod %s on node %s status: %s",
				curPod.Name, curPod.Status.HostIP, string(printPodNotReadyStatus(curPod.Status)))
		}
	}
	option := metav1.DeleteOptions{}
	if isForceDelete {
		log.GetLogger().Infof("pod %s will be force deleted", pod.Name)
		deletionPolicy := metav1.DeletePropagationForeground
		gracePeriodSeconds := int64(0)
		option = metav1.DeleteOptions{PropagationPolicy: &deletionPolicy, GracePeriodSeconds: &gracePeriodSeconds}
	}
	if err := c.kubeClient.CoreV1().Pods(c.nameSpace).Delete(context.TODO(), pod.Name, option); err != nil {
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        c.nameSpace,
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          pod.Name,
			OperationOptions: option,
		})
		log.GetLogger().Warnf("failed to delete custom container pod %s: %s", pod.Name, err.Error())
	} else {
		log.GetLogger().Infof("succeed to delete pod %s for %s", pod.Name, utils.GetFullFuncNameByPod(pod))
	}
	if !utils.IsXGPUStartError(waitErr) {
		addOn.rollback()
	}
}

func (c *customContainerMGR) waitWorkerReady(ctx context.Context, pod *corev1.Pod) error {
	req := fhttp.AcquireRequest()
	defer fhttp.ReleaseRequest(req)
	err := buildRequest(pod, req)
	if err != nil {
		return err
	}
	resp := fhttp.AcquireResponse()
	defer fhttp.ReleaseResponse(resp)

	newCtx, cancel := context.WithTimeout(ctx,
		(livenessProbeDelay+defaultLivenessProbePeriodSeconds*defaultLivenessProbeFailureThreshold)*time.Second)
	defer cancel()
loop:
	for i := 0; i < waitWorkerReadyRetryTimes; i++ {
		select {
		case <-newCtx.Done():
			log.GetLogger().Warnf("wait worker ready timeout %s", pod.Name)
			break loop
		default:
		}
		err := c.workerClient.DoTimeout(req, resp, waitWorkerReadyRequestTimeout)
		if err == nil && resp.StatusCode() == fhttp.StatusOK {
			log.GetLogger().Infof("worker for pod: %s is ready, i: %d", pod.Name, i)
			return nil
		}

		time.Sleep(waitWorkerReadyBackoff)
	}

	log.GetLogger().Errorf("failed to wait worker ready for pod: %s", pod.Name)

	if err := c.kubeClient.CoreV1().Pods(c.nameSpace).Delete(context.TODO(), pod.Name,
		metav1.DeleteOptions{}); err != nil {
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        c.nameSpace,
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          pod.Name,
			OperationOptions: metav1.DeleteOptions{},
		})
		log.GetLogger().Warnf("failed to delete custom container pod %s: %s", pod.Name, err.Error())
	} else {
		log.GetLogger().Infof("succeed to delete pod %s for %s", pod.Name, utils.GetFullFuncNameByPod(pod))
	}

	return errors.New("failed to wait worker ready")
}

func (c *customContainerMGR) notifyWorker(ctx context.Context, pod *corev1.Pod) {
	req := fhttp.AcquireRequest()
	defer fhttp.ReleaseRequest(req)
	curPod := &corev1.Pod{}
	var err error
	curPod, err = c.waitPodInfo(ctx, pod)
	if err != nil {
		return
	}
	err = buildRequest(curPod, req)
	if err != nil {
		return
	}
	req.Header.Add("RUNTIME_CONTAINER_ID", getRuntimeContainerID(curPod))
	resp := fhttp.AcquireResponse()
	defer fhttp.ReleaseResponse(resp)

	for i := 0; i < notifyWorkerRetryTimes; i++ {
		err := c.workerClient.DoTimeout(req, resp, waitWorkerReadyRequestTimeout)
		if err == nil && resp.StatusCode() == fhttp.StatusOK {
			log.GetLogger().Infof("notify worker for pod: %s success, i: %d", pod.Name, i)
			return
		}
		time.Sleep(waitWorkerReadyRequestTimeout)
	}
	log.GetLogger().Warnf("failed to notify worker for pod: %s", pod.Name)
	return
}

func (c *customContainerMGR) waitPodInfo(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	for {
		select {
		case <-ctx.Done():
			log.GetLogger().Warnf("failed to get custom container %s podIP and runtime containerID", pod.Name)
			return nil, errors.New("failed to get custom container podIP and runtime containerID")
		default:
			curPod, err := c.kubeClient.CoreV1().Pods(c.nameSpace).Get(context.TODO(), pod.Name,
				metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if curPod.Status.PodIP != "" && getRuntimeContainerID(curPod) != "" {
				log.GetLogger().Infof("succeed get custom container %s podIP and runtime containerID", curPod.Name)
				return curPod, nil
			}
			time.Sleep(waitWorkerReadyRequestTimeout)
		}
	}
}

func buildRequest(pod *corev1.Pod, req *fhttp.Request) error {
	handler := getWorkerHealthHandler()
	if handler.HTTPGet == nil {
		log.GetLogger().Errorf("only http probe is supported for pod: %s", pod.Name)
		return errors.New("only http probe is supported")
	}

	host := handler.HTTPGet.Host
	if host == "" {
		host = pod.Status.PodIP
	}
	port := handler.HTTPGet.Port.String()
	if port != "" {
		host = host + ":" + port
	}
	u := url.URL{
		Scheme: string(handler.HTTPGet.Scheme),
		Host:   host,
		Path:   handler.HTTPGet.Path,
	}

	req.SetRequestURI(u.String())
	req.Header.SetMethod(fhttp.MethodGet)

	for _, header := range handler.HTTPGet.HTTPHeaders {
		req.Header.Add(header.Name, header.Value)
	}
	return nil
}

func getRuntimeContainerID(pod *corev1.Pod) string {
	var containerID string
	statues := pod.Status.ContainerStatuses
	for _, statue := range statues {
		if statue.Name == utils.RuntimeContainerName {
			containerID = strings.TrimPrefix(statue.ContainerID, "docker://")
			containerID = strings.TrimPrefix(containerID, "containerd://")
			break
		}
	}
	return containerID
}

func (c *customContainerMGR) createPodSpec(name string, spec *faasfunction.FunctionServiceSpec, pool *Pool,
	req common.PodRequest) (*corev1.Pod, error) {
	annotations := make(map[string]string, 0)
	annotations[usedLabelKey] = utils.FalseString
	annotations[constants.OndemandKey] = utils.TrueString
	if commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		annotations[constants.IsCAEKey] = utils.TrueString
		annotations[constants.Properties] = utils.GeneratePropertiesAnnotation(spec.Configuration.Properties)
		annotations[constants.MinInsNum] = strconv.Itoa(int(spec.Configuration.AutoScalerConfig.MinimumScale))
	}

	labels := make(map[string]string, defaultLabelLength)
	labels[utils.IsPoolPodLabelKey] = utils.TrueString
	labels[coldStartPoolLabelKey] = utils.TrueString
	labels[utils.StandardLabelKey] = "custom-container"
	defObjectMeta := metav1.ObjectMeta{
		Name:            name,
		Namespace:       c.nameSpace,
		Annotations:     annotations,
		Labels:          labels,
		OwnerReferences: getThisOwnerReference(),
	}
	var cs *componentSet
	var err error
	var terminationGracePeriodSeconds int64
	if commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		terminationGracePeriodSeconds = caeTerminationGracePeriodSeconds
		cs, err = c.describeCAEComponentSet(spec, pool, name)
	} else {
		terminationGracePeriodSeconds = *getGracePeriodSeconds(c.config.GcConfig)
		cs, err = c.describeComponentSet(spec, pool)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to describe component set: %s", err.Error())
		return nil, err
	}

	defInitContainers := c.describeInitContainers(cs, spec)
	defContainers, err := c.describeContainers(cs, spec, pool, req)
	if err != nil {
		log.GetLogger().Errorf("failed to describe containers: %s", err.Error())
		return nil, err
	}

	var fsGroup int64 = workerUserGroup

	return &corev1.Pod{
		ObjectMeta: defObjectMeta,
		Spec: corev1.PodSpec{
			HostAliases:                   c.config.PoolList.HostAliases,
			Volumes:                       cs.buildVolumes(),
			ImagePullSecrets:              getImagePullSecrets(),
			InitContainers:                defInitContainers,
			Containers:                    defContainers,
			RestartPolicy:                 corev1.RestartPolicyNever,
			AutomountServiceAccountToken:  new(bool),
			EnableServiceLinks:            new(bool),
			TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
			SecurityContext:               &corev1.PodSecurityContext{FSGroup: &fsGroup},
			PriorityClassName:             c.config.CustomContainerConfig.CustomImageClass,
		},
	}, nil
}

// calculateNotSchedulerList return value, []string is the values of can't scheduler list
// bool is use default scheduler or not
func (c *customContainerMGR) calculateNotSchedulerList(funcSvcID string) ([]string, bool, int) {
	nodeMirroredDiskCacheLen := 0

	if funcSvcID == "" {
		log.GetLogger().Errorf("failed to get funcSvdID when calculate not scheduler list: %s", funcSvcID)
		return []string{}, true, nodeMirroredDiskCacheLen
	}

	if !config.GetSchedulerWithDisk() {
		return []string{}, true, nodeMirroredDiskCacheLen
	}

	customImageSize := cache.WorkerCache.GetCustomImgSize(funcSvcID)
	if customImageSize == 0 {
		log.GetLogger().Warn("failed to get custom image size when calculate not scheduler list")
		return []string{}, true, nodeMirroredDiskCacheLen
	}

	notSchedulerList := make([]string, 0)

	allNodeNotGetDiskCache := true

	alreadyExistNodes := cache.WorkerCache.GetAlreadyExistNodes(funcSvcID)

	cache.NodeMirroredDiskCache.Range(func(key, value any) bool {
		nodeIP, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to assert node mirrored disk key")
			return false
		}

		leftSpace, ok := value.(string)
		if !ok {
			log.GetLogger().Errorf("failed to assert node mirrored disk value")
			return false
		}

		nodeMirroredDiskCacheLen++

		// if this node exist pod now, don't need to add to no scheduler list
		if _, exist := alreadyExistNodes[nodeIP]; exist {
			allNodeNotGetDiskCache = false
			return true
		}

		leftSpaceOfBytes := cache.ConvertNodeMirroredDiskCacheToBytes(leftSpace)
		log.GetLogger().Debugf("left space of bytes is %s:%d, nodeIp: %s", leftSpace, leftSpaceOfBytes, nodeIP)

		// it means not get left space now
		if leftSpaceOfBytes <= 0 {
			notSchedulerList = append(notSchedulerList, nodeIP)
		} else if leftSpaceOfBytes-customImageSize <= 0 {
			notSchedulerList = append(notSchedulerList, nodeIP)
		}

		// this means it can get data from bus, we need to use not scheduler list
		if leftSpace != "" {
			// after range if allNodeNotGetDiskCache is true, need use default scheduler mod
			allNodeNotGetDiskCache = false
		}

		return true
	})

	log.GetLogger().Infof("calculate not scheduler list, customImageSize is: %d, funcSvcID: %s, not scheduler"+
		" len is %d, allNodeNotGetDiskCache is %t", customImageSize, funcSvcID, len(notSchedulerList), allNodeNotGetDiskCache)

	if allNodeNotGetDiskCache {
		return []string{}, true, nodeMirroredDiskCacheLen
	}

	return convertNodeIPToNodeName(notSchedulerList), false, nodeMirroredDiskCacheLen
}

func convertNodeIPToNodeName(notSchedulerList []string) []string {
	nodeNameList := make([]string, 0)
	if len(notSchedulerList) == 0 {
		return []string{}
	}

	for _, nodeIP := range notSchedulerList {
		thisNodeName := cache.GetNodeNameByNodeIP(nodeIP)
		if thisNodeName != "" {
			nodeNameList = append(nodeNameList, thisNodeName)
		}
	}
	log.GetLogger().Debugf("notSchedulerList is: %+v, nodeNameList is: %+v", notSchedulerList, nodeNameList)
	return nodeNameList
}

func (c *customContainerMGR) describeComponentSet(spec *faasfunction.FunctionServiceSpec, pool *Pool) (
	*componentSet, error) {

	cs := newComponentSet()
	c.addCommonComponentSet(cs, pool)
	cs.add(&cgroupFreezer{})
	cs.add(&cgroupMemory{})
	cs.add(&fusionSTDLog{
		hostPath:  c.config.VolumeConfig.LogHostPath.UserLog,
		mountPath: c.config.VolumeConfig.UserLogMountPath,
	})
	cs.add(&fusionServiceLog{
		hostPath:        c.config.VolumeConfig.LogHostPath.ServiceLog,
		workerMountPath: c.config.VolumeConfig.LogMountPath,
	})
	cs.add(&runtimeLanguage{language: "custom-container"})
	return cs, nil
}

func (c *customContainerMGR) addCommonComponentSet(cs *componentSet, pool *Pool) {
	cs.add(&shareWorkerSecretData{})
	cs.add(&workerConfig{})
	cs.add(&podConfig{})
	cs.add(&runtimeManagerConfig{})
	cs.add(&cidr{})
	cs.add(&traceEnv{})
	cs.add(&timeZone{})
	cs.add(&podInfo{})
	cs.add(&localCache{enable: false})
	cs.add(&cmNamespace{nameSpace: os.Getenv(constants.PodNamespaceEnvKey)})
	cs.add(&obs{enable: c.config.EnableMinio})
	cs.add(&workerAuthCheck{enable: c.config.WorkerAuthEnable})
	cs.add(&funcBranch{})
	cs.add(&dockerRootDir{})
	cs.add(&dockerSocket{})
	cs.add(&containerdDir{})
	cs.add(&containerdLogsDir{})
	cs.add(&workerLogEnv{})
	cs.add(&workerContentSignatureCheck{enable: c.config.WorkerContentSignatureEnable})
	cs.add(&workerResourceLimitEnv{cpuLimit: pool.LimitCPU, memoryLimit: pool.LimitMemory})
	cs.add(&workerIAMTokenEnv{enableIAMToken: utils.IsIAMTokenFeatureEnable()})

	if config.GetLocalAuth() != nil {
		cs.add(&localAuthSecret{})
	} else {
		cs.add(&authSecret{})
	}
	cs.add(&tempWorkerKeySecret{})
	if config.EnableHTTPS() {
		cs.add(&tlsSecret{})
	}
}

func (c *customContainerMGR) describeCAEComponentSet(spec *faasfunction.FunctionServiceSpec,
	pool *Pool, name string) (
	*componentSet, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config: %s", err.Error())
		return nil, err
	}

	cs := newComponentSet()
	c.addCommonComponentSet(cs, pool)
	// cae function cannot mount /opt/function/code because it has been set when cae image built
	cs.add(&caeFunctionCodeData{deployDir: cfg.DeployDir, metricsDir: metricsDir})
	cs.add(&userFunctionTmpData{})
	cs.add(&runtimeConfig{})
	cs.add(&runtimeManagerConfig{restoreOpts: cfg.PoolMgrConfig.CheckpointPodConfig.CRIUCheckpointOpts,
		storageType:               cfg.PoolMgrConfig.CheckpointPodConfig.SnapshotStorageType,
		businessType:              spec.Configuration.BusinessType,
		disableCAERuntimePreStart: c.config.DisableCAERuntimePreStart,
		snapshotDeployment:        spec.SnapshotDeployment,
		runtimeTermTimeout:        spec.Configuration.RuntimeTermTimeout,
		enableCAEDirectConnection: cfg.PoolMgrConfig.EnableCAEDirectConnection})
	cs.add(&presetDependencyConfig{})
	cs.add(&runtimeMgrLogEnv{})
	cs.add(&runtimeLanguage{language: constants.FusionLanguage})
	cs.add(&socketFile{})

	if len(spec.Configuration.LogPolicy.LogPaths) == 0 {
		cs.add(&fusionSTDLog{
			hostPath:  c.config.VolumeConfig.LogHostPath.UserLog,
			mountPath: c.config.VolumeConfig.UserLogMountPath,
		})
	}
	cs.add(&fusionServiceLog{
		hostPath:            c.config.VolumeConfig.LogHostPath.ServiceLog,
		workerMountPath:     c.config.VolumeConfig.LogMountPath,
		runtimeMgrMountPath: c.config.VolumeConfig.RuntimeMgrLogPath,
		runtimeLogMountPath: c.config.VolumeConfig.RuntimeLogMountPath,
		enableRemoteDebug:   spec.Configuration.EnableRemoteDebug,
	})
	cs.add(&datasystemPath{})

	addUserLogConfigComponentSet(cs, spec, name)

	if utils.EnableNewCaeBaseImage() {
		cs.add(&caeTools{})
	}

	FileMountConfig := spec.Configuration.FileMountConfig
	if isNeedMountFile(FileMountConfig) || len(FileMountConfig.SecretFiles) > 0 {
		configFileInfo := &customContainerConfigFile{
			fileMountConfigs: FileMountConfig.MountFiles,
			secretFiles:      FileMountConfig.SecretFiles,
			podName:          name,
		}
		cs.add(configFileInfo)
	}
	// add remote debug env to worker and runtime container when enable remote debug
	if spec.Configuration.EnableRemoteDebug {
		cs.add(&debugAgentConfig{})
		cs.add(&debugAgentLogEnv{})
		cs.add(&remoteDebug{remoteDebugArgs: spec.Configuration.RemoteDebugArgs})
	}

	cs.add(&runtimeFileHandleNum{})

	return cs, nil
}

func addUserLogConfigComponentSet(cs *componentSet, spec *faasfunction.FunctionServiceSpec, podName string) {
	if len(spec.Configuration.LogPolicy.LogPaths) == 0 {
		return
	}
	logPathsMap := constructRuntimeMountPaths(spec.Configuration.LogPolicy.LogPaths,
		spec.Configuration.FunctionVersionURN, podName)
	cs.add(&userLogConfig{
		hostPath:          spec.Configuration.LogPolicy.HostPath,
		workerMountPath:   logConfigWorkerMountPath,
		runtimeMountPaths: logPathsMap,
	})
	cs.add(&runtimeLogPathConfig{
		enableLogPath: true,
		logPaths:      constructRuntimeLogPathsString(logPathsMap),
	})
}

func constructRuntimeMountPaths(logPaths []string, funcVersionURN, podName string) map[string]struct{} {
	mountPaths := make(map[string]struct{})
	for _, logPath := range logPaths {
		if dir, ok := tools.ExtractFirstLevelDir(logPath, funcVersionURN, podName); ok {
			mountPaths[dir] = struct{}{}
		}
	}
	return mountPaths
}

func constructRuntimeLogPathsString(logPaths map[string]struct{}) string {
	var paths []string
	for logPath := range logPaths {
		paths = append(paths, logPath)
	}
	return strings.Join(paths, ";")
}

func (c *customContainerMGR) describeInitContainers(cs *componentSet, spec *faasfunction.FunctionServiceSpec,
) []corev1.Container {
	name := string(containerWorkerInit)
	return []corev1.Container{
		{
			Name:  name,
			Image: c.config.PoolContainerTags.WorkerInitImageTag,
			Env:   cs.buildEnvVar(containerWorkerInit),
			SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{"NET_ADMIN", "NET_RAW"},
				Drop: []corev1.Capability{"ALL"},
			}},
			VolumeMounts: cs.buildMounts(containerWorkerInit),
		},
	}
}

func (c *customContainerMGR) describeContainers(cs *componentSet, spec *faasfunction.FunctionServiceSpec,
	pool *Pool, req common.PodRequest) ([]corev1.Container, error) {
	runtimeEnv, err := c.generateRuntimeEnv(spec, req)
	if err != nil {
		log.GetLogger().Errorf("failed to get runtime env: %s", err.Error())
		return nil, err
	}
	if commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		whiteList := getCustomJavaArgsWhiteList()
		streamContentTypeWhiteList := utils.GetStreamContentTypeWhiteList()
		runtimeEnv = append(runtimeEnv, cs.buildEnvVar(containerRuntime)...)
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  constants.BusinessType,
			Value: spec.Configuration.BusinessType,
		})
		// append user defined java args
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  constants.CustomJavaArgs,
			Value: spec.Configuration.CustomJavaArgs,
		})
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  constants.CustomJavaArgsWhiteList,
			Value: whiteList,
		})
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  constants.StreamContentTypeWhiteList,
			Value: streamContentTypeWhiteList,
		})
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  constants.FastStartEnable,
			Value: strconv.FormatBool(config.IsFastStartEnable(spec.Configuration.TenantID)),
		})
	}

	runtimeContainerSpec := c.getRuntimeContainerSpec(cs, spec, pool, runtimeEnv)
	workerContainerSpec := c.getWorkerContainerSpec(cs, spec, pool, req)
	debugAgentContainerSpec := c.getAgentContainerSpec(cs)
	if spec.Configuration.EnableRemoteDebug {
		return []corev1.Container{runtimeContainerSpec, debugAgentContainerSpec, workerContainerSpec}, nil
	}

	return []corev1.Container{runtimeContainerSpec, workerContainerSpec}, nil
}

func (c *customContainerMGR) getAgentContainerSpec(cs *componentSet) corev1.Container {
	return corev1.Container{
		Name:         string(containerAgent),
		Env:          cs.buildEnvVar(containerAgent),
		VolumeMounts: cs.buildMounts(containerAgent),
		Image:        c.config.PoolContainerTags.DebugAgentImageTag,
	}
}

func (c *customContainerMGR) getWorkerContainerSpec(cs *componentSet, spec *faasfunction.FunctionServiceSpec,
	pool *Pool, req common.PodRequest) corev1.Container {
	workerResource := getWorkerResource(pool)
	workerLimitMemory := workerResource.Limits.Memory().Value() >> mbShift
	privileged := true
	workerEnv := append(append(cs.buildEnvVar(containerWorker),
		getWorkerEnv(workerLimitMemory)...), getCustomWorkerEnv(req)...)
	workerEnv = generateWorkerEnv(workerEnv, spec, req)

	livenessProbe := getWorkerLivenessProbe(defaultLivenessProbeFailureThreshold,
		defaultLivenessProbePeriodSeconds)
	if commonutils.IsCAEFunc(spec.Configuration.BusinessType) &&
		atomic.LoadUint32(&c.config.DisableCAEReadinessProbe) == 1 {
		// disable livenessProbe for CAE function
		livenessProbe = nil
	}
	workerContainerSpec := corev1.Container{
		Name:           string(containerWorker),
		Image:          c.config.PoolContainerTags.WorkerImageTag,
		Ports:          getWorkerPort(),
		Args:           cs.buildArgs(containerWorker),
		Env:            workerEnv,
		Resources:      workerResource,
		VolumeMounts:   cs.buildMounts(containerWorker),
		ReadinessProbe: livenessProbe,
		LivenessProbe: getCustomWorkerLivenessProbe(defaultCustomLivenessProbeFailureThreshold,
			defaultCustomLivenessProbeFailureSeconds),
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  sanitizeCAP(c.config.WorkerCaps.Add),
				Drop: sanitizeCAP(c.config.WorkerCaps.Drop),
			},
			Privileged: &privileged,
		},
	}
	return workerContainerSpec
}

func (c *customContainerMGR) getRuntimeContainerSpec(cs *componentSet, spec *faasfunction.FunctionServiceSpec,
	pool *Pool, runtimeEnv []corev1.EnvVar) corev1.Container {
	imagePullPolicy := corev1.PullAlways
	if enableSpecialImagePullPolicy {
		imagePullPolicy = corev1.PullIfNotPresent
	}
	runtimeContainerSpec := corev1.Container{
		Name:            string(containerRuntime),
		Image:           strings.TrimSpace(spec.Configuration.CustomContainerConfig.Image),
		ImagePullPolicy: imagePullPolicy,
		Env:             runtimeEnv,
		Resources:       getRuntimeResource(pool),
		VolumeMounts:    cs.buildMounts(containerRuntime),
		Command:         getRuntimeCommand(spec),
		Args:            spec.Configuration.CustomContainerConfig.Args,
		WorkingDir:      spec.Configuration.CustomContainerConfig.WorkingDir,
		SecurityContext: c.getRuntimeSecurityContext(spec),
	}
	return runtimeContainerSpec
}

func getRuntimeCommand(spec *faasfunction.FunctionServiceSpec) []string {
	if commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		return []string{"bash", "-c", safeCommandRuntime}
	}
	return spec.Configuration.CustomContainerConfig.Command
}

func generateWorkerEnv(workerEnv []corev1.EnvVar, spec *faasfunction.FunctionServiceSpec,
	req common.PodRequest) []corev1.EnvVar {
	workerEnv = addBusinessTypeToWorkerEnv(workerEnv, spec)
	if FilterCheckpointCustom(req) {
		workerEnv = append(workerEnv, corev1.EnvVar{
			Name:  "RUNNING_MODE",
			Value: "CHECKPOINT",
		})
	}
	if needUploadSnapshot(req) {
		workerEnv = append(workerEnv, corev1.EnvVar{
			Name:  "need_upload_snapshot",
			Value: "true",
		})
	}
	if req.IsNeedDeleteSnapshot {
		workerEnv = append(workerEnv, corev1.EnvVar{
			Name:  "need_delete_snapshot",
			Value: "true",
		})
	}
	workerEnv = generateSnapshotTagEnv(workerEnv, spec)

	return generateStreamAkSkEnv(workerEnv, spec)
}

func generateSnapshotTagEnv(workerEnv []corev1.EnvVar, spec *faasfunction.FunctionServiceSpec) []corev1.EnvVar {
	if !commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		return workerEnv
	}

	return generateSnapshotTagWithEnv(workerEnv, spec.Configuration.FunctionName)
}

func generateSnapshotTagWithEnv(workerEnv []corev1.EnvVar, param string) []corev1.EnvVar {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get %s config: %s", param, err.Error())
		return workerEnv
	}
	workerEnv = append(workerEnv, corev1.EnvVar{
		Name:  constants.RuntimeManagerTag,
		Value: utils.SplitImageTagByColon(cfg.PoolMgrConfig.PoolContainerTags.RuntimeImageTag),
	}, corev1.EnvVar{
		Name:  constants.RuntimeManagerSize,
		Value: strconv.FormatInt(cfg.PoolMgrConfig.MultiSnapshotConfig.SnapshotCheckInfo.JavaRuntimeSize, decimal),
	})
	return workerEnv
}

func generateStreamAkSkEnv(env []corev1.EnvVar, spec *faasfunction.FunctionServiceSpec) []corev1.EnvVar {
	if !commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		return env
	}
	streamClientAkSkConfigList := utils.GetStreamClientAkSkConfigList()
	if len(streamClientAkSkConfigList) > 0 {
		for _, item := range streamClientAkSkConfigList {
			if item.TenantID == spec.Configuration.TenantID {
				env = append(env, corev1.EnvVar{
					Name:  constants.WorkerSystemAccessKey,
					Value: item.StreamAKey,
				})
				env = append(env, corev1.EnvVar{
					Name:  constants.WorkerSystemSecretKey,
					Value: item.StreamSKey,
				})
				break
			}
		}
	}
	return env
}

func addBusinessTypeToWorkerEnv(env []corev1.EnvVar, spec *faasfunction.FunctionServiceSpec) []corev1.EnvVar {
	return append(env, []corev1.EnvVar{
		{
			Name:  constants.BusinessType,
			Value: spec.Configuration.BusinessType,
		},
	}...)
}

func (c *customContainerMGR) generateRuntimeEnv(spec *faasfunction.FunctionServiceSpec,
	req common.PodRequest) ([]corev1.EnvVar, error) {
	runtimeEnv, err := c.getRuntimeEnv(spec)
	if err != nil {
		log.GetLogger().Errorf("failed to get runtime env: %s", err.Error())
		return nil, err
	}
	if FilterCheckpointCustom(req) {
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  "RUNNING_MODE",
			Value: "CHECKPOINT",
		}, corev1.EnvVar{
			Name:  constants.CheckpointRestoreOpts,
			Value: c.config.CheckpointPodConfig.CRIUCheckpointOpts,
		})
	}
	if needUploadSnapshot(req) {
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  "need_upload_snapshot",
			Value: "true",
		})
	}
	if req.IsNeedDeleteSnapshot {
		runtimeEnv = append(runtimeEnv, corev1.EnvVar{
			Name:  "need_delete_snapshot",
			Value: "true",
		})
	}
	runtimeEnv = generateStreamAkSkEnv(runtimeEnv, spec)
	return runtimeEnv, err
}

func (c *customContainerMGR) getRuntimeEnv(spec *faasfunction.FunctionServiceSpec) ([]corev1.EnvVar, error) {
	findInFunctionServiceEnvVar := func(key string) string {
		for _, ev := range spec.Configuration.Env {
			if ev.Name == key {
				return ev.Value
			}
		}
		log.GetLogger().Warnf("failed to find env %s in function service spec", key)
		return ""
	}

	encryptedEnvironment := findInFunctionServiceEnvVar(utils.EnvironmentKey)
	encryptedUserData := findInFunctionServiceEnvVar(utils.EncryptedUserDataKey)

	var (
		envJSON, userData []byte
		err               error
	)
	if encryptedEnvironment != "" {
		if envJSON, err = localauth.Decrypt(encryptedEnvironment); err != nil {
			return nil, fmt.Errorf("decrypt user's env error: %s", err.Error())
		}
	}
	if encryptedUserData != "" {
		if userData, err = localauth.Decrypt(encryptedUserData); err != nil {
			return nil, fmt.Errorf("decrypt user's data error: %s", err.Error())
		}
	}

	env, err := parseJSONEnv(envJSON)
	if err != nil {
		log.GetLogger().Errorf("failed to parse env json: %s", err.Error())
		return nil, err
	}
	env = appendNvidiaEnv(env)

	envUserData, err := parseJSONEnv(userData)
	if err != nil {
		log.GetLogger().Errorf("failed to parse user data: %s", err.Error())
		return nil, err
	}
	return append(env, envUserData...), nil
}

func appendNvidiaEnv(env []corev1.EnvVar) []corev1.EnvVar {
	isExist := false
	for idx := range env {
		if strings.HasPrefix(env[idx].Name, "LD_LIBRARY_PATH") {
			env[idx].Value = env[idx].Value + ":/usr/local/nvidia/lib64"
			isExist = true
			break
		}
	}
	if !isExist {
		env = append(env, corev1.EnvVar{
			Name:  "LD_LIBRARY_PATH",
			Value: "/usr/local/nvidia/lib64",
		})
	}
	return env
}

func (c *customContainerMGR) getRuntimeSecurityContext(spec *faasfunction.FunctionServiceSpec,
) *corev1.SecurityContext {
	if commonutils.IsCAEFunc(spec.Configuration.BusinessType) {
		return &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  sanitizeCAP(c.config.RuntimeCaps.Add),
				Drop: sanitizeCAP(c.config.RuntimeCaps.Drop),
			},
		}
	}
	runAsNonRoot := true
	uid := int64(spec.Configuration.CustomContainerConfig.UID)
	gid := int64(spec.Configuration.CustomContainerConfig.GID)
	uid, gid = sanitizeFuncMountUser(uid, gid)
	return &corev1.SecurityContext{
		RunAsUser:    &uid,
		RunAsGroup:   &gid,
		RunAsNonRoot: &runAsNonRoot,
		Capabilities: &corev1.Capabilities{
			Add:  []corev1.Capability{"KILL"},
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

func (c *customContainerMGR) updatePod(pod *corev1.Pod) {
	if c.deleteTerminatedPod(pod) {
		return
	}

	used, ok := pod.Annotations[usedLabelKey]
	if !ok {
		log.GetLogger().Warnf("pod %s without used label, which is ignored, deployNode: %s", pod.Name, pod.Spec.NodeName)
		return
	}
	if used != utils.FalseString {
		// used pod
		return
	}

	waiterIf, exist := c.onDemandWaitMap.Load(pod.Name)
	if !exist {
		log.GetLogger().Warnf("unknown pod %s, already notified? deployNode: %s", pod.Name, pod.Spec.NodeName)
		return
	}
	waiter, ok := waiterIf.(*utils.PodWaiter)
	if !ok || waiter == nil {
		log.GetLogger().Errorf("failed to assert for pod: %s, deployNode: %s", pod.Name, pod.Spec.NodeName)
		return
	}

	unschedulable := isPodUnschedulable(pod)
	old := waiter.SetUnschedulable(unschedulable)
	if unschedulable != old {
		log.GetLogger().Infof("pod %s unschedulable status changes from %t to %t", pod.Name, old, unschedulable)
	}
	if unschedulable {
		log.GetLogger().Debugf("pod %s is unschedulable", pod.Name)
		return
	}

	err, ok := isPodReadyOrError(pod)
	if !ok {
		log.GetLogger().Debugf("pod %s isn't ready or back off, deployNode: %s", pod.Name, pod.Spec.NodeName)
		return
	}

	log.GetLogger().Infof("notify custom container pod: %s, deployNode: %s", pod.Name, pod.Spec.NodeName)
	waiter.NotifyStatus(utils.PodStatus{Pod: pod, Err: err})
}

func (c *customContainerMGR) deleteTerminatedPod(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp == nil {
		return false
	}

	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == string(containerWorker) {
			if status.Ready {
				// wait for worker to finish
				return false
			}
			break
		}
	}

	propagationPolicy := metav1.DeletePropagationForeground
	option := metav1.DeleteOptions{PropagationPolicy: &propagationPolicy}
	err := c.kubeClient.CoreV1().Pods(c.nameSpace).Delete(context.TODO(), pod.Name, option)
	if err != nil && !utils.IsPodNotFound(err) {
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        c.nameSpace,
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          pod.Name,
			OperationOptions: option,
		})
		log.GetLogger().Warnf("failed to delete pod: %s err %s", pod.Name, err.Error())
	} else {
		log.GetLogger().Infof("succeed to delete pod %s for %s", pod.Name, utils.GetFullFuncNameByPod(pod))
	}

	return true
}

type customContainerRegcred struct {
	secret     *corev1.Secret
	spec       *faasfunction.FunctionServiceSpec
	swrHandler *swrHandler
	dataFlight *swrDataFlight
	name       string
	nameSpace  string
	kubeClient kubernetes.Interface
	traceID    string
}

func newCustomContainerRegcred(c *customContainerMGR, regcredName string, spec *faasfunction.FunctionServiceSpec,
	swrHandler *swrHandler, traceID string) *customContainerRegcred {
	return &customContainerRegcred{
		spec:       spec,
		swrHandler: swrHandler,
		dataFlight: c.dataFlight,
		name:       regcredName,
		nameSpace:  c.nameSpace,
		kubeClient: c.kubeClient,
		traceID:    traceID,
	}
}

func (c *customContainerRegcred) create(ctx context.Context) error {
	secret, err := c.createSpec(ctx)
	if err != nil {
		return err
	}

	mountFileConfigs := c.spec.Configuration.FileMountConfig
	if isNeedMountFile(mountFileConfigs) {
		deployer, deployerError := commonObs.NewDeployer(mountFileConfigs.S3Config)
		if deployerError != nil || deployer == nil {
			return deployerError
		}
		for _, mountFile := range mountFileConfigs.MountFiles {
			downloadErr := addContainerMountFileToSecret(mountFile, secret, deployer)
			if downloadErr != nil {
				log.GetLogger().Errorf("download file error %s", downloadErr.Error())
				return downloadErr
			}
		}
	}
	// add mount file to secret from meta etcd
	for _, mountFile := range mountFileConfigs.SecretFiles {
		if mountFile.FileMountPath == "" || mountFile.EncryptedFileContent == "" {
			log.GetLogger().Warnf("fileMountPath %s or encryptedFileContent %s is empty with %s",
				mountFile.FileMountPath, mountFile.EncryptedFileContent, c.traceID)
			continue
		}
		fileContent, err := localauth.Decrypt(mountFile.EncryptedFileContent)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt file content for %s with error %s", c.traceID, err.Error())
			return err
		}
		// secret key is a file name, such as abc.txt
		// mount path will use mountFile.FileMountPath, such as /tmp/abc.txt
		// secret value is decrypted file content
		secret.Data[path.Base(mountFile.FileMountPath)] = fileContent
	}

	curSecret, err := c.kubeClient.CoreV1().Secrets(c.nameSpace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	c.secret = curSecret
	return nil
}

func checkMountFile(FileMountConfig types.FileMountConfig) snerror.SNError {
	s3Config := FileMountConfig.S3Config
	mountFiles := FileMountConfig.MountFiles
	if s3Config == nil {
		return snerror.NewSNError(snerror.RuntimeMountErrorCode, "Failed to mount the file to the customized container."+
			"The S3 config is empty.")
	}
	if mountFiles == nil {
		return snerror.NewSNError(snerror.RuntimeMountErrorCode, "Failed to mount the file to the customized container."+
			"The mountFiles config is empty.")
	}
	if s3Config.AccessKey == "" || s3Config.SecretKey == "" || s3Config.Endpoint == "" {
		return snerror.NewSNError(snerror.RuntimeMountErrorCode, "Failed to mount the file to the customized container."+
			"The S3 config format error.")
	}

	if mountFiles == nil || len(mountFiles) < minMountFileSize || len(mountFiles) > maxMountFileSize {
		return snerror.NewSNError(snerror.RuntimeMountErrorCode, "Failed to mount the file to the customized container."+
			"The mountFiles config format error.")
	}
	for index, fileConfig := range mountFiles {
		if fileConfig.BucketID == "" || fileConfig.ObjectID == "" {
			return snerror.NewSNError(snerror.RuntimeMountErrorCode, fmt.Sprintf("Failed to mount the file to the "+
				"customized container. The mountFiles config %d format error.", index))
		}
	}
	return nil
}

func isNeedMountFile(FileMountConfig types.FileMountConfig) bool {
	s3Config := FileMountConfig.S3Config
	mountFiles := FileMountConfig.MountFiles
	if s3Config == nil {
		return false
	}
	if mountFiles == nil {
		return false
	}
	return true
}

func addContainerMountFileToSecret(mountFileConfig *types.MountFile, secret *corev1.Secret,
	deployer *commonObs.S3Deployer) error {
	configFile, downloadError := deployer.DownLoadFromObs(mountFileConfig.BucketID, mountFileConfig.ObjectID)
	if downloadError != nil {
		return downloadError
	}
	secret.Data[path.Base(mountFileConfig.FilePath)] = configFile
	return nil
}

func (c *customContainerRegcred) createSpecWithUserAuth() (*corev1.Secret, error) {
	// can set both user and pwd empty if the image can be public access
	if c.spec.Configuration.CustomContainerConfig.User == "" && c.spec.Configuration.CustomContainerConfig.Pwd == "" {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:            c.name,
				Namespace:       c.nameSpace,
				OwnerReferences: getThisOwnerReference(),
			},
			Data: map[string][]byte{},
		}, nil
	}

	data, err := utils.CreateSecretDataWithUserAuth(c.spec.Configuration.CustomContainerConfig.User,
		c.spec.Configuration.CustomContainerConfig.Pwd, c.spec.Configuration.CustomContainerConfig.Image)

	if err != nil {
		log.GetLogger().Errorf("failed to create secret data for %s", c.spec.Configuration.FunctionName)
		return nil, err
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.name,
			Namespace:       c.nameSpace,
			OwnerReferences: getThisOwnerReference(),
		},
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: data,
		},
		Type: corev1.SecretTypeDockerConfigJson,
	}, nil
}

func (c *customContainerRegcred) createSpec(ctx context.Context) (*corev1.Secret, error) {
	if c.spec.Configuration.CustomContainerConfig.UseUserAuth {
		return c.createSpecWithUserAuth()
	}

	if c.swrHandler == nil || c.spec.Configuration.CustomContainerConfig.IsPublic {
		log.GetLogger().Infof("will get image from public repo, traceID:%s", c.traceID)
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:            c.name,
				Namespace:       c.nameSpace,
				OwnerReferences: getThisOwnerReference(),
			},
			Data: map[string][]byte{},
		}, nil
	}

	secretPath := fmt.Sprintf("v2/manage/utils/secret?projectname=%s", c.spec.Configuration.ProjectName)
	data, err := c.dataFlight.do(c.spec.Configuration.FunctionVersionURN, func() ([]byte, error) {
		data, err := c.swrHandler.doRequest(ctx, http.MethodPost, secretPath, nil)
		if err != nil && !strings.Contains(err.Error(), "The throttling threshold has been reached") {
			c.dataFlight.remove(c.spec.Configuration.FunctionVersionURN)
			return data, err
		}
		// The swr data get for the next minute.
		time.AfterFunc(1*time.Minute, func() { c.dataFlight.remove(c.spec.Configuration.FunctionVersionURN) })
		return data, err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to request swr secret: %s, traceID: %s", err.Error(), c.traceID)
	}

	log.GetLogger().Infof("get swr secret successfully, traceID: %s", c.traceID)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.name,
			Namespace:       c.nameSpace,
			OwnerReferences: getThisOwnerReference(),
		},
		Data: map[string][]byte{corev1.DockerConfigJsonKey: data},
		Type: corev1.SecretTypeDockerConfigJson,
	}, nil
}

func (c *customContainerRegcred) delete(ctx context.Context) error {
	return c.kubeClient.CoreV1().Secrets(c.nameSpace).Delete(ctx, c.name, metav1.DeleteOptions{})
}

func (c *customContainerRegcred) setOwner(ctx context.Context, pod *corev1.Pod) error {
	t := true
	c.secret.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         "v1",
		Kind:               "Pod",
		Name:               pod.Name,
		UID:                pod.UID,
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}}
	_, err := c.kubeClient.CoreV1().Secrets(c.nameSpace).Update(ctx, c.secret, metav1.UpdateOptions{})
	return err
}

type swrHandler struct {
	spec            *faasfunction.FunctionServiceSpec
	errRespHandlers utils.SWRErrRespHandlers
	traceID         string
}

func newSWRHandler(spec *faasfunction.FunctionServiceSpec, handlers utils.SWRErrRespHandlers, traceID string) *swrHandler {
	return &swrHandler{
		spec:            spec,
		errRespHandlers: handlers,
		traceID:         traceID,
	}
}

func (s *swrHandler) doRequest(ctx context.Context, method, path string, extra utils.SWRErrRespHandlers,
) ([]byte, error) {
	agencyID := s.spec.Configuration.Role.XRole
	if s.spec.Configuration.Role.AppXRole != "" {
		agencyID = s.spec.Configuration.Role.AppXRole
	}
	agency := utils.GetUserAgencyFromCacheMap(s.spec.Configuration.TenantID, s.spec.Configuration.DomainID, agencyID)

	rawDecryptedToken, err := localauth.Decrypt(agency.Token)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt agency token: %s, traceID: %s", err.Error(), s.traceID)
		return nil, err
	}
	decryptedToken := string(rawDecryptedToken)
	commonutils.ClearByteMemory(rawDecryptedToken)

	swrURL := fmt.Sprintf("https://%s/%s", config.GetSWRAPI(), path)
	req, err := http.NewRequestWithContext(ctx, method, swrURL, nil)
	if err != nil {
		commonutils.ClearStringMemory(decryptedToken)
		log.GetLogger().Errorf("failed to new request: %s, traceID: %s", err.Error(), s.traceID)
		return nil, err
	}
	req.Header.Set(constants.HeaderAuthToken, decryptedToken)
	client := &http.Client{}
	resp, err := client.Do(req)
	commonutils.ClearStringMemory(decryptedToken)
	if err != nil {
		log.GetLogger().Errorf("failed to request swr with url %s: %s, traceID: %s", swrURL, err.Error(),
			s.traceID)
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.GetLogger().Warnf("failed to close response body: %s, traceID: %s", err.Error(), s.traceID)
		}
	}()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read swr response body with status %d, %s, traceID: %s",
			resp.StatusCode, err.Error(), s.traceID)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, s.handleErrorResponse(resp.StatusCode, data, extra)
	}
	return data, nil
}

func (s *swrHandler) handleErrorResponse(statusCode int, data []byte, extra utils.SWRErrRespHandlers) error {
	if extra != nil {
		if handler, ok := extra[statusCode]; ok {
			return handler(data)
		}
	}

	if s.errRespHandlers != nil {
		if handler, ok := s.errRespHandlers[statusCode]; ok {
			return handler(data)
		}
	}

	// Unhandled error
	return fmt.Errorf("swr request error: %s", string(data))
}

type imageVerifyFlightItem struct {
	mux    sync.Mutex
	result error
}

type imageVerifyFlight struct {
	m   map[string]*imageVerifyFlightItem
	mux sync.Mutex
}

func newImageVerifyFlight() *imageVerifyFlight {
	return &imageVerifyFlight{
		m: make(map[string]*imageVerifyFlightItem),
	}
}

func (f *imageVerifyFlight) do(id string, fn func() error) error {
	f.mux.Lock()
	if item, ok := f.m[id]; ok {
		f.mux.Unlock()
		item.mux.Lock()
		result := item.result
		item.mux.Unlock()
		return result
	}

	item := &imageVerifyFlightItem{}
	f.m[id] = item
	item.mux.Lock()
	f.mux.Unlock()

	err := fn()
	item.result = err
	item.mux.Unlock()
	return err
}

func (f *imageVerifyFlight) remove(id string) {
	f.mux.Lock()
	delete(f.m, id)
	f.mux.Unlock()
}

type swrDataFlightItem struct {
	mux    sync.Mutex
	data   []byte
	result error
}

type swrDataFlight struct {
	m   map[string]*swrDataFlightItem
	mux sync.Mutex
}

func newSwrDataFlight() *swrDataFlight {
	return &swrDataFlight{
		m: make(map[string]*swrDataFlightItem),
	}
}

func (f *swrDataFlight) do(funcURN string, fn func() ([]byte, error)) ([]byte, error) {
	f.mux.Lock()
	if item, ok := f.m[funcURN]; ok {
		f.mux.Unlock()
		item.mux.Lock()
		result := item.result
		data := item.data
		item.mux.Unlock()
		return data, result
	}

	item := &swrDataFlightItem{}
	f.m[funcURN] = item
	item.mux.Lock()
	f.mux.Unlock()

	date, err := fn()
	item.result = err
	item.data = date
	item.mux.Unlock()
	return date, err
}

func (f *swrDataFlight) remove(funcURN string) {
	f.mux.Lock()
	delete(f.m, funcURN)
	f.mux.Unlock()
}

type imageVerifier struct {
	swrHandler *swrHandler
	traceID    string
}

// NOTE: this is not a complete struct. Refer to swr api documentations for more.
// https://support.huaweicloud.com/api-swr/swr_02_0035.html
type swrImageInfo struct {
	Size int `json:"size"`
}

func newImageVerifier(swrHandler *swrHandler, traceID string) *imageVerifier {
	return &imageVerifier{
		swrHandler: swrHandler,
		traceID:    traceID,
	}
}

func (i *imageVerifier) verify(ctx context.Context, image string) error {
	imageName, valid := parseImageName(image)
	if !valid {
		return snerror.NewSNError(snerror.FunctionExceptionCode, fmt.Sprintf("%s is not a valid swr image "+
			"name", image))
	}

	path := fmt.Sprintf("v2/manage/namespaces/%s/repos/%s/tags?tag=%s",
		imageName.nameSpace,
		imageName.repository,
		imageName.tag)
	data, err := i.swrHandler.doRequest(ctx, http.MethodGet, path, map[int]func([]byte) error{
		http.StatusNotFound: func(_ []byte) error {
			return snerror.NewSNError(snerror.FunctionExceptionCode, fmt.Sprintf("image %s does not exist",
				image))
		},
	})
	if err != nil {
		log.GetLogger().Errorf("failed to request swr list repository tags: %s, path %s, traceID: %s",
			err.Error(), path, i.traceID)
		return err
	}

	var infos []swrImageInfo
	if err := json.Unmarshal(data, &infos); err != nil {
		log.GetLogger().Errorf("failed to unmarshal swr response: %s, got: %s, traceID: %s",
			err, string(data), i.traceID)
		return err
	}
	if len(infos) == 0 {
		return snerror.NewSNError(snerror.FunctionExceptionCode, fmt.Sprintf("image %s does not exist", image))
	}

	info := infos[0] // We expect only one item in the list.
	if info.Size > customContainerMaxImageSize {
		return snerror.NewSNError(snerror.FunctionExceptionCode, fmt.Sprintf(
			"image %s is too large. %d > %d bytes", image, info.Size, customContainerMaxImageSize))
	}

	return nil
}

func isPodUnschedulable(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled {
			return condition.Status == corev1.ConditionFalse && condition.Reason == "Unschedulable"
		}
	}
	return false
}

func getCrashPodLogs(pod *corev1.Pod) string {
	var limitBytes int64 = maxTailLogSize
	var tailLines int64 = maxTailLogLine

	podLogOpts := corev1.PodLogOptions{}
	podLogOpts.Container = string(containerRuntime)
	podLogOpts.LimitBytes = &limitBytes
	podLogOpts.TailLines = &tailLines

	clientset := utils.GetKubernetesClient()
	if clientset == nil {
		log.GetLogger().Errorf("failed to get k8s client")
		return ""
	}
	req := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &podLogOpts)

	crashPodLog, err := req.Stream(context.Background())
	if err != nil {
		log.GetLogger().Errorf("failed to open stream from podlogs req: %s", err.Error())
		return ""
	}
	defer crashPodLog.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, crashPodLog)

	if err != nil {
		log.GetLogger().Errorf("failed to copy information from crashPodLog to buf: %s", err.Error())
		return ""
	}
	return buf.String()
}

func isPodNoSpace(pod *corev1.Pod) bool {
	if pod == nil {
		log.GetLogger().Errorf("check pod no space get nil pod")
		return false
	}

	clientSet := utils.GetKubernetesClient()
	if clientSet == nil {
		log.GetLogger().Errorf("failed to get k8s client")
		return false
	}

	fieldSelector := fmt.Sprintf(fieldSelectOfPodName, pod.Name)

	events, err := clientSet.CoreV1().Events(pod.Namespace).List(context.TODO(),
		metav1.ListOptions{FieldSelector: fieldSelector, TypeMeta: metav1.TypeMeta{Kind: podKind}})

	if err != nil {
		log.GetLogger().Errorf("failed to get event when check is pod no space, err: %s", err.Error())
		return false
	}

	for _, item := range events.Items {
		log.GetLogger().Debugf("check custom container event, item msg is: %s", item.Message)
		if strings.Contains(item.Message, noSpaceMessage) {
			return true
		}
	}

	return false
}

func getCrashPodLogsWithNoCAE(pod *corev1.Pod, msg string) string {
	if pod.Annotations == nil || (pod.Annotations != nil &&
		pod.Annotations[constants.IsCAEKey] != utils.TrueString) {
		if logs := getCrashPodLogs(pod); logs != "" {
			msg += "\n" + logs
		}
	}
	return msg
}

func isPodReadyOrError(pod *corev1.Pod) (snerror.SNError, bool) {
	// Return a non-nil customContainerErrorMessage and true indicates the pod has entered a non-recoverable error
	// state.
	if utils.IsPodReady(pod) {
		return nil, true
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != string(containerRuntime) {
			continue
		}

		if status.State.Terminated != nil && status.State.Terminated.Reason == "StartError" {
			return snerror.New(snerror.GettingPodErrorCode, fmt.Sprintf("%s: %s",
				status.State.Terminated.Reason, status.State.Terminated.Message)), true
		}

		if pod.DeletionTimestamp == nil && status.State.Terminated != nil &&
			pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
			msg := fmt.Sprintf("%s: %s", status.State.Terminated.Reason, status.State.Terminated.Message)
			msg = getCrashPodLogsWithNoCAE(pod, msg)
			return snerror.NewSNError(snerror.RuntimeProcessExit, msg), true
		}

		if status.State.Waiting == nil {
			return nil, false
		}

		reason := status.State.Waiting.Reason

		if reason == "ImagePullBackOff" && isPodNoSpace(pod) {
			return snerror.NewSNError(snerror.ClusterOverloadCode,
				fmt.Sprintf("%s:, %s, the node is: %s", reason, common.FailedToPullUpImageOfNoSpace, pod.Spec.NodeName)), true
		}

		if reason == "CreateContainerError" ||
			reason == "ImagePullBackOff" {
			return snerror.NewSNError(snerror.FunctionExceptionCode,
				fmt.Sprintf("%s: %s", reason, status.State.Waiting.Message)), true
		}

		if reason == "CrashLoopBackOff" {
			msg := ""
			if terminated := status.LastTerminationState.Terminated; terminated != nil {
				msg += fmt.Sprintf("%s: %s", terminated.Reason, terminated.Message)
			} else {
				msg += "The application inside the container keeps crashing"
			}
			msg = getCrashPodLogsWithNoCAE(pod, msg)
			return snerror.NewSNError(snerror.RuntimeProcessExit, msg), true
		}

		return nil, false
	}

	log.GetLogger().Warnf("failed to find container runtime for pod %s", pod.Name)
	return nil, false
}

func configGPU(pod *corev1.Pod, req common.PodRequest, gpuMemoryCapacity int64, nodeAffinityKey string) {
	var nodeAffinityValue []string
	nodeAffinityValue = append(nodeAffinityValue, utils.GpuNodeAffinityValue)
	pod.Spec.Affinity.NodeAffinity = utils.DescribeGPUNodeAffinity(nodeAffinityKey, nodeAffinityValue, req.GpuType)
	for _, container := range pod.Spec.Containers {
		if container.Name == utils.RuntimeContainerName {
			utils.ConfigGPUResource(container.Resources, req.GpuMemory, gpuMemoryCapacity)
			break
		}
	}

	pod.Name = strings.ToLower(fmt.Sprintf("custom.image.gpu-%d-%d-%d-%s",
		req.CPU, req.Mem, req.GpuMemory, commonutils.UniqueID()))
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      "node-type",
		Operator: corev1.TolerationOpEqual,
		Value:    "gpu",
		Effect:   corev1.TaintEffectNoSchedule,
	})
	pod.Spec.RestartPolicy = "Never"
	log.GetLogger().Infof("configure custom container gpu pod, funcName: %s, traceID: %s, pod name: %s, "+
		"gpu memory is: %d, gpu type is %s", req.FuncName, req.TraceID, pod.Name, req.GpuMemory, req.GpuType)
}

func configDemandNodes(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
	var required []corev1.NodeSelectorTerm
	if nodeAffinity != nil {
		required = append(required, corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      nodeAffinity.NodeAffinityKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   nodeAffinity.NodeAffinityValue,
			}},
		})
	}
	if len(demandNodes) != 0 {
		nodeSelectorTerm := corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      k8sHostNameKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   demandNodes,
			}},
		}
		if forceDeploy {
			required = append(required, nodeSelectorTerm)
		}
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: required,
		},
	}
}

func configPodSchedulePolicyWithTaggedLabels(pod *corev1.Pod, podScheduleInfo cache.PodScheduleInfo) {
	log.GetLogger().Debugf("podName: %s, podScheduleInfo %v", pod.Name, podScheduleInfo)
	if podScheduleInfo.TaggedScheduleLabel {
		configPodRequiredSchedulePolicy(pod, podScheduleInfo)
		configPodPreferredSchedulePolicyWithLabels(pod, podScheduleInfo)
		return
	}
	if !podScheduleInfo.TaggedScheduleLabel && len(podScheduleInfo.AntiAffinityNodes) > 0 {
		configPodRequiredSchedulePolicy(pod, podScheduleInfo)
	}
}

func configPodRequiredSchedulePolicy(pod *corev1.Pod, podScheduleInfo cache.PodScheduleInfo) {
	var required []corev1.NodeSelectorRequirement
	if podScheduleInfo.TaggedScheduleLabel && len(podScheduleInfo.AffinityNodes) > 0 {
		affinityMatchExpressions := corev1.NodeSelectorRequirement{
			Key:      k8sHostNameKey,
			Operator: corev1.NodeSelectorOpIn,
			Values:   podScheduleInfo.AffinityNodes,
		}
		required = append(required, affinityMatchExpressions)
	}
	if len(podScheduleInfo.AntiAffinityNodes) > 0 {
		antiAffinityNodeSelectorTerm := corev1.NodeSelectorRequirement{
			Key:      k8sHostNameKey,
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   podScheduleInfo.AntiAffinityNodes,
		}
		required = append(required, antiAffinityNodeSelectorTerm)
	}
	configDemandPodWithSchedulePolicy(&pod.Spec, required)
}

func configPodSchedulePolicyWithLabels(pod *corev1.Pod, funSvcID string) {
	podScheduleInfo := cache.WorkerCache.GetScheduleLabelNodes(funSvcID)
	log.GetLogger().Debugf("config pod schedule policy with labels, FunSvcID %s, podScheduleInfo %v",
		funSvcID, podScheduleInfo)
	configPodSchedulePolicyWithTaggedLabels(pod, podScheduleInfo)
}

func configPodPreferredSchedulePolicyWithLabels(pod *corev1.Pod, podScheduleInfo cache.PodScheduleInfo) {
	var preferredSchedulingTerm corev1.PreferredSchedulingTerm
	if len(podScheduleInfo.PreferredAffinityNodes) != 0 {
		preferredSchedulingTerm = corev1.PreferredSchedulingTerm{
			Weight: defaultPreferredWeight,
			Preference: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      k8sHostNameKey,
					Operator: corev1.NodeSelectorOpIn,
					Values:   podScheduleInfo.PreferredAffinityNodes,
				}},
			},
		}
		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &corev1.Affinity{}
		}
		if pod.Spec.Affinity.NodeAffinity == nil {
			pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
		}
		pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, preferredSchedulingTerm)
	}
}

func configDNS(pod *corev1.Pod, funcSvcID string) error {
	domainNames, err := utils.GetDNSCache().GetDNSDomainNames(funcSvcID)
	if err != nil {
		log.GetLogger().Errorf("failed to get domain info with funcSvcID %s", funcSvcID)
		return err
	}
	for hostname, ips := range domainNames {
		for _, ip := range ips {
			pod.Spec.HostAliases = append(pod.Spec.HostAliases, corev1.HostAlias{
				IP:        ip,
				Hostnames: []string{hostname},
			})
		}
	}
	log.GetLogger().Debugf("set hostaliase for func: %s, domains: %+v", funcSvcID, domainNames)
	return nil
}

func parseJSONEnv(data []byte) ([]corev1.EnvVar, error) {
	var (
		res    []corev1.EnvVar
		envMap map[string]interface{}
	)
	if data != nil {
		err := json.Unmarshal(data, &envMap)
		commonutils.ClearByteMemory(data)
		if err != nil {
			return nil, fmt.Errorf("unmashal env error: %s", err.Error())
		}
		for k, v := range envMap {
			res = append(res, corev1.EnvVar{
				Name:  k,
				Value: fmt.Sprintf("%s", v),
			})
		}
	}
	return res, nil
}

func sanitizeFuncMountUser(uid, gid int64) (int64, int64) {
	if uid == 0 || uid == minIDValue {
		uid = constants.DefaultRuntimeUID
	}
	if gid == 0 || gid == minIDValue {
		gid = constants.DefaultRuntimeGID
	}
	return uid, gid
}

func getTimeoutDuration(seconds *int64, defaultDuration time.Duration) time.Duration {
	if seconds != nil {
		defaultDuration = time.Duration(*seconds) * time.Second
	}
	return defaultDuration
}

func parseImageName(image string) (imageName, bool) {
	for _, r := range image {
		if !isAlphaNumeric(r) && r != '-' && r != '.' && r != '_' && r != '/' && r != ':' {
			return imageName{}, false
		}
	}

	tokens := strings.SplitN(image, "/", imageNameTokenMinSize)
	if len(tokens) != imageNameTokenMinSize {
		return imageName{}, false
	}

	nameSpace := tokens[1]
	if nameSpace == "" {
		return imageName{}, false
	}
	repositoryAndTag := strings.Split(tokens[len(tokens)-1], ":")
	repository := repositoryAndTag[0]
	repoTokens := strings.Split(repository, "/")
	for _, token := range repoTokens {
		if token == "" {
			return imageName{}, false
		}
	}

	var tag string
	if len(repositoryAndTag) == imageRepoAndTagSize {
		tag = repositoryAndTag[1]
	} else if len(repositoryAndTag) == 1 {
		tag = "latest"
	}
	if tag == "" || strings.Contains(tag, "/") {
		return imageName{}, false
	}

	return imageName{
		nameSpace:  nameSpace,
		repository: strings.Replace(repository, "/", "$", -1), // swr repository name escape,
		tag:        tag,
	}, true
}

func isAlphaNumeric(r rune) bool {
	return ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9')
}

func getCustomJavaArgsWhiteList() string {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config: %s", err.Error())
		return ""
	}
	if len(cfg.CustomJavaArgsWhiteList) == 0 {
		return ""
	}
	whiteList := make(map[string]bool)
	for _, args := range cfg.CustomJavaArgsWhiteList {
		whiteList[args] = true
	}
	whiteListStr, err := json.Marshal(whiteList)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal whiteList, error: %s", err.Error())
		return ""
	}
	return string(whiteListStr)
}
