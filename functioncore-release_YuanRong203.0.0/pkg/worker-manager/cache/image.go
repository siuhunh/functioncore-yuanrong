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

package cache

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	batchinformer "k8s.io/client-go/informers/batch/v1"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

const (
	defaultK8sTimeout = 10 * time.Second
	defaultSWRTimeout = 10 * time.Second

	defaultImageRefreshInterval = 900 * time.Second

	cacheSecretPrefix = "cache-secret-"
	cacheJobPrefix    = "cache-job-"

	appImageCache = "image-cache"

	defaultImageCacheCurrency = 20

	imageNodeCacheClearTimeDiff = 5 * time.Minute
	imageNodeCacheValidTime     = 10 * time.Minute

	defaultWaitReadyTimeout = 300
)

var (
	defaultImageManager *ImageManager
	deletePolicy        = metav1.DeletePropagationForeground
)

func parseImageRefreshInterval() time.Duration {
	env := os.Getenv("IMAGE_REFRESH_INTERVAL")
	if env == "" {
		return defaultImageRefreshInterval
	}
	if val, err := strconv.Atoi(env); err == nil && val >= 0 {
		return time.Duration(val) * time.Second
	}
	return defaultImageRefreshInterval
}

func parseImageCacheCurrency() int {
	env := os.Getenv("IMAGE_CACHE_CURRENCY")
	if env == "" {
		return defaultImageCacheCurrency
	}
	if val, err := strconv.Atoi(env); err == nil && val > 0 {
		return val
	}
	return defaultImageCacheCurrency
}

var disableSWRAddon = struct {
	disabled bool
	once     sync.Once
}{}

func isSWRAddonDisabled() bool {
	disableSWRAddon.once.Do(func() {
		cfg, err := config.GetConfig()
		if err != nil {
			return
		}
		disableSWRAddon.disabled = cfg.PoolMgrConfig.CustomContainerConfig.DisableSWRAddOn
	})
	return disableSWRAddon.disabled
}

// ImageCache cache status for image
type ImageCache struct {
	initTag     string
	namespace   string
	funcSvcID   string
	crSignature string
	// key: nodeName  value:jobName
	jobs       map[string]string
	funcMeta   *faasfunction.FunctionService
	secretName string
	sync.RWMutex
}

func (ic *ImageCache) generateSecretData() ([]byte, error) {
	if ic.funcMeta.Spec.Configuration.CustomContainerConfig.UseUserAuth {
		user := ic.funcMeta.Spec.Configuration.CustomContainerConfig.User
		pwd := ic.funcMeta.Spec.Configuration.CustomContainerConfig.Pwd
		if user == "" && pwd == "" {
			return []byte{}, nil
		}
		return utils.CreateSecretDataWithUserAuth(user, pwd,
			ic.funcMeta.Spec.Configuration.CustomContainerConfig.Image)
	}

	if isSWRAddonDisabled() {
		return []byte{}, nil
	}
	secretPath := fmt.Sprintf("v2/manage/utils/secret?projectname=%s",
		ic.funcMeta.Spec.Configuration.ProjectName)
	agencyID := ic.funcMeta.Spec.Configuration.Role.XRole
	if ic.funcMeta.Spec.Configuration.Role.AppXRole != "" {
		agencyID = ic.funcMeta.Spec.Configuration.Role.AppXRole
	}
	agency := utils.GetUserAgencyFromCacheMap(ic.funcMeta.Spec.Configuration.TenantID,
		ic.funcMeta.Spec.Configuration.DomainID, agencyID)
	ctx, cancel := context.WithTimeout(context.Background(), defaultSWRTimeout)
	defer cancel()
	return utils.RequestSWR(ctx, secretPath, http.MethodPost, agency, nil)
}

func (ic *ImageCache) createSecretSpec() (*corev1.Secret, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ic.namespace,
			Labels: map[string]string{
				"app": appImageCache,
			},
			Annotations: map[string]string{
				"funcSvcID": ic.funcSvcID,
			},
		},
		// default secret type for empty secret
		Type: corev1.SecretTypeOpaque,
	}
	if os.Getenv(constants.PodNamespaceEnvKey) != constants.TrustworthinessNameSpace {
		secret.OwnerReferences = []metav1.OwnerReference{
			utils.DescribeOwnerReferences(),
		}
	}
	if ic.secretName != "" {
		secret.Name = ic.secretName
	} else {
		secret.GenerateName = cacheSecretPrefix
	}

	data, err := ic.generateSecretData()
	if err != nil {
		log.GetLogger().Warnf("failed to create secret for func %s, err %s", ic.funcSvcID, err.Error())
		return nil, err
	}

	if len(data) == 0 {
		secret.Data = map[string][]byte{}
	} else {
		secret.Data = map[string][]byte{corev1.DockerConfigJsonKey: data}
		secret.Type = corev1.SecretTypeDockerConfigJson
	}
	return secret, nil
}

func (ic *ImageCache) describeJobInitContainers() []corev1.Container {
	return []corev1.Container{
		{
			Name:    "init",
			Image:   ic.initTag,
			Command: []string{"cp", "/usr/bin/echo", "/tmp/bin"},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "bin",
					MountPath: "/tmp/bin",
				},
			},
			ImagePullPolicy: corev1.PullIfNotPresent,
		},
	}
}

func (ic *ImageCache) describeJobContainers() []corev1.Container {
	return []corev1.Container{
		{
			Name:            "runtime",
			Image:           ic.funcMeta.Spec.Configuration.CustomContainerConfig.Image,
			Command:         []string{"cp", "/tmp/bin/echo", "/tmp/bin/echo1"},
			ImagePullPolicy: corev1.PullAlways,
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "bin",
					MountPath: "/tmp/bin",
				},
			},
		},
	}
}

func (ic *ImageCache) describeJobVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "bin",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
}

func (ic *ImageCache) describeJobImagePullSecrets() []corev1.LocalObjectReference {
	return []corev1.LocalObjectReference{
		{
			Name: "default-secret",
		},
		{
			Name: "myregcred",
		},
		{
			Name: ic.secretName,
		},
	}
}

func (ic *ImageCache) describeJobToleration() []corev1.Toleration {
	return []corev1.Toleration{
		{
			Key:      "node-type",
			Operator: corev1.TolerationOpEqual,
			Value:    "gpu",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}
}

func (ic *ImageCache) createJobSpec(node string) *batchv1.Job {
	backoffLimit := int32(0)
	activeDeadlineSeconds := getTimeoutDuration()
	ttl := int32(60)
	labels := map[string]string{
		"app": appImageCache,
	}
	annotation := map[string]string{
		"funcSvcID":   ic.funcSvcID,
		"node":        node,
		"crSignature": ic.crSignature,
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cacheJobPrefix,
			Namespace:    ic.namespace,
			Labels:       labels,
			Annotations:  annotation,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   ic.namespace,
					Labels:      labels,
					Annotations: annotation,
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": node,
					},
					InitContainers:   ic.describeJobInitContainers(),
					Containers:       ic.describeJobContainers(),
					Volumes:          ic.describeJobVolumes(),
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: ic.describeJobImagePullSecrets(),
					Tolerations:      ic.describeJobToleration(),
				},
			},
		},
	}
	if os.Getenv(constants.PodNamespaceEnvKey) != constants.TrustworthinessNameSpace {
		job.OwnerReferences = []metav1.OwnerReference{
			utils.DescribeOwnerReferences(),
		}
	}
	return job
}

// ImageManager manages ImageCache
type ImageManager struct {
	namespace      string
	initTag        string
	jobInformer    batchinformer.JobInformer
	secretInformer coreinformer.SecretInformer
	kubeClient     kubernetes.Interface

	enable uint32

	// key:nodeName@imageName => val: time to cache image on node
	imageNodeCached map[string]time.Time

	pool *ants.Pool

	imageCaches sync.Map

	sync.Mutex
}

// NewImageManager create ImageManager
func NewImageManager(initTag string, kubeClient kubernetes.Interface, jobInformer batchinformer.JobInformer,
	secretInformer coreinformer.SecretInformer) (*ImageManager, error) {
	pool, err := ants.NewPool(parseImageCacheCurrency(), ants.WithPreAlloc(true))
	if err != nil {
		log.GetLogger().Errorf("failed to init pool for image manager %s", err.Error())
		return nil, err
	}

	im := &ImageManager{
		initTag:         initTag,
		namespace:       utils.FunctionNameSpace(),
		jobInformer:     jobInformer,
		secretInformer:  secretInformer,
		kubeClient:      kubeClient,
		pool:            pool,
		imageNodeCached: make(map[string]time.Time),
	}

	secretInformer.Informer().AddEventHandler(im.generateSecretHandler())

	jobInformer.Informer().AddEventHandler(im.generateJobHandler())

	return im, nil
}

// RunImageManager start and run default ImageManager
func RunImageManager(stopCh <-chan struct{}) error {
	workerInitTag := os.Getenv("WORKER_INIT_TAG")
	im, err := NewImageManager(workerInitTag, utils.GetKubernetesClient(), utils.GetJobInformer(),
		utils.GetSecretInformer())
	if err != nil {
		return err
	}
	defaultImageManager = im
	defaultImageManager.initImageNodeCache()
	go defaultImageManager.Start(stopCh)
	return nil
}

// Enable enables ImageManager
func (im *ImageManager) Enable() {
	atomic.StoreUint32(&im.enable, 1)
}

// Disable disables ImageManager
func (im *ImageManager) Disable() {
	atomic.StoreUint32(&im.enable, 0)
	im.imageCaches.Range(func(key, value any) bool {
		im.imageCaches.Delete(key)
		return true
	})
	im.Lock()
	defer im.Unlock()
	for key := range im.imageNodeCached {
		delete(im.imageNodeCached, key)
	}
}

// Start starts to refresh image cache background
func (im *ImageManager) Start(stopCh <-chan struct{}) {
	interval := parseImageRefreshInterval()
	if interval <= 0 {
		log.GetLogger().Infof("refresh image is disabled")
		return
	}
	clearTime := interval + imageNodeCacheClearTimeDiff
	log.GetLogger().Infof("start to run image refresh with interval %v", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadUint32(&im.enable) == 0 {
				continue
			}
			im.refresh()
			im.clearImageNodeCache(clearTime)
		case <-stopCh:
			return
		}
	}
}

func (im *ImageManager) refresh() {
	log.GetLogger().Infof("start to refresh image cache")
	im.imageCaches.Range(func(key, value any) bool {
		ic, ok := value.(*ImageCache)
		if !ok {
			return true
		}
		ic.Lock()
		defer ic.Unlock()
		nodes := make([]string, 0, len(ic.jobs))
		for node := range ic.jobs {
			nodes = append(nodes, node)
		}

		im.cacheImages(ic, nodes)
		return true
	})
}

func (im *ImageManager) initImageNodeCache() {
	req, err := labels.NewRequirement("app", selection.Equals, []string{appImageCache})
	if err != nil {
		log.GetLogger().Warnf("failed to new job requirement, err: %s", err.Error())
		return
	}
	s := labels.NewSelector()
	jobs, err := im.jobInformer.Lister().List(s.Add(*req))
	if err != nil {
		log.GetLogger().Warnf("failed to list job from informer, err: %s", err.Error())
		return
	}
	for _, job := range jobs {
		if job.CreationTimestamp.IsZero() || len(job.Spec.Template.Spec.Containers) == 0 {
			continue
		}
		key := job.Annotations["node"] + "@" + job.Spec.Template.Spec.Containers[0].Image
		im.imageNodeCached[key] = job.CreationTimestamp.Time
	}
}

func (im *ImageManager) clearImageNodeCache(diff time.Duration) {
	im.Lock()
	defer im.Unlock()
	for key, val := range im.imageNodeCached {
		if time.Since(val) > diff {
			delete(im.imageNodeCached, key)
		}
	}
}

func (im *ImageManager) needUpdateImageOnNode(image, node string) bool {
	im.Lock()
	defer im.Unlock()
	key := node + "@" + image
	oldTime, ok := im.imageNodeCached[key]
	if !ok {
		im.imageNodeCached[key] = time.Now()
		return true
	}
	if time.Since(oldTime) < imageNodeCacheValidTime {
		return false
	}
	im.imageNodeCached[key] = time.Now()
	return true
}

func (im *ImageManager) cacheImages(ic *ImageCache, targetNodes []string) {
	if len(targetNodes) == 0 {
		return
	}
	secretUpdated := false
	for _, node := range targetNodes {
		if !im.needUpdateImageOnNode(ic.funcMeta.Spec.Configuration.CustomContainerConfig.Image, node) {
			continue
		}
		if !secretUpdated {
			secretUpdated = true
			secret, err := ic.createSecretSpec()
			if err != nil {
				log.GetLogger().Warnf("failed to create secret spec for func %s %s", ic.funcSvcID, err.Error())
				return
			}
			secret, err = im.createOrUpdateSecret(secret)
			if err != nil {
				_ = im.deleteSecret(ic.secretName)
				ic.secretName = ""
				log.GetLogger().Warnf("failed to create secret for func %s %s", ic.funcSvcID, err.Error())
				return
			}
			ic.secretName = secret.Name
		}
		spec := ic.createJobSpec(node)
		job, err := im.createJob(spec)
		if err != nil {
			log.GetLogger().Warnf("failed to create job for node %s of func %s, err: %s",
				node, ic.funcSvcID, err.Error())
			ic.jobs[node] = ""
			continue
		}
		ic.jobs[node] = job.Name
	}
}

// UpdateImageCache updates image cache
func (im *ImageManager) UpdateImageCache(funcSvcID, crSignature string, funcMeta *faasfunction.FunctionService,
	targetNodes []string) {
	im.pool.Submit(func() {
		im.updateImageCache(funcSvcID, crSignature, funcMeta, targetNodes)
	})
}

func (im *ImageManager) updateImageCache(funcSvcID, crSignature string, funcMeta *faasfunction.FunctionService,
	targetNodes []string) {
	log.GetLogger().Infof("start to cache image for %s in %v", funcSvcID, targetNodes)
	if funcMeta == nil {
		log.GetLogger().Warnf("funcMeta is nil when update image cache, %s", funcSvcID)
		return
	}
	ic, ok := im.getImageCache(funcSvcID)
	if !ok {
		ic = im.loadOrStoreImageCache(funcSvcID, &ImageCache{
			initTag:   im.initTag,
			namespace: im.namespace,
			funcMeta:  funcMeta,
			funcSvcID: funcSvcID,
			jobs:      map[string]string{},
		})
	}
	if ic == nil {
		log.GetLogger().Warnf("image cache is nil when update image cache, %s", funcSvcID)
		return
	}
	ic.Lock()
	defer ic.Unlock()
	if ic.crSignature == crSignature {
		return
	}
	ic.crSignature = crSignature
	ic.funcMeta = funcMeta
	for _, jobName := range ic.jobs {
		if jobName == "" {
			continue
		}
		_ = im.deleteJob(jobName)
	}
	ic.jobs = make(map[string]string, len(targetNodes))
	for _, node := range targetNodes {
		ic.jobs[node] = ""
	}
	im.cacheImages(ic, targetNodes)
}

// DeleteImageCache delete image cache
func (im *ImageManager) DeleteImageCache(funcSvcID string) {
	im.pool.Submit(func() {
		im.deleteImageCache(funcSvcID)
	})
}

func (im *ImageManager) deleteImageCache(funcSvcID string) {
	log.GetLogger().Infof("start to delete image cache for %s", funcSvcID)
	icIf, loaded := im.imageCaches.LoadAndDelete(funcSvcID)
	if !loaded {
		return
	}
	ic, ok := icIf.(*ImageCache)
	if !ok {
		return
	}
	ic.Lock()
	defer ic.Unlock()
	if err := im.deleteSecret(ic.secretName); err != nil {
		log.GetLogger().Warnf("failed to delete secret %s, err: %s", ic.secretName, err)
	}
	for node, jobName := range ic.jobs {
		err := im.deleteJob(jobName)
		if err != nil {
			log.GetLogger().Warnf("failed to delete job %s for node %s, err: %s", jobName, node, err.Error())
			continue
		}
	}
	log.GetLogger().Infof("image cache deleted for func %s", funcSvcID)
}

func (im *ImageManager) createOrUpdateSecret(secret *corev1.Secret) (*corev1.Secret, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultK8sTimeout)
	defer cancel()
	if secret.Name == "" {
		return im.kubeClient.CoreV1().Secrets(im.namespace).Create(ctx, secret, metav1.CreateOptions{})
	}
	oldSecret, err := im.secretInformer.Lister().Secrets(im.namespace).Get(secret.Name)
	if err != nil {
		log.GetLogger().Warnf("failed to get secret by informer %s %s", secret.Name, err.Error())
		return nil, err
	}
	if oldSecret.Type != secret.Type {
		if err = im.deleteSecret(secret.Name); err != nil {
			log.GetLogger().Warnf("failed to delete secret %s, err: %s", secret.Name, err.Error())
			return nil, err
		}
		secret.Name = ""
		secret.GenerateName = "cache-secret-"
		return im.kubeClient.CoreV1().Secrets(im.namespace).Create(ctx, secret, metav1.CreateOptions{})
	}
	return im.kubeClient.CoreV1().Secrets(im.namespace).Update(ctx, secret, metav1.UpdateOptions{})
}

func (im *ImageManager) getImageCache(funcSvcID string) (*ImageCache, bool) {
	icIf, ok := im.imageCaches.Load(funcSvcID)
	if !ok {
		return nil, false
	}
	ic, ok := icIf.(*ImageCache)
	return ic, ok
}

func (im *ImageManager) loadOrStoreImageCache(funcSvcID string, newIC *ImageCache) *ImageCache {
	icIf, loaded := im.imageCaches.LoadOrStore(funcSvcID, newIC)
	if !loaded {
		return newIC
	}
	ic, ok := icIf.(*ImageCache)
	if ok {
		return ic
	}
	return nil
}

func (im *ImageManager) deleteSecret(secretName string) error {
	if secretName == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultK8sTimeout)
	defer cancel()
	err := im.kubeClient.CoreV1().Secrets(im.namespace).Delete(ctx, secretName,
		metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
	if err != nil {
		log.GetLogger().Warnf("failed to delete secret %s %s", secretName, err.Error())
		return err
	}
	return nil
}

func (im *ImageManager) createJob(job *batchv1.Job) (*batchv1.Job, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultK8sTimeout)
	defer cancel()
	newJob, err := im.kubeClient.BatchV1().Jobs(im.namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return newJob, nil
}

func (im *ImageManager) deleteJob(jobName string) error {
	if jobName == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultK8sTimeout)
	defer cancel()
	err := im.kubeClient.BatchV1().Jobs(im.namespace).Delete(ctx, jobName,
		metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
	if err != nil {
		log.GetLogger().Warnf("failed to delete job %s %s", jobName, err.Error())
		return err
	}
	return nil
}

func (im *ImageManager) generateJobHandler() cache.ResourceEventHandler {
	return cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			job, ok := obj.(*batchv1.Job)
			if !ok {
				return false
			}
			return job.Labels["app"] == appImageCache
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				job, ok := obj.(*batchv1.Job)
				if !ok {
					return
				}
				im.OnUpdateJob(job)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				job, ok := newObj.(*batchv1.Job)
				if !ok {
					return
				}
				im.OnUpdateJob(job)
			},
			DeleteFunc: func(obj interface{}) {
				job, ok := obj.(*batchv1.Job)
				if !ok {
					return
				}
				im.OnDeleteJob(job)
			},
		},
	}
}

func (im *ImageManager) generateSecretHandler() cache.ResourceEventHandler {
	return cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			secret, ok := obj.(*corev1.Secret)
			if !ok {
				return false
			}
			return secret.Labels["app"] == appImageCache
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				secret, ok := obj.(*corev1.Secret)
				if !ok {
					return
				}
				im.OnUpdateSecret(secret)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				secret, ok := newObj.(*corev1.Secret)
				if !ok {
					return
				}
				im.OnUpdateSecret(secret)
			},
			DeleteFunc: func(obj interface{}) {
				secret, ok := obj.(*corev1.Secret)
				if !ok {
					return
				}
				im.OnDeleteSecret(secret)
			},
		},
	}
}

func (im *ImageManager) isJobCompleteOrFail(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			log.GetLogger().Infof("job completed %s for func %s on node %s",
				job.Name, job.Annotations["funcSvcID"], job.Annotations["node"])
			return true
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			log.GetLogger().Warnf("job failed %s for func %s on node %s",
				job.Name, job.Annotations["funcSvcID"], job.Annotations["node"])
			return true
		}
	}
	return false
}

// OnUpdateSecret handle secret update event
func (im *ImageManager) OnUpdateSecret(secret *corev1.Secret) {
	if secret.DeletionTimestamp != nil {
		return
	}
	funcSvcID := secret.Annotations["funcSvcID"]
	ic, ok := im.getImageCache(funcSvcID)
	if !ok {
		_ = im.deleteSecret(secret.Name)
		return
	}
	ic.Lock()
	if ic.secretName != secret.Name && ic.secretName != "" {
		old := ic.secretName
		ic.secretName = secret.Name
		ic.Unlock()
		_ = im.deleteSecret(old)
		return
	}
	ic.secretName = secret.Name
	ic.Unlock()
}

// OnDeleteSecret handle secret delete event
func (im *ImageManager) OnDeleteSecret(secret *corev1.Secret) {
	funcSvcID := secret.Annotations["funcSvcID"]
	ic, ok := im.getImageCache(funcSvcID)
	if !ok {
		return
	}
	ic.Lock()
	if secret.Name == ic.secretName {
		ic.secretName = ""
	}
	ic.Unlock()
}

// OnUpdateJob handle job update event
func (im *ImageManager) OnUpdateJob(job *batchv1.Job) {
	if job.DeletionTimestamp != nil {
		return
	}
	if im.isJobCompleteOrFail(job) {
		_ = im.deleteJob(job.Name)
		return
	}
	funcSvcID := job.Annotations["funcSvcID"]
	ic, ok := im.getImageCache(funcSvcID)
	if !ok {
		_ = im.deleteJob(job.Name)
		return
	}
	ic.Lock()
	if ic.crSignature != job.Annotations["crSignature"] {
		ic.Unlock()
		_ = im.deleteJob(job.Name)
		return
	}
	node := job.Annotations["node"]
	ic.jobs[node] = job.Name
	ic.Unlock()
}

// OnDeleteJob handle job delete event
func (im *ImageManager) OnDeleteJob(job *batchv1.Job) {
	funcSvcID := job.Annotations["funcSvcID"]
	ic, ok := im.getImageCache(funcSvcID)
	if !ok {
		return
	}
	ic.Lock()
	if ic.crSignature != job.Annotations["crSignature"] {
		ic.Unlock()
		return
	}
	node := job.Annotations["node"]
	if ic.jobs[node] == job.Name {
		ic.jobs[node] = ""
	}
	ic.Unlock()
}

// UpdateImageCache update ImageCache using defaultImageManager
func UpdateImageCache(funcSvcID, crSignature string, funcMeta *faasfunction.FunctionService, targetNodeIPs []string) {
	if defaultImageManager == nil {
		return
	}
	defaultImageManager.UpdateImageCache(funcSvcID, crSignature, funcMeta, convertNodeIPToNodeName(targetNodeIPs))
}

// DeleteImageCache delete ImageCache using defaultImageManager
func DeleteImageCache(funcSvcID string) {
	if defaultImageManager == nil {
		return
	}
	defaultImageManager.DeleteImageCache(funcSvcID)
}

// UpdateImageCacheEnableStatus update enable status of defaultImageManager
func UpdateImageCacheEnableStatus() {
	if defaultImageManager == nil {
		return
	}
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get config: %s", err.Error())
		return
	}
	if cfg.ImageAffinityCacheConfig.DisableImageAffinityCache != 0 {
		log.GetLogger().Infof("image cache disabled")
		defaultImageManager.Disable()
		return
	}
	log.GetLogger().Infof("image cache enabled")
	defaultImageManager.Enable()
}

func convertNodeIPToNodeName(notSchedulerList []string) []string {
	nodeNameList := make([]string, 0)
	if len(notSchedulerList) == 0 {
		return []string{}
	}

	for _, nodeIP := range notSchedulerList {
		thisNodeName := GetNodeNameByNodeIP(nodeIP)
		if thisNodeName != "" {
			nodeNameList = append(nodeNameList, thisNodeName)
		}
	}
	return nodeNameList
}

func getTimeoutDuration() int64 {
	imageConfig, err := config.GetConfig()
	if err == nil && imageConfig.PoolMgrConfig.CustomContainerConfig.WaitReadyTimeout != nil {
		return *imageConfig.PoolMgrConfig.CustomContainerConfig.WaitReadyTimeout
	}
	return defaultWaitReadyTimeout
}
