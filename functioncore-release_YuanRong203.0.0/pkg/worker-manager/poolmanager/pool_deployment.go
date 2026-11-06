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

package poolmanager

import (
	"os"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

const (
	work128               = 128
	cpuRestoreThreshold   = 1024
	workerLimitCPU        = 1000 // workerLimit improve to 1000 before decompression
	workerRequestCPU      = 100
	workerRequestMemory   = 100
	lowerWorkerRequestCPU = 50
	podAntiAffinityWeight = 100
	terminationLogPath    = "/var/tmp/termination-log"
	safeCommandRuntime    = "chmod 750 /home/sn && echo 'source /etc/profile' " +
		">> /home/sn/.bashrc && chmod 550 /home/sn && /home/sn/bin/entrypoint"
	safeCommandWorker = "echo 'source /etc/profile' >> /home/sn/.bashrc && /home/sn/bin/entrypoint"
	poolSchedulerName = "functioncore-scheduling"
	metricsDir        = "/home/sn/metrics"
)

var (
	ssdNodeKey   = os.Getenv("SSD_NODE_KEY")
	ssdNodeValue = os.Getenv("SSD_NODE_VALUE")
)

type poolDeploymentBuilder struct {
	config config.PoolMgrConfig
}

func newPoolDeploymentBuilder(config config.PoolMgrConfig) *poolDeploymentBuilder {
	return &poolDeploymentBuilder{
		config: config,
	}
}

func (b *poolDeploymentBuilder) build(pool *Pool, ownerUID types.UID) *appsv1.Deployment {
	defObjectMeta := b.describeObjectMeta(pool.Name, pool.Label, ownerUID)
	var podTemplateSpec corev1.PodTemplateSpec
	var err error
	if pool.isCaePool {
		podTemplateSpec, err = b.getCaePodTemplateSpec(pool)
	} else {
		podTemplateSpec, err = b.getPodTemplateSpec(pool)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to get pod template spec with error: %s", err.Error())
		return nil
	}
	return &appsv1.Deployment{
		ObjectMeta: defObjectMeta,
		Spec: appsv1.DeploymentSpec{
			Replicas: &pool.PoolSize,
			Selector: &metav1.LabelSelector{
				MatchLabels: pool.Label,
			},
			Template: podTemplateSpec,
			Strategy: appsv1.DeploymentStrategy{
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge: &intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "100%",
					},
					MaxUnavailable: &intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "100%",
					},
				},
			},
		},
	}
}

func (b *poolDeploymentBuilder) getCaePodTemplateSpec(pool *Pool) (corev1.PodTemplateSpec, error) {
	cs, err := b.describeCaeComponentSet(pool)
	if err != nil {
		log.GetLogger().Errorf("failed to describe cae component set: %s", err.Error())
		return corev1.PodTemplateSpec{}, err
	}

	defContainers := b.describeCaeContainers(pool, cs)
	return b.constructPodTemplateSpecCommon(pool, cs, defContainers), nil
}

func (b *poolDeploymentBuilder) getPodTemplateSpec(pool *Pool) (corev1.PodTemplateSpec, error) {
	cs, err := b.describeComponentSet(pool)
	if err != nil {
		log.GetLogger().Errorf("failed to describe component set: %s", err.Error())
		return corev1.PodTemplateSpec{}, err
	}

	var defContainers []corev1.Container
	if pool.NonIsolated {
		defContainers = b.describeNonIsolatedContainers(pool, cs)
	} else {
		defContainers = b.describeContainers(pool, cs)
	}
	return b.constructPodTemplateSpecCommon(pool, cs, defContainers), nil
}

func (b *poolDeploymentBuilder) constructPodTemplateSpecCommon(pool *Pool, cs *componentSet,
	defContainers []corev1.Container) corev1.PodTemplateSpec {
	fsGroup := new(int64)
	*fsGroup = workerUserGroup

	defInitContainers := b.describeInitContainers(cs)

	podTemplateSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      pool.Label,
			Annotations: pool.Annotations,
		},
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				PodAntiAffinity: describePodAntiAffinity(pool.Standard),
				NodeAffinity: getNodeAffinity(pool, b.config.PoolList.NodeAffinity.NodeAffinityKey,
					b.config.PoolList.NodeAffinity.NodeAffinityValue),
			},
			HostAliases:                   b.config.PoolList.HostAliases,
			Volumes:                       cs.buildVolumes(),
			ImagePullSecrets:              getImagePullSecrets(),
			InitContainers:                defInitContainers,
			Containers:                    defContainers,
			EnableServiceLinks:            new(bool),
			AutomountServiceAccountToken:  new(bool),
			TerminationGracePeriodSeconds: getGracePeriodSeconds(b.config.GcConfig),
			SecurityContext:               &corev1.PodSecurityContext{FSGroup: fsGroup},
		},
	}

	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get cfg when constructPodTemplateSpecCommon err: %s", err.Error())
	} else if cfg.SchedulingEnable {
		podTemplateSpec.Spec.SchedulerName = poolSchedulerName
	}
	return podTemplateSpec
}

func getNodeAffinity(pool *Pool, nodeAffinityKey string, nodeAffinityValues []string) *corev1.NodeAffinity {
	nodeAffinity := describeNodeAffinity(nodeAffinityKey, nodeAffinityValues, pool.Label, true)
	// runtime name is not fusion, it will prepare for SSD nodes
	if !strings.Contains(string(pool.Standard), "fusion") &&
		!strings.Contains(string(pool.Standard), "cae") {
		nodeAffinity = describeNodeAffinity(ssdNodeKey, []string{ssdNodeValue}, pool.Label, false)
	}
	return nodeAffinity
}

func (b *poolDeploymentBuilder) describeObjectMeta(name string, labels map[string]string, ownerUID types.UID,
) metav1.ObjectMeta {
	t := true
	meta := metav1.ObjectMeta{
		Namespace: utils.FunctionNameSpace(),
		Name:      name,
		Labels:    labels,
	}
	if os.Getenv(constants.PodNamespaceEnvKey) != constants.TrustworthinessNameSpace {
		meta.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion:         "apps/v1",
				BlockOwnerDeletion: &t,
				Controller:         &t,
				Kind:               "Deployment",
				Name:               "worker-manager",
				UID:                ownerUID,
			},
		}
	}
	return meta
}

func (b *poolDeploymentBuilder) describeCaeComponentSet(pool *Pool) (*componentSet, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config: %s", err.Error())
		return nil, err
	}

	cs := b.describeComponentSetCommon(cfg.DeployDir, pool)
	cs.add(&runtimeManagerConfig{restoreOpts: cfg.PoolMgrConfig.CheckpointPodConfig.CRIUCheckpointOpts,
		storageType:               cfg.PoolMgrConfig.CheckpointPodConfig.SnapshotStorageType,
		businessType:              constants.BusinessTypeCAE,
		disableCAERuntimePreStart: 1,
		enableCAEDirectConnection: cfg.PoolMgrConfig.EnableCAEDirectConnection})
	cs.add(&caePoolWorkerRuntimeShareConfig{})
	b.describeComponentSetForCaeLogs(cs)
	if utils.EnableNewCaeBaseImage() {
		cs.add(&caeTools{})
	}
	return cs, nil
}

func (b *poolDeploymentBuilder) describeComponentSetForCaeLogs(cs *componentSet) {
	caeLogHostPath := "/var/log/cff/default/caelogs"
	caeLogMountPath := "/home/caelog"
	caeLogConfigPath := "/home/sn/log/logconfig"
	if len(b.config.VolumeConfig.LogHostPath.CaeLog) > 0 {
		caeLogHostPath = b.config.VolumeConfig.LogHostPath.CaeLog
	}
	cs.add(&caePoolConfig{
		hostPath:      caeLogHostPath,
		mountPath:     caeLogMountPath,
		logConfigPath: caeLogConfigPath,
	})
	if b.config.VolumeConfig.PackageHostPath != "" {
		cs.add(&hostPackage{path: b.config.VolumeConfig.PackageHostPath})
	}
}

func (b *poolDeploymentBuilder) describeComponentSet(pool *Pool) (*componentSet, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config: %s", err.Error())
		return nil, err
	}

	cs := b.describeComponentSetCommon(cfg.DeployDir, pool)
	cs.add(&runtimeManagerConfig{restoreOpts: cfg.PoolMgrConfig.CheckpointPodConfig.CRIUCheckpointOpts})
	return cs, nil
}

func (b *poolDeploymentBuilder) describeComponentSetCommon(deployDir string, pool *Pool) *componentSet {
	cs := newComponentSet()
	cs.add(&runtimeFileHandleNum{})
	cs.add(&userFunctionCodeData{deployDir: deployDir, metricsDir: metricsDir})
	cs.add(&userFunctionTmpData{})
	cs.add(&shareWorkerSecretData{})
	cs.add(&workerConfig{})
	cs.add(&podConfig{})
	cs.add(&runtimeConfig{})
	cs.add(&timeZone{})
	cs.add(&cidr{})
	cs.add(&traceEnv{})
	cs.add(&podInfo{})
	cs.add(&localCache{enable: b.config.EnableLocalCache})
	cs.add(&cmNamespace{nameSpace: os.Getenv(constants.PodNamespaceEnvKey)})
	cs.add(&obs{enable: b.config.EnableMinio})
	cs.add(&workerAuthCheck{enable: b.config.WorkerAuthEnable})
	cs.add(&funcBranch{})
	cs.add(&runtimeLanguage{language: pool.Language})
	cs.add(&presetDependencyConfig{})
	cs.add(&cudaDependencyConfig{})
	cs.add(&trace{runtimeLimitMemory: pool.LimitMemory})
	cs.add(&socketFile{})
	cs.add(&workerLogEnv{})
	cs.add(&dockerRootDir{})
	cs.add(&dockerSocket{})
	cs.add(&containerdDir{})
	cs.add(&runtimeMgrLogEnv{})
	cs.add(&workerContentSignatureCheck{enable: b.config.WorkerContentSignatureEnable})
	cs.add(&workerResourceLimitEnv{cpuLimit: pool.LimitCPU, memoryLimit: pool.LimitMemory})
	cs.add(&datasystemPath{})
	if b.config.VolumeConfig.PackageHostPath != "" {
		cs.add(&hostPackage{path: b.config.VolumeConfig.PackageHostPath})
	}
	b.describeComponentSetForLogs(cs)
	b.describeComponentSetForSecrets(cs)
	cs.add(&workerIAMTokenEnv{enableIAMToken: utils.IsIAMTokenFeatureEnable()})
	return cs
}

func (b *poolDeploymentBuilder) describeComponentSetForLogs(cs *componentSet) {
	cs.add(&fusionSTDLog{
		hostPath:  b.config.VolumeConfig.LogHostPath.UserLog,
		mountPath: b.config.VolumeConfig.UserLogMountPath,
	})
	cs.add(&fusionServiceLog{
		hostPath:            b.config.VolumeConfig.LogHostPath.ServiceLog,
		workerMountPath:     b.config.VolumeConfig.LogMountPath,
		runtimeMgrMountPath: b.config.VolumeConfig.RuntimeMgrLogPath,
		runtimeLogMountPath: b.config.VolumeConfig.RuntimeLogMountPath,
	})
}

func (b *poolDeploymentBuilder) describeComponentSetForSecrets(cs *componentSet) {
	if config.GetLocalAuth() != nil {
		cs.add(&localAuthSecret{shouldMountRuntime: false})
	} else {
		cs.add(&authSecret{})
	}

	cs.add(&tempWorkerKeySecret{})

	if b.config.CustomCAConfig.CustomCAEnable {
		cs.add(&caSecret{mountPath: b.config.CustomCAConfig.CaFilePath})
	}

	if config.EnableHTTPS() {
		cs.add(&tlsSecret{})
	}

	if GetPoolManager().DecoyFileEnabled || config.IsDecoyFileEnabled() {
		log.GetLogger().Infof("start to mount decoy secret")
		cs.add(&decoySecret{})
	}
}

func (b *poolDeploymentBuilder) describeInitContainers(cs *componentSet) []corev1.Container {
	name := string(containerWorkerInit)
	return []corev1.Container{
		{
			Name:  name,
			Image: b.config.PoolContainerTags.WorkerInitImageTag,
			Env:   cs.buildEnvVar(containerWorkerInit),
			SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
				Add:  []corev1.Capability{"NET_ADMIN", "NET_RAW"},
				Drop: []corev1.Capability{"ALL"},
			}},
			VolumeMounts: cs.buildMounts(containerWorkerInit),
		},
	}
}

func (b *poolDeploymentBuilder) describeNonIsolatedContainers(pool *Pool, cs *componentSet) []corev1.Container {
	resourceBuilder := newResourceRequirementsBuilder()
	if pool.LimitMemory == work128 {
		resourceBuilder.setCPU(lowerWorkerRequestCPU, pool.WorkerLimitCPU)
	} else {
		resourceBuilder.setCPU(workerRequestCPU, pool.WorkerLimitCPU)
	}
	resourceBuilder.setMemory(workerRequestMemory, pool.LimitMemory)
	resourceBuilder.setEphemeralStorage(defaultRequestEphemeralStorage, pool.EphemeralStorage)
	resourceBuilder.setCustomResource(pool.CustomResources)
	resources := resourceBuilder.build()

	return []corev1.Container{
		{
			Name:           string(containerWorker),
			Image:          b.config.PoolContainerTags.WorkerImageTag,
			Ports:          getWorkerPort(),
			Args:           cs.buildArgs(containerWorker),
			Env:            append(cs.buildEnvVar(containerWorker), getWorkerEnv(pool.LimitMemory)...),
			Resources:      resources,
			VolumeMounts:   cs.buildMounts(containerWorker),
			ReadinessProbe: getWorkerReadinessProbe(),
			LivenessProbe: getWorkerLivenessProbe(pool.LivenessProbeFailureThreshold,
				pool.LivenessProbePeriodSeconds),
			SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
				Add:  sanitizeCAP(b.config.WorkerCaps.Add),
				Drop: sanitizeCAP(b.config.WorkerCaps.Drop),
			}},
		},
	}
}

func (b *poolDeploymentBuilder) describeCaeContainers(pool *Pool, cs *componentSet) []corev1.Container {
	runtimeContainerSpec := corev1.Container{
		Name:                   string(containerRuntime),
		Image:                  b.config.PoolContainerTags.CaeBaseImageTag,
		ImagePullPolicy:        corev1.PullAlways,
		Env:                    cs.buildEnvVar(containerRuntime),
		Resources:              getRuntimeResource(pool),
		VolumeMounts:           cs.buildMounts(containerRuntime),
		Command:                []string{"bash", "-c", safeCommandRuntime},
		TerminationMessagePath: terminationLogPath,
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  sanitizeCAP(b.config.RuntimeCaps.Add),
				Drop: sanitizeCAP(b.config.RuntimeCaps.Drop),
			},
		},
	}
	return []corev1.Container{runtimeContainerSpec, b.constructWorkerContainerSpecCommon(pool, cs)}
}

func (b *poolDeploymentBuilder) describeContainers(pool *Pool, cs *componentSet) []corev1.Container {
	// first pull up runtime container then worker container. otherwise worker cannot connect to runtime
	runtimeContainerSpec := corev1.Container{
		Name:                   string(containerRuntime),
		Image:                  b.config.PoolContainerTags.RuntimeImageTag,
		Env:                    cs.buildEnvVar(containerRuntime),
		Command:                []string{"bash", "-c", safeCommandRuntime},
		Resources:              getRuntimeResource(pool),
		TerminationMessagePath: terminationLogPath,
		VolumeMounts:           cs.buildMounts(containerRuntime),
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  sanitizeCAP(b.config.RuntimeCaps.Add),
				Drop: sanitizeCAP(b.config.RuntimeCaps.Drop),
			},
		},
	}
	return []corev1.Container{runtimeContainerSpec, b.constructWorkerContainerSpecCommon(pool, cs)}
}

func (b *poolDeploymentBuilder) constructWorkerContainerSpecCommon(pool *Pool, cs *componentSet) corev1.Container {
	workerResource := getWorkerResource(pool)
	privileged := true
	workerLimitMemory := workerResource.Limits.Memory().Value() >> mbShift
	workerContainerSpec := corev1.Container{
		Name:           string(containerWorker),
		Image:          b.config.PoolContainerTags.WorkerImageTag,
		Ports:          getWorkerPort(),
		Args:           cs.buildArgs(containerWorker),
		Env:            append(cs.buildEnvVar(containerWorker), getWorkerEnv(workerLimitMemory)...),
		Command:        []string{"bash", "-c", safeCommandWorker},
		Resources:      workerResource,
		VolumeMounts:   cs.buildMounts(containerWorker),
		ReadinessProbe: getWorkerReadinessProbe(),
		LivenessProbe: getWorkerLivenessProbe(pool.LivenessProbeFailureThreshold,
			pool.LivenessProbePeriodSeconds),
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add:  sanitizeCAP(b.config.WorkerCaps.Add),
				Drop: sanitizeCAP(b.config.WorkerCaps.Drop),
			},
			Privileged: &privileged,
		},
	}
	workerContainerSpec.Env = generateSnapshotTagWithEnv(workerContainerSpec.Env, pool.Name)
	return workerContainerSpec
}

func describePodAntiAffinity(standard standard) *corev1.PodAntiAffinity {
	return &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
			{
				Weight: podAntiAffinityWeight,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.StandardLabelKey: string(standard),
						},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
			getWeightedPodAffinityTermOfSystemComponents(),
		},
	}
}

func getWeightedPodAffinityTermOfSystemComponents() corev1.WeightedPodAffinityTerm {
	return corev1.WeightedPodAffinityTerm{
		Weight: 30,
		PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: labelApp, Operator: metav1.LabelSelectorOpIn, Values: components},
					{Key: labelCffType, Operator: metav1.LabelSelectorOpIn, Values: labelValueOfVPCPullTrigger},
				},
			},
			TopologyKey: k8sHostNameKey,
		},
	}
}

func describeNodeAffinity(key string, values []string, labels map[string]string,
	isFusionRuntime bool) *corev1.NodeAffinity {
	expressions := make([]corev1.NodeSelectorRequirement, 0, utils.DefaultSliceSize)
	expressions = append(expressions, corev1.NodeSelectorRequirement{
		Key:      key,
		Operator: corev1.NodeSelectorOpIn,
		Values:   values,
	})
	if isFusionRuntime && ssdNodeKey != "" && ssdNodeValue != "" {
		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key:      ssdNodeKey,
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   []string{ssdNodeValue},
		})
	}
	if _, ok := labels[utils.LabelNodeType]; ok && config.IsHeterogeneousScheduling() {
		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key:      utils.LabelNodeType,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{labels[utils.LabelNodeType]},
		})
	}
	if utils.IsVpaFeatureEnabled() {
		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key:      constants.NodeVpaPausePodFaultsLabelKey,
			Operator: corev1.NodeSelectorOpDoesNotExist,
			Values:   []string{},
		})
	}
	return &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: expressions,
			}},
		},
	}
}
