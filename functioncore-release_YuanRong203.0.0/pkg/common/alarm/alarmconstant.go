/*
 * Copyright (c) 2021 Huawei Technologies Co. Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS WITHOUT WARRANTIES OF ANY KIND
 * EITHER EXPRESS OR IMPLIED INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package alarm is used to notify alarm message to FG
package alarm

import "functioncore/pkg/common/constants"

const (
	// Report report Type
	Report = "1"
	// Clear report Type
	Clear = "2"
)

const (
	// CriticalSeverity critical alarm severity
	CriticalSeverity = "1"
	// ImportantSeverity Alarm Severity
	ImportantSeverity = "2"
	// Set the timeout interval for reporting requests.
	requestTimeoutInterval = 10
	// HeaderAlarmAuthTimestamp is the timestamp for alarm authorization
	HeaderAlarmAuthTimestamp = "X-CFF-Timestamp-Auth"
	// HeaderAlarmAuthorization is authorization
	HeaderAlarmAuthorization = "X-CFF-Authorization"
)

const (
	routerEtcdConnectionAlarmName                           = "数据面路由etcd连接异常"
	metadataEtcdConnectionAlarmName                         = "数据面元etcd连接异常"
	redisConnectionAlarmName                                = "数据面Redis连接异常"
	criticalRedisConnectionAlarmName                        = "数据面Redis连接持续异常"
	vpcExceededThresholdAlarmName                           = "数据面VPC网卡不足"
	swrConnectionAlarmName                                  = "数据面SWR连接异常"
	obsConnectionAlarmName                                  = "数据面OBS连接异常"
	insufficientMinInstanceAlarmName                        = "数据面预留实例拉起异常"
	routerEtcdLeaseAlarmName                                = "数据面路由etcd续租异常"
	ntpOffsetAlarmName                                      = "数据面NTP时钟偏移超出阈值"
	idleModeNotificationAlarmName                           = "数据面空闲模式通知异常"
	poolDeploymentAlarmName                                 = "数据面pool deployment更新失败"
	rotatedTokenAlarmName                                   = "数据面获取token异常"
	restartingPodAlarmName                                  = "数据面pod重启告警"
	faaSSchedulerConnectionAlarmName                        = "数据面FaaS Scheduler连接异常"
	routerETCDConnectionAbnormalContinuouslyAlarmName       = "数据面路由etcd连接持续异常"
	metaETCDConnectionAbnormalContinuouslyAlarmName         = "数据面元etcd连接持续异常"
	etcdOperationFailedContinuouslyAlarmName                = "数据面操作etcd持续异常"
	ntpConnectionAlarmName                                  = "数据面NTP服务连接异常"
	insufficientEphemeralStorageName                        = "数据面节点临时存储不足"
	checkpointAbnormalName                                  = "数据面制作快照持续异常"
	checkpointExceedLimitName                               = "数据面制作快照超过并发数限制异常"
	caeMetaEtcdConnectionAlarmName                          = "数据面CAE元etcd连接异常"
	caeMetaEtcdConnectionAbnormalContinuouslyAlarmName      = "数据面CAE元etcd连接持续异常"
	routerEtcdRegistryAlarmName                             = "数据面路由etcd注册异常"
	caeRouterEtcdConnectionAlarmName                        = "数据面CAE路由etcd连接异常"
	caeRouterEtcdConnectionAbnormalContinuouslyAlarmName    = "数据面CAE路由etcd连接持续异常"
	metaEtcdProxyConnectionAlarmName                        = "数据面元etcd proxy连接异常"
	metaEtcdProxyConnectionAbnormalContinuouslyAlarmName    = "数据面元etcd proxy连接持续异常"
	caeMetaEtcdProxyConnectionAlarmName                     = "数据面CAE元etcd proxy连接异常"
	caeMetaEtcdProxyConnectionAbnormalContinuouslyAlarmName = "数据面CAE元etcd proxy连接持续异常"
	autoScalerCheckWorkManagerUnHealthyAlarmName            = "autoscaler连接workmanager健康检查异常"
	streamHandlerConnectionAbnormalAlarmName                = "数据面Stream-Handler连接持续异常"
	customImageSchedulerFailedAlarmName                     = "数据面磁盘空间不足导致自定义镜像调度失败"
	streamClientAlarmName                                   = "数据面访问数据系统异常"
	vpaPausePodAlarmName                                    = "数据面垂直弹性pause pod异常"
	caeAvailableClusterAlarmName                            = "微服务实例分配异常"
	busTenantLimitAlarmName                                 = "bus租户实例限流"
	preloadRequestAlarmName                                 = "预加载请求cffAgent异常"
	preloadWorkerPanicAlarmName                             = "预加载worker异常panic"
	patPodCreateAlarmName                                   = "创建patPod异常"
	checkpointQueueAlarmName                                = "数据面快照制作排队时间过长"
	restoreFailedName                                       = "数据面快照恢复失败"
	migrateStateIDFailedName                                = "数据面状态ID迁移持续失败"
	createPodRespFormatErrName                              = "数据面创建POD响应反序列化失败"
)

const (
	etcdErrCause                      = "abnormal network;incorrect cluster configuration;abnormal database service;internal service error;"
	redisErrCause                     = "abnormal network;incorrect cluster configuration;abnormal database service;internal service error;"
	vpcErrCause                       = "VPC nic has exceeded threshold;"
	swrErrCause                       = "abnormal network;incorrect cluster configuration;abnormal database service;internal service error;"
	obsErrCause                       = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	minInsErrCause                    = "internal service error; insufficient environmental resources;"
	routerEtcdLeaseCause              = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	ntpErrCause                       = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	idleRetryCause                    = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	poolCause                         = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	rotTokenCause                     = "abnormal network;incorrect cluster configuration;internal service error;"
	podCause                          = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	schedulerCause                    = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	ntpConErrCause                    = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	insufficientEphemeralStorageCause = "no available nodes;the instance pool is not deployed on an available node;"
	checkCause                        = "abnormal checkpoint;internal service error;"
	userMinInsErrCause                = "user code err;VPC not found;VPC permission is insufficient;"
	routerEtcdRegistryCause           = "abnormal network;incorrect cluster configuration;abnormal storage service;internal service error;"
	streamErrCause                    = "abnormal network;incorrect cluster configuration;" +
		"abnormal stream-handler service;internal service error;"
	customImageSchedulerFailedCause = "no available nodes;the custom container is not deployed on an available node;"
	streamClientErrCause            = "abnormal network;incorrect cluster configuration;abnormal database service;internal service error;"
	vpaPausePodErrCause             = "insufficient node resources;pause pod image err;"
	busTenantLimitErrCause          = "bus may be in tenant traffic limit;"
	preloadRequestErrCause          = "abnormal network;abnormal cffAgent;abnormal node;"
	preloadWorkerPanicErrCause      = "worker panic;"
	patPodCreateErrCause            = "invoke vpc-controller error or timeout;"
	migrateStateIDErrCause          = "no available nodes;"
	createPodRespFormatErrCause     = "create pod resp format failed;"
)

var (
	routerETCDAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-routerETCD",
		constants.FunctionTask:     "functiontask-routerETCD",
		constants.InstanceManager:  "instancemanager-routerETCD",
		constants.StateManager:     "statemanager-routerETCD",
		constants.FaaSScheduler:    "faasscheduler-routerETCD",
	}
	metadataETCDAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-metadataETCD",
		constants.FunctionTask:     "functiontask-metadataETCD",
		constants.InstanceManager:  "instancemanager-metadataETCD",
		constants.CacheManager:     "cachemanager-metadataETCD",
		constants.StateManager:     "statemanager-metadataETCD",
		constants.FaaSScheduler:    "faasscheduler-metadataETCD",
	}
	redisAlarmResourceID = map[string]string{
		constants.FunctionTask:    "functiontask-redis",
		constants.InstanceManager: "instancemanager-redis",
		constants.StateManager:    "statemanager-redis",
	}
	vpcAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-VPC",
	}
	swrAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-swr",
	}
	obsAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-obs",
		constants.CacheManager:    "cachemanager-obs",
	}
	routerETCDLeaseAlarmResourceID = map[string]string{
		constants.FaaSScheduler: "faasscheduler-routerETCDLease",
	}
	ntpAlarmResourceID = map[string]string{
		"meta-etcd": "meta-etcd-ntp",
	}
	minInsAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-minInstance",
	}
	idleRetryAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-idle-mode-retry",
	}
	poolDeployAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-pool-deploy",
	}
	rotatedTokenAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-iam",
		constants.CacheManager:    "cachemanager-iam",
	}
	restartingPodsAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-restarting-pods",
	}
	faaSSchedulerAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-faasscheduler",
	}
	routerETCDCriticalAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-routerETCD-critical",
		constants.FunctionTask:     "functiontask-routerETCD-critical",
		constants.InstanceManager:  "instancemanager-routerETCD-critical",
		constants.StateManager:     "statemanager-routerETCD-critical",
		constants.FaaSScheduler:    "faasscheduler-routerETCD-critical",
	}
	metadataETCDCriticalAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-metadataETCD-critical",
		constants.FunctionTask:     "functiontask-metadataETCD-critical",
		constants.InstanceManager:  "instancemanager-metadataETCD-critical",
		constants.CacheManager:     "cachemanager-metadataETCD-critical",
		constants.StateManager:     "statemanager-metadataETCD-critical",
		constants.FaaSScheduler:    "faasscheduler-metadataETCD-critical",
	}
	etcdOperationFailedContinuouslyResourceID = map[string]string{
		constants.InstanceManager: "RouterEtcdOperationFailedContinuously",
	}
	insufficientEphemeralStorageResourceID = map[string]string{
		constants.InstanceManager: "insufficient-ephemeral-storage",
	}
	checkpointAbnormalResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-checkpoint",
	}
	checkpointAbnormalCriticalResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-checkpoint-critical",
	}
	caeMetaETCDAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-caeMetaETCD",
		constants.FunctionTask:     "functiontask-caeMetaETCD",
		constants.InstanceManager:  "instancemanager-caeMetaETCD",
		constants.FaaSScheduler:    "faasscheduler-caeMetaETCD",
	}
	caeMetaETCDCriticalAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-caeMetaETCD-critical",
		constants.FunctionTask:     "functiontask-caeMetaETCD-critical",
		constants.InstanceManager:  "instancemanager-caeMetaETCD-critical",
		constants.FaaSScheduler:    "faasscheduler-caeMetaETCD-critical",
	}
	routerETCDRegistryAlarmResourceID = map[string]string{
		constants.FunctionTask: "functiontask-routerETCDRegistry",
	}
	caeRouterETCDAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-caeRouterETCD",
		constants.FunctionTask:     "functiontask-caeRouterETCD",
		constants.InstanceManager:  "instancemanager-caeRouterETCD",
	}
	caeRouterETCDCriticalAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-caeRouterETCD-critical",
		constants.FunctionTask:     "functiontask-caeRouterETCD-critical",
		constants.InstanceManager:  "instancemanager-caeRouterETCD-critical",
	}
	metaEtcdProxyAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-metaEtcdProxy",
		constants.FunctionTask:     "functiontask-metaEtcdProxy",
		constants.InstanceManager:  "instancemanager-metaEtcdProxy",
		constants.CacheManager:     "cachemanager-metaEtcdProxy",
		constants.StateManager:     "statemanager-metaEtcdProxy",
		constants.FaaSScheduler:    "faasscheduler-metaEtcdProxy",
	}
	metaEtcdProxyCriticalAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-metaEtcdProxy-critical",
		constants.FunctionTask:     "functiontask-metaEtcdProxy-critical",
		constants.InstanceManager:  "instancemanager-metaEtcdProxy-critical",
		constants.CacheManager:     "cachemanager-metaEtcdProxy-critical",
		constants.StateManager:     "statemanager-metaEtcdProxy-critical",
		constants.FaaSScheduler:    "faasscheduler-metaEtcdProxy-critical",
	}
	caeMetaEtcdProxyAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-caeMetaEtcdProxy",
		constants.FunctionTask:     "functiontask-caeMetaEtcdProxy",
		constants.InstanceManager:  "instancemanager-caeMetaEtcdProxy",
		constants.FaaSScheduler:    "faasscheduler-caeMetaEtcdProxy",
	}
	caeMetaEtcdProxyCriticalAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-caeMetaEtcdProxy-critical",
		constants.FunctionTask:     "functiontask-caeMetaEtcdProxy-critical",
		constants.InstanceManager:  "instancemanager-caeMetaEtcdProxy-critical",
		constants.FaaSScheduler:    "faasscheduler-caeMetaEtcdProxy-critical",
	}
	autoScalerCheckWorkManagerUnHealthyID = map[string]string{
		constants.Autoscaler: "autoscaler-check-workmanager-unhealthy",
	}
	streamAlarmResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-streamhandler",
	}
	customImageSchedulerAlarmResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-custom-image-scheduler",
	}
	streamClientResourceID = map[string]string{
		constants.FunctionAccessor: "functionaccessor-streamclient",
	}
	vpaPausePodResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-vpa-pausePod",
	}
	caeAvailableClusterAlarmResourceID = map[string]string{
		constants.Autoscaler: "autoscaler-cae-availableCluster",
	}
	tenantTrafficLimitAlarmResourceID = map[string]string{
		constants.FunctionTask: "bus-tenant-limit",
	}
	preloadRequestResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-preload-request",
	}
	preloadWorkerPanicResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-preload-worker-panic",
	}
	patPodCreateErrResourceID = map[string]string{
		constants.InstanceManager: "instancemanager-create-pat-error",
	}
	migrateStateIDFailedResourceID = map[string]string{
		constants.StateManager: "migrate-stateID-failed",
	}
	createPodRespFormatErrResourceID = map[string]string{
		constants.InstanceManager: "create-pod-resp-format-err",
	}
)

const (
	routerEtcdConnectionAlarmID                           = "FSS_1001"
	metadataEtcdConnectionAlarmID                         = "FSS_1002"
	redisConnectionAlarmID                                = "FSS_1003"
	criticalRedisConnectionAlarmID                        = "FSS_1004"
	vpcExceededThresholdAlarmID                           = "FSS_1005"
	swrConnectionAlarmID                                  = "FSS_1006"
	obsConnectionAlarmID                                  = "FSS_1007"
	insufficientMinInstanceAlarmID                        = "FSS_1008"
	routerEtcdLeaseAlarmID                                = "FSS_1009"
	ntpOffsetAlarmID                                      = "FSS_1010"
	idleModeNotificationAlarmID                           = "FSS_1011"
	poolDeploymentAlarmID                                 = "FSS_1012"
	rotatedTokenAlarmID                                   = "FSS_1013"
	restartingPodAlarmID                                  = "FSS_1014"
	faaSSchedulerConnectionAlarmID                        = "FSS_1015"
	routerETCDConnectionAbnormalContinuouslyAlarmID       = "FSS_1016"
	metaETCDConnectionAbnormalContinuouslyAlarmID         = "FSS_1017"
	etcdOperationFailedContinuouslyAlarmID                = "FSS_1018"
	ntpConnectionAlarmID                                  = "FSS_1019"
	insufficientEphemeralStorageAlarmID                   = "FSS_1020"
	checkpointAbnormalAlarmID                             = "FSS_1021"
	checkpointExceedLimitAlarmID                          = "FSS_1022"
	caeMetaEtcdConnectionAlarmID                          = "FSS_1023"
	caeMetaEtcdConnectionAbnormalContinuouslyAlarmID      = "FSS_1024"
	routerEtcdRegistryAlarmID                             = "FSS_1025"
	caeRouterEtcdConnectionAlarmID                        = "FSS_1026"
	caeRouterEtcdConnectionAbnormalContinuouslyAlarmID    = "FSS_1027"
	metaEtcdProxyConnectionAlarmID                        = "FSS_1028"
	metaEtcdProxyConnectionAbnormalContinuouslyAlarmID    = "FSS_1029"
	caeMetaEtcdProxyConnectionAlarmID                     = "FSS_1030"
	caeMetaEtcdProxyConnectionAbnormalContinuouslyAlarmID = "FSS_1031"
	autoScalerCheckWorkManagerUnHealthyAlarmID            = "FSS_1032"
	streamHandlerConnectionAbnormalAlarmID                = "FSS_1033"
	customImageSchedulerFailedAlarmID                     = "FSS_1034"
	streamClientAlarmID                                   = "FSS_1035"
	vpaPausePodAlarmID                                    = "FSS_1036"
	caeAvailableClusterAlarmID                            = "FSS_1037"
	busTenantLimitAlarmID                                 = "FSS_1038"
	preloadRequestAlarmID                                 = "FSS_1039"
	preloadWorkerPanicAlarmID                             = "FSS_1040"
	patPodCreateErrAlarmID                                = "FSS_1041"
	checkpointQueueAlarmID                                = "FSS_1042"
	checkpointRestoreFailedAlarmID                        = "FSS_1043"
	migrateStateIDFailedAlarmID                           = "FSS_1044"
	createPodRespFormatErrAlarmID                         = "FSS_1045"
)
