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

// Package alarm is used to notify alarm message
package alarm

// Type alarm type
type Type int

// enum of alarm type
const (
	// RouterEtcdConnection alarm type
	RouterEtcdConnection Type = iota
	// MetadataEtcdConnection alarm type
	MetadataEtcdConnection
	// RedisConnection alarm type
	RedisConnection
	// CriticalRedisConnection alarm type
	CriticalRedisConnection
	// VPCExceededThreshold alarm type
	VPCExceededThreshold
	// SWRConnection alarm type
	SWRConnection
	// OBSConnection alarm type
	OBSConnection
	// InsufficientMinInstance alarm type
	InsufficientMinInstance
	// RouterEtcdRegistry alarm type
	RouterEtcdLease
	// NTPOffset alarm type
	NTPOffset
	// IdleModeNotification alarm type
	IdleModeNotification
	// PoolDeployment alarm type
	PoolDeployment
	// RotatedToken alarm type
	RotatedToken
	// RestartingPod alarm type
	RestartingPod
	// FaaSSchedulerConnection alarm type
	FaaSSchedulerConnection
	// RouterETCDConnectionAbnormalContinuously alarm type
	RouterETCDConnectionAbnormalContinuously
	// MetaETCDConnectionAbnormalContinuously alarm type
	MetaETCDConnectionAbnormalContinuously
	// EtcdOperationFailedContinuously the router etcd service fails continuously.
	EtcdOperationFailedContinuously
	// NTPConnection alarm type
	NTPConnection
	// InsufficientEphemeralStorage pod selection failure due to insufficient temporary storage
	InsufficientEphemeralStorage
	// CheckpointAbnormalContinuously checkpointing keeps failing
	CheckpointAbnormalContinuously
	// CheckpointConcurrentNumExceedLimit checkpointing concurrent num exceed limit
	CheckpointConcurrentNumExceedLimit
	// UserInsufficientMinInstance alarm type
	UserInsufficientMinInstance
	CAEMetaEtcdConnection
	CAEMetaETCDConnectionAbnormalContinuously
	// RouterEtcdRegistry alarm type
	RouterEtcdRegistry

	// CAERouterEtcdConnection -
	CAERouterEtcdConnection
	// CAERouterETCDConnectionAbnormalContinuously -
	CAERouterETCDConnectionAbnormalContinuously
	// MetaEtcdProxyConnection alarm type
	MetaEtcdProxyConnection
	// MetaEtcdProxyConnectionAbnormalContinuously alarm type
	MetaEtcdProxyConnectionAbnormalContinuously
	// CAEMetaEtcdProxyConnection alarm type
	CAEMetaEtcdProxyConnection
	// CAEMetaEtcdProxyConnectionAbnormalContinuously alarm type
	CAEMetaEtcdProxyConnectionAbnormalContinuously

	// AutoScalerCheckWorkManagerUnHealthy -
	AutoScalerCheckWorkManagerUnHealthy

	// StreamHandlerConnection -
	StreamHandlerConnection

	// CustomImageSchedulerFailed failed to scheduler custom image
	CustomImageSchedulerFailed

	// StreamClient -
	StreamClient

	// VpaPausePod -
	VpaPausePod

	CAEAvailableCluster

	// BusTenantLimit -
	BusTenantLimit

	// PreloadRequest -
	PreloadRequest

	// PreloadWorkerPanic -
	PreloadWorkerPanic

	// CheckpointQueueLongTime -
	CheckpointQueueLongTime

	// PatPodCreateFailed failed to create pat-service
	PatPodCreateFailed

	// CheckpointRestoreFailed -
	CheckpointRestoreFailed

	MigrateStateIDFailed

	// CreatePodRespFormatErr -
	CreatePodRespFormatErr

	typeSize

	// AutoScalerInstance -
	AutoScalerInstance
)

// ResourceID get resourceID
func (t Type) ResourceID(serviceName string) string {
	return [...]map[string]string{routerETCDAlarmResourceID, metadataETCDAlarmResourceID, redisAlarmResourceID,
		redisAlarmResourceID, vpcAlarmResourceID, swrAlarmResourceID, obsAlarmResourceID, minInsAlarmResourceID,
		routerETCDLeaseAlarmResourceID, ntpAlarmResourceID, idleRetryAlarmResourceID,
		poolDeployAlarmResourceID, rotatedTokenAlarmResourceID,
		restartingPodsAlarmResourceID, faaSSchedulerAlarmResourceID,
		routerETCDCriticalAlarmResourceID, metadataETCDCriticalAlarmResourceID,
		etcdOperationFailedContinuouslyResourceID, ntpAlarmResourceID,
		insufficientEphemeralStorageResourceID, checkpointAbnormalResourceID, checkpointAbnormalResourceID,
		minInsAlarmResourceID, caeMetaETCDAlarmResourceID, caeMetaETCDCriticalAlarmResourceID,
		routerETCDRegistryAlarmResourceID, caeRouterETCDAlarmResourceID, caeRouterETCDCriticalAlarmResourceID,
		metaEtcdProxyAlarmResourceID, metaEtcdProxyCriticalAlarmResourceID, caeMetaEtcdProxyAlarmResourceID,
		caeMetaEtcdProxyCriticalAlarmResourceID, autoScalerCheckWorkManagerUnHealthyID,
		streamAlarmResourceID, customImageSchedulerAlarmResourceID, streamClientResourceID,
		vpaPausePodResourceID, caeAvailableClusterAlarmResourceID, tenantTrafficLimitAlarmResourceID,
		preloadRequestResourceID, preloadWorkerPanicResourceID, checkpointAbnormalCriticalResourceID,
		patPodCreateErrResourceID, checkpointAbnormalResourceID, migrateStateIDFailedResourceID,
		createPodRespFormatErrResourceID}[t][serviceName]
}

// Cause get cause
func (t Type) Cause() string {
	return [...]string{etcdErrCause, etcdErrCause, redisErrCause,
		redisErrCause, vpcErrCause, swrErrCause, obsErrCause,
		minInsErrCause, routerEtcdLeaseCause, ntpErrCause, idleRetryCause,
		poolCause, rotTokenCause, podCause, schedulerCause,
		etcdErrCause, etcdErrCause, etcdErrCause, ntpConErrCause, insufficientEphemeralStorageCause,
		checkCause, checkCause, userMinInsErrCause, etcdErrCause, etcdErrCause, routerEtcdRegistryCause, etcdErrCause,
		etcdErrCause, etcdErrCause, etcdErrCause, etcdErrCause, etcdErrCause, checkCause, streamErrCause,
		customImageSchedulerFailedCause, streamClientErrCause, vpaPausePodErrCause, etcdErrCause,
		busTenantLimitErrCause, preloadRequestErrCause, preloadWorkerPanicErrCause,
		checkCause, patPodCreateErrCause, checkCause, migrateStateIDErrCause, createPodRespFormatErrCause}[t]
}

// Severity get severity
func (t Type) Severity() string {
	for _, criticalType := range [...]Type{RouterEtcdRegistry, RouterEtcdLease, CriticalRedisConnection,
		InsufficientMinInstance, PoolDeployment, RotatedToken, FaaSSchedulerConnection,
		RouterETCDConnectionAbnormalContinuously, MetaETCDConnectionAbnormalContinuously,
		EtcdOperationFailedContinuously, VPCExceededThreshold, InsufficientEphemeralStorage,
		CAEMetaETCDConnectionAbnormalContinuously, CAERouterETCDConnectionAbnormalContinuously,
		MetaEtcdProxyConnectionAbnormalContinuously, CAEMetaEtcdProxyConnectionAbnormalContinuously,
		StreamHandlerConnection, CheckpointQueueLongTime, MigrateStateIDFailed, CreatePodRespFormatErr} {
		if t == criticalType {
			return CriticalSeverity
		}
	}
	return ImportantSeverity
}

// NeedSuppressed need suppressed or not
func (t Type) NeedSuppressed() bool {
	for _, noNeedSuppressedType := range [...]Type{CriticalRedisConnection, SWRConnection,
		RouterETCDConnectionAbnormalContinuously, MetaETCDConnectionAbnormalContinuously,
		CAEMetaETCDConnectionAbnormalContinuously, CAERouterETCDConnectionAbnormalContinuously,
		MetaEtcdProxyConnectionAbnormalContinuously, CAEMetaEtcdProxyConnectionAbnormalContinuously,
		AutoScalerCheckWorkManagerUnHealthy, PreloadRequest, MigrateStateIDFailed} {
		if t == noNeedSuppressedType {
			return false
		}
	}
	return true
}

// AlarmName get alarmName
func (t Type) AlarmName() string {
	return [...]string{routerEtcdConnectionAlarmName, metadataEtcdConnectionAlarmName, redisConnectionAlarmName,
		criticalRedisConnectionAlarmName, vpcExceededThresholdAlarmName, swrConnectionAlarmName,
		obsConnectionAlarmName, insufficientMinInstanceAlarmName, routerEtcdLeaseAlarmName, ntpOffsetAlarmName,
		idleModeNotificationAlarmName, poolDeploymentAlarmName, rotatedTokenAlarmName, restartingPodAlarmName,
		faaSSchedulerConnectionAlarmName, routerETCDConnectionAbnormalContinuouslyAlarmName,
		metaETCDConnectionAbnormalContinuouslyAlarmName, etcdOperationFailedContinuouslyAlarmName,
		ntpConnectionAlarmName, insufficientEphemeralStorageName, checkpointAbnormalName, checkpointExceedLimitName,
		insufficientMinInstanceAlarmName, caeMetaEtcdConnectionAlarmName,
		caeMetaEtcdConnectionAbnormalContinuouslyAlarmName, routerEtcdRegistryAlarmName,
		caeRouterEtcdConnectionAlarmName, caeRouterEtcdConnectionAbnormalContinuouslyAlarmName,
		metaEtcdProxyConnectionAlarmName, metaEtcdProxyConnectionAbnormalContinuouslyAlarmName,
		caeMetaEtcdProxyConnectionAlarmName, caeMetaEtcdProxyConnectionAbnormalContinuouslyAlarmName,
		autoScalerCheckWorkManagerUnHealthyAlarmName, streamHandlerConnectionAbnormalAlarmName,
		customImageSchedulerFailedAlarmName, streamClientAlarmName, vpaPausePodAlarmName,
		caeAvailableClusterAlarmName, busTenantLimitAlarmName, preloadRequestAlarmName, preloadWorkerPanicAlarmName,
		checkpointQueueAlarmName, patPodCreateAlarmName, restoreFailedName, migrateStateIDFailedName,
		createPodRespFormatErrName}[t]
}

// AlarmID get AlarmID
func (t Type) AlarmID() string {
	return [...]string{routerEtcdConnectionAlarmID, metadataEtcdConnectionAlarmID, redisConnectionAlarmID,
		criticalRedisConnectionAlarmID, vpcExceededThresholdAlarmID, swrConnectionAlarmID,
		obsConnectionAlarmID, insufficientMinInstanceAlarmID, routerEtcdLeaseAlarmID, ntpOffsetAlarmID,
		idleModeNotificationAlarmID, poolDeploymentAlarmID, rotatedTokenAlarmID, restartingPodAlarmID,
		faaSSchedulerConnectionAlarmID, routerETCDConnectionAbnormalContinuouslyAlarmID,
		metaETCDConnectionAbnormalContinuouslyAlarmID, etcdOperationFailedContinuouslyAlarmID,
		ntpConnectionAlarmID, insufficientEphemeralStorageAlarmID, checkpointAbnormalAlarmID, checkpointExceedLimitAlarmID,
		insufficientMinInstanceAlarmID, caeMetaEtcdConnectionAlarmID,
		caeMetaEtcdConnectionAbnormalContinuouslyAlarmID, routerEtcdRegistryAlarmID,
		caeRouterEtcdConnectionAlarmID, caeRouterEtcdConnectionAbnormalContinuouslyAlarmID,
		metaEtcdProxyConnectionAlarmID, metaEtcdProxyConnectionAbnormalContinuouslyAlarmID,
		caeMetaEtcdProxyConnectionAlarmID, caeMetaEtcdProxyConnectionAbnormalContinuouslyAlarmID,
		autoScalerCheckWorkManagerUnHealthyAlarmID, streamHandlerConnectionAbnormalAlarmID,
		customImageSchedulerFailedAlarmID, streamClientAlarmID, vpaPausePodAlarmID, caeAvailableClusterAlarmID,
		busTenantLimitAlarmID, preloadRequestAlarmID, preloadWorkerPanicAlarmID, checkpointQueueAlarmID,
		patPodCreateErrAlarmID, checkpointRestoreFailedAlarmID, migrateStateIDFailedAlarmID,
		createPodRespFormatErrAlarmID}[t]
}
