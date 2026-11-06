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

package fusion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/tcpbustool"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/rpc/connect"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
)

const (
	userDataPath          = "/runtime/tmp"
	systemDataPath        = "/runtime/var-tmp"
	runtimeLogPath        = "/home/sn/log/service/runtime"
	userDataCheckInterval = 20 * time.Millisecond
	// ^/home/sn/log/service/runtime/(java|nodejs|python|php|csharp)-runtime(_[\d]+)?\.log(\.[\d]{16,17}(\.zip)?)?$
	logFilePattern = "^" + runtimeLogPath +
		"/(\\.gc.*|gc.*$|(java|nodejs|python|python2|php|csharp|cangjie|cppbin|customrpc)" +
		"-runtime(_|.[\\d]+)?\\.log((\\.[\\d]{16,17}(\\.zip)?)|((\\.gz)?))?$)"
	cangjieLogFilePattern = "^" + runtimeLogPath + "/cangjie-runtime_\\d{4}(.[\\d]+)?\\.log"

	reconnectRuntimeTimeout = 20 * time.Second

	keepsCrashingGracePeriod = 1 * time.Second
	limitRestartTimes        = 3
	limitRestartTimeDuration = 5 * time.Second

	defaultCustomMetricsCollectTime   = 15 * time.Second
	defaultCustomMetricsReportTimeout = 10
	defaultReportErrMsgContextTimeout = 1 * time.Second
)

var (
	logCompressFileRegex        = regexp.MustCompile(logFilePattern)
	cangjieLogCompressFileRegex = regexp.MustCompile(cangjieLogFilePattern)
	recRuntimeExitWhenRestore   = false
	bootHealthCheckChan         = make(chan bool, 1)
)

type newRuntimeFn func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error)

type fusionRuntime struct {
	eh           *exceptionHandler
	crashLimiter *limiter

	newRuntimeFn newRuntimeFn
	handlers     runtime.Handlers
	userCodePath string

	rt     runtime.Runtime
	mux    sync.Mutex
	cancel context.CancelFunc

	eventCnt             int64 // atomic
	isRuntimeProcessDead int32 // atomic

	isUserDatasExpansion bool
	subscribeOnce        sync.Once

	urnWithVersion string
}

func newFusionRuntime(conf *config.Configuration, handlers runtime.Handlers, fn newRuntimeFn,
) (*fusionRuntime, error) {
	r := &fusionRuntime{
		crashLimiter:         newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:                   newExceptionHandler(),
		newRuntimeFn:         fn,
		handlers:             handlers,
		userCodePath:         conf.DeploymentConfig.DeployDir,
		isUserDatasExpansion: conf.RuntimeConfig.ResourceMetaData.EnableTmpExpansion,
		urnWithVersion:       conf.RuntimeConfig.URN,
	}
	r.handlers.RecvProcessEventHandler = r.decorateRecvProcessEventHandler(r.handlers.RecvProcessEventHandler,
		conf, r.handlers.RequestTimeoutHandler)
	r.handlers.GetExceptionHandler = r.eh.getException
	r.handlers.CheckRuntimeStatusHandler = r.eh.checkException

	return r, nil
}

// WaitStartHealthCheckOK wait boot health check ok
func WaitStartHealthCheckOK() bool {
	return <-bootHealthCheckChan
}

func (r *fusionRuntime) startupHeartbeatCheck(heartbeat types.Heartbeat) {
	ticker := time.NewTicker(time.Second * time.Duration(heartbeat.PeriodSeconds))
	defer ticker.Stop()
	failedTimes := 0
	succeedTimes := 0
	for {
		select {
		case <-ticker.C:
			ok, err := r.StartupHealthz(heartbeat.Path, heartbeat.Timeout)
			if err != nil {
				succeedTimes = 0
				failedTimes++
				log.GetLogger().Warnf("do startup heartbeat check failed:%s, count:%d", err.Error(), failedTimes)
				if failedTimes >= heartbeat.FailureThreshold {
					bootHealthCheckChan <- false
				}
				continue
			}
			if ok {
				failedTimes = 0
				succeedTimes++
				log.GetLogger().Debugf("do startup heartbeat check ok count:%d", succeedTimes)
			} else {
				succeedTimes = 0
				failedTimes++
				log.GetLogger().Warnf("do startup heartbeat check failed count:%d", failedTimes)
			}
			if succeedTimes >= heartbeat.SuccessThreshold {
				bootHealthCheckChan <- true
				return
			}
			if failedTimes >= heartbeat.FailureThreshold {
				bootHealthCheckChan <- false
			}
		case _, ok := <-runtime.ProcessExitSignal:
			if !ok {
				log.GetLogger().Infof("stop startup heartbeat check")
				return
			}
		}
	}
}

// AliveHeartbeatCheck alive heartbeat check
func AliveHeartbeatCheck(fusionRuntimeIf runtime.Runtime, heartbeat types.Heartbeat) error {
	fusionRt, ok := fusionRuntimeIf.(*fusionRuntime)
	if !ok {
		log.GetLogger().Errorf("alive heartbeat check failed: fusionRuntime %#v assert failed", fusionRuntimeIf)
		return errors.New("alive heartbeat check failed for fusionRuntime assert failed")
	}
	go fusionRt.aliveHeartbeatCheck(heartbeat)
	return nil
}

func (r *fusionRuntime) aliveHeartbeatCheck(heartbeat types.Heartbeat) {
	ticker := time.NewTicker(time.Second * time.Duration(heartbeat.PeriodSeconds))
	defer ticker.Stop()
	failedTimes := 0
	for {
		select {
		case <-ticker.C:
			ok, err := r.Healthz(heartbeat.Path, heartbeat.Timeout)
			if err != nil {
				failedTimes++
				log.GetLogger().Errorf("do alive heartbeat check failed:%s, count:%d", err.Error(), failedTimes)
				if failedTimes >= heartbeat.FailureThreshold {
					errMsg := "The number of runtime health check failures reaches the threshold, " +
						"runtime exit, error: " + err.Error()
					r.reportErrorMessageToBus(errMsg)
					runtime.CloseExitChannel()
				}
				continue
			}
			if ok {
				failedTimes = 0
				log.GetLogger().Debugf("do alive heartbeat check ok")
			} else {
				failedTimes++
				log.GetLogger().Errorf("do alive heartbeat check failed count:%d", failedTimes)
			}
			if failedTimes >= heartbeat.FailureThreshold {
				errMsg := "The number of runtime health check failures reaches the threshold, " +
					"runtime exit, error: resp not ok"
				r.reportErrorMessageToBus(errMsg)
				runtime.CloseExitChannel()
			}
		case _, ok := <-runtime.ProcessExitSignal:
			if !ok {
				log.GetLogger().Infof("stop alive heartbeat check")
				return
			}
		}
	}
}

func (r *fusionRuntime) reportErrorMessageToBus(errMsg string) {
	if r.handlers.ReportErrMsgHandler == nil {
		log.GetLogger().Errorf("r.handlers.ReportErrMsgHandler is nil")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultReportErrMsgContextTimeout)
	err := r.handlers.ReportErrMsgHandler(ctx, r.urnWithVersion, errMsg, runtime.GetProperties())
	cancel()
	if err != nil {
		log.GetLogger().Warnf("failed to report error message, err %s", err.Error())
	}
}

func isRuntimeProcessStatusRunningOrRecover() bool {
	runtimeProcessStatus := runtime.BackgroundProcessLimit.Status.Get()
	return runtimeProcessStatus == tcpmsg.RuntimeProcessStatusRunning ||
		runtimeProcessStatus == tcpmsg.RuntimeProcessStatusRecover
}

func (r *fusionRuntime) heartbeatCheck(heartbeatFailureThreshold int, checkPeriodSeconds int,
	heartbeat types.Heartbeat, language string) {
	consecutiveTimes := 0
	consecutiveSuccessTimes := 0
	successThreshold := 1
	d := time.Duration(checkPeriodSeconds) * time.Second
	if heartbeat.HealthEnable && language == "http" {
		heartbeatFailureThreshold = heartbeat.FailureThreshold
		checkPeriodSeconds = heartbeat.PeriodSeconds
		d = time.Duration(heartbeat.FirstCheckDelay) * time.Second
		successThreshold = heartbeat.SuccessThreshold
	}
	checkPeriodSecondsDuration := time.Duration(checkPeriodSeconds) * time.Second
	t := time.NewTimer(d)
	for {
		select {
		case <-t.C:
			if !isRuntimeProcessStatusRunningOrRecover() {
				t.Reset(checkPeriodSecondsDuration)
				continue
			}
			isHealthy, err := r.Healthz(heartbeat.Path, heartbeat.Timeout)
			if err == nil && !isHealthy {
				log.GetLogger().Errorf("failed to check health of runtime and killed runtime")
				r.killRuntime(&runtime.Error{
					Code:    snerror.RuntimeUnHealthy,
					Message: snerror.ErrText(snerror.RuntimeUnHealthy),
				})

				consecutiveTimes = 0
				consecutiveSuccessTimes = 0
				t.Reset(checkPeriodSecondsDuration)
				continue
			}

			if err == nil && isHealthy {
				if consecutiveSuccessTimes < successThreshold {
					consecutiveSuccessTimes++
					consecutiveTimes = 0
				}
				if consecutiveSuccessTimes == successThreshold {
					consecutiveSuccessTimes = 0
					consecutiveTimes = 0
				}
				t.Reset(checkPeriodSecondsDuration)
				continue
			}

			if err.Error() == snerror.ErrText(snerror.RPCClientEmptyErrorCode) {
				t.Reset(checkPeriodSecondsDuration)
				continue
			}

			if err.Error() != snerror.ErrText(snerror.RPCClientEmptyErrorCode) {
				consecutiveTimes++
				consecutiveSuccessTimes = 0
			}

			if consecutiveTimes >= heartbeatFailureThreshold {
				log.GetLogger().Errorf("failed to check health of runtime and killed runtime, error: %s",
					err.Error())
				r.killRuntime(&runtime.Error{
					Code:    snerror.RuntimeUnHealthy,
					Message: snerror.ErrText(snerror.RuntimeUnHealthy),
				})

				consecutiveTimes = 0
			}

			t.Reset(checkPeriodSecondsDuration)
		case _, ok := <-runtime.ProcessExitSignal:
			if !ok {
				t.Stop()
			}
			log.GetLogger().Infof("stop the task of heartbeat check")
			return
		}
	}
}

func (r *fusionRuntime) monitorDiskUsage() error {
	loadMark := path.Join(r.userCodePath, worker.LoadedFilename)
	userCodeMonitor, err := diskusage.New(r.userCodePath, diskusage.AddProcessHooks(func(event fsnotify.Event) {
		if event.Name == loadMark {
			return
		}
		log.GetLogger().Infof("modified %+v %s", event, r.userCodePath)
		r.killRuntime(&runtime.Error{
			Code:    snerror.DiskUsageExceed,
			Message: snerror.ErrText(snerror.DiskUsageExceed),
		})
	}))
	if err != nil {
		log.GetLogger().Errorf("failed to new user-code monitor: %s", err.Error())
		return err
	}

	runtimeLogMonitor, err := diskusage.New(runtimeLogPath, diskusage.AddProcessHooks(func(event fsnotify.Event) {
		if event.Op&fsnotify.Write > 0 || event.Op&fsnotify.Chmod > 0 {
			return
		}
		if logCompressFileRegex.MatchString(event.Name) || cangjieLogCompressFileRegex.MatchString(event.Name) {
			log.GetLogger().Infof("watched log compress file %s, skip", event.Name)
			return
		}
		log.GetLogger().Infof("modified %+v %s", event, runtimeLogPath)
		r.killRuntime(&runtime.Error{
			Code:    snerror.DiskUsageExceed,
			Message: snerror.ErrText(snerror.DiskUsageExceed),
		})
	}))
	if err != nil {
		userCodeMonitor.Close()
		log.GetLogger().Errorf("failed to new runtime-log monitor: %s", err.Error())
		return err
	}
	if err := r.monitorTmpDiskUsage(); err != nil {
		userCodeMonitor.Close()
		runtimeLogMonitor.Close()
		log.GetLogger().Errorf("failed to monitor tmp disk usage: %s", err.Error())
		return err
	}
	return nil
}

func (r *fusionRuntime) monitorTmpDiskUsage() error {
	userDataMonitor, err := diskusage.New(userDataPath, func(m *diskusage.Monitor) error {
		m.SetCheckInode()
		return nil
	})
	if err != nil {
		log.GetLogger().Errorf("failed to new user-datas monitor: %s", err.Error())
		return err
	}

	systemDataMonitor, err := diskusage.New(systemDataPath)
	if err != nil {
		userDataMonitor.Close()
		log.GetLogger().Errorf("failed to new var-tmp monitor: %s", err.Error())
		return err
	}
	go r.checkTmpDiskUsage(userDataMonitor, systemDataMonitor)
	go r.checkTmpInodes(userDataMonitor)
	return nil
}

func (r *fusionRuntime) checkTmpInodes(monitor *diskusage.Monitor) {
	defer func() {
		monitor.Close()
	}()
	if checkpointStarted == nil {
		log.GetLogger().Warnf("checkPointStarted is nil")
		return
	}
	for {
		select {
		case <-checkpointStarted:
			log.GetLogger().Infof("checkpoint started, stop monitor temp dir inode")
			return
		case _, ok := <-runtime.ProcessExitSignal:
			if !ok {
				return
			}
		default:
		}

		r.doCheckTmpInode(monitor)
		time.Sleep(userDataCheckInterval)
	}
}

func (r *fusionRuntime) doCheckTmpInode(monitor *diskusage.Monitor) {
	var userInodeMaxSize int64
	if r.isUserDatasExpansion {
		userInodeMaxSize = constants.ExpansionUserInodesMaxSize
	} else {
		userInodeMaxSize = constants.DefaultUserInodesMaxSize
	}
	fistMonitorInodes := monitor.FirstInode()
	currentInodes := monitor.CurrentInode()
	monitorSize := currentInodes - fistMonitorInodes
	if monitorSize < userInodeMaxSize {
		return
	}
	log.GetLogger().Infof("user tmp directory inodes exceeded, %d", monitorSize)

	r.killRuntime(&runtime.Error{
		Code:    snerror.INodeUsageExceed,
		Message: snerror.ErrText(snerror.INodeUsageExceed),
	})

	r.cleanTmpDisk()
	return
}

func (r *fusionRuntime) checkTmpDiskUsage(monitor *diskusage.Monitor, systemDataMonitor *diskusage.Monitor) {
	defer func() {
		monitor.Close()
		systemDataMonitor.Close()
	}()

	if checkpointStarted == nil {
		log.GetLogger().Warnf("checkPointStarted is nil")
		return
	}
	for {
		select {
		case <-checkpointStarted:
			log.GetLogger().Infof("checkpoint started, stop monitor disk")
			return
		case _, ok := <-runtime.ProcessExitSignal:
			if !ok {
				return
			}
		default:
		}

		r.doCheckTmpDiskUsage(monitor, systemDataMonitor)
		time.Sleep(userDataCheckInterval)
	}
}

func (r *fusionRuntime) doCheckTmpDiskUsage(monitor *diskusage.Monitor, systemDataMonitor *diskusage.Monitor) {
	var userDataMaxSize int64
	if r.isUserDatasExpansion {
		userDataMaxSize = constants.ExpansionUserDataMaxSize
	} else {
		userDataMaxSize = constants.DefaultUserDataMaxSize
	}
	monitorSize := monitor.Size()
	var systemDataDiff int64
	if diff := systemDataMonitor.Diff(); diff > 0 {
		systemDataDiff = diff
	}
	runtime.RunMetrics.UpdateStorageUsed(monitorSize + systemDataDiff)
	if monitorSize+systemDataDiff <= userDataMaxSize {
		return
	}

	log.GetLogger().Infof("user tmp directory exceed limit, %d, %d", monitorSize, systemDataDiff)

	r.killRuntime(&runtime.Error{
		Code:    snerror.DiskUsageExceed,
		Message: snerror.ErrText(snerror.DiskUsageExceed),
	})

	r.cleanTmpDisk()
}

func (r *fusionRuntime) cleanTmpDisk() {
	dir, err := ioutil.ReadDir(userDataPath)
	if err != nil {
		log.GetLogger().Warnf("failed to read dir: %s, %s", userDataPath, err.Error())
		return
	}
	for _, d := range dir {
		path := path.Join(userDataPath, d.Name())
		if err := os.RemoveAll(path); err != nil {
			log.GetLogger().Warnf("failed to remove path: %s, %s", path, err.Error())
		}
	}
}

func (r *fusionRuntime) getRuntime() runtime.Runtime {
	return r.rt
}

func (r *fusionRuntime) load(ctx context.Context, conf *config.Configuration) error {
	mountUser := conf.FuncMountConfig.FuncMountUser
	if mountUser.UserID == constants.DefaultWorkerUID || mountUser.GroupID == constants.DefaultWorkerGID {
		log.GetLogger().Errorf("mountUser uid or gid cannot be system user")
		return errors.New("mountUser uid or gid cannot be system user")
	}
	start := time.Now()
	rsp := initRuntimeProcess()
	if rsp.Err != "" {
		log.GetLogger().Errorf("error when init runtime processes: %s", rsp.Err)
		if rsp.Code != 0 {
			return snerror.New(rsp.Code, rsp.Err)
		}
		return errors.New(rsp.Err)
	}
	log.GetLogger().Debugf("initRuntimeProcess cost: %v", time.Since(start))

	err := resetDNSConfig(conf.DNSConfig, conf.Language == worker.CustomRuntimeLanguage)
	if err != nil {
		log.GetLogger().Warnf("failed to reset dns config, dnsconfig: %v, error: %s",
			conf.DNSConfig, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to reset dns config, dnsconfig %s, cost: %s", conf.DNSConfig,
		time.Since(start))

	if !isDefaultUIDGID(mountUser.UserID, mountUser.GroupID) || rsp.Code == tcpmsg.RuntimeRestarted {
		log.GetLogger().Infof("runningMode is %v, "+
			"reset the rpc client pool to nil, and then reconnect", conf.RunningMode)
		if conf.RunningMode == constants.RunningModeRestore {
			if !r.prestartRuntime(ctx, conf) {
				// wait for backoff start
				return nil
			}
		} else {
			runtime.Registry.Prestart(conf.Language, &conf.RuntimeConfig)
		}
	}

	rt, err := r.newRuntimeFn(conf, r.handlers)
	// when restore failed, runtime manager will normally restart runtime.so reconnect java runtime by unix
	if recRuntimeExitWhenRestore {
		conf.RunningMode = constants.RunningModeNormal
		log.GetLogger().Infof("hasRuntimeExit newRuntimeFn")
		rt, err = r.newRuntimeFn(conf, r.handlers)
		log.GetLogger().Errorf("Restore failed,  reconnect java runtime by unix")
	}
	if err != nil {
		log.GetLogger().Errorf("failed to new runtime: %s", err.Error())
		return err
	}
	r.rt = rt

	if err := r.monitorDiskUsage(); err != nil {
		log.GetLogger().Errorf("failed to monitor disk usage: %s", err.Error())
		return err
	}
	if commonUtils.IsCAEFunc(utils.GetBusinessType()) && len(conf.RuntimeConfig.CustomMetricsReport.Handler) != 0 {
		log.GetLogger().Infof("start obtains custom report metrics")
		go r.obtainsCustomReportMetrics(conf.RuntimeConfig.CustomMetricsReport)
	}
	if utils.IsHeartbeatEnable(conf.RuntimeConfig.Language) && commonUtils.IsCAEFunc(utils.GetBusinessType()) &&
		utils.IsHealthCheckEnable(conf.RuntimeConfig.StartupHeartbeat) {
		log.GetLogger().Infof("do startup heartbeat check for CAE")
		go r.startupHeartbeatCheck(conf.RuntimeConfig.StartupHeartbeat)
	}
	if utils.IsHeartbeatEnable(conf.RuntimeConfig.Language) && (len(conf.RuntimeConfig.Heartbeat.Handler) != 0 ||
		conf.RuntimeConfig.Heartbeat.HealthEnable) {
		if commonUtils.IsCAEFunc(utils.GetBusinessType()) && utils.IsHealthCheckEnable(conf.RuntimeConfig.Heartbeat) {
			log.GetLogger().Info("cae alive health check has configured, ignore default health check")
			return nil
		}
		if !utils.IsM2SMode(conf.RunningMode, conf.RuntimeConfig.Heartbeat.Handler) {
			log.GetLogger().Infof("start heartbeat check goroutine for language %s", conf.RuntimeConfig.Language)
			go r.heartbeatCheck(conf.HeartbeatConfig.FailureThreshold, conf.HeartbeatConfig.CheckPeriodSeconds,
				conf.RuntimeConfig.Heartbeat, conf.RuntimeConfig.Language)
		}
	}
	return nil
}

func resetDNSConfig(dnsConfig types.DNSConfig, isCustomContainer bool) error {
	if len(dnsConfig.DNS) == 0 && len(dnsConfig.DNSSearch) == 0 && len(dnsConfig.DNSOptions) == 0 {
		return nil
	}
	body, err := json.Marshal(dnsConfig)
	if err != nil {
		return err
	}
	if runtime.RuntimeManagerClient != nil {
		resp := runtime.RuntimeManagerClient.SendMessage(tcpmsg.BuildMessage(tcpmsg.ResetDNSConfig, body))
		if resp.Err != "" {
			return errors.New(resp.Err)
		}
		return nil
	}
	log.GetLogger().Warnf("reset dns config while runtimeManagerClient is nil")
	if isCustomContainer {
		err = commonUtils.ResetDNSConfig(dnsConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *fusionRuntime) obtainsCustomReportMetrics(customMetricsReport types.CustomMetricsReport) {
	customMetricsCollectTime := defaultCustomMetricsCollectTime
	customMetricsReportTimeout := defaultCustomMetricsReportTimeout
	if customMetricsReport.CollectTime > 0 {
		customMetricsCollectTime = time.Duration(customMetricsReport.CollectTime) * time.Second
	}
	if customMetricsReport.Timeout > 0 {
		customMetricsReportTimeout = customMetricsReport.Timeout
	}

	t := time.NewTimer(customMetricsCollectTime)
	for {
		select {
		case <-t.C:
			if !isRuntimeProcessStatusRunningOrRecover() {
				t.Reset(customMetricsCollectTime)
				continue
			}
			result, err := r.ReportCustomMetrics(customMetricsReportTimeout)

			if err != nil {
				log.GetLogger().Errorf("failed to get custom report metrics, error: %s", err.Error())
				t.Reset(customMetricsCollectTime)
				continue
			}

			log.GetLogger().Debugf("obtainsCustomReportMetrics, result: %s %d",
				string(result), len(string(result)))
			if len(string(result)) != 0 {
				runtime.RunMetrics.UpdateCustomReportMetrics(tcpbustool.CustomReportMetric{
					Val:              string(result),
					CustomReportTime: time.Now().Unix(),
				})
			}
			t.Reset(customMetricsCollectTime)
		case _, ok := <-runtime.ProcessExitSignal:
			t.Stop()
			if !ok {
				log.GetLogger().Infof("obtainsCustomReportMetrics: processExitSignal return not ok")
				return
			}
			log.GetLogger().Infof("stop the task of obtain custom metrics")
			return
		}
	}
}

func (r *fusionRuntime) processCSharpNewRuntimeError(err error) error {
	if r.handlers.GetExceptionHandler != nil {
		if runtimeException := r.handlers.GetExceptionHandler(); runtimeException != nil {
			log.GetLogger().Errorf("CSharp runtime process got error: %v", runtimeException)
			if runtimeException.Code == snerror.RuntimeProcessExit {
				cache.GetMessageQueue().Block()
				return nil
			}
		}
	}
	return err
}

// GetRestoreDead get restore dead signal
func GetRestoreDead() bool {
	return recRuntimeExitWhenRestore
}

// HotLoad implements runtime.Runtime
func (r *fusionRuntime) HotLoad(ctx context.Context, conf *config.Configuration) error {
	r.mux.Lock()
	defer r.mux.Unlock()

	r.subscribeOnce.Do(func() {
		go r.subscribeWorkerErr()
	})

	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	if err := r.load(ctx, conf); err != nil {
		if strings.Contains(conf.Language, utils.CSharp) {
			return r.processCSharpNewRuntimeError(err)
		}
		return err
	}
	runtime.NewRuntimeState(utils.IsDirectFunc(conf.Language))
	rt := r.rt

	if err := rt.HotLoad(ctx, conf); err != nil {
		log.GetLogger().Warnf("failed to hotload: %s", err.Error())
		runtime.SaveExtensionFailedLog(conf, err)

		// restore/checkpoint mode does not support restarting.
		if conf.RunningMode == constants.RunningModeCheckpoint || conf.RunningMode == constants.RunningModeRestore ||
			recRuntimeExitWhenRestore == true {
			return err
		}

		// If hotload fails with user error in generalization stage, we kill the runtime and wait for an invoke request
		// to redo the hotload.
		var (
			code int32
			msg  string
		)
		if snErr, ok := err.(snerror.SNError); ok && snerror.IsUserError(snErr) {
			code = int32(snErr.Code())
			msg = snErr.Error()
		} else {
			return err
		}
		cache.GetMessageQueue().Error(err)
		cache.GetMessageQueue().Block()
		r.killRuntime(&runtime.Error{
			Code:    code,
			Message: msg,
		})
		if code == snerror.RuntimeInitTimeoutCode {
			cache.SetLoadErr(snerror.New(snerror.InnerRuntimeInitTimeoutCode, msg))
		}
	}
	return nil
}

func (r *fusionRuntime) waitIfKeepsCrashing() {
	if r.crashLimiter.add() {
		time.Sleep(keepsCrashingGracePeriod)
	}
}

func (r *fusionRuntime) reconnect(ctx context.Context, conf *config.Configuration, runtimeErr *runtime.Error) {
	_, prestartable := runtime.Registry.GetCreator(conf.Language).(runtime.PrestartableCreator)
	if prestartable && !strings.Contains(conf.Language, utils.CSharp) {
		resp := runtime.RuntimeManagerClient.SendMessage(tcpmsg.BuildMessage(tcpmsg.RestartRuntimeProcess, nil))
		if resp.Err != "" {
			r.closeExit(errors.New(resp.Err))
			runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusReconnectFailed)
			r.eh.clearException()
			return
		}
		if !r.prestartRuntime(ctx, conf) {
			runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusReconnectFailed)
			r.eh.clearException()
			return
		}
	}
	r.eh.clearException()
	var concurrency uint32 = 0
	var directRequest bool
	if (runtime.PodApplier != constants.WorkerManagerApplier && !utils.IsDirectFunc(conf.Language)) ||
		(runtime.PodApplier == constants.WorkerManagerApplier && runtime.GetIdleMode()) {
		// Wait until the upcoming request arrives in the message queue. After then, we start hot-loading the runtime
		// as if we are invoking it for the first time.
		concurrency, directRequest = cache.GetMessageQueue().WaitForBacklog()
		timeout := time.Duration(conf.RuntimeConfig.Timeout+
			conf.RuntimeConfig.Initializer.InitializerTimeout) * time.Second
		cleanTmpDiskWithReconnect(runtimeErr)
		summarySpan := runtime.NewSummarySpan(constants.InvokeStage, "", conf, timeout)
		runtime.ConResource.StartOccupation(concurrency, summarySpan.StartTime(), false, directRequest)
		r.reconnectInternal(ctx, conf, prestartable)
		concurrentStartTime := runtime.ConResource.StopOccupation(concurrency, false, directRequest)
		summarySpan.Finish(&runtime.Error{})
		runtime.ConResource.AppendTimeWindows(concurrentStartTime, summarySpan.EndTime(), constants.InvokeStage)
	} else {
		cleanTmpDiskWithReconnect(runtimeErr)
		r.reconnectInternal(ctx, conf, prestartable)
	}
}

func cleanTmpDiskWithReconnect(runtimeErr *runtime.Error) {
	if runtimeErr != nil {
		if runtimeErr.Code == snerror.INodeUsageExceed {
			log.GetLogger().Infof("inode exceed, start to clear tmp dir")
			utils.CleanTmpDisk()
			log.GetLogger().Infof("inode exceed, finish to clear tmp dir")
		}
	}
}

func (r *fusionRuntime) reconnectInternal(ctx context.Context, conf *config.Configuration, prestartable bool) {
	r.waitIfKeepsCrashing()
	if !prestartable || strings.Contains(conf.Language, utils.CSharp) {
		resp := runtime.RuntimeManagerClient.SendMessage(tcpmsg.BuildMessage(tcpmsg.RestartRuntimeProcess, nil))
		if resp.Err != "" {
			runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusReconnectFailed)
			r.closeExit(errors.New(resp.Err))
			return
		}
	}
	rt, err := r.newRuntimeFn(conf, r.handlers)
	if err != nil {
		log.GetLogger().Errorf("failed to new runtime: %s", err.Error())
		if strings.Contains(conf.Language, utils.CSharp) && r.handlers.GetExceptionHandler != nil {
			if runtimeException := r.handlers.GetExceptionHandler(); runtimeException != nil {
				err = snerror.New(int(runtimeException.Code), runtimeException.Message)
			}
		}
		r.handleReconnectError(err)
		runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusReconnectFailed)
		return
	}
	log.GetLogger().Debugf("start hotload")
	if err := rt.HotLoad(ctx, conf); err != nil {
		log.GetLogger().Warnf("hotLoadFail | %s", commonUtils.ExtractErrMsg(err.Error()))
		r.handleReconnectError(err)
		runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusReconnectFailed)
		return
	}
	r.rt = rt
	log.GetLogger().Infof("hotload finished")
	cache.GetMessageQueue().UnblockAndFlush(nil)
	runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusRunning)
}

func (r *fusionRuntime) HandlerRestoreDead() {
	recRuntimeExitWhenRestore = true
	connect.RecRuntimeExitMsg()
	log.GetLogger().Infof("RestoreDead, SendMessage restart")
	resp := runtime.RuntimeManagerClient.SendMessage(tcpmsg.BuildMessage(tcpmsg.RestartRuntimeProcess, nil))
	if resp.Err != "" {
		log.GetLogger().Infof("resp.Err != nil. %s", resp.Err)
		r.closeExit(errors.New(resp.Err))
		return
	}
}

// Reconnect -
func (r *fusionRuntime) Reconnect(conf *config.Configuration) {
	if r == nil || conf == nil {
		return
	}
	if conf.RunningMode == constants.RunningModeCheckpoint {
		// checkpoint will kill runtime process, no need to reconnect
		log.GetLogger().Infof("runningMode %s skip reconnect", conf.RunningMode)
		return
	}
	if r.cancel == nil {
		return
	}
	defer func() {
		runtime.GetDirectRuntimeState().Unblock()
	}()
	log.GetLogger().Infof("reconnect")
	atomic.StoreInt32(&r.isRuntimeProcessDead, 1)
	r.cancel()

	conf.ReconnectTimes = connect.RestartReconnectTimes
	runtime.BackgroundProcessLimit.Status.Set(tcpmsg.RuntimeProcessStatusReconnect)
	cache.GetMessageQueue().Block()
	if r.eh.checkException() == nil {
		resp := runtime.RuntimeManagerClient.SendMessage(tcpmsg.BuildMessage(tcpmsg.GotRuntimeException, nil))
		var runtimeErr *runtime.Error
		if resp.Code == 0 {
			runtimeErr = &runtime.Error{
				Code:    snerror.WorkerInternalErrorCode,
				Message: "failed to get exception from runtime-manager",
			}
		} else {
			runtimeErr = &runtime.Error{
				Code:    int32(resp.Code),
				Message: snerror.ErrText(resp.Code),
			}
			if strings.Contains(conf.Language, utils.CSharp) &&
				resp.Code == snerror.RuntimeProcessExit && resp.Err != "" {
				runtimeErr.Message = resp.Err
			}
		}

		if resp.Code == snerror.RuntimeDirForbidden {
			status.SetRuntimeDirInvalidStatus()
		}
		r.eh.setException(runtimeErr)
		cache.GetMessageQueue().Error(snerror.New(int(runtimeErr.Code), runtimeErr.Message))
	}
	r.mux.Lock()

	atomic.StoreInt32(&r.isRuntimeProcessDead, 0)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	runtimeErr := r.eh.checkException()
	r.reconnect(ctx, conf, runtimeErr)

	r.mux.Unlock()
}

func (r *fusionRuntime) closeExit(err error) {
	log.GetLogger().Warnf("failed to %s", err.Error())
	cache.GetMessageQueue().UnblockAndFlush(err)
	runtime.CloseExitChannel()
}

func (r *fusionRuntime) prestartRuntime(ctx context.Context, conf *config.Configuration) bool {
	timer := time.NewTimer(reconnectRuntimeTimeout)
	defer timer.Stop()
	prestartChannel := make(chan struct{})
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			err := runtime.Registry.Prestart(conf.Language, &conf.RuntimeConfig)
			if err == nil {
				close(prestartChannel)
				return
			}
			log.GetLogger().Warnf("failed to prestart runtime %s", err.Error())
		}
	}()
	select {
	case <-timer.C:
		r.closeExit(fmt.Errorf("failed to prestart runtime after: %v", reconnectRuntimeTimeout))
		return false
	case <-prestartChannel:
		log.GetLogger().Infof("prestartRuntime | reconnect")
		return true
	case <-ctx.Done():
		log.GetLogger().Infof("context canceled during prestart")
		return false
	}
}

func (r *fusionRuntime) handleReconnectError(err error) {
	var (
		code int32
		msg  string
	)
	if snErr, ok := err.(snerror.SNError); ok && snerror.IsUserError(snErr) {
		code = int32(snErr.Code())
		msg = snErr.Error()
	} else {
		code = snerror.WorkerInternalErrorCode
		msg = err.Error()
	}

	cache.GetMessageQueue().UnblockAndFlush(err)
	cache.GetMessageQueue().Block()

	r.killRuntime(&runtime.Error{
		Code:    code,
		Message: msg,
	})
}

// Healthz implements runtime.Runtime
func (r *fusionRuntime) Healthz(path string, timeout int) (bool, error) {
	return r.getRuntime().Healthz(path, timeout)
}

// StartupHealthz return rpc server startup health
func (r *fusionRuntime) StartupHealthz(path string, timeout int) (bool, error) {
	return r.getRuntime().StartupHealthz(path, timeout)
}

// Healthz implements runtime.Runtime
func (r *fusionRuntime) ReportCustomMetrics(timeout int) ([]byte, error) {
	return r.getRuntime().ReportCustomMetrics(timeout)
}

// SendProcessEvent implements runtime.Runtime
func (r *fusionRuntime) SendProcessEvent(ic invokectx.InvokeCtx, message runtime.InvokeRequest,
	conf *config.Configuration) {
	cnt := atomic.AddInt64(&r.eventCnt, 1)
	if cnt == 1 {
		r.mux.Lock()
	}
	if runtime.ExtensionMgr.GetExtensionEnable() {
		runtime.ExtensionMgr.SendRequest(ic.ID, message.TraceID, time.Duration(conf.Timeout)*time.Second)
	}

	if r == nil || r.getRuntime() == nil {
		log.GetLogger().Errorf("runtime is nil")
		return
	}
	r.getRuntime().SendProcessEvent(ic, message, conf)
}

func (r *fusionRuntime) decorateRecvProcessEventHandler(hdr runtime.RecvProcessEventHandler,
	conf *config.Configuration, judger runtime.RequestTimeoutHandler) runtime.RecvProcessEventHandler {
	return func(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
		if runtime.ExtensionMgr.GetExtensionEnable() {
			runtime.ExtensionMgr.WaitResp(ic.ID)
		}
		cnt := atomic.AddInt64(&r.eventCnt, -1)
		if cnt == 0 {
			r.mux.Unlock()
		}
		if resp.Error != nil {
			if judger(ic, conf, resp.Error.Code) {
				r.killRuntime(&runtime.Error{
					Code:    snerror.RuntimeInvokeTimeoutCode,
					Message: snerror.ErrText(snerror.RuntimeInvokeTimeoutCode),
				})
				log.GetLogger().Infof("Python %s in PodName %s timeout, kill runtime.", resp.Summary.FuncDigest,
					resp.Summary.PodName)
			}
		}
		hdr(ic, resp)
	}
}

// ProcessCancelEvent implements runtime.Runtime
func (r *fusionRuntime) ProcessCancelEvent(message *runtime.CancelRequest) *runtime.CancelResponse {
	r.killRuntime(&runtime.Error{
		Code:    snerror.RequestCanceled,
		Message: snerror.ErrText(snerror.RequestCanceled),
	})
	return &runtime.CancelResponse{
		InvokeID: message.InvokeID,
	}
}

func (r *fusionRuntime) killRuntime(runtimeErr *runtime.Error) {
	if runtimeErr == nil {
		log.GetLogger().Warnf("nil runtime err")
		return
	}

	if atomic.LoadInt32(&r.isRuntimeProcessDead) != 0 {
		log.GetLogger().Infof("runtime is dead, ignore kill code: %d", runtimeErr.Code)
		return
	}

	cache.GetMessageQueue().Block()
	if runtimeErr.Code == snerror.INodeUsageExceed {
		cache.GetMessageQueue().SetNoBlockType()
		cache.GetMessageQueue().Error(errors.New(runtimeErr.Message))
	}
	log.GetLogger().Infof("killRuntime | %d", runtimeErr.Code)
	r.eh.setException(runtimeErr)
	msg := tcpmsg.BuildMessage(tcpmsg.KillRuntimeProcess, nil)
	err := runtime.RuntimeManagerClient.SendMessageWithNoResp(msg)
	if err != nil {
		log.GetLogger().Warnf("failed to kill runtime: %s", err.Error())
	}
}

// PreStop pre stop user function
func (r *fusionRuntime) PreStop() error {
	rt := r.rt
	err := rt.PreStop()
	preStopped = true
	return err
}

// RequestUpdateAgency update agency user function
func (r *fusionRuntime) RequestUpdateAgency() error {
	if r == nil || r.rt == nil {
		errMsg := "runtime is nil"
		log.GetLogger().Errorf(errMsg)
		return errors.New(errMsg)
	}
	return r.rt.RequestUpdateAgency()
}

// GetException get runtime exception error info
func (r *fusionRuntime) GetException(shouldWait bool) *runtime.Error {
	err := r.eh.checkException()
	if shouldWait {
		err = r.eh.getExceptionSlow()
	}
	return err
}

// subscribeWorkerErr -
func (r *fusionRuntime) subscribeWorkerErr() {
	for {
		select {
		case errMsg, ok := <-runtime.GetWorkerErrChan():
			if !ok {
				log.GetLogger().Errorf("workerErr channel is closed")
				return
			}
			log.GetLogger().Warnf("receive WorkerErr")
			r.killRuntime(&errMsg)
			runtime.RuntimeManagerClient.SendMessageWithNoResp(tcpmsg.BuildMessage(tcpmsg.StopRuntimeManager, nil))
			runtime.ProcessExitError = snerror.New(snerror.WorkerPingVpcGatewayError, snerror.ErrText(snerror.
				WorkerPingVpcGatewayError))
			close(runtime.ProcessExitSignal)
		case <-runtime.ProcessExitSignal:
			log.GetLogger().Infof("stop subscribeWorkerErr")
			return
		}
	}
}
