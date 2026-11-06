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

// Package functionlog gets function services from URNs
package functionlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger"
	"functioncore/pkg/common/logger/async"
	loggerConfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/utils"
)

const (
	innerLogSeparator = ","
	funcLogSeparator  = ";"

	// Max length of function log in "tail" mode
	maxTailLogSizeKB = 2 * 1024
	// Max length of normal instance initializer log
	maxInitLogSizeMB = 10 * 1024 * 1024
	// InitializerInvokeID -
	InitializerInvokeID = "initializer"
	// LoadInvokeID -
	LoadInvokeID = "load"
	// RestoreHookInvokeID -
	RestoreHookInvokeID = "restore"
	// PreStopInvokeID -
	PreStopInvokeID = "preStop"

	// LogTimeFormat -
	LogTimeFormat = "2006-01-02T15:04:05Z"
	// FuncLogTimeFormat -
	FuncLogTimeFormat = "2006-01-02T15:04:05.000Z"
	// NanoLogLayout  -
	NanoLogLayout = "2006-01-02T15:04:05.999999999Z07:00"
	// DateTimeLayoutWithZeroPaddedNanos  -
	DateTimeLayoutWithZeroPaddedNanos = "2006-01-02T15:04:05.000000000Z07:00"

	// RuntimeLogFormat  -
	RuntimeLogFormat = "2006-01-02 15:04:05.999999999Z07:00"

	userLogTimeFormat = "2006-01-02 15:04:05.999"

	logTankServiceLength = 2
	originalLocation     = 0
	alternativeLocation  = 1
	userFuncLogChanCap   = 5000
	funcInfoMapCap       = 10
	userLogKey           = "log"
	fileMode             = 0640
	cacheLimit           = 10 * 1 << 20 // 10 mb
	uint64Width          = 8

	dateIndex    = 0
	timeIndex    = 1
	traceIDIndex = 2
	minLength    = 3
	traceIDLen   = 36
)

var (
	functionLogger   *FunctionLogger
	createLoggerErr  error
	createLoggerOnce sync.Once
)

// FunctionLogger process function's log
type FunctionLogger struct {
	logger          *zap.Logger
	fieldsPool      *sync.Pool
	logPool         *sync.Pool
	logLevel        string
	logAbsPath      string
	logNameTemplate string
}

// MapMarshal add zapLogger fields with map object
type MapMarshal struct {
	data map[string]string
}

// MarshalLogObject MapMarshal marshal
func (m *MapMarshal) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for k, v := range m.data {
		enc.AddString(k, v)
	}
	return nil
}

// NewFunctionLogger -
func NewFunctionLogger(logger *zap.Logger, logLevel string, logPath string) *FunctionLogger {
	return &FunctionLogger{
		logger: logger,
		fieldsPool: &sync.Pool{New: func() interface{} {
			return newLogFields()
		}},
		logPool: &sync.Pool{New: func() interface{} {
			return NewFunctionLog()
		}},
		logLevel:        logLevel,
		logAbsPath:      logPath,
		logNameTemplate: filepath.Base(logPath),
	}
}

type logTankService struct {
	originalGroupID     string
	originalStreamID    string
	alternativeGroupID  string
	alternativeStreamID string
}

// GetFunctionLogger -
func GetFunctionLogger(cfg *config.Configuration) (*FunctionLogger, error) {
	createLoggerOnce.Do(func() {
		functionLogger, createLoggerErr = newLTSFunctionLogger(cfg)
		if createLoggerErr != nil {
			return
		}
		if functionLogger == nil {
			createLoggerErr = errors.New("failed to new FunctionLogger")
			return
		}
	})
	return functionLogger, createLoggerErr
}

// SetLogLevel -
func (f *FunctionLogger) SetLogLevel(level string) {
	log.GetLogger().Debugf("set function log level: %s", level)
	f.logLevel = level
}

// NewLogRecorder creates a LogRecorder from InvokeCtx
func (f *FunctionLogger) NewLogRecorder(ic invokectx.InvokeCtx, traceID, stage string, opts ...LogRecorderOption,
) *LogRecorder {
	logRecorder := getLogRecorder(ic, lrRange.Mark)

	logRecorder.f = f
	binary.BigEndian.PutUint64(logRecorder.invokeID[:], ic.ID)
	logRecorder.traceID = traceID
	logRecorder.logLevel = f.logLevel
	logRecorder.stage = stage
	logRecorder.separatedWithLineBreak = true
	for _, opt := range opts {
		opt(logRecorder)
	}

	return logRecorder
}

// GetLogRecorder gets an existing LogRecorder from InvokeCtx
func (f *FunctionLogger) GetLogRecorder(ic invokectx.InvokeCtx) *LogRecorder {
	return getLogRecorder(ic, lrRange.Mark)
}

// GetLogRecorderFromInvokeID gets an LogRecorder from invokeID. User can get InvokeID from
// *LogRecorder.InvokeID(). The bool result reports whether the LogRecorder was present.
func (f *FunctionLogger) GetLogRecorderFromInvokeID(b []byte) (*LogRecorder, bool) {
	if len(b) < uint64Width {
		return nil, false
	}
	invokeID := binary.BigEndian.Uint64(b)
	ic, ok := invokectx.DefaultMetrics.GetInvokeCtx(invokeID)
	if !ok {
		return nil, false
	}
	return getLogRecorder(ic, lrRange.Mark), true
}

// WriteStdLog write std logs shown on all requests
func (f *FunctionLogger) WriteStdLog(logString, timeStamp string, isReserved bool, nanoTimestamp string) {
	if lrRange.Len() == 1 {
		lines := strings.Split(logString, "\n")
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			curLog := line + "\n"
			logData := f.AcquireLog()
			logData.Time = timeStamp
			logData.NanoTime = nanoTimestamp
			logData.IsStdLog = true
			logData.Level = worker.FuncLogLevelInfo
			logData.Message.WriteString(curLog)
			traceID, parseFlag := ParseUserLog(curLog)
			if parseFlag {
				f.RangeLoggerWithTraceID(logData, traceID)
			} else {
				rangeLogRecorder(func(ic invokectx.InvokeCtx, logRecorder *LogRecorder) bool {
					if !isReserved && (logRecorder.Stage() != constants.InvokeStage &&
						logRecorder.Stage() != constants.ColdStartStage) {
						logRecorder.WriteLimit(logData)
						return false
					}
					logRecorder.RealTimeWrite(logData)
					return false
				})
			}
		}
	} else {
		f.concurrentPrintWithTraceID(logString, timeStamp, nanoTimestamp)
	}
}

func (f *FunctionLogger) concurrentPrintWithTraceID(logStr string, timeStamp, nanoTimestamp string) {
	lines := strings.Split(logStr, "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		curLog := line + "\n"
		l := f.AcquireLog()
		l.Time = timeStamp
		l.NanoTime = nanoTimestamp
		l.IsStdLog = true
		l.Level = worker.FuncLogLevelInfo
		l.Message.WriteString(curLog)
		traceID, parseFlag := ParseUserLog(curLog)
		if parseFlag {
			f.RangeLoggerWithTraceID(l, traceID)
		} else {
			f.WriteDefaultLog(l)
		}
	}
}

// ParseUserLog Standard Log Format: $(timestamp) $(traceID) $(content)
func ParseUserLog(log string) (string, bool) {
	userLog := strings.Split(log, " ")
	if len(userLog) < minLength {
		return "", false
	}

	timestamp := userLog[dateIndex] + " " + userLog[timeIndex]
	_, err := time.Parse(userLogTimeFormat, timestamp)
	if err != nil {
		return "", false
	}

	traceID := userLog[traceIDIndex]
	if len(traceID) != traceIDLen {
		return "", false
	}
	return traceID, true
}

// AcquireLog retrieves a FunctionLog struct from the cached pool
func (f *FunctionLogger) AcquireLog() *FunctionLog {
	functionLogIf := f.logPool.Get()
	functionLog, ok := functionLogIf.(*FunctionLog)
	if !ok {
		log.GetLogger().Errorf("failed to assert FunctionLog")
		return nil
	}
	functionLog.Reset()
	return functionLog
}

// ReleaseLog puts a FunctionLog struct to the cached pool
func (f *FunctionLogger) ReleaseLog(functionLog *FunctionLog) {
	f.logPool.Put(functionLog)
}

// WriteDefaultLogWithTraceID -
func (f *FunctionLogger) WriteDefaultLogWithTraceID(functionLog *FunctionLog, traceID string) {
	f.write(&internalFunctionLog{
		FunctionLog:  functionLog,
		FunctionName: utils.LocalFuncURN.Name,
		TraceID:      traceID,
		Stage:        constants.LoadStage,
		Index:        constants.DefaultFuncLogIndex,
	})
}

// WriteDefaultLog -
func (f *FunctionLogger) WriteDefaultLog(functionLog *FunctionLog) {
	f.write(&internalFunctionLog{
		FunctionLog:  functionLog,
		FunctionName: utils.LocalFuncURN.Name,
		TraceID:      "default",
		Stage:        constants.InitializeStage,
		Index:        constants.DefaultFuncLogIndex,
	})
}

// RefreshFileModTime timer of refreshing file modified time in case of the user log file being deleted
func (f *FunctionLogger) RefreshFileModTime(stopCh chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Warnf("empty stop channel")
		return
	}
	ticker := time.NewTicker(timeModInterval)
	log.GetLogger().Infof("start to regularly modify the file modification time")
	for {
		select {
		case <-ticker.C:
			f.refreshWithRetry()
		case <-stopCh:
			log.GetLogger().Warnf("received the runtime exit signal and stopped refreshing the file " +
				"modification time")
			ticker.Stop()
			return
		}
	}
}

func (f *FunctionLogger) refreshWithRetry() {
	newTime := time.Now()
	for i := 0; i < timeModRetry; i++ {
		path := logger.GetLogName(f.logNameTemplate)
		log.GetLogger().Infof("logNameTemplate: %s, current keep alive log path: %s ", f.logNameTemplate, path)
		if path == "" {
			path = f.logAbsPath
		}
		if err := os.Chtimes(path, newTime, newTime); err != nil {
			log.GetLogger().Warnf("failed to change the modification time of the user log file: %s",
				err.Error())
			continue
		}
		log.GetLogger().Infof("succeeded to change the modification time of the user log file %s", path)
		break
	}
}

func newZapLogger(fullPath, messageKey string) (*zap.Logger, error) {
	coreInfo, err := loggerConfig.GetCoreInfoFromEnv()
	if err != nil {
		log.GetLogger().Errorf("failed to get core info: %s", err.Error())
		return nil, err
	}
	coreInfo.FilePath = fullPath
	if messageKey == userLogKey {
		coreInfo.IsUserLog = true
	}
	sink, err := logger.CreateSink(coreInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to create sink: %s", err.Error())
		return nil, err
	}

	ws := async.NewAsyncWriteSyncer(sink, async.WithCachedLimit(cacheLimit))

	encoderConfig := zapcore.EncoderConfig{
		LevelKey:     "Level",
		NameKey:      "Logger",
		MessageKey:   messageKey,
		CallerKey:    "CallerKey",
		LineEnding:   zapcore.DefaultLineEnding,
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	rollingFileEncoder := zapcore.NewJSONEncoder(encoderConfig)

	priority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.DebugLevel
	})

	return zap.New(zapcore.NewCore(rollingFileEncoder, ws, priority)), nil
}

func newLTSFunctionLogger(cfg *config.Configuration) (*FunctionLogger, error) {
	path := os.Getenv("function-log")
	if path == "" {
		path = cfg.StartArgs.LogDir
	}
	if err := commonUtils.ValidateFilePath(path); err != nil {
		log.GetLogger().Errorf("failed to valid log path, err: %s", err.Error())
		return nil, err
	}

	path = filepath.Join(path, utils.LocalFuncURN.TenantID)
	c := &urnutils.ComplexFuncName{}
	c.ParseFrom(utils.LocalFuncURN.Name)
	l := &logTankService{}
	l.extractLogTankService(cfg.RuntimeConfig.LogTankService.GroupID, cfg.RuntimeConfig.LogTankService.StreamID)

	if err := createFunctionInfoFile(c); err != nil {
		log.GetLogger().Errorf("failed to create function info file, %s", err.Error())
		return nil, err
	}

	// functionName@serviceID@version@podName@time#logGroupId#logStreamId
	functionLogName := fmt.Sprintf("%s@%s@%s@%s@%s#%s#%s#%s", c.FuncName, c.ServiceID,
		utils.LocalFuncURN.Version, os.Getenv("PodName"), time.Now().Format("20060102150405"),
		l.originalGroupID, l.originalStreamID, cfg.UserLogTag)

	fullPath, err := getAbsFilePath(path, functionLogName)
	if err != nil {
		return nil, err
	}

	rollingLogger, err := newZapLogger(fullPath, userLogKey)
	if err != nil {
		log.GetLogger().Errorf("failed to new zapLogger, %s", err.Error())
		return nil, err
	}

	rollingLogger = createLTSLogger(rollingLogger, c, l, cfg.RuntimeConfig.LtsCustomTag)

	return NewFunctionLogger(rollingLogger, cfg.RuntimeConfig.FuncLogLevel, fullPath), nil
}

func createLTSLogger(rollingLogger *zap.Logger, c *urnutils.ComplexFuncName, l *logTankService,
	customTagMap map[string]string) *zap.Logger {
	rollingLogger = rollingLogger.With(zapcore.Field{
		Key:    "projectId",
		Type:   zapcore.StringType,
		String: utils.LocalFuncURN.TenantID,
	}, zapcore.Field{
		Key:    "podName",
		Type:   zapcore.StringType,
		String: os.Getenv("PodName"),
	}, zapcore.Field{
		Key:    "package",
		Type:   zapcore.StringType,
		String: c.ServiceID,
	}, zapcore.Field{
		Key:    "function",
		Type:   zapcore.StringType,
		String: c.FuncName,
	}, zapcore.Field{
		Key:    "version",
		Type:   zapcore.StringType,
		String: utils.LocalFuncURN.Version,
	}, zapcore.Field{
		Key:    "stream",
		Type:   zapcore.StringType,
		String: "stdout",
	}, zapcore.Field{
		Key:    "instanceId",
		Type:   zapcore.StringType,
		String: os.Getenv("POD_ID"),
	}, zapcore.Field{
		Key:    "newLogGroupId",
		Type:   zapcore.StringType,
		String: l.alternativeGroupID,
	}, zapcore.Field{
		Key:    "newLogStreamId",
		Type:   zapcore.StringType,
		String: l.alternativeStreamID,
	})
	if len(customTagMap) != 0 {
		log.GetLogger().Infof("append lts custom tag to ltsLogger, lens: %d", len(customTagMap))
		rollingLogger = rollingLogger.With(zap.Object("lts_custom_tag", &MapMarshal{data: customTagMap}))
	}
	return rollingLogger
}

func (l *logTankService) extractLogTankService(groupID, streamID string) {
	if strings.Contains(groupID, funcLogSeparator) && strings.Contains(streamID, funcLogSeparator) {
		splitGroupID := strings.Split(groupID, funcLogSeparator)
		splitStreamID := strings.Split(streamID, funcLogSeparator)
		if len(splitGroupID) != logTankServiceLength || len(splitStreamID) != logTankServiceLength {
			l.originalGroupID = groupID
			l.originalStreamID = streamID
			return
		}
		l.originalGroupID = splitGroupID[originalLocation]
		l.alternativeGroupID = splitGroupID[alternativeLocation]
		l.originalStreamID = splitStreamID[originalLocation]
		l.alternativeStreamID = splitStreamID[alternativeLocation]
		return
	}
	l.originalGroupID = groupID
	l.originalStreamID = streamID
}

func createFunctionInfoFile(complexFuncName *urnutils.ComplexFuncName) error {
	coreInfo, err := loggerConfig.GetCoreInfoFromEnv()
	if err != nil {
		return err
	}
	// urn:fss:projectID:function:package:function-name:version.info
	infoFileName := strings.Join([]string{"urn:fss", utils.LocalFuncURN.TenantID,
		"function", complexFuncName.ServiceID, complexFuncName.FuncName,
		utils.LocalFuncURN.Version + ".info"}, ":")
	f, err := os.OpenFile(filepath.Join(coreInfo.FilePath, infoFileName), os.O_RDONLY|os.O_CREATE, fileMode)
	if err != nil {
		return err
	}
	defer f.Close()

	return nil
}

func getAbsFilePath(path, fileName string) (string, error) {
	logPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	fullPath := filepath.Join(logPath, fileName+".log")
	return fullPath, nil
}
