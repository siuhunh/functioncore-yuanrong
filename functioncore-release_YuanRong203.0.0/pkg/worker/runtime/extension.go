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

package runtime

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/urnutils"
	commUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/telemetry"
	"functioncore/pkg/worker/utils"
)

const (
	envExtensionAPIADDR    = "EXTENSION_API_ADDR"
	extensionPath          = "/opt/function/code/extension"
	maxExtensionNum        = 10
	extensionSuffix        = ".sh"
	extensionBashName      = "extension"
	defaultHTTPHeaderLimit = 1048576
	registerURL            = "/extension/register"
	extensionName          = "FunctionGraph-Extension-Name"
	extensionAddress       = "FunctionGraph-Extension-Address"
	extensionInvokePath    = "/extension/invoke"
	extensionHeader        = "FunctionGraph-Extension-Name"
	startPort              = 8081
	endPort                = 8090
	baseAddress            = "127.0.0.1"
	addrSplit              = ":"
	addrLens               = 2
)

var (
	// ExtensionMgr -
	ExtensionMgr *ExtensionManager
	// ExtensionLoadOnce -
	ExtensionLoadOnce sync.Once
	// ExtensionInitOnce -
	ExtensionInitOnce sync.Once
)

// Extension -
type Extension struct {
	Process   *os.Process
	stdReader *functionlog.STDLogger
	name      string
	address   string
}

// ExtensionInfo -
type ExtensionInfo struct {
	httpClient *http.Client
	once       sync.Once
	name       string
	address    string
	isExist    bool
}

// InitNotify -
type InitNotify struct {
	name string
	err  error
}

// ExtensionManager -
type ExtensionManager struct {
	httpServer *http.Server
	extension  map[string]*ExtensionInfo
	reqMap     map[uint64]*sync.WaitGroup
	sync.Mutex
	initChan           chan InitNotify
	stateChan          chan struct{}
	funcName           string
	funcVersion        string
	valid              bool
	enable             bool
	healthy            bool
	extNum             int
	invokedFunctionUrn string
	logger             *functionlog.FunctionLogger
}

// ExtensionErrorInfo -
type ExtensionErrorInfo struct {
	ErrorType string `json:"errorType" valid:"optional"`
	ErrorMsg  string `json:"errorMessage" valid:"optional"`
}

// RegisterResponse -
type RegisterResponse struct {
	FuncName    string             `json:"functionName" valid:"optional"`
	FuncVersion string             `json:"functionVersion" valid:"optional"`
	ErrorInfo   ExtensionErrorInfo `json:"errorInfo" valid:"optional"`
}

func newHandler(extMgr *ExtensionManager) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.POST(registerURL, func(c *gin.Context) {
		extMgr.Register(c.Writer, c.Request)
	})
	router.POST(constants.TelemetryPath, func(c *gin.Context) {
		telemetry.OpenTelemetryAPI(c.Writer, c.Request)
	})
	return router
}

// GetExtensionManager -
func GetExtensionManager(conf *config.Configuration) *ExtensionManager {
	ExtensionLoadOnce.Do(func() {
		ExtensionMgr = &ExtensionManager{
			extension: make(map[string]*ExtensionInfo),
			reqMap:    make(map[uint64]*sync.WaitGroup),
			initChan:  make(chan InitNotify, maxExtensionNum),
			stateChan: make(chan struct{}, maxExtensionNum),
		}
		ExtensionMgr.startExtensionServer(conf)
		if !conf.RMConfig.RuntimeConfigs[worker.ExtensionScript].Enable {
			log.GetLogger().Infof("The extension script is disable.")
			return
		}
		if err := ExtensionMgr.generateExtensionMgr(conf); err != nil {
			log.GetLogger().Infof("failed to generate extension manager, error: %s", err.Error())
			return
		}
		log.GetLogger().Info("extension http server running")
		return
	})
	return ExtensionMgr
}

func (extMgr *ExtensionManager) generateExtensionMgr(conf *config.Configuration) error {
	names, flag := CheckExtension()
	if !flag || len(names) > maxExtensionNum || len(names) == 0 {
		log.GetLogger().Warnf("Extension is invalid, names: %v, flag: %t", names, flag)
		return errors.New("failed to check extension valid")
	}
	logger, err := functionlog.GetFunctionLogger(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to get functionLogger: %s", err.Error())
		return err
	}
	extMgr.logger = logger
	extMgr.extNum = len(names)
	extMgr.healthy = true
	extMgr.enable = true
	extMgr.invokedFunctionUrn = conf.URN
	baseURN, err := urnutils.GetFuncInfoWithVersion(conf.URN)
	if err == nil {
		extMgr.funcName = baseURN.Name
		extMgr.funcVersion = baseURN.Version
	}
	for _, name := range names {
		info := &ExtensionInfo{name: name}
		extMgr.extension[name] = info
	}
	return nil
}

func (extMgr *ExtensionManager) startExtensionServer(conf *config.Configuration) {
	address := fmt.Sprintf("%s:%d", worker.LocalServerIP,
		conf.RMConfig.RuntimeConfigs[worker.ExtensionScript].Port)
	server := &http.Server{
		Addr:           address,
		Handler:        newHandler(extMgr),
		MaxHeaderBytes: defaultHTTPHeaderLimit,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.GetLogger().Errorf("failed to start the http server, error: %s", err.Error())
		}
		extMgr.healthy = false
	}()
}

func (extMgr *ExtensionManager) notifyExtensionState(extensionName string, err error) {
	notifyRes := InitNotify{
		name: extensionName,
		err:  err,
	}
	extMgr.initChan <- notifyRes
}

func (extMgr *ExtensionManager) writeExtensionLog(err error) {
	if err == nil {
		return
	}
	if extMgr.logger != nil {
		logMsg := fmt.Sprintf("failed to received extension request response, err: %s", err.Error())
		extMgr.logger.WriteStdLog(logMsg, time.Now().UTC().Format(functionlog.FuncLogTimeFormat),
			PodApplier == constants.WorkerManagerApplier,
			time.Now().UTC().Format(functionlog.NanoLogLayout))
	}
}

// SendRequest -
func (extMgr *ExtensionManager) SendRequest(icID uint64, traceID string, requestTimeout time.Duration) {
	extMgr.Lock()
	extWg := &sync.WaitGroup{}
	extMgr.reqMap[icID] = extWg
	extMgr.Unlock()

	for _, exInfo := range extMgr.extension {
		extWg.Add(1)
		go func(exInfo *ExtensionInfo, wg *sync.WaitGroup) {
			defer func() {
				wg.Done()
			}()
			_, err := exInfo.sendHTTPRequest(traceID, extMgr.invokedFunctionUrn, requestTimeout, exInfo.name)
			if err != nil {
				extMgr.writeExtensionLog(err)
				log.GetLogger().Warnf("request exInfo: %s resp is nil, err is: %s", exInfo.name, err.Error())
				return
			}
		}(exInfo, extWg)
	}
}

// WaitResp -
func (extMgr *ExtensionManager) WaitResp(icID uint64) {
	extMgr.Lock()
	wg := extMgr.reqMap[icID]
	extMgr.Unlock()
	wg.Wait()

	extMgr.Lock()
	delete(extMgr.reqMap, icID)
	extMgr.Unlock()
}

// GetExtensionEnable -
func (extMgr *ExtensionManager) GetExtensionEnable() bool {
	extMgr.Lock()
	enableFlag := extMgr.valid && extMgr.enable
	extMgr.Unlock()
	return enableFlag
}

func (extMgr *ExtensionManager) addExtensionInfo(info *ExtensionInfo) {
	extMgr.Lock()
	extMgr.extension[info.name] = info
	extMgr.Unlock()
}

// GetExtensionState -
func (extMgr *ExtensionManager) GetExtensionState() {
	log.GetLogger().Error("failed to exec extension process, check extension code")
	extMgr.stateChan <- struct{}{}
}

func setRegisterResult(w http.ResponseWriter, resp *RegisterResponse) {
	w.WriteHeader(http.StatusOK)
	data, err := json.Marshal(resp)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal response: %s", err.Error())
		return
	}
	_, err = w.Write(data)
	if err != nil {
		log.GetLogger().Warnf("failed to write response body: %s", err.Error())
		return
	}
}

func isValidAddress(addr string) bool {
	addrSlice := strings.Split(addr, addrSplit)
	if len(addrSlice) != addrLens {
		log.GetLogger().Errorf("invalid extension address and port, addr %s", addrSlice)
		return false
	}
	if addrSlice[0] != baseAddress {
		log.GetLogger().Errorf("invalid extension address, addr %s", addrSlice)
		return false
	}
	port, err := strconv.Atoi(addrSlice[1])
	if err != nil {
		log.GetLogger().Errorf("invalid extension port, addr %s", addrSlice)
		return false
	}
	if port < startPort || port > endPort {
		log.GetLogger().Errorf("invalid extension port, addr %s", addrSlice)
		return false
	}
	return true
}

// Register -
func (extMgr *ExtensionManager) Register(w http.ResponseWriter, request *http.Request) {
	var isExist bool
	extensionName := request.Header.Get(extensionName)
	extensionAddress := request.Header.Get(extensionAddress)
	addrFlag := isValidAddress(extensionAddress)
	log.GetLogger().Infof("current extensionName: %s, extensionAddress: %s", extensionName, extensionAddress)
	defer request.Body.Close()
	if extensionName == "" || !addrFlag {
		errMsg := fmt.Sprintf("current extension name [%s] is not exist or address [%s] invalid",
			extensionName, extensionAddress)
		extMgr.notifyExtensionState(extensionName, errors.New(errMsg))
		response := &RegisterResponse{
			ErrorInfo: ExtensionErrorInfo{
				ErrorType: "InvalidExtensionRegistrationParameter",
				ErrorMsg:  "The extension names and address are essential",
			},
		}
		setRegisterResult(w, response)
		log.GetLogger().Warnf("failed to Register, extensionName: %s, extensionAddress: %s",
			extensionName, extensionAddress)
		return
	}
	extMgr.Lock()
	value, ok := extMgr.extension[extensionName]
	if ok {
		isExist = value.isExist
	}
	extMgr.Unlock()
	if isExist {
		log.GetLogger().Warnf(" The extension %s has been registered ", extensionName)
		return
	}
	if !ok {
		extMgr.notifyExtensionState(extensionName, errors.New("current extension name is not exist"))
		response := &RegisterResponse{
			ErrorInfo: ExtensionErrorInfo{
				ErrorType: "InvalidExtensionRegistrationParameter",
				ErrorMsg:  "The registration extension name is incorrect",
			},
		}
		setRegisterResult(w, response)
		log.GetLogger().Warnf("current extension name is not exist, name: %s", extensionName)
		return
	}
	extMgr.notifyExtensionState(extensionName, nil)
	extMgr.addExtensionInfo(&ExtensionInfo{name: extensionName, address: extensionAddress, isExist: true})
	response := &RegisterResponse{
		FuncName:    extMgr.funcName,
		FuncVersion: extMgr.funcVersion,
	}
	setRegisterResult(w, response)
	log.GetLogger().Infof("succeed to register extension [%s] ", extensionName)
}

func (extMgr *ExtensionManager) awaitExtRegisterReady(timeout time.Duration) error {
	var finishNum int
	extMgrTicker := time.NewTicker(timeout)
	log.GetLogger().Infof("start to wait for extension register request")
	for {
		select {
		case <-extMgrTicker.C:
			log.GetLogger().Errorf("extension ticker is timeout")
			return errors.New("register execution timed out")

		case <-extMgr.stateChan:
			log.GetLogger().Errorf("extension process is exit")
			return errors.New("the extension process exit abnormally, please check code")

		case res, ok := <-extMgr.initChan:
			if !ok {
				log.GetLogger().Errorf("extMgr.initChan is closed")
				return errors.New("extension register init channel is closed")
			}
			if res.err != nil {
				log.GetLogger().Errorf("failed to register extension, error: %s.", res.err.Error())
				return errors.New("failed to register extension")
			}
			finishNum++
			if finishNum == extMgr.extNum {
				log.GetLogger().Infof("all extension register successfully")
				return nil
			}
		}
	}
}

// EnableStdLogCollection collect std log and call back
func (e *Extension) EnableStdLogCollection(cb func([]byte)) {
	if e.stdReader == nil {
		e.stdReader = functionlog.CreateSTDLogger()
	}
	e.stdReader.RegisterLogCallback(cb)
}

// RunWrapperByID -
func (e *Extension) RunWrapperByID(createOptions *types.CreateOptions) (*os.Process, error) {
	cmd, err := e.buildCmd(createOptions.Name, createOptions.Conf)
	if err != nil {
		log.GetLogger().Errorf("build extension cmd error: %s", err.Error())
		return nil, err
	}
	if err = utils.SetCmdCredByID(cmd, createOptions.UID, createOptions.GID); err != nil {
		log.GetLogger().Errorf("set cmd cred by id error: %s", err.Error())
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		log.GetLogger().Errorf("started the extension error: %s", err.Error())
		return nil, err
	}
	if cmd.Process == nil {
		log.GetLogger().Errorf("cmd.Process is nil")
		return nil, err
	}
	log.GetLogger().Infof("start extension cmd successful, extension: %s", createOptions.Name)
	return cmd.Process, nil
}

func (e *Extension) buildCmd(name string, conf *config.Configuration) (*exec.Cmd, error) {
	extensionPath := extensionPath + "/" + name
	log.GetLogger().Infof("extension path is %s", extensionPath)

	_, err := os.Stat(extensionPath)
	if err != nil && os.IsNotExist(err) {
		log.GetLogger().Errorf("invalid extension directories and Files, %s", extensionPath)
		return nil, snerror.New(snerror.ExtensionShellNotFound, snerror.ErrText(snerror.ExtensionShellNotFound))
	}

	args := []string{extensionPath}
	cmd := exec.Command(args[0], args[1:]...)
	log.GetLogger().Infof("extension process args: %v", args)

	extensionAPIADDR := fmt.Sprintf("%s:%d", worker.LocalServerIP,
		conf.RMConfig.RuntimeConfigs[worker.ExtensionScript].Port)
	cmd.Env = append(cmd.Env, envExtensionAPIADDR+"="+extensionAPIADDR)

	cmd.Stdout = e.stdReader
	cmd.Stderr = e.stdReader
	return cmd, nil
}

func (e *ExtensionInfo) sendHTTPRequest(traceID, urn string, requestTimeout time.Duration,
	name string) (*http.Response, error) {
	baseURLPath := fmt.Sprintf("http://%s%s", e.address, extensionInvokePath)
	log.GetLogger().Debugf("prepare to request base url: %s", baseURLPath)
	reqBody := fmt.Sprintf(`{"traceId":"%s","invokedFunctionUrn":"%s"}`,
		traceID, urn)
	req, err := http.NewRequest("POST", baseURLPath, bytes.NewBuffer([]byte(reqBody)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(extensionHeader, e.name)

	e.getHTTPClient(requestTimeout)
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return resp, nil
}

func (e *ExtensionInfo) getHTTPClient(requestTimeout time.Duration) {
	e.once.Do(func() {
		e.httpClient = &http.Client{
			Timeout: requestTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
			},
		}
	})
}

// RunExtensionProcess run user extension runtime process
func RunExtensionProcess(createOptions *types.CreateOptions, logReporter func([]byte)) (*os.Process, error) {
	var err error
	extensionProcess := &Extension{}
	// enable std log collection before custom runtime started
	extensionProcess.EnableStdLogCollection(logReporter)

	if extensionProcess.Process, err = extensionProcess.RunWrapperByID(createOptions); err != nil {
		log.GetLogger().Errorf("failed to run extension process, %s", err.Error())
		return nil, err
	}
	return extensionProcess.Process, nil
}

// CheckExtension -
func CheckExtension() ([]string, bool) {
	var extensionName []string
	files, err := ioutil.ReadDir(extensionPath)
	if err != nil {
		log.GetLogger().Infof("failed to open the extension directory. The extension file is not available: %s", err.Error())
		return nil, false
	}
	for _, fi := range files {
		if fi.IsDir() {
			continue
		} else {
			if err := checkExtensionName(fi.Name()); err != nil {
				continue
			}
			extensionName = append(extensionName, fi.Name())
			log.GetLogger().Infof("extension name is %s", extensionName)
		}
	}
	if len(extensionName) == 0 {
		return nil, false
	}
	return extensionName, true
}

func checkExtensionName(name string) error {
	if strings.HasSuffix(name, extensionSuffix) {
		if strings.Contains(name, extensionBashName) {
			return nil
		}
	}
	log.GetLogger().Warnf("invalid extension name, name: %s", name)
	return errors.New("invalid extension name")
}

// StartExtensionProcess start extension
func startExtensionProcess() tcpmsg.RuntimeManagerRSP {
	msg := tcpmsg.BuildMessage(tcpmsg.InitExtensionProcess, []byte{})
	return RuntimeManagerClient.SendMessage(msg)
}

// InitExtensionProcess -
func InitExtensionProcess(timeout time.Duration, configuration *config.Configuration,
	functionLog *functionlog.FunctionLogger) error {
	var err error
	ExtensionInitOnce.Do(func() {
		if ExtensionMgr == nil || !ExtensionMgr.enable {
			log.GetLogger().Infof("ExtensionMgr is nil or extension is disable.")
			return
		}
		if !ExtensionMgr.healthy {
			log.GetLogger().Error("failed to start worker extension server")
			err = snerror.New(snerror.ExtensionExecFailed,
				fmt.Sprintf(snerror.ErrText(snerror.ExtensionExecFailed), "failed to start worker extension server"))
			return
		}

		traceID := commUtils.UniqueID()
		summarySpan := NewSummarySpan(constants.ExtensionStage, traceID, configuration,
			timeout)
		ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
		defer invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
		ic.SetIsExtension(true)
		logRecorder := functionLog.NewLogRecorder(ic, traceID, constants.ExtensionStage,
			functionlog.WithLogHeaderFooterGenerator(summarySpan),
			functionlog.WithLogOptionTail(),
		)
		if PodApplier == constants.WorkerManagerApplier {
			logRecorder.FinishHeader(ic)
			defer logRecorder.FinishFooter(ic)
		} else {
			defer logRecorder.FinishCopy(ic)
		}
		var extensionErr *Error
		err = ExtensionMgr.runExtensionProcess(timeout)
		if err != nil {
			sErr, ok := err.(snerror.SNError)
			if ok {
				extensionErr = &Error{Code: (int32)(sErr.Code()), Message: sErr.Error()}
			}
		} else {
			ExtensionMgr.valid = true
		}
		summarySpan.Finish(extensionErr)
		ConResource.SetExtensionSummary(summarySpan)
		ExtensionDuration = summarySpan.SubDuration()
	})
	return err
}

func (extMgr *ExtensionManager) runExtensionProcess(timeout time.Duration) error {
	if !extMgr.enable {
		log.GetLogger().Warn("extension is invalid.")
		return nil
	}
	resp := startExtensionProcess()
	if resp.Err != "" {
		log.GetLogger().Errorf("Failed to start the extension, error: %s", resp.Err)
		return snerror.New(snerror.ExtensionExecFailed,
			fmt.Sprintf(snerror.ErrText(snerror.ExtensionExecFailed), "user code execution error"))
	}
	err := extMgr.awaitExtRegisterReady(timeout)
	if err != nil {
		log.GetLogger().Errorf("extension register fail or failed to exec extension, "+
			"check extension code, error: %s", err.Error())
		return snerror.New(snerror.ExtensionExecFailed, fmt.Sprintf(snerror.ErrText(snerror.ExtensionExecFailed),
			err.Error()))
	}
	log.GetLogger().Info("succeed to exec extension")
	return nil
}

func saveExtensionFailedLog(conf *config.Configuration) {
	if PodApplier != constants.WorkerManagerApplier {
		for i := 0; i < len(functionlog.LogRecorderBox); i++ {
			functionlog.LogRecorderBox[i].FinishSave()
		}
	}
	functionlog.Sync(conf)
}

// SaveExtensionFailedLog -
func SaveExtensionFailedLog(conf *config.Configuration, err error) {
	if err != nil {
		sErr, ok := err.(snerror.SNError)
		if ok && sErr.Code() == snerror.ExtensionExecFailed {
			if len(functionlog.LogRecorderBox) == 0 {
				log.GetLogger().Warn("LogRecorderBox is empty")
				return
			}
			saveExtensionFailedLog(conf)
		}
	}
}
