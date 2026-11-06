[TOC]

# AR.SR.932aa1df.001【错误码整改】150500错误码场景区分，细化150500错误码

## 1. 需求场景分析

150500错误码涵盖了太多的异常场景，不利于现网问题定位，包括json序列化和反序列化错误、vpc报错、函数元信息获取失败等。

### 1.1 特性需求来源与价值概述

需求来源：现网150500问题定位总结。

价值概述：精细化150500错误码，快速定位问题。

### 1.2特性场景分析

150500错误码涵盖了太多的异常场景，不利于现网问题定位，包括：

1、从worker-manager缓存中获取函数元信息失败

2、解析请求中的label信息异常

3、解析bus-proxy发送的扩容请求信息异常

4、向vpc-controller申请pat-service，返回“no operational permissions vpc, check the func xrole permissions”、“no available vpc pat instance”以外的错误或60s超时

5、初始化特化请求时，获取配置信息失败或序列化请求异常

6、向worker发送特化请求异常，或worker返回的异常信息序列化异常

7、中心缩容上报请求返回内容序列化异常

8、缩容请求返回内容序列化异常

9、向worker发送更新Pat信息返回内容序列化异常

10、向worker发送闲置模式请求返回内容序列化异常

11、向worker发送预留实例迁移计费切换请求返回内容序列化异常

12、java冷启动快照制作&函数冷启动阶段异常



## 2. 特性/功能实现原理

1、从worker-manager缓存中获取函数元信息失败
```go
func refreshSilentFunc(requestInfo *config.RequestInfo, funcSvcID string) error {
	key := fmt.Sprintf(constants.SilentFuncKey,
		requestInfo.TenantID, requestInfo.FunctionName, requestInfo.FunctionVersion)
	_, err := cache.SingleFlight.Do(funcSvcID, func() (interface{}, error) {
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		getResp, err := etcd3.GetMetaEtcdClient().GetResponse(defaultEtcdCtx, key)
		if err != nil {
            // ......
			log.GetLogger().Errorf("failed to query from etcd through key %s, err: %s", key, err.Error())
			return nil, snerror.New(utils.StatusInternalServerError, "failed to query from etcd")
		}
		if len(getResp.Kvs) == 0 {
			log.GetLogger().Errorf("failed to get function meta from etcd by key: %s", key)
			return nil, snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
		}
		// ......
		return nil, snerror.New(snerror.RefreshSilentFunc, snerror.ErrText(snerror.RefreshSilentFunc))
	})
	return err
}
```
场景：只有当特化阶段，本地缓存的function service为空且函数为静默函数时才会触发此函数
修改：错误码由 150500 修改为 150501，错误信息是 `failed to query from etcd`

2、解析请求中的label信息异常
```go
func (p *PodFetcher) getPodFromNodeManager(ctx context.Context, funSvc *faasfunction.FunctionService,
	req *config.RequestInfo) (*corev1.Pod, error) {
	forceDeploy, err := strconv.ParseBool(req.ForceDeploy)
	if err != nil {
		log.GetLogger().Errorf("failed to parse force deploy label %s", req.ForceDeploy)
		return nil, err
	}
	nodeLabel := make([]map[string]string, 0, utils.DefaultSliceSize)
	if len(req.NodeLabel) != 0 {
		err = json.Unmarshal([]byte(req.NodeLabel), &nodeLabel)
		if err != nil {
			log.GetLogger().Errorf("failed to parse node label %s", req.NodeLabel)
			return nil, err
		}
	}
```

修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 `failed to parse node label`

3、向vpc-controller申请pat-service，返回“no operational permissions vpc, check the func xrole permissions”、“no available vpc pat instance”以外的错误或60s超时

```go
	// acquire pat pods from vpc controller
	if err = p.VpcHandler.HandleVpcFunctionInfo(service, reqInfo); err != nil {
		log.GetLogger().Errorf("failed to handler vpc function info with funcName: %s, traceID: %s, error: %s",
			reqInfo.FunctionName, reqInfo.TraceID, err.Error())
		if deleteErr := p.DeletePodByK8S(pod.Name, reqInfo.TraceID); deleteErr != nil {
			log.GetLogger().Warnf("failed to delete pod %s by k8s with traceID %s and error %s",
				pod.Name, reqInfo.TraceID, deleteErr.Error())
		}
		if err.Error() == utils.ErrNoOperationalPermissionsVpc.Error() {
			err = snerror.New(snerror.VpcNoOperationalPermissions, utils.ErrNoOperationalPermissionsVpc.Error())
		}
		if err.Error() == utils.ErrNoAvailableVpcPatInstance.Error() {
			err = snerror.New(snerror.NotEnoughNIC, snerror.ErrText(snerror.NotEnoughNIC))
		}
		if strings.Contains(err.Error(), utils.ErrVPCNotFound.Error()) {
			err = snerror.New(snerror.VPCNotFound, snerror.ErrText(snerror.VPCNotFound))
		}
		return nil, err
	}
```
修改：其他 vpc 错误和超时的错误，统一错误码为 150437，错误信息保留 vpc controller 的返回信息

4、初始化特化请求时，获取配置信息失败或序列化请求异常

```go
func initSpecializeRequest(pod *corev1.Pod, functionRequest commontypes.FunctionSpecRequest,
	reqInfo *config.RequestInfo, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
	req, err := initRequestToWorker(pod.Status.PodIP, constants.LoadFunctionPath, httpsEnable)

	if err != nil {
		return nil, nil, err
	}
	// ......
	body, err := json.Marshal(functionRequest)
	if err != nil {
		return nil, nil, err
	}
	req.ResetBody()
	req.SetBody(body)

	response := fhttp.AcquireResponse()
	return req, response, nil
}
```
修改：json marshal unmarshal 错误码统一改为 150599，保留原始报错信息

5、向worker发送特化请求返回的异常信息序列化异常

```go
func (p *PodFetcher) sendGeneralizeRequestToWorker(pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (string, error) {
	// ......
	err = p.workerClient.DoTimeout(req, response, timeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send request to node(%s) of pod(%s) ip(%s) "+
			"for worker(%s), traceID(%s) and error %s",
			pod.Status.HostIP, pod.Name, pod.Status.PodIP, reqInfo.WorkerID, reqInfo.TraceID, err.Error())
		return "", err
	}
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to generalize pod(%s), node: %s, worker: %s, respCode: %d, respBody: %s",
			pod.Name, pod.Status.HostIP, reqInfo.WorkerID, response.Header.StatusCode(), string(response.Body()))
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker")
			return "", snerror.NewWithFmtMsg(utils.StatusInternalServerError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		if snErr.Code == snerror.DownLoadCodeErrorCode {
			alarm.GetClient().ReportAlarm(alarm.OBSConnection,
				fmt.Sprintf("error msg=%s,", snErr.Message))
		}
		return "", snerror.New(snErr.Code, snErr.Message)
	}
	alarm.GetClient().ClearAlarm(alarm.OBSConnection)
	return string(response.Header.Peek(constants.HeaderLoadDuration)), nil
}
```

修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to unmarshal response body

6、中心缩容上报请求返回内容序列化异常

```go
func (m *FunctionAPI) setMetricsResponse(statusCode int, msg string, w http.ResponseWriter) {
	res := ReportMetricsResponse{
		Code:    statusCode,
		Message: msg,
	}
	resBody, err := json.Marshal(res)
	if err != nil {
		log.GetLogger().Errorf("failed to set metrics response %s", err.Error())
		resBody = snerror.Marshal(snerror.New(utils.StatusInternalServerError,
			"report metrics error"))
	}
	w.WriteHeader(statusCode)
	w.Write(resBody)
}
```
场景： 
a) bus-proxy向worker-manager上报metrics信息，worker-manager返回结果时，触发此函数
b) res内容为组件自生成，序列化异常概率低
修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to marshal metrics response

7、缩容请求返回内容序列化异常

```go
func (m *FunctionAPI) setDeleteResponse(statusCode int, reserved bool, message string, w http.ResponseWriter) {
	res := DeleteWorkerResponse{
		Code:     statusCode,
		Reserved: reserved,
		Message:  message,
	}
	resBody, err := json.Marshal(res)
	if err != nil {
		log.GetLogger().Errorf("failed to set delete response %s", err.Error())
		response := snerror.Marshal(snerror.New(utils.StatusInternalServerError,
			"function instance deletion error"))
		_, writeErr := w.Write(response)
		if writeErr != nil {
			log.GetLogger().Errorf("failed to write resBody %s", writeErr.Error())
			return
		}
		return
	}
	w.WriteHeader(statusCode)
	w.Write(resBody)
}
```

修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to marshal delete worker response

8、向worker发送更新Pat信息返回内容序列化异常

```go
func (p *PodFetcher) sendPatPodIPToWorker(instance v1alpha1.WorkerInstance, traceID string,
	timeout time.Duration, patPodInfo, endpointTalentPatPodInfo []commontypes.NATConfigure) error {
	// ......
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to set pat pod ip to pod(%s), node: %s, applier: %s, respCode: %d",
			instance.PodName, instance.IP, instance.Applier, response.Header.StatusCode())
		snBadResp := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snBadResp)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker with error: %s", err.Error())
			return snerror.NewWithFmtMsg(utils.StatusInternalServerError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		return snerror.New(snBadResp.Code, snBadResp.Message)
	}

	return nil
}
```

修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to unmarshal response body

9、向worker发送闲置模式请求返回内容序列化异常

```go
func (p *PodFetcher) sendIdleModeRequestToWorker(instance v1alpha1.WorkerInstance, idleMode bool,
	traceID string, timeout time.Duration) error {
	// ......
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to set idle mode to pod(%s), node: %s, applier: %s, respCode: %d, respBody: %s",
			instance.PodName, instance.IP, instance.Applier, response.Header.StatusCode(), string(response.Body()))
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker with error: %s", err.Error())
			return snerror.NewWithFmtMsg(utils.StatusInternalServerError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		return snerror.New(snErr.Code, snErr.Message)
	}
	return nil
}
```
修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to unmarshal response body

10、向worker发送预留实例迁移计费切换请求返回内容序列化异常

```go
func (p *PodFetcher) setBillingSwitchToWorker(instance v1alpha1.WorkerInstance, enableBilling bool,
	traceID string, timeout time.Duration) error {
	req, response, err := initBillingSwitchRequest(instance.IP, traceID, p.httpsEnable, enableBilling)
	// ......
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to set bill to pod(%s), node: %s, applier: %s, respCode: %d, respBody: %s",
			instance.PodName, instance.IP, instance.Applier, response.Header.StatusCode(), string(response.Body()))
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker with error: %s", err.Error())
			return snerror.NewWithFmtMsg(utils.StatusInternalServerError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		return snerror.New(snErr.Code, snErr.Message)
	}
	log.GetLogger().Infof("succeed to set billing with %t for %s and applier %s with %s",
		enableBilling, instance.PodName, instance.Applier, traceID)
	return nil
}
```

修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to unmarshal response body

11、java冷启动快照制作&函数冷启动阶段异常

```go
func (m *manager) processMultiVersion(err error, snapshotResult *types.SnapshotDeploymentSpec,
	checkpointMode string) ([]byte, error) {
	imageTag := m.getRuntimeManagerImageTag()
	if err != nil {
		return handleMultiVersionError(err, imageTag, snapshotResult)
	}

	m.appendMultiVersion(snapshotResult, checkpointMode)
	value, err := json.Marshal(snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to processMultiVersion marshal snapshotResult: %s", err.Error())
		return nil, err
	}

	return value, nil
}
```

12、java冷启动快照制作&函数冷启动阶段异常
```go
func (m *manager) processSingleVersion(err error, snapshotResult *types.SnapshotDeploymentSpec) ([]byte, error) {
	if err != nil {
		return handleSingleVersionError(err)
	}

	// guaranteed compatibility for SnapshotCheckInfo data structure
	snapshotResult.SnapshotCheckInfo = nil
	value, err := json.Marshal(snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to processSingleVersion marshal snapshotResult: %s, traceID: %s", err.Error())
		return nil, err
	}

	return value, nil
}
```

修改：json marshal unmarshal 错误码统一改为 150599，错误信息为 failed to marshal snapshot config


### 2.3 约束

本需求只增加了内部错误码，不涉及用户错误码。

#### 2.3.1上下游依赖需求

无

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

不涉及目录权限、进程权限等相关安全内容修改，安全性影响可控

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及用户相关敏感数据

不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

不涉及

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

无

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

无

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

不涉及

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

无

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

未增加资源消耗；

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

新增错误码均为系统错误码

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

不涉及

## 4. 用例

| 场景                                                                     | 原错误码 | 修改后的错误码（新增） | 自测方式 |
|------------------------------------------------------------------------| -------- | ---------------------- | -------- |
| 1、worker-manager从元etcd中获取函数元信息失败（get response from etcd error）     | 150500   | 150501                 | UT       |
| 2、解析请求中的label信息异常（json unmarshal）                                      | 150500   | 150599                 | UT       |
| 3、解析bus-proxy发送的扩容请求信息异常（json unmarshal）                               | 150500   | 150599                 | UT       |
| 4、向vpc-controller申请pat-service，特定vpc报错以外的错误或60s超时（create vpc pat error） | 150500   | 150437                 | UT       |
| 5、初始化特化请求时，获取配置信息失败或序列化请求异常（json marshal）                              | 150500   | 150599                 | UT       |
| 6、向worker发送特化请求，worker返回的异常信息序列化异常（json unmarshal）                     | 150500   | 150599                 | UT       |
| 7、中心缩容上报请求返回内容序列化异常（json unmarshal）                                    | 150500   | 150599                 | UT       |
| 8、缩容请求返回内容序列化异常（json marshal）                                          | 150500   | 150599                 | UT       |
| 9、向worker发送更新Pat信息返回内容序列化异常（json unmarshal）                            | 150500   | 150599                 | UT       |
| 10、向worker发送闲置模式请求返回内容序列化异常（json unmarshal）                            | 150500   | 150599                 | UT       |
| 11、向worker发送预留实例迁移计费切换请求返回内容序列化异常（json unmarshal）                      | 150500   | 150599                 | UT       |
| 12、java冷启动快照制作&函数冷启动阶段异常（json marshal）                                 | 150500   | 150599                 | UT       |


*注：属于内部可靠性优化，涉及增加用例，同时需要保证已有功能用例正常执行*
