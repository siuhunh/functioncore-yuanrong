[TOC]

# 未交付功能的安全防护

## 1. Story整体设计

### 1. 功能描述
1. 未交付代码功能安全防护
2. 未交付功能的安全防护
3. prometheus 端口关。所有第三方组件功能接口关闭
4. Functask功能关
5. frontend对外接口审视关闭
6. worker对外功能接口审视关闭

### 2. 场景分析
未交付代码功能安全防护和未交付功能的安全防护，主要是关闭有状态和函数间调用功能（添加配置开关）。

prometheus端口关。所有第三方组件功能接口关闭，优先关闭端口处理。
（1）prometheus：先增加开关关闭普罗米修斯相关功能，要把普罗米修斯搞成插件的话工作量应该需要一人月（王胤文）。
（2）redis: functiontask的有状态客户端，已使用function_branch配置关闭。worker-manager使用华为云redis, 用于上报实例监控指标给functionGraph。
（3）efk: 日志采集能力由functionGrpah提供，使用了efk的组件admin-service不交付。
（4）etcd: 由functionGrpah提供etcd，不涉及。
（5）minio: 不交付，使用华为云obs，不涉及。
（6）keycloak: 不交付，iam-adaptor组件也不交付，iam-client已使用function_branch配置关闭。
（7）mysql: 不交付，使用华为云RDS, 不涉及。

Functask功能关，不提供有状态和函数间调用功能（添加配置开关）。

frontend对外接口审视关闭，只保留健康检查和调用函数接口，其余接口关闭（添加配置开关）。

worker对外功能接口审视关闭，关闭函数间调用和取消函数接口（添加配置开关）。

去除各语言runtime的snsdk。

内核能力：kernel-manager kerner-invoker组件不部署即可，当前不交付这两个组件。

交付组件：FunctionAccessor、FunctionTask、worker、runtime(java/nodejs/python/custom)、runtime manager、 function instance manager

其他组件：admin-service、iam-adaptor、kernelinvoker、kernel-worker、function-repository、http-access、kafka-access、kernel-manager、state-manager均不交付，不考虑
		
### 3. 方案详细设计
value.yaml添加全局配置项functionCapability，为functioncore提供的函数能力。
functionCapability配置项的值参照function_branch，分别为0和1，1为向FunctionGraph交付的函数能力。
当functionCapability: 1时，当前只向FunctionGraph提供函数调用功能，不提供有状态和函数间调用等功能。
当functionCapability: 0时，则提供当前所有函数功能。

注：functionCapability默认为1，非交付functiongraph需手动改为0，需归档发群。
注：function_branch配置项整合到functionCapability，减少配置项。

#### 3.1 FunctionAccessor
FunctionAccessor的config.json新增配置项functionCapability， 其值为value.yaml中的全局配置项functionCapability。
FunctionAccessor对外接口如下：
``````
	router.GET("/healthz", healthz)
	router.GET("/aliases", AliasHandler)
	router.GET("/workers", WorkersHandler)
	router.GET("/serverless/v1/query", QueryHandler)
	router.POST("/serverless/v1/functions/{function-name}/invocations", InvokeHandler)
	router.GET("/serverless/v1/invocation/{futureID}", QueryInvokeHandler)
	router.DELETE("/serverless/v1/invocation/{futureID}", CancelHandler)
``````
当functionCapability为1时，只开启/healthz和/serverless/v1/functions/{function-name}/invocations接口。
当functionCapability为0时，上述接口全部开启。

在函数调用处理函数InvokeHandler，当functionCapability为1时，将函数调用请求内的请求头x-state-key删除。

其他：frontend提供了查询redis密码的接口，经确认为接口/serverless/v1/query，该接口在functionCapability为1时不开启。

#### 3.2 FunctionTask
functionTask的config.json新增配置项functionCapability， 其值为value.yaml中的全局配置项functionCapability。

functionTask对外HTTP接口如下：
``````
	http.HandleFunc("/healthz", handleHealthCheck)
	http.HandleFunc("/api/futureStore/detail", handleFutureStoreDetail)
``````
当functionCapability为1时，只开启/healthz接口。

busproxy对外接口，当前注册了HandReq和HandleCancel接口：
``````
 httpserver.CreateHTTPServer(listenAddr, busProxy.HandReq, busProxy.HandleCancel).Start()
``````
当functionCapability为1时，移除busProxy.HandleCancel，不提供取消函数执行功能。位置：proxy.go的startProxy函数

同时当functionCapability为1时，只提供workerDispatcher，不提供taskDispatcher，taskDispatcher处理发送给functiontask的请求。

#### 3.3 worker
worker的config.json新增配置项functionCapability， 其值为value.yaml中的全局配置项functionCapability。
当functionCapability为1时，不注册函数CallByBus、CancelByBus和Cancel接口。

#### 3.4 python runtime
执行build.sh脚本: sh build.sh -p FunctionGraph
1. 不执行gen_pb_fdk-python.sh脚本，不复制fdk目录下的文件到output目录
2. 编译后删除"${OUTPUT_DIR}"/pkg/runtime/python/目录下的pythonRT.py、sncsdk.py、executor.py、stateful_event.py文件
3. server.py内的process_cancel方法（处理来自worker的取消函数执行请求），经评估无法屏蔽此处代码，但是在worker层面屏蔽了cancel接口，与春哥确认可行
4. rpc.proto下的函数间调用和取消函数执行消息体结构是否要删除。 与春哥确认，worker关闭函数间调用接口即可。

#### 3.5 nodejs runtime
执行build.sh脚本: sh build.sh -p FunctionGraph
1. 不执行gen_pb_fdk-nodejs.sh脚本，不复制fdk目录下的文件到output目录
2. 编译后删除"${OUTPUT_DIR}"/pkg/runtime/nodejs/目录下的snsdk目录、snsdk.js、stateful_event.js文件
3. wapper.js内函数间调用功能移动到元戎的runtimeHandler.js文件中
4. rpc.proto下的函数间调用和取消函数执行消息体结构是否要删除。 与春哥确认，worker关闭函数间调用接口即可。

#### 3.6 java runtime
java runtime 只提供了core sdk，未提供snsdk，不用修改。

#### 3.7 custom runtime
custom runtime代码在worker内，按3.3处理即可。

#### 3.8 prometheus端口关
value.yaml添加prometheusEnable开关配置项，为true时启动prometheus server, 为false不启动。默认为false.

#### 3.9 runtimemanager 
Dockerfile去除对snsdk和fdk相关文件的复制。


### 4. 上下游依赖需求
​	无

## 2. 可信软件

1. 安全性（Security） 
   *【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 
   不涉及
2. 韧性（Resilience）
   *【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
   不涉及
3. 隐私性（Privacy） 
   *【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
   不涉及
4. 可靠性（Reliability）
   *【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
   不涉及
5. 可用性（Availability）
   不涉及
6. 安全（Safety）
   *【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
   不涉及

## 3. 用例

| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | --------------------------------- | ---------------------------------------- |
| 1        | 乌兰察布发起函数调用请求                                        | 乌兰察布操作界面，创建函数，点击测试      |          调用成功返回函数结果                 |
| 2        | 有状态调用（nodejs）                                          | 乌兰察布操作界面，创建函数，编写有状态调用逻辑，保存点击测试   |  调用失败返回required(snsdk)不存在错误信息    |
| 3        | 有状态调用（python）                                          | 乌兰察布操作界面，创建函数，编写有状态调用逻辑，保存点击测试   |  调用失败返回import snsdk不存在错误信息       |
| 4        | 有状态调用（java）                                            | 乌兰察布操作界面，创建函数，编写有状态调用逻辑，保存点击测试   |  调用失败返回import snsdk不存在错误信息       |
| 2        | 函数内进行函数间调用（nodejs）                                  | 乌兰察布操作界面，创建函数，编写函数间调用逻辑，保存点击测试   |  调用失败返回required(snsdk)不存在错误信息    |
| 3        | 函数内进行函数间调用（python）                                  | 乌兰察布操作界面，创建函数，编写函数间调用逻辑，保存点击测试   |  调用失败返回import snsdk不存在错误信息       |
| 4        | 函数内进行函数间调用（java）                                    | 乌兰察布操作界面，创建函数，编写函数间调用逻辑，保存点击测试   |  调用失败返回import snsdk不存在错误信息       |
| 5        | prometheus 端口关                                            | 部署数据面，检查FunctionAccessor、Function Task的开发端口  |  prometheus 端口关闭       |

注：补充worker 模拟socket发起函数间调用返回的错误码
