[TOC]

# YuanRong设计文档模板

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

https://wiki.huawei.com/domains/867/wiki/5357/WIKI20221010361486

ServiceBridge基于元戎交付版本实现功能，保证bridge相关功能正常使用，连通用户函数与数据库之间的访问，将BridgeManager的功能整合到StateManager，Runtime提供系统调用API

### 1.2特性场景分析

1.支持将系统调用路由到StateManager

 （1） 接收client函数发出的getBridgeFunc请求，发往StateManager申请stateKey，请求中带有client实例信息，并根据返回的stateKey信息创建状态路由  

 （2） 接收client函数发出release的请求，发往StateManager，StateManager将请求发送给BridgeManager

2.对于标识为bridge的有状态函数实例，不要缩容。不管理该函数实例的伸缩

 （1）bus针对 bridge有状态函数实例，不启动1分钟空闲就将pod缩掉检测的协程

 （2）worker-manager 针对bridge有状态函数实例，不启用中心缩容逻辑

3. runtime-java提供系统调用(支持BridgeFunc和Session的申请和释放)

（1）runtime-java为client函数提供getBridgeFunc和release两个接口 

 使用方式由

Function func = new Function(context)

func.getInstance(bridgeFuncName, instanceId)

ObjectRef<JsonObject> ref = func.invoke(request.toString())

变更为

Function func = new Function(context)

String stateKey = func.getBridgeFunc(bridgeFuncName, "")

func.getInstance(bridgeFuncName, stateKey)

ObjectRef<JsonObject> ref = func.invoke(request.toString())

## 2. 特性/功能实现原理

### 2.1总体方案

### 用户函数调用getBridgeFunc函数



![scale](http://image.huawei.com/tiny-lts/v1/images/5bbaa89430e0f508a1bf187bf25c67ae_1173x657.png)

#### 2.1.1 getBridgeFunc函数调用不带第二个参数的情况

1. 用户函数在调用bridgefunc之前，需要通过Runtime提供的getBridgeFunc接口，获取到BridgeManager分配的instanceName, instanceName也就是stateKey

2. client函数调用系统SDK，SDK将请求发送至functiontask，functiontask向state-manager中的BridgeManager发送申请stateKey的请求

3. functiontask根据BridgeManager插件返回的stateKey结合tenantID、serviceID、funcName等信息计算出stateID, 然后进行状态路由的创建

4. 状态路由创建也就是往etcd上写 入

   key: "/sn/stateworkers/business/s.BusinessID/tenant/s.TenantID/function/s.Function/version/s.Version/s.Zone/s.StateID"

   value: {stateID: nodeID} 

   functiontask识别stateID绑定的nodeID是否为本节点对应的task， 如果是，则进行创建stateID对应的状态请求队列的操作，如果stateID对应的nodeID并非当前节点，则进行一次转发给functionbus，functionbus进行路由查找之后将请求转发给目标functiontask进行处理，在目标functiontask中进行创建stateID对应的状态请求队列的操作。

#### 2.1.2 getBridgeFunc函数调用带第二个参数的情况

​    1.用户函数在调用bridgefunc之前，调用getBridgeFunc函数，getBridgeFunc函数第二个参数即为前面调用getBridgeFunc函数返回的stateKey

​    2.client函数调用系统SDK，SDK将请求发送至functiontask，functiontask向state-manager中的BridgeManager发送请求, functiontask通过tenantID、serviceID、funcName等信息计算出stateID，将stateID等信息通过getBridgeFunc接口传递给bridgeManager

   3.functiontask根据接收到BridgeManager返回的getBridgeFunc响应之后，再计算一次stateID, 判断stateID相关信息在redis中是否存在，若存在，则直接返回

### 用户函数调用release接口

![scaledown](http://image.huawei.com/tiny-lts/v1/images/b7ba784a8fe02e2bd5a4065deca17e70_1140x619.png)

1.client用户函数中主动调用release接口

2.BridgeManager插件接收由functiontask发出的release请求，请求中包含client的函数实例信息，functiontask给state-manager发送releaseBridge请求，state-manager将请求发送给BridgeManager, BridgeManager根据引用计数决策是否需要进行terminate处理，

3.如果需要进行terminate处理，则执行删除etcd上写入的/sn/stateworkers/business/s.BusinessID/tenant/s.TenantID/function/s.Function/version/s.Version/s.Zone/s.StateID信息，

redis中state-hash:{TenantID:serviceID}:funcName的对应stateID的信息，以及删除本地缓存信息

4.更新redis上state_counter_map保存的信息中对应租户的状态个数信息， 状态个数减一以及删除本地缓存信息

4.如果判断到stateID对应的任务请求队列在本节点对应的functiontask上，则删除状态请求队列本地缓存中对应此次stateID的信息

5.如果判断到stateID对应的任务请求队列不在本节点对应的functiontask上，对将请求转发给functiontask, functiontask将请求转发给bus进行一次路由查找后，将请求转发给stateID绑定的functiontask进行删除请求队列本地缓存中对应此次stateID的信息的动作



#### getBridgeFunc接口

Runtime-java新增接口public String getBridgeFunc(String functionName, String instanceName)

1. 检查 functionName和  instanceName(instanceName存在的情况下) 是否合规，不合规则返回相应的错误信息

2. getBridgeFunc方法中创建filter类型的method方法请求发给worker, 请求中新增podName信息，worker将请求透传给bus

3. bus接收到请求之后， 将请求转发给functiontask, functiontask识别到方法类型为filter的方法之后，调用对应的filter方法, 根据pb结构得到如下请求结构体, 其中PodName只有在getBridgeFunc接口中会设置，通过PodName进行区分是getBridgeFunc接口调用还是generateID()方法调用

   ```
   type RequestPb struct {
   	state         protoimpl.MessageState
   	sizeCache     protoimpl.SizeCache
   	unknownFields protoimpl.UnknownFields
   
   	Key      string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
   	FuncName string `protobuf:"bytes,2,opt,name=funcName,proto3" json:"funcName,omitempty"`
   	StateID  string `protobuf:"bytes,3,opt,name=stateID,proto3" json:"stateID,omitempty"`
   	PodName  string `protobuf:"bytes,4,opt,name=podName,proto3" json:"podName,omitempty"`
   }
   ```

   

   当识别到len(req.Key) == 0 && len(req.PodName) != 0 或者len(req.Key) != 0 && len(req.PodName) != 0时，functiontask的filter方法中需要进行发送

   https(http):// state-manager:8087/getBridgeFuncStateKey给state-manager，state-manager在接收到此请求之后通过方法调用BridgeManager的ApplyStateKey (clientInstance ClienInstance) (StateKey, error ) 方法获取stateKey

   其中ApplyStateKey方法入参clientInstance 为如下类型的结构体

   ```
   type ClientInstance struct {
   	PodName       string
   	AssignStateId string
   	FuncInfo      *BridgeFuncInfo
   }
   
   type BridgeFuncInfo struct {
   	TenantId       string
   	ServiceId      string
   	BridgeFuncName string
   }
   ```

   

   bridgeFuncName 为getBridgeFunc方法的入参，podName从环境变量获取, assignStateKey为getBridgeFunc获取的返回值，

   tenantID、serviceID等信息在functiontask获取，将这些信息获取之后组装到ClienInstance 结构体中

4. 如果用户在getBridgeFunc函数调用时传递了第二个参数instanceName，那么functiontask会根据tenantID、serviceID、bridgeFuncName、stateKey计算得到stateID,  stateID赋值给AssignStateId字段，发送请求给state-manager

   ```
   // GenStateIDByKey returns stateID by serviceID, functionName and key
   func GenStateIDByKey(tenantID string, serviceID string, funcName, key string) string {
      // if stateKey is empty, stateID is generated by default.
      if len(key) == 0 {
         return uuid.New().String()
      }
      stateID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(tenantID+serviceID+funcName+key))
      return stateID.String()
   }
   ```

   

5. functiontask在成功获取到BridgeManager返回的stateKey之后，如果识别到redis中已经存在state-hash:{tenantID:serviceID}:funcName中对应stateID的hash值已经存在，则会立即返回

6. 如果redis中已经存在state-hash:{tenantID:serviceID}:funcName中对应stateID的hash值不存在， 则会进行创建路由的操作

   FunctionTask发送"https://" + addr + "/createstate"请求给StateManager进行创建路由的操作

  7.状态路由创建也就是往etcd上写 入

key: "/sn/s.TypeKey/business/s.BusinessID/tenant/s.TenantID/function/s.Function/version/s.Version/s.Zone/s.StateID"

value: {stateID: nodeID} 

8.创建完路由之后，需要创建请求处理队列，如果识别到路由信息中stateID绑定的functiontask nodeID为本节点，或者当前functiontask上已经存在{${tenantID}:${serviceID}}:stateID:${stateID}对应的请求队列，则在当前functiontask上创建请求处理队列

9.如果识别到stateID绑定的functiontask nodeID不为本节点，或者当前functiontask上不存在{{tenantID}:{serviceID}}:stateID:{stateID}对应的请求队列， 则进行一次functiontask->bus->functiontask的转发，转发到目标functiontask上进行创建{${tenantID}:${serviceID}}:stateID:{stateID}对应的队列的操作

10.函数执行成功，函数返回stateID和stateKey信息给runtime设置到Function对象中，函数返回值为从bridgeManager申请的stateKey信息



[poc设计](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=19018ff2d11c4882bee665c2f7539656)![poc设计](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2n95?t=1667960028111)









### Release逻辑

Runtime-java新增public void release()接口

1.Runtime-java给worker发送methodType为terminate类型的函数调用请求， 请求中新增podName信息

2.worker将请求透传给bus, bus将请求转发给本节点所在的functiontask

3.functiontask识别到methodType为terminate的请求调用之后，source来源是SourceFunctionTaskToFunctionTask, common.SourceRuntimeToCentralFunctionTask的请求给state-manager发送releaseBridge请求之后，若releaseBridge函数返回bool值为true，代表需要执行本次terminate操作，若releaseBridge函数返回bool值为false，代表不需要执行本次terminate操作

4.functiontask接收到请求之后, 组装出结构体发送请求给state-manager

其中ClienInstance结构体如下所示

```
type ClientInstance struct {
   FuncInfo      *BridgeFuncInfo
   PodName       string
   AssignStateID string
}

type BridgeFuncInfo struct {
   TenantID       string
   ServiceID      string
   BridgeFuncName string
}
```

, podName、bridgeFuncName 、AssignStateId 由runtime在请求中带给state-manager

5.terminate操作中执行删除注册的key: "/sn/s.TypeKey/business/s.BusinessID/tenant/s.TenantID/function/s.Function/version/s.Version/s.Zone/s.StateID"

value: {stateID: nodeID} 路由信息，更新缓存中以及redis中租户下的state个数信息, 删除redis中哈希表{{tenantID}:{serviceID}}:stateID:{stateID}中对应stateID的信息

6.bus监听到key: "/sn/s.TypeKey/business/s.BusinessID/tenant/s.TenantID/function/s.Function/version/s.Version/s.Zone/s.StateID"被删除之后，会通过给worker-manager发送缩容请求，将对应的pod实例给缩容掉

![poc设计1](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2n9E?t=1667962935031)



### Bus缩容逻辑

函数源数据信息中新增IsBridgeFunction字段

```
// FuncMetaData define meta data of functions
type FuncMetaData struct {
   Layers                  []*types.Layer        `json:"layers" valid:",optional"`
   Name                    string                `json:"name"`
   FunctionDescription     string                `json:"description" valid:"stringlength(1|1024)"`
   FunctionURN             string                `json:"functionUrn"`
   ReversedConcurrency     int                   `json:"reversedConcurrency" valid:"int"`
   Tags                    map[string]string     `json:"tags" valid:",optional"`
   FunctionUpdateTime      string                `json:"functionUpdateTime" valid:",optional"`
   FunctionVersionURN      string                `json:"functionVersionUrn"`
   CodeSize                int                   `json:"codeSize" valid:"int"`
   CodeSha256              string                `json:"codeSha256" valid:"stringlength(1|64),optional"`
   CodeSha512              string                `json:"codeSha512" valid:"stringlength(1|128),optional"`
   Handler                 string                `json:"handler" valid:"stringlength(1|255)"`
   Runtime                 string                `json:"runtime" valid:"stringlength(1|63)"`
   Timeout                 int64                 `json:"timeout" valid:"required"`
   Version                 string                `json:"version" valid:"stringlength(1|32)"`
   VersionDescription      string                `json:"versionDescription" valid:"stringlength(1|1024)"`
   DeadLetterConfig        string                `json:"deadLetterConfig" valid:"stringlength(1|255)"`
   LatestVersionUpdateTime string                `json:"latestVersionUpdateTime" valid:",optional"`
   PublishTime             string                `json:"publishTime" valid:",optional"`
   BusinessID              string                `json:"businessId"  valid:"stringlength(1|32)"`
   MetaID                  string                `json:"id" valid:",optional"`
   FuncID                  string                `json:"func_id" valid:",optional"`
   FuncName                string                `json:"func_name" valid:",optional"`
   DomainID                string                `json:"domain_id" valid:",optional"`
   ProjectName             string                `json:"project_name" valid:",optional"`
   Service                 string                `json:"service" valid:",optional"`
   Dependencies            string                `json:"dependencies" valid:",optional"`
   RevisionID              string                `json:"revisionId" valid:"stringlength(1|20),optional"`
   CreateTime              string                `json:"created" valid:",optional"`
   VPCTriggerImage         string                `json:"vpcTriggerImage" valid:",optional"`
   EnableAuthInHeader      bool                  `json:"enable_auth_in_header" valid:"optional"`
   IsStatefulFunction      bool                  `json:"isStatefulFunction" valid:"optional"`
   DNSDomainCfg            []types.DNSDomainInfo `json:"dns_domain_cfg" valid:",optional"`
   IsStreamEnable          bool                  `json:"isStreamEnable" valid:"optional"`
   IsBridgeFunction        bool                  `json:"isBridgeFunction" valid:"optional"`
}
```



```
// ConfigurationSpec define configuration spec
type ConfigurationSpec struct {
   Layers                []*types.Layer              `json:"layers"`
   FunctionName          string                      `json:"functionName,omitempty"`
   TenantID              string                      `json:"tenantId,omitempty"`
   FunctionVersion       string                      `json:"functionVersion,omitempty"`
   FunctionURN           string                      `json:"functionUrn"`
   Description           string                      `json:"description,omitempty"`
   DeadLetterConfig      DeadLetterConfigSpec        `json:"deadletterConfig,omitempty"`
   Env                   []corev1.EnvVar             `json:"env,omitempty"`
   FunctionVersionURN    string                      `json:"functionVersionUrn,omitempty"`
   Handler               HandlerSpec                 `json:"handler"`
   Libs                  LibSpec                     `json:"libs,omitempty"`
   Resource              ResourceSpec                `json:"resource,omitempty"`
   Runtime               RuntimeSpec                 `json:"runtime"`
   AutoScalerConfig      AutoScalerConfig            `json:"autoScalerConfig,omitempty"`
   Version               VersionSpec                 `json:"version,omitempty"`
   Created               string                      `json:"created,omitempty"`
   LastModified          string                      `json:"lastModified,omitempty"`
   Timeout               int64                       `json:"timeout,omitempty"`
   ReversionID           string                      `json:"reversionId,omitempty"`
   BusinessID            string                      `json:"businessId,omitempty"`
   Initializer           Initializer                 `json:"initializer" valid:",optional"`
   Role                  Role                        `json:"role" valid:",optional"`
   FuncVpc               Vpc                         `json:"funcVpc,omitempty"`
   EndpointTenantVpc     Vpc                         `json:"endpointTenantVpc,omitempty"`
   VPCTriggerImage       string                      `json:"vpcTriggerImage,omitempty"`
   DomainID              string                      `json:"domain_id" valid:",optional"`
   LogTankService        logservice.LogTankService   `json:"logTankService" valid:",optional"`
   TraceService          trace.TraceService          `json:"traceService" valid:",optional"`
   FuncMountConfig       FuncMountConfig             `json:"mount_config" valid:",optional"`
   ProjectName           string                      `json:"project_name" valid:",optional"`
   CustomContainerConfig types.CustomContainerConfig `json:"custom_container_config" valid:",optional"`
   EnableAuthInHeader    bool                        `json:"enable_auth_in_header" valid:"optional"`
   IsStatefulFunction    bool                        `json:"isStatefulFunction" valid:"optional"`
   Heartbeat             types.Heartbeat             `json:"heartbeat" valid:",optional"`
   DNSDomainCfg          []types.DNSDomainInfo       `json:"dns_domain_cfg" valid:",optional"`
   IsStreamEnable        bool                        `json:"isStreamEnable" valid:"optional"`
   IsBridgeFunction      bool                        `json:"isBridgeFunction" valid:"optional"`
}
```



```
// FuncMetaData define meta data of functions
type FuncMetaData struct {
   FunctionVersionURN string `json:"functionVersionUrn"`
   IsStatefulFunction bool   `json:"isStatefulFunction" valid:"option"`
   IsBridgeFunction   bool   `json:"isBridgeFunction" valid:"optional"`
}
```

上述三个结构体均新增一个bool类型的字段

```
IsBridgeFunction bool   `json:"isBridgeFunction" valid:"optional"`
```

通过此字段来标识是否是bridge函数，bridge函数一定是有状态函数，有状态函数不一定是bridge函数



[poc设计2](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=da557af8104d4bb78bfbd42f69e19a94)![poc设计2](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2n9G?t=1667964965048)





**worker-manager中心缩容逻辑**

同样通过标识，识别到是bridge函数的情况下，不进行缩容处理



### 2.2模块详细设计

#### 2.2.1 functionbus子模块设计

##### 2.2.1.1模块实现

*以时序图的形式描述模块的功能需求和非功能需求的实现过程*

如2.1章节中所示

##### 2.2.1.2模块对外接口定义

*描述模块的对外接口（如果有），所有外部模块只关注模块接口，不关注模块实现*

### Runtime-Java新增接口

public String getBridgeFunc(String functionName, String instanceName) 

返回的String为stateKey 



public void release()

### Runtime-java 和bus中filter.proto和terminate.proto发生变化

filter.proto

```
syntax = "proto3";

package filter;

option go_package = "api/filter";

option java_package = "com.huawei.sn.msgstruct";
option java_outer_classname = "Filter";

message RequestPb {
    string key = 1;
    string funcName = 2;
    string stateID = 3;
    string podName = 4; -----新增
}

message ResponsePb {
    int32 code = 1;
    string message = 2;
    string stateID = 3;
    string stateKey = 4;-----新增
}
```



terminate.proto

```
syntax = "proto3";

package terminate;

option go_package = "api/terminate";

option java_package = "com.huawei.sn.msgstruct";
option java_outer_classname = "Terminate";

message RequestPb {
    string stateID = 1;
    string futureID = 2;
    int32 countOfFuncName = 3;
    string funcName = 4;
    string podName = 5; -----新增
}

message ResponsePb {
    int32 code = 1;
    string message = 2;
    string futureID = 3;
}
```



### state-manager新增接口

```
handleParamGetBridgeFunc := &server.HandleParam{
   HandleFunc: func(g *gin.Context) {
      getBridgeFuncStateKey(g.Writer, g.Request)
   },
   Path:   "/getBridgeFuncStateKey",
   Method: "POST",
}

server.RegisterRouter(handleParamGetBridgeFunc, "getBridgeFunc")

handleParamReleaseBridge := &server.HandleParam{
   HandleFunc: func(g *gin.Context) {
      releaseBridge(g.Writer, g.Request)
   },
   Path:   "/releaseBridge",
   Method: "POST",
}

server.RegisterRouter(handleParamReleaseBridge, "releaseBridge")
```

### functiontask新增方法类型

context中新增GetBridgeFunc和ReleaseBridge两个方法

```
type Context interface {
   LoggerContext
   ReqIDListContext
   StateQueueMapContext
   FutureStoreContext
   ExtFutureStoreContext
   ServiceIDContext

   TenantID() string

   CallTask(method int32, req RequestWithStateID, resp Serializable)
   CreateState(funcName string, stateID string) (string, string, common.TaskError)
   InvokeConfig(version string, callKernel bool, futureID string, requestAttributes map[string]int32) interface{}

   SetResult(resultStruct Serializable)
   SetResultWithLog(resultStruct Serializable, log string)
   SetResultWithCode(code int32, resultStruct interface{}, log string)

   IsSameNode(stateID string) bool

   Clone() Context
   GetBridgeFunc(funcName string, clientInstance bridgetypes.ClientInstance) (string, common.TaskError) -----新增
   ReleaseBridge(funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError)   -----新增
}
```



sdk/bridgefunc/funcsdk.go中新增两个方法

```
GetBridgeFunc(clientInstance bridgetypes.ClientInstance, traceID string) (string, snerror.SNError) 
ReleaseBridge(clientInstance bridgetypes.ClientInstance, traceID string) (bool, snerror.SNError)
```



communication.go中新增两个方法

```
ReleaseBridgeByHTTP(funcName string, clientInstance bridgetypes.ClientInstance, traceID string) (InvokeBridgeFuncResponse, snerror.SNError)
GetBridgeFuncByHTTP(funcName string, clientInstance bridgetypes.ClientInstance, traceID string) 
```



##### 2.2.1.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*

### 2.3 约束

#### 2.3.1上下游依赖需求

【serviceBridge】监听Bridge函数的异常，释放Bridge资源：AR.SR.IREQ2ecc3fc2.002.004
【serviceBridge】BridgeManager插件支持BridgeFunc的管理和扩缩容：AR.SR.IREQ2ecc3fc2.002.003
【serviceBridge】支持从etcd读取、监听BridgeFunc的配置和更新：AR.SR.IREQ2ecc3fc2.002.002
【serviceBridge】支持Bridge函数的创建：AR.SR.IREQ2ecc3fc2.002.001

 【functioncore】有状态函数支持单实例多并发

#### 2.3.2其他约束

目前只支持java语言的函数， 其他语言暂不支持

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

1.FunctionTask向BridgeManager插件申请stateKey异常、状态路由创建失败，主调函数会直接返回错误，不会产生异常流程

2. 用户函数crash，用户函数没来得及发送releaseBridge请求进行terminate处理的情况下，bridgeManager通过监听etcd /sn/workers路径被删除事件，

   进行/deleteState和删除redis哈希表中对应stateID的操作，bus监听到/sn/stateworkers路径被删除事件，判断redis哈希表中对应stateID不存在并且stateID对应的任务请求队列在本task的情况下，进行删除stateQueue中关于本次stateID信息的操作

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

本次数据面单独转测，因此用例主要针对如何单独测试数据面

| **序号** | **用例名**                                          | 用例类型 | **测试步骤** | **预期结果**                                                 |
| -------- | --------------------------------------------------- | -------- | ------------ | ------------------------------------------------------------ |
| 1        | getBridgeFunc函数正常调用                           |          |              | getBridgeFunc函数正常调用, 函数调用成功                      |
| 2        | getBridgeFunc异常调用                               |          |              | getBridgeFunc异常调用，从错误日志观察到错误原因              |
| 3        | release正常调用                                     |          |              | release正常调用，插件中的缓存信息被清理掉，路由以及redis等数据被清理掉 |
| 4        | release异常调用                                     |          |              | 日志打印调用异常                                             |
| 5        | bridge函数pod超过1分钟没有处理请求，pod不会被缩容掉 |          |              | bridge函数pod超过1分钟没有处理请求，pod不会被缩容掉          |
|          | 中心缩容场景                                        |          |              | bridge函数不会被中心缩容掉                                   |



