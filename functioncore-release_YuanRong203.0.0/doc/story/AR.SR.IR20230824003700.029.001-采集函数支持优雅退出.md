[TOC]

# AR.SR.IR20230824003700.029.001 【functioncore】采集函数支持优雅退出

## 1. 需求场景分析

场景：函数在退出前，需要发个信号给业务，做一些处理，例如：资源回收，断开连接，发送告警事件等，预计需要时间1min

### 1.1 特性需求来源与价值概述

需求来源：在函数实例销毁时，需要能够通知在该函数实例内运行用户函数业务，做一些业务处理，例如：资源回收，断开连接，发送告警事件等

价值概述：提升竞争力，支持在函数实例销毁前调用客户配置的preStop钩子函数

### 1.2特性场景分析

函数元数据新增PreStop钩子函数配置，有该配置的函数实例在销毁时执行其配置的preStop钩子函数，逻辑如下：

[preStop钩子函数-Open Model](https://clouddragon.huawei.com/cloudmodeling/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=5a94e8a443114d92a15088bcab6d7165&versionUUID=)![preStop钩子函数](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/ffd4ac183c3e4a69ae91e5acd23c78d4?t=1697022059771)

函数实例销毁正常场景：
1. 用户修改函数代码配置；
2. 函数实例最后一个请求处理结束，1分钟内没有收到请求；不会写计费信息
3. 预留实例数减少；
均是 worker-manager 发送 /shutDown 请求给函数实例 worker
4. 中心缩容

## 2. 特性/功能实现原理

### 2.1 PreStop钩子函数定义和新增配置

1. 一个最简单的PreStop方法定义如下所示

```
def preStop(context):
   print("preStop invoked")
```
只有一个参数context，并且不支持返回值

2. 函数元数据新增PreStop钩子函数配置，该配置在extendedMetaData下，跟initializer平级，json格式如下

```
   "extendedMetaData" : {
        "pre_stop" : {
           "pre_stop_handler": "index.preStop"
           "pre_stop_timeout": 30
        }
    }
```
注：pre_stop_timeout 最大90s

### 2.2 worker-manager 与 worker 通讯修改

1. worker-manager 发送 /load 特化请求到 worker 需要新增 pre_stop 配置

2. worker-manager 发送 /shutdown 实例退出请求到 worker 需要添加ak/sk/Token请求头，如下：
```
agency := utils.GetUserAgencyFromCacheMap(tenantID, domainID, agencyID)
req.Header.Set(constants.HeaderAccessKey, agency.AccessKey)
req.Header.Set(constants.HeaderSecretKey, agency.SecretKey)
req.Header.Set(constants.HeaderAuthToken, agency.Token)
```
worker收到/shutdown路径请求时提取这三个参数，共PreStop钩子函数的Context.GetAccessKey()等使用

### 2.3 worker 修改

1. Worker Runtime添加PreStop接口，并在各个Runtime内实现该接口

```
PreStop(conf *config.Configuration) error
```

2. 计费支持 
  

PreStop钩子函数执行耗时也计入费用，费用计算规则跟普通函数执行计费规则一致
注：计费信息需要通过TimeWindows异步上报到redis（单独上报该计费）

3. 日志支持

PreStop钩子函数执行打印日志进行采集上报，客户可在函数日志界面查看到改日志，日志打印如下：
```
2023-10-11T08:07:58Z Start PreStop request 'fe143873-ebdd-4f02-a6a5-84dd6606549c', version: latest
2023-10-11T08:07:58Z fe143873-ebdd-4f02-a6a5-84dd6606549c INFO preStop invoked
2023-10-11T08:07:58Z Finish PreStop request 'fe143873-ebdd-4f02-a6a5-84dd6606549c', duration: 2.912ms, billing duration: 3ms, memory used: 60.562MB, billing memory: 256MB
```

4. PreStop失败

PreStop执行超时，其他失败情况跟函数执行过程中出现的情况保持一致，例如内存超限、磁盘超限；
PreStop失败时仅打印日志，无法随函数调用请求返回错误信息，并且不进行重试，有且仅执行一次；
PreStop失败不影响实例销毁，不返回错误信息给WorkerManager；

5. 检测函数实例是否冻结

在预留实例且未开启闲置模式以为的其他场景，函数实例在最后一次请求处理完成返回后3秒没有收到新的请求，会冻结runtime进程，所以在执行PreStop时需要先解冻；

### 2.4 worker 与 runtime 通讯

1. rpc.proto 新增PreStop消息定义
```
// PreStopRequest is request to invoke preStop function in runtime
// worker ===> runtime
message PreStopRequest {
    // Unique ID of invocation
    string traceID = 1;
    map<string, string> extra = 2;
}

// PreStopResponse is response of function preStop in runtime
// runtime ===> worker
message PreStopResponse {
    Error error = 1;
}
```
新增PreStop RequestType(19)和ResponseType(20) 

### 2.5 runtime修改

1. runtime在load阶段加载PreStop函数（如果配置），同时检验PreStop函数是否只配置了一个入参context
  
2. runtime 收到 worker发送的 PreStop 请求时执行用户配置的PreStop函数，执行完成后返回响应给 worker

### 2.6 约束

#### 2.6.1 上下游依赖需求

管理面前端高级设置添加PreStop配置，并且管理面支持写入PreStop配置到元数据ETCD 

#### 2.6.2 runtime支持

本需求优先支持Java Runtime

## 3. 可信软件

### 3.1安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*

不涉及目录权限、进程权限等相关安全内容修改，安全性影响可控

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*

内部可靠性优化，不涉及对外功能变动

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

PreStop仅在函数销毁时执行，不涉及

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

不涉及

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

无

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

新增该场景相关用例

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

无

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

未增加资源消耗；

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

新增PreStop钩子函数介绍资料

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

新增功能，不影响升级

## 4. 用例

| **序号** | **用例名**                           | 用例类型 | **测试步骤**                                            | **预期结果** |
| -------- | ---------------------------------- | -------- | ----------------------------------------------------- | ------------ |
| 1        | PreStop函数实际不存在                 | UT      | 创建函数，添加PreStop配置，但是实际不存在                   | 函数执行返回PreStop不存在错误信息 |
| 2        | PreStop参数个数错误                  | UT      | 创建函数，添加PreStop配置，PreStop函数参数非1个              | 函数执行返回PreStop参数个数异常 |
| 3        | 修改配置触发PreStop                  | UT      | 创建函数，添加PreStop配置，PreStop函数正常，触发函数执行，然后修改配置触发销毁  | 函数正常执行，PreStop日志在函数日志能够查看到 |
| 4        | 修改函数代码触发PreStop               | UT      | 创建函数，添加PreStop配置，PreStop函数正常，触发函数执行，然后修改代码触发销毁  | 函数正常执行，PreStop日志在函数日志能够查看到 |
| 5        | 一分钟销毁触发PreStop                 | UT      | 创建函数，添加PreStop配置，PreStop函数正常，触发函数执行，然后一分钟不调用 | 函数正常执行，一分钟后实例销毁，PreStop日志在函数日志能够查看到 |
| 6        | PreStop context提供环境变量/AK/SK/Token  | UT    | 创建函数，添加PreStop配置，PreStop函数打印AK/SK/Token，触发函数执行，然后修改代码触发销毁  | 函数正常执行，PreStop日志能够看到环境变量/AK/SK/Token等信息 |
| 7        | PreStop执行超时                      | UT      | 创建函数，添加PreStop配置（超时3s），PreStop函数执行10s，触发函数执行，然后修改配置触发销毁  | 函数正常执行，在函数日志能够查看到PreStop |
