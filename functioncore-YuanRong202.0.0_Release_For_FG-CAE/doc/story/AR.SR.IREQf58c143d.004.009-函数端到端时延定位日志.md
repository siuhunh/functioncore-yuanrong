[TOC]

# 函数时延端到端日志打印

## 1. 需求场景分析

### 1.1特性需求来源与价值概述

需求来源于云短信业务时延分析，当前数据面日志打印存在两个问题：

1.frontend日志打印不全，存在日志丢失现象

2.时延数据无法在LTS上快速检索，例如快速找到某个函数大于1s的请求

问题影响时延问题的快速定位定界，需要优化，提升时延相关问题的定位定界效率

### 1.2 场景分析

1.函数调用由RDispatcher发给Frontend进入数据面，经过的链路如下

[函数调用](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=4c171cc9f792464db90a8b0856256b48)![函数调用](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Fp3?t=1657593587049)

## 2. 特性/功能实现原理

### 2.1总体方案

#### 2.1.1Frontend日志打印不采样

当前frontend日志打印采用zaplog开源框架，日志采用zaplog的采样打印能力，在QPS比较高时，存在日志丢失现象

关闭frontend日志采样，打印所有日志，这块数据面单独压测，日志不会丢失。

#### 2.1.2functioncore日志格式变更

为了支持时延数据能在LTS上快速检索，需要修改functioncore日志打印格式，支持LTS格式化处理，类似管理面组件的日志

![img](file:///C:/Users/c00464709/AppData/Roaming/eSpace_Desktop/UserData/c00464709/imagefiles/originalImgfiles/BFF703D1-E943-44AD-A82C-6A8E44D4D037.png)

修改后的普通日志打印格式为：

时间戳|日志级别|代码行|POD name|集群|content

2022-11-08 02:34:04.575|INFO|frontend/function_instance_manager.go:246|frontend-57554fdf9-n5z4z|cluster001|deployed address of defaultaz-#-pool22-300-128-fusion-6f5598f8d6-x5szl@7.218.90.21:22423 is 7.218.90.21

修改后的时延日志打印格式为：

时间戳|日志级别|代码行|POD name|集群|函数名|租户名|TraceID|时延|组件时延|bus node ip|worker pod name|innerCode |content

2022-11-11 03:01:32.104|INFO|frontend/function_handler.go:137|frontend-7c7ffc4fd4-rs55q|cluster001|0@base@testyrjava11001|7e1ad6a6-cc5c-44fa-bd54-25873f72a86a|f3e463a7-a311-4b2f-b224-770939922040|0.015452|7.218.100.25|defaultaz-#-pool35-6600-8192-fusion-d8d7dd9c6-nmr27|innerCode 0|invoke end in frontend, frontend cost 0.000056 bus cost 0.000143 worker cost 0.015253

#### 2.1.3frontend、functionbus时延日志优化

当前frontend日志只打印了整体耗时以及自身的耗时，没有打印functionbus、worker的耗时，这块可以优化

1.functionbus将worker耗时返回frontend

2.frontend增加functionbus、worker的耗时打印

2022-11-14 08:33:39.363|INFO|frontend/function_handler.go:138|frontend-77d65b75f9-fs7gq|cluster001|0@base@testyrjava11001|7e1ad6a6-cc5c-44fa-bd54-25873f72a86a|8fc46485-5c19-4db1-aab2-fbe4a3b06c0c|0.017308|0.001463:0.002010:0.013835|7.218.96.124|defaultaz-#-pool23-400-256-fusion-68d6db5875-zbdqf|innerCode 0|invoke end in frontend

3.functiontask耗时打印

2022-11-14 08:33:40.539|INFO|fasthttpserver/server.go:176|functiontask-2btcf|cluster001|0@base@testyrjava11001|7e1ad6a6-cc5c-44fa-bd54-25873f72a86a|8fc46485-5c19-4db1-aab2-fbe4a3b06c0c|0.014048|0.000214:0.000191:0.013835|invoke end in bus

frontend与functiontask打印时间单位统一为s，支持LTS大小搜索

3.worker耗时打印

2022-11-14 08:35:25.211|INFO|invoke/server.go:661|pool23-400-256-fusion-68d6db5875-zbdqf|0@base@testyrjava11001|7e1ad6a6-cc5c-44fa-bd54-25873f72a86a|5c450915-7ec7-4b92-a3c5-2fa99917b806|0.002647|0.000049:0.002598|invoke end in worker, invokeID 5

### 2.3 约束

#### 2.3.1上下游依赖需求

日志检索依赖LTS格式化能力，当前LTS还未支持

#### 2.3.2其他约束

1.worker cost是最后一次worker请求执行的时间，如果有在worker进行重试的场景，这个时间会不准

2.当前贵阳一已经关闭日志采样，但是短信压测时，LTS上frontend日志丢失还是比较严重。

关闭采样后，数据面可以确保日志全量打印。

当时进后台，看到4fb7f80b-2089-4abb-8288-5c6c8d85949d这个traceID有打印，LTS上rdispatcher也有对应日志打印，但是LTS上数据面没有这条日志，怀疑日志采集上报这块有性能问题，导致日志丢失。

这块后续压测和张睿一起定位

## 3. 可信软件

### 3.1安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*

内部优化，不涉及

### 3.2韧性（Resilience）

内部优化，不涉及

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

内部优化，不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

内部优化，不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

无

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

无

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

本地对性能测试，单个frontend 2万QPS，日志整改前后性能基本无差异

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

无新增资源消耗

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部优化，客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

workerCost字段不填无影响，frontend、bus可独立升级，

### 3.13后续演进

## 4. 用例