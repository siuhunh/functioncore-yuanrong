[TOC]

# AR.SR.IREQ02597546.041.001 异步函数调用支持停止已运行的函数

## Story 整体设计

### 功能描述
+ 函数支持停止（cancel）已运行的函数

### 场景分析
+ 用户通过先前调用时返回的request id申请停止该函数的执行；
+ 如果该函数有调用其他子函数，默认子函数也会被停止；
+ 暂时只支持单实例单并发模式，直接杀死runtime。单实例多并发预留接口，未来由各语言runtime自行实现。

### 方案详细设计

**管理面触发cancel**

1. 管理面将cancel请求写入元数据etcd，格式为，超时时间12小时（异步函数最大执行时间）

    key: 
    /sn/cancel/[request-id]

    value:
    ```json
    {
        "recursive": true, // 是否停止正在调用的子函数，默认为true
        "force": true, // 是否直接杀死runtime，默认为true。该值为单实例多并发预留
    }
    ```

2. proxy正常调用时会记录自己收到的请求，
    1. 在该请求发往worker时记录workerID
    2. 在该请求成功发往其他proxy时删除记录
3. 当proxy订阅到etcd中的cancel数据时，
    1. 如果请求未到worker，则将request中的Canceled字段写为true，请求回到proxy时（从FunctionInstanceManager）判断Canceled是否为true，是则直接返回
    2. 如果请求已到worker，发送cancel请求到相应worker
4. worker正常调用时记录正在执行的请求所调用的其他子函数的request-id（future-id）
5. worker收到来自proxy的cancel请求时，
    1. recursive为true则返回该请求正在调用的其他子函数的request-id（future-id）给proxy，否则返回一个空列表
    2. force为true时自定义镜像函数worker自行退出，其他类函数发送restart命令给runtime manager，runtime manager杀死runtime
6. proxy收到worker的返回后将子函数的request-id（future-id）写成etcd中一条新的cancel数据，供其他proxy cancel，超时时间12小时（异步函数最大执行时间）
8. 管理面收到了请求的结果后删除etcd中的元数据

**函数触发其他函数的cancel**

各语言runtime接口暂不开放，流程类似，worker发送cancel请求给proxy后，如果被该proxy所认识，则直接cancel，否则写成etcd中一条新的cancel数据，供其他proxy cancel。

#### 外部交互分析&&上下游依赖需求

管理面将用户的cancel请求写入元数据etcd

### 对外接口

1. 取消的异步函数执行结果的返回码为4028
2. 函数停止后计费结束

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

1. 管理面应校验request id是否属于该用户正在调用的请求
2. 函数触发其他函数的cancel时应校验request id是否属于该函数正在调用的请求

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

**proxy异常重启、升级**
proxy启动时会向所在节点的worker建链并询问这些worker正在执行的请求。恢复后才会正常订阅cancel数据

**请求在FunctionInstanceManager中proxy异常**
链接断开，frontend会往其他proxy重试

#### 可用性 Availability
*处于按要求执行状态的能力*  
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  
不涉及

## LLT 用例
|  序号 | 预置条件 | 预期结果 |
| ------------ | ------------ | ------------ |
|  1 | 执行函数，函数代码开始运行后cancel | cancel成功，子函数同被cancel |
|  2 | 执行函数，函数仍在特化时cancel | cancel成功 |
|  3 | 执行函数，函数执行的过程中proxy重启，重启后cancel | cancel成功，子函数同被cancel |
|  4 | 执行函数（超过15分钟），函数代码开始运行后cancel | cancel成功，子函数同被cancel |
|  5 | 执行函数(超过15分钟），函数执行的过程中proxy重启，重启后cancel | cancel成功，子函数同被cancel |
