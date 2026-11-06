[TOC]

# 【数据面】java runtime提供heartbeat回调，检测java卡死场景 

## Story 整体设计

### 功能描述
目前只有针对runtime进程是否存在以及runtime异常时检测runtime是否oom的机制，没有针对java进程是否卡死异常的检测， java runtime提供heartbeat回调，支持检测java卡死场景

### 场景分析
+ 针对java类型函数，用户创建函数之后，开启心跳检测配置，配置心跳检测函数入口之后，worker在特化阶段将心跳检测函数通过环境变量传递给runtime-java 。worker通过定时发送心跳探测请求给runtime-java进行健康状态, runtime-java在自己的心跳检测函数里面反射调用用户配置的心跳检测函数，然后将检测结果返回给worker。 针对进程卡死造成6次没有收到心跳响应以及一次收到心跳响应不正常的场景，worker主动给runtime-manager发送kill掉runtime-java的请求，runtime-manager就会执行kill掉runtime-java的操作。

### 方案详细设计

#### 现状分析
当前现状是: 

1. runtime-manager有检测runtime进程是否存在的机制
2. worker向runtime-manager报错进程异常之后，当runtime-manager会去检查runtime是否oom并进行相应的处理

#### 方案设计

[【doc】【functioncore】java runtime提供heartbeat回调，检测java卡死场景](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=21965f5978c34f4f88c4add68181e341)![【doc】【functioncore】java runtime提供heartbeat回调，检测java卡死场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1ucG?t=1653103986463)



1. 用户在页面配置心跳检测函数入口，管理面更新etcd中函数的元数据信息
2. worker-manager监听到etcd中数据变化之后, worker manager在Worker初始化流程中，将心跳检测函数配置项HeartbeatHandler下发到worker
3. worker发送load请求的时候通过环境变量func-FAAS_HEARTBEAT_HANDLER传递心跳检测函数路径给runtime-java
4. worker特化完成之后，判断如果runtimeConfig中language包含java关键字同时配置了心跳检测函数配置时，则启动对应的心跳检测协程
5. worker发起rpc请求调用runtime-java心跳探测接口
6. runtime-java根据请求type识别到是心跳请求之后，根据心跳函数信息进行反射调用
7. 将响应信息返回给worker
8. 当worker 6次未收到runtime心跳信息或者1次收到runtime心跳信息不正常时,  发送KillRuntimeProcess类型事件给runtime-manager，runtime-manager kill掉runtime进程



正常情况下，心跳调用流程

[【doc】【functioncore】java runtime提供heartbeat回调，检测java卡死场景](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=21965f5978c34f4f88c4add68181e341)![【doc】【functioncore】java runtime提供heartbeat回调，检测java卡死场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1ucG?t=1653103986463)

1. 定时检测时间到时，当判断runtime当前状态为非RuntimeProcessStatusRunning或者非RuntimeProcessStatusRecover状态时，则不执行此次心跳探测，也即worker不会给runtime-java发送rpc请求，runtime因为3秒内没有请求导致cpu冻结的场景下，runtime状态为RuntimeProcessStatusPaused状态，此时worker会跳过此次心跳检测

   若此时runtime正在重启，则此时runtime状态也不为RuntimeProcessStatusRunning或者RuntimeProcessStatusRecover状态， 此时worker会跳过此次心跳检测

2. 定时检测时间到时，判断runtime当前状态为RuntimeProcessStatusRunning或者RuntimeProcessStatusRecover状态时， worker发送消息类型为HealthRequestType的请求给runtime-java

3. runtime-java获取环境变量func-FAAS_HEARTBEAT_HANDLER值进行反射调用

4. 若此次用户配置的心跳函数返回true，则代表此次心跳执行正常，runtime-java返回ok给worker

5. worker定时间隔5秒重复执行上述流程



异常情况下，心跳调用流程

[【doc】【functioncore】java runtime提供heartbeat回调，检测java卡死场景](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=21965f5978c34f4f88c4add68181e341)![【doc】【functioncore】java runtime提供heartbeat回调，检测java卡死场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1ucG?t=1653103986463)

 

1. worker发送消息类型为HealthRequestType的请求给runtime-java
2. 5秒调用一次， 调用超时时间3秒，若worker 6次没收到心跳响应或者1次收到心跳响应代表不健康，worker则发送KillRuntimeProcess事件通知runtime-manager 杀掉runtime进程

# 开源软件选型

无

#### 外部交互分析&&上下游依赖需求
【数据面】workmanager/worker支持配置心跳 

【管理面】支持函数配置heartbeat检测 

### 对外接口
*描述对外新增的接口*
+ 无
### 约束
1.心跳检测函数返回值为bool类型

2.心跳检测函数暂不支持带参数

2.目前只支持runtime-java心跳检测，其他语言不支持

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

#### 可用性 Availability
*处于按要求执行状态的能力*

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*

## LLT 用例
| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
| 1 | Java配置正常心跳检测函数冷启动 | 1.Java函数配置正常心跳检测函数<br />2.冷启动函数 |函数成功执行，runtime向worker上报正常心跳（runtime日志打印）|
| 2 | Java配置心跳检测函数冷启动 | 1.Java配置心跳检测函数<br />2.冷启动函数 |runtime向worker多次上报异常心跳（runtime日志打印）,  runtime-manager kill掉runtime进程|
| 3 | Java配置心跳检测函数冷启动 | 1.Java配置心跳检测函数<br />2.冷启动函数 |构造runtime进程DTZ僵死状态，worker检测到心跳异常后通知runtime-manager kill掉runtime进程|

