[TOC]

# AR.SR.IREQ02597546.033.001 Busproxy升级兼容（函数处理）

## Story 整体设计

### 功能描述
BusProxy升级过程中，BusProxy会重启，重启过程中worker不能退出。
+ worker在60s后仍无法连接到busproxy，则退出。
+ worker与busproxy失联后，等待busproxy重连，连接后，通知当前的并发数量及并发能力。
+ 当请求处理完成，worker通知busproxy当前的处理能力，及是否可以接收新请求。

### 场景分析
+ 用户请求正在执行中，functiontask退出且重启成功。
+ 用户请求正在执行中，functiontask退出且重启失败。

### 方案详细设计
#### 方案设计
+ Busproxy升级兼容（函数处理）

![AR.SR.IREQ02597546.033.001-Busproxy升级兼容（函数处理）](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/17D4?t=1634110761111)

1. FunctionAccessor收到Invoke请求，调用doRequestWithRetry发送给functiontask再调用workerBroker的doRequest，发送至worker执行Function
2. FunctionTask升级退出，worker与busproxy的连接断开EOF
3. worker等待busproxy重新连接，若超60s依然没有bus连接则退出
4. FunctionTask重启后主动与worker建立连接创建WorkerBroker
5. FunctionAccessor重新发送Invoke请求，functiontask在observe worker时向worker获取当前请求数量并记录
6. functiontask重新doRequest，执行结束后返回Response

#### 外部交互分析&&上下游依赖需求
无

### 对外接口
无

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

#### 可用性 Availability
*处于按要求执行状态的能力*  
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  
不涉及

## LLT 用例
|  序号 | 预置条件 | 预期结果 |
| ------------ | ------------ | ------------ |
|  1 | 用户请求正在执行中，functiontask退出且重启失败| 60s 后worker开始退出 |
|  2 | 用户请求正在执行中，functiontask退出且重启成功（函数Sleep30秒；函数元信息MinInstance: 1, MaxInstance: 1, ConcurrentNum： 1）| 重启完毕，待worker第一次请求执行完毕后，开始执行retry请求并在30s后返回结果 |
|  2 | 用户请求正在执行中，functiontask退出且重启成功（函数Sleep30秒；函数元信息MinInstance: 1, MaxInstance: 2, ConcurrentNum： 1）| 重启完毕，pod扩容到2个，开始执行retry请求并在30s后返回结果 |
