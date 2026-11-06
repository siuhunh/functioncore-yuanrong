[TOC]

# FunctionAccessor 与 BusProxy 有状态支持

关联AR:
+ ARID：AR.SR.IREQ02597546.038.007 

## Story 整体设计 

### 功能描述

1. FunctionAccessor 与 BusProxy 支持有状态调用。——12.03
2. FunctionAccessor 与 BusProxy 支持函数间调用。——12.03

### 场景分析

### 方案详细设计

#### 现状分析
#### 方案设计

![FunctionAccessor和BusProxy支持有状态](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1FoF?t=1638173760429)

相关适配修改：

1. FunctionAccessor 删除遗留拦截校验逻辑，保留 stateKey 传入 BusProxy。
2. 当前有状态函数调用不支持指定 cpu/memory 调度，发送扩容请求时取默认规格。

rdispatcher 接收前端传入的有状态key：

* stateKey: ${stateKey}

rdispatcher 解析后传入 FunctionAccessor：

* header: X-State-Key:{"cmd":"invoke","source":"rdispatcher","statekey":${stateKey}}

#### 开源软件选型
#### 外部交互分析&&上下游依赖需求

1. functiontask，statemanager 做好功能适配。—— 丁震
2. 各 runtime 做好接口适配。—— 殷瑞栋
3. 有状态函数调用依赖 rdispatcher 解析用户指定的 stateKey 传入数据面。—— 刘方凯
4. 前端界面适配，有状态函数开关，有状态 stateKey配置。—— 郑亚林

### 对外接口
### 约束
*特性约束描述*

stateKey 限制：

- 支持大小写字母和数字，最短1位，小于128位。

  正确示例：a、1、a1、A1

  错误示例：a$

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

| 序号 |       用例名       |                           测试步骤                           |                           预期结果                           |
| :--: | :----------------: | :----------------------------------------------------------: | :----------------------------------------------------------: |
|  1   | 有函数函数正常调度 | 1. 编写有状态函数<br>2. 指定有状态函数key(stateKey)，进行多次调度 | 1. 函数调度成功<br>2. 多次调用，状态发生变化(函数模板需更改状态) |
|  2   |  普通函数正常调用  |              1. 创建普通函数<br>2. 发起函数调度              |                       1. 函数调度成功                        |

