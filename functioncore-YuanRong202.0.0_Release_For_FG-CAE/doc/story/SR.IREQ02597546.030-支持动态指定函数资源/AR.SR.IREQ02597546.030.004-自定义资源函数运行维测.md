[TOC]

# AR.SR.IREQ02597546.030.004 自定义资源函数运行维测

## Story 整体设计

### 功能描述
+ 不同规格实例数信息统计在redis中
+ 日志中返回资源（Memory）的描述

### 场景分析
+ 一个函数请求调用不同规格的实例，redis中记录实例个数总和。
+ 请求调用时指定不同规格，调用界面返回的日志中包含资源（Memory）的描述。

### 方案详细设计
#### 方案设计
+ 目前redis中保存实例信息如下：(详见需求：AR-20210630-013-001-支持函数统计信息输出对接)
  ![redis中保存实例信息](https://icommander.ssl.csitool.rnd.huawei.com/host2/csi-tool-pro/pro-icommander/b028cacebabf4fd7b61d484b49b4c198/image.png?AWSAccessKeyId=GWQRKFEFDMSUHM36WN7V&Expires=1629966193&Signature=vNAL5EQmlHhZNbVSBLqRS2jOQ6I%3D)
  
  key为 "metrics_function_instance_" + ClusterID
  
  value为map格式 (key: FuncKey value: InstanceNum)
  
  当前存储的实例个数仅为相同规格，现需支持同一函数不同规格合并统计数量（如：函数Function01 拥有规格300-128和400-256 统计InstanceNum为2）


+ 当前界面端返回的日志信息如下：
  ![AR.SR.IREQ02597546.030.004-自定义资源函数运行维测](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/11KQ?t=1629965038781)
  
  需要在日志中新增函数调用的Memory规格（如：billing memory：128MB）
#### 外部交互分析&&上下游依赖需求
+ 依赖需求：AR.SR.IREQ02597546.030.001-FuncInstanceManager动态指定函数资源

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
|  1 | 调用不同规格函数 查看redis中存储实例个数| 返回正确数量 |
|  2 | 页面上同一函数指定调用不同规格 查看返回日志信息| 返回指定的billing memory |
