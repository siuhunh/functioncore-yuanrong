[TOC]

# 函数日志关联性优化

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

需求来源：客户需求

需求描述：当函数调用失败时，能通过请求id关联到函数load、init日志

需求价值：用户体验提升

### 1.2 特性场景分析

当用户函数调用失败时，能方便的通过请求id来追溯到函数load、init日志，进行故障排查。

## 2. 特性/功能实现原理

### 2.1 现状

**invoke失败场景：**

[函数调用失败](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=4021d3c7749945159ecb26a0787a3fde)![函数调用失败](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2g0R?t=1663256424168)

在函数监控页面查看其日志：

[函数相关日志](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=dd64929096eb4cd6ab23d8c620e757d8)![函数相关日志](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2g0S?t=1663256518430)

可以看到本次冷启动产生了3条日志，请求ID各不相同：

- 蓝色框内的2条日志分别为load、init日志，它们对应的请求ID并非管理面下发的traceID，而是worker在loadfunction时内部生成的UUID；
- 红色框内的日志是invoke调用日志，它对应的请求ID是管理面下发的traceID；



用户的直观感受：

- 从请求ID来看，很难直接将最后一条调用失败的日志与前两条load、init日志进行关联，有疑惑；



**init失败，导致invoke失败：**

[init失败](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=dd6f8f6a0d324f04b4da095a29b7d846)![init失败](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2k5T?t=1664306855353)

用户遇到的问题：

- 用户在页面/api返回体中能看到部分init失败原因，只是缺乏具体的Traceback信息；
- 若要继续追溯日志，用户第一反应是用invoke id去LTS中检索，结果无任何日志；故用户也获取不到instanceId去唯一确定init日志。

### 2.2 总体方案

**目标方案：全面优化日志布局呈现，提升用户体验**

- 拉管理面SE、数据面SE、产品经理讨论，对日志呈现要做成什么样子，尚未达成一致；
- 例如是否要在日志页面对用户呈现instance概念、如何呈现并引导用户定位，需要产品经理石敏、继光细化讨论，给出目标方案；
- 目标方案在实现时还依赖管理面配合，且数据面当前的init、invoke阶段就是异步执行的，也很难直接关联；

基于如上困难点，与继光达成一致，本需求中先通过短期方案，解决用户最痛点的问题，即：**init失败导致invoke失败后，用invoke id检索不到任何日志，来进行进一步的init日志关联检索。**



**短期方案：在init失败时，也补充invoke日志信息，辅助用户定位**

- 当init失败时，会默认补充一条invoke id相关的日志
- 用户通过invoke id，能够在LTS中找到对应日志，并从中提取instanceid
- 用户根据instanceid + stage做过滤检索，即可以关联上之前的init日志

[效果展示](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=6d24e42561cd4cd5abbb3e2a50e3b2c8)![效果展示](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2k5U?t=1664307995083)



### 2.3 模块详细设计

#### 2.3.1 worker子模块设计

##### 2.3.1.1模块实现

1. worker在收到bus的invoke请求时，若检测到init失败，不直接返回，而是记录一条内容为空的invoke日志
1. 该invoke日志会被采集到LTS中，虽然内容为空，但是日志字段包含了关键的instanceid信息，用户基于此可以进行后续的init日志关联

##### 2.3.1.2模块对外接口定义

不涉及新增接口

##### 2.3.1.3模块类设计

不涉及新增类

### 2.3 约束

#### 2.3.1上下游依赖需求

不涉及

#### 2.3.2其他约束

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

需补充资料来指导用户进行日志的关联定位。

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名**   | 用例类型 | **测试步骤**                            | **预期结果**                                    |
| -------- | ------------ | -------- | --------------------------------------- | ----------------------------------------------- |
| 1        | 普通实例调用 | ST       | 多runtime，构造init失败，观察其调用日志 | 返回的request id在LTS中可以检索默认的invoke日志 |
| 2        | 预留实例调用 | ST       | 多runtime，构造init失败，观察其调用日志 | 返回的request id在LTS中可以检索默认的invoke日志 |