[TOC]

# AR.SR.IR20230331002271.001.002-worker-manager 提供在存储资源充足的节点，主动拉起10G临时存储 pod 的能力

> 父需求设计文档（支持10G临时存储）：https://codehub-y.huawei.com/YuanRong_Kernel/functioncore/files?ref=FunctionGraph_master&filePath=doc%2Fstory%2FAR.SR.IREQcfc64ab5.008.002-%E6%94%AF%E6%8C%8110G%E4%B8%B4%E6%97%B6%E5%AD%98%E5%82%A8.md&isFile=true

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

**客户需求：**

现状：极端场景，**可用pool pod都落在有存储风险的节点上，但是集群内依然有存储充足的节点（上面正好没有pool pod），此时无法响应10G用户函数的请求** 

诉求：worker-manager 提供在存储节点创建 10G pod 的能力，解决大规格临时存储pod可用性遗留问题

![可靠性问题](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/3vUB)

### 1.2 特性场景分析

> 父需求SR.IR20230331002271.001——worker-manager实例维护能力可用性增强，支持差异化资源池弹性权重，完善支持10G临时存储pod能力，触发保持一致。

(1）场景触发条件及对象：当用户在函数设置页面打开“临时存储扩容”开关后，会将该函数对应的实例都更新，新实例的/tmp目录可写上限为10G；

(2）使用时间及频度：用户配置其高级功能，并触发新实例调度时；

## 2. 特性/功能实现原理

### 2.1 总体方案

#### 2.1.1 设计目标和原理

##### 2.1.1.1 设计目标

- **功能性：**能够根据用户的配置，动态为其调整函数实例的/tmp目录的可用上限大小
- **可靠性：**在满足用户诉求的同时，保证宿主机的磁盘不被打爆
- **性能：**建议沿用pool池起的机制，避免现拉实例对弹性能力的影响（极端场景会存在新创pod情况）
- **成本：**在性能、成本之间做平衡，避免成本飙升
- **平滑升级改造：**现网的Default节点、CA扩容节点有平滑改造的方案

##### 2.1.1.2 设计原理

**需保证节点资源充足（Cpu、Memory、ephemeralStorage）的情况下，能创建出pod！**

剩余资源满足要求：尝试创建pod，使用k8s提供的调度能力（nodeSelector），创建Pod的时候将 `nodeSelector` 字段添加到 Pod 的规约中，Kubernetes 只会将 Pod 调度到拥有你所指定的每个标签的节点上。


#### 2.1.2 方案概览

![整体方案](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/3vaI)

1. worker-manager自身缓存会维护各个node资源信息（cpu、memory、以及**remainEphemeralStorage**）。

2. 当有个10G函数请求进来切pool中无可用的pod时，会根据node目前情况尝试去创建临时Pod。

   2.1 目前剩余资源满足要求：尝试创建pod，使用k8s提供的调度能力（nodeSelector），创建Pod的时候将 `nodeSelector` 字段添加到 Pod 的规约中，Kubernetes 只会将 Pod 调度到拥有你所指定的每个标签的节点上。

   2.2 目前剩余资源不满足要求：不会创建pod，打印资源不足相关日志。

### 2.2 模块详细设计

#### 2.2.1 数据面子模块设计

![时序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/3vZR)

**核心流程：**

worker-manager**初始化**时：

1. 从ETCD中获取node资源信息；
2. 初始化构建缓存；

worker-manager获取**node更新事件**时：

1. 更新缓存

worker-manager接受到**扩容请求（10G函数）**时：

1. 查询缓存
2. 返回无风险的节点列表
3. 符合要求的pod（流程如上图）
4. 不符合要求的pod（流程如上图）

worker-manager接受到**缩容请求（10G函数）**时：

1. 更新缓存

### 2.3 约束

- 10G大小固定，暂不支持用户自定义；
- 配置开关改变时，已有的函数实例要重建；
- 计费暂不进行设计；
- 本特性先不考虑与异构规格VM的交叉；
- 非pool池起的函数实例暂不支持；

### 2.4 工作量评估

## 3. 可信软件

### 3.1安全性（Security） 

不涉及

### 3.2韧性（Resilience）

worker-manager故障重启后，会根据K8S事件重建Node缓存，作为特化时的选择依据；

### 3.3隐私性（Privacy）

配置项不涉及用户相关敏感数据；

临时存储检测时不打印租户ID等用户敏感信息；

### 3.4可靠性（Reliability）

若本集群的所有节点都有临时存储被打爆的风险，则多集群重试兜底；

worker-manager故障重启，支持缓存重建；

支持缓存定时校准；

### 3.5可用性（Availability）

不涉及

### 3.6安全（Safety）

不涉及

### 3.7可维护性

不涉及

### 3.8可测试性

黑盒测试场景有关键的维测日志；

LLT覆盖率达到85%；

### 3.9性能

10G函数在特化选择前多一步过滤动作，通过缓存实现，相对于其他备选方案，对性能影响最小；

### 3.10资源

复用etcd key，未加重etcd负载；

扩展Node缓存，新增字段即可；

定时校准Node缓存，多一个定时协程，遍历更新时通过Node名称、Pod标签做过滤，避免短时间消耗大量内存；

### 3.11资料

涉及，管理面统一对外提供；

### 3.12升级兼容性

## 4. 用例

| **序号** | **用例名**                                                   | 用例类型 | **测试步骤**                                                 | **预期结果**                                                 |
| -------- | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | 资源充足，但是pool中无可用pod                                | 手工黑盒 | 创建10G函数                                                  | 函数创建成功                                                 |
| 2        | storage资源不充足，但是pool中无可用pod                       | 手工黑盒 | 创建10G函数                                                  | 函数创建失败（单集群会创建失败，多集群会转发请求），worker-manager日志中有相关提示 |
| 3        | 资源充足，但是pool中无可用pod（pod还未特化删除worker-manager——优雅删除） | 可靠性   | 创建10G函数，当pod还未running时，优雅删除worker-manager（主） | 函数返回失败，pod会被删除                                    |
| 4        | 资源充足，但是pool中无可用pod（pod还未特化删除worker-manager——强删） | 可靠性   | 创建10G函数，当pod还未running时，强制删除worker-manager（主） | 函数返回**超时**，pod会在新worker-manager（主）起来被删除    |
| 5        | 资源充足，但pool中无可用pod（pod特化后，删除worker-manager——优雅删除） | 可靠性   | 创建10G函数，当podrunning（已经特化）时，优雅删除worker-manager（主） | 函数返回成功，pod会被删除                                    |
| 6        | 资源充足，但pool中无可用pod（pod特化后，删除worker-manager——强删） | 可靠性   | 创建10G函数，当podrunning（已经特化）时，强删worker-manager（主） | 函数返回成功，pod会在新worker-manager（主）起来一段时间不用会被删除 |
