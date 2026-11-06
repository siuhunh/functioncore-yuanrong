[TOC]

# AR.SR.d5193e1e.007 【functioncore】worker 能够动态感知Pat Serverice IP更新

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

需求背景：来源于异常场景，Pat Service Pod 被删除后，重新拉起的 Pat Service Pod 对应的 IP 地址发生变化，当前 worker 无法感知 Pat Service Pod 对应的 IP 地址发生变化。

本需求针对解决的可靠性场景是：双网卡场景。在一个 worker 实例有两个网卡的情况下，其中一个故障了，worker 会切换到另一个网卡。但是当前故障的网卡信息没有被清理和替换，所以本需求就是做这个事情，来清理和替换故障的网卡信息。

### 1.2特性场景分析

*描述该特性的业务使用场景内容包括：*

（1）场景触发条件：Pat Service Pod 被删除后重启，Pat Service Pod 对应的 IP 地址发生变化

（2）关键任务操作：a. 更新 workermanager 本地缓存 b. 通知 worker c. worker 重建 IP 地址映射关系

## 2. 特性/功能实现原理

### 2.1总体方案

1. pat service pod 被删除，pod name 和 pod ip 都发生变化，vpc controller 插件**复用已有的 VpcChannel** 通知 workermanager 新老 pat service pod；
2. workermanager 获取到新老 pat pod，根据老的 pat pod，从缓存中获取到 pat pod 关联的 worker；使用新 pat pod name 更新缓存；
3. **workermanager 和 worker 之间新增一条链路**，用于 workermanager 向所有关联的 worker 发送一个请求，请求包含了新 pat pod ip 地址；
4. worker 收到 pat pod ip 地址之后，更新本地保存的 pat pod ip，依赖 ip ping 协程的健康检查机制，当 ping 不通老的 ip 时，重新建立和新的 pat pod ip 之间的映射关系；
5. 向所有关联的 worker 发送一个请求，如果因为网络等原因发送失败，则持续发送，间隔时间是 5s，直到成功或 worker 消亡。

[【functioncore】worker能够动态感知Pat Serverice IP更新-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=41c8261f53ae4798bbdb43b77b4da590&versionUUID=)![【functioncore】worker能够动态感知Pat Serverice IP更新](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Jdq?t=1659148978030)

### 2.2模块详细设计

#### 2.2.1 worker manager子模块设计

##### 2.2.1.1模块实现

1. pat service pod 被删除，pod name 和 pod ip 都发生变化，vpc controller 插件**复用已有的 VpcChannel** 通知 workermanager 新老 pat service pod；
2. workermanager 获取到新老 pat pod，根据老的 pat pod，从缓存中获取到 pat pod 关联的 worker；使用新 pat pod name 更新缓存；
3. **workermanager 和 worker 之间新增一条链路**，用于 workermanager 向所有关联的 worker 发送一个请求，请求包含了新 pat pod ip 地址；

[【functioncore】worker 能够动态感知Pat Serverice IP更新-workermanager-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=b5ca24ea044b4302a7d86e8f808be3cc&versionUUID=)![【functioncore】worker 能够动态感知Pat Serverice IP更新-workermanager](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2NCH?t=1659147835470)

##### 2.2.1.2模块对外接口定义

*描述模块的对外接口（如果有），所有外部模块只关注模块接口，不关注模块实现*

##### 2.2.1.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*

无

#### 2.2.2 worker 子模块设计

##### 2.2.2.1模块实现

1. worker 收到 pat pod ip 地址之后，更新本地保存的 pat pod ip，依赖 ip ping 协程的健康检查机制，当 ping 不通老的 ip 时，重新建立和新的 pat pod ip 之间的映射关系；
2. 向所有关联的 worker 发送一个请求，如果因为网络等原因发送失败，则持续发送，间隔时间是 1s，直到成功或 worker 消亡。

[【functioncore】worker 能够动态感知Pat Serverice IP更新-worker-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=88f1d9a10cd1470ba36389af53a003f8&versionUUID=)![【functioncore】worker 能够动态感知Pat Serverice IP更新-worker](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2NCR?t=1659597106353)

##### 2.2.2.2模块对外接口定义

复用已有的数据结构

##### 2.2.2.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*

无

### 2.3 约束

目前 pat service pod 的 ip 地址发生变化，只通知与其相关联的 worker 实例 pod。

#### 2.3.1上下游依赖需求

依赖管理面 vpc controller 插件通知 workermanager 消亡的 pat service pod 和 重新拉起的 pat service pod 、pod ip 信息

**管理面需求编号**：暂无

**通信方式**：由于 vpc controller 和 workermanager 共进程，通知方式：channel 通信

传递的**数据结构**是：

```go
// NATConfigure include nat configure info for worker
type NATConfigure struct {
	ContainerCidr  string              `json:"containerCidr"`
	HostVmCidr     string              `json:"hostVmCidr"`
	PatContainerIP string              `json:"patContainerIP"`
	PatVmIP        string              `json:"patVmIP"`
	PatPortIP      string              `json:"patPortIP"`
	PatMacAddr     string              `json:"patMacAddr"`
	PatGateway     string              `json:"patGateway"`
	PatPodName     string              `json:"patPodName"`
	TenantCidr     string              `json:"tenantCidr"`
	NatSubnetList  map[string][]string `json:"natSubnetList"`
    IsDeleted      bool                `json:"isDeleted"`     // 新增字段，用于记录 pat pod 是否被删除
}

// 新增数据结构，用于和 vpc controller 插件通信
type ChangedPatPodInfo struct {
	TenantID              string // 用于拼接 pat key
	PatPod                []types.NATConfigure
	EndpointPatPod        []types.NATConfigure
}
```

#### 2.3.2其他约束

NA

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

内部优化，不涉及

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

内部优化，不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

内部优化，不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

新的 Pat Pod IP 地址通知到 worker 后，worker 走之前重建 IP 地址映射的流程，重建失败依赖已有重试。

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

无

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

针对可靠性场景，一般场景不影响。

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

给 worker 发送消息，会启用一个 go 协程，用于通知 worker pat service pod ip 地址发生变化，发送完成后 go 协程就退出了。

使用协程池，控制协程的数量。

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部优化，客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

支持组件独立升级

## 4. 用例

| **序号** | **用例名**               | 用例类型 | **测试步骤**                                                 | **预期结果** |
| -------- | ------------------------ | -------- | ------------------------------------------------------------ | ------------ |
| 1        | 双网卡场景下，测试可靠性 | LLT      | 1. 向 worker 发送第一张 pat service pod 网卡 IP 地址发生变化的消息，观察 worker 是否会切换到第二张网卡，预期会切换到第二张网卡<br />2. 删除第二张 pat service pod 网卡，观察 worker 是否会切换到被代替的新的第一张 pat service pod 网卡 | 符合预期     |

*注：依赖 vpc 函数测试，ST 用例环境无法创建 vpc 函数。*

遗留问题：

- worker 通过健康检查来触发ip、route的重建，不要盲目进行所有的重建，影响已有的请求

本需求针对解决的可靠性场景是：双网卡场景。在一个 worker 实例有两个网卡的情况下，其中一个故障了，worker 会切换到另一个网卡。但是当前故障的网卡信息没有被清理和替换，所以本需求就是做这个事情，来清理和替换故障的网卡信息。

如果在清理和替换的过程中，第二张网卡也故障了，那么清理和替换过程就会中止，worker 实例也会退出，根据需要拉起新的 worker 实例。

- worker-manager 重启的场景下，vpc 插件通知的新老网卡信息丢失，导致无法通知到 worker，如何处理？

这个场景不做特殊处理，当新老网卡信息丢失时，此时 worker 已经切换到另一张网卡了，如果另一张网卡故障，则 worker 实例退出，根据需要拉起新的 worker 实例，使用新的网卡来处理请求。