[TOC]

# AR.SR.IREQf58c143d.001.002 【functioncore】patservice资源的生命周期管理机制优化

## 1. 需求场景分析

1. worker-manager重启后，既能保证patservice资源不残留，也能保证patservice资源不被误删除
2. 可能涉及对管理面vpc插件的增强诉求

### 1.1 特性需求来源与价值概述

需求背景：需求来源于可靠性场景。

- pat service 被误删除的场景：worker-manager 重启，缓存清空，worker-manager 启动时默认调用 delete resource 方法删除所有的 pat pod，delete resource 方法会把 pat pod 加入到删除队列中，vpc 插件延迟一个小时后删除 pat pod。当预留实例使用这个 pat pod 时，一个小时后，因为 pat pod 被删除，预留实例健康检查失败退出。

### 1.2特性场景分析

*描述该特性的业务使用场景内容包括：*

（1）场景触发条件：worker-manager 重启后，缓存清空，一个小时后，pat pod 被删除；

（2）关键任务操作：a. 重启后重建 workermanager 本地缓存 b. 当创建了新的 pat pod 时通知 worker c. worker 重建 IP 地址映射关系

## 2. 特性/功能实现原理

### 2.1  正常场景下 worker-manager 重启流程

1. vpc 插件启动时默认把所有 pat pod 加入到删除队列中（删除队列一个小时后真正删除）
2. worker-manager 遍历当前的实例 pod，如果缓存中没有这个 pod 函数对应的 pat 信息或只有一个，则调用 create resource 接口，这个接口预期返回老的 pat 信息；
3. worker-manager 遍历当前的 pull trigger pod，如果缓存中没有这个 pod 函数对应的 pat 信息或只有一个，则调用 create resource 接口，这个接口预期返回老的 pat 信息；
4. worker-manager 建立 pat pod 和实例 pod 或 pull trigger pod 之间的 map 缓存；

[正常场景下 worker-manager 重启恢复 pat service-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=00e0177a3c5b4d81b2a241b5b8889369&versionUUID=)![正常场景下 worker-manager 重启恢复 pat service](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2mzI?t=1666059145390)

### 2.2  redis 数据异常场景下 worker-manager 重启流程

1. vpc 插件启动时无法从 redis 中获取数据，只能从 k8s 环境上获取所有的 pat 信息，全部加入删除队列（删除队列一个小时后真正删除）
2. worker-manager 遍历当前的实例 pod，如果缓存中没有这个 pod 函数对应的 pat 信息或只有一个，就调用 create resource 接口，返回 pat 信息，此 pat 为新创建的 pat；
3. worker-manager 遍历当前的 pull trigger pod，如果缓存中没有这个 pod 函数对应的 pat 信息或只有一个，就调用 create resource 接口，返回 pat 信息，此 pat 为新创建的 pat；
4. worker-manager 发送一条请求通知 worker 新的 pat 信息；
5. worker 拿到新的 pat 信息，替换老的 pat；
6. worker-manager 建立新 pat pod 和实例 pod 或 pull trigger pod 之间的 map 缓存；

[redis 数据异常场景下 worker-manager 重启恢复 pat service-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=b5436974fb0145d9b5e02336e968a6be&versionUUID=)![redis 数据异常场景下 worker-manager 重启恢复 pat service](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2mzP?t=1666059166378)

### 2.3 约束

暂无

#### 2.3.1上下游依赖需求

依赖管理面 vpc 插件实现以下功能点：

- vpc 插件启动时默认把所有 pat pod 加入到删除队列中；
- vpc 插件启动时 redis 数据要和 k8s 环境中的 pat 数据统一，以 k8s 环境信息为准；
- create resource 接口返回的 pat 信息中要记录是否是新创建的 pat；（增加一个字段记录是否为新创建的 pat pod）
- redis 数据丢失的场景下，要把 k8s 环境中的所有 pat pod 加入到删除队列中；
- 保证在 pat pod 没有出现异常时，create resource 接口不会创建第三个 pat pod；（create resource 接口默认返回两个 pat，如果返回一个 pat，则继续调用一次 create resource 接口，保证一个函数对应两个 pat）

**管理面需求编号**：US2022101800049

传递的**数据结构**是：

```go
// 增加一个字段记录是否为新创建的 pat pod
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
	IsDeleted      bool                `json:"isDeleted"`
	IsNewCreated   bool                `json:"isNewCreated"`  // 新增字段，用于记录 pat 是否是新创建的
}

// 复用字段标识 create resource 接口必须返回两个初始化好的 pat
// 1. needInit 为 false，默认流程，先返回一个初始化好的 pat，再启 go 协程初始化第二个 pat；
// 2. needInit 为 true，返回两个初始化好的 pat；
// VpcControllerRequester define request message for vpc_controller
type VpcControllerRequester struct {
	TraceId  string   `json:"trace_id"`
	Delegate Delegate `json:"delegate"`
	Vpc      Vpc      `json:"vpc"`
	NeedInit bool     `json:"need_init"` // 复用已有字段
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

worker-manager 重启后，会重建缓存信息，保证 pat pod 不被误删；

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

在 worker-manager 启动的时候，创建了一个 go 协程来 recover 缓存数据，在 go 协程遍历所有 pod，保证一个函数调用 create resource 接口一次；

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部优化，客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

单组件代码修改，支持组件独立升级

## 4. 用例

| **序号** | **用例名**                                                   | 用例类型 | **测试步骤**                                                 | **预期结果** |
| -------- | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ | ------------ |
| 1        | wm 重启后，recover 缓存数据，调用 create resource 接口返回已有 pat pod | LLT      | 1. mock create resource 接口返回一个老的 pat pod<br />2. 调用 recover 方法，预期能够在缓存中建立 pat pod 和实例 pod 之间的缓存映射关系 | 符合预期     |
| 2        | wm 重启后，recover 缓存数据，调用 create resource 接口返回已有 pat pod | LLT      | 1. mock create resource 接口返回一个新创建的 pat pod<br />2. 调用 recover 方法，预期能够在缓存中建立 pat pod 和实例 pod 之间的缓存映射关系，并发送请求给 worker | 符合预期     |
| 3        | wm 重启后，recover 缓存数据，此时 cache 中保存了 pod 对应函数的 pat 信息 | LLT      | 1. 无需调用 create resource 接口<br />2. 调用 recover 方法，预期能够拿到 pat 信息，建立 pat pod 和实例 pod 的缓存映射关系，并根据 pat 是否为新建，决定是否发送请求给 worker | 符合预期     |
| 4        | 端到端验证场景                                               | 手动     | 1. 手动重启 worker-manager<br />2. 观察 worker-manager 对缓存的构建情况<br />3. 观察插件是否能够感知到 vpc pat pod 被使用，并打印从删除队列中移除的日志，保证 pat pod 正在被函数使用时，不会被误删 | 符合预期     |

*注：依赖 vpc 函数测试，ST 用例环境无法创建 vpc 函数。*