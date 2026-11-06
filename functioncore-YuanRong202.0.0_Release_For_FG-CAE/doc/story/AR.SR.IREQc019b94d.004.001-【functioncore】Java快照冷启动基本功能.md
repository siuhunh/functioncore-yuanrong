[TOC]

# 【functioncore】Java快照冷启动基本功能

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

目前基于Spring等框架开发的Java微服务，内存使用容量为数百兆到几G，并且启动时虚拟机的对Java程序加载过程和Spring Bean的初始化的时间都较长，时间达到了分钟级，且受JIT影响，Java应用还有需要逐渐达到最高性能的慢启动过程，这些因素都导致了云原生弹性在我们的服务中受到较大局限，希望研究一种针对Java应用快速启动容器的解决方案。支撑的场景包括华为ERP、消费者云AGC等业务。

https://wiki.huawei.com/domains/867/wiki/5357/WIKI20220928343428

### 1.2特性场景分析

对开启冷启动快照功能的函数（当前只支持java类型函数 java8/java11），制作冷启动快照，后续该函数的冷启动走快照恢复流程。

## 2. 特性/功能实现原理

### 2.1总体方案

[冷启快照](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=452a8a89dc334cd982527966438b1cb5)![冷启快照](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2oNq?t=1665991184382)

### 2.2模块详细设计

*各模块的具体实现，包括模块的模块实现、对外接口定义、模块类设计*

1. 管理面：

元数据 etcd 新增 ：

```
key:
/snapshot/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}/id/{snapshotID}

成功：
value ----- json:
type SnapshotDeploymentSpec struct {
    // 管理面指定位置信息，并控制删除。
	AppID          string `json:"appId"`
	BucketID       string `json:"bucketId"`
	ObjectID       string `json:"objectId"`
	BucketURL      string `json:"bucketUrl"`
	Sha256         string `json:"sha256,omitempty"` // 成功写回
	Size           int64  `json:"size,omitempty"`   // 成功写回
}

eg：
{
        "appId": "",
        "bucketId": "bucket-test-log1",
        "objectId": "snapshot-train-sw-test.zip",
        "bucketUrl": "",
        "sha256": "6f927368126cfe105dbf8b869c0155b54fa62a836d538fdf6b8319c032ec34b8",
        "size": 79456328
}
    
失败：
{"code":4211,"message":"runtime initialization timed out after %s"}
```

函数元数据扩展新增：

```
value ----- json:
	SnapshotDeployment *types.SnapshotDeploymentSpec `json:"snapshotDeployment,omitempty"`
	// ID 为管理面对该版本函数元数据的唯一标识
	ID      string `json:"ID"`
	
	
eg：
    "snapshotMetaData": {
        "appId": "",
        "bucketId": "bucket-test-log1",
        "objectId": "snapshot-train-sw-test.zip",
        "bucketUrl": "",
        "sha256": "6f927368126cfe105dbf8b869c0155b54fa62a836d538fdf6b8319c032ec34b8",
        "size": 79456328
    },
```

2. worker manager：

   新增 checkpoint pod 管理：

   - [ ] 创建 checkpoint pod：（超时时间可配置）

     ​	name：  “checkpoint-*” ，

     ​	标签  "standard"："checkpoint-pod" 

     ​	环境变量  RUNNING_MODE ： CHECKPOINT

   - [ ] 删除 checkpoint pod：

     ​	查询到快照制作完成后，删除该pod

   - [ ] 初始化：

     ​	查询到环境上所有checkpoint pod并删除。
     
     其他同普通pool pod。（TODO：额外cpu/内存/磁盘配额，可配置。）

   异常处理：

   ​	重试 3 次 。用户加载函数/初始化流程出现失败，则checkpoint失败。

   超时处理：

   ​	拉起 checkpoint pod 超时时间可配置（60s）；等待快照 ready 超时时间可配置（5min）；TODO 各内存制作快照时间规格-》制定超时时间

   并发处理：

   ​	每个集群支持最多并发10个快照请求（可配置）

3.  worker / runtime manager:

   - [ ] 以 RUNNING_MODE ： CHECKPOINT 模式启动的，收到特化请求后做 checkpoint 动作，并做准备工作：解除句柄数限制/磁盘检测限制等
- [ ] 特化请求携带 SnapshotDeployment 的以 restore 方式启动

### 2.3 约束

#### 2.3.1上下游依赖需求

见 2.2 第一节 管理面。

#### 2.3.2其他约束

1. 仅可以对发布版本制作快照，发布的版本不允许修改函数元数据，不会产生同一版本可能制作多次快照的场景
2. 如果在快照制作完成前，下发该函数版本的请求，数据面会以原冷启动流程运行函数实例

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

数据面 obs账号 新增上传权限；

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

管理面需要负责：

1. snapshot包的清理
2. snapshot包和对应函数的一致性检查
3. snapshot包制作的超时
5. 均衡分发制作snapshot包任务给多个集群
6. 预留实例相关场景

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

快照以加密形式存储到 obs 中。

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

如果restore失败，则退避成原冷启动方式启动，同时产生告警。

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

数据面单独测试，需要 mock 管理面行为。

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

后续可靠性设计补充 并发快照阈值控制。

### 3.11资料

基本功能阶段，暂不涉及

### 3.12升级兼容性

支持解耦升级。
管理面单独升级：新增字段，数据面不解析
数据面单独升级：对新增字段为空的场景，复用已有流程。

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤**                                                 | **预期结果**                   |
| -------- | ---------- | -------- | ------------------------------------------------------------ | ------------------------------ |
| 1        | 端到端验证 | 手动     | 1. 写入制作快照的key 2. 等待约1min 3. 访问obs，拿到写入的快照信息，并写入元数据 4. 调用 | 按照快照恢复流程进行快速冷启动 |
| 2        | 端到端验证 | 手动     | 在用例1的任意流程中，重启 worker manager                     | checkpoint容器无残留           |