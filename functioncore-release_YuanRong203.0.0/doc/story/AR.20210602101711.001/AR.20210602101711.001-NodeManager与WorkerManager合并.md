[TOC]
# NodeManager与WorkerManager合并设计说明书
关联AR:
+ ARID1: AR.20210602101711.001
## Story 整体设计

### 功能描述
**需求**：node manager当前只与worker manager有交互，而且两个模块存在可复用逻辑，考虑将两个模块合并以减少functioncore的资源占用

**作用**：去除node manager模块，将其功能合并到worker manager中，由worker manager负责管理worker池

**约束**：无

### 场景分析
当前代码中node manager与worker manager都有kubernetes pod informer逻辑，两个模块的核心逻辑如下图所示：

[workermanager&nodemanager](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=b8ced24642fe4723855fc29c1e872f2e)![workermanager&nodemanager](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qZr?t=1623937590429)

可以看出合并两个模块除了可以省去一次api调用开销以外，也可以复用Informer这一块的核心逻辑，精简项目代码

### 方案详细设计
#### 现状分析

#### 方案设计
合并worker manager与node manager的informer逻辑，将pool manager逻辑放入worker manager中，并复用worker manager的reactor来作为pool manager的事件源，通知pool manager池中pod的变化：

[merge_nodemanager](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=a0e59f6135d74833b26a67be857a5205)![merge_nodemanager](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qdE?t=1623979146115)

#### 开源软件选型
不涉及

#### 外部交互分析&&上下游依赖需求
1. 部署脚本中删除node manager相关代码，其配置文件中的内容增加到worker manager中
2. 所有pool的属主将从node manager转变为worker manager，worker manager重启将导致pool的重建

### 对外接口
删除了Node Manager /getpod/ 接口

### 约束
无

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

## HLT 用例
|序号|测试用例|测试步骤|预期结果|
| :---: | :---: | :---: | :---: |
|1| worker manager 拉起worker池|使用最新部署脚本，部署完成后无node manager模块，查看worker池是否被拉起|worker池正常拉起|