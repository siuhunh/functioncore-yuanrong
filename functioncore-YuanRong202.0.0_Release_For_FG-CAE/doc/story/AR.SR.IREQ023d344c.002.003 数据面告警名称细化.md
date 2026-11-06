[TOC]

# AR.SR.IREQ023d344c.002.003 数据面告警名称细化

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

数据面告警名称细化，现在数据面告警名称统一为Pod区访问异常，难以快速界定是哪里出现异常

### 1.2特性场景分析

现有告警类型共19种，即对这19种告警类型设置不同的告警名称。

## 2. 特性/功能实现原理

### 2.1总体方案

在发送给管理面的告警信息结构中新增告警名称字段

```go
// AlarmInfo describes alarm info
type AlarmInfo struct {
   ---
   AlarmName   string `json:"alarmname,omitempty"`
}
```

告警名称定义

```go
// functioncore
const (
    routerEtcdConnectionAlarmName                     = "数据面路由etcd连接异常"
    metadataEtcdConnectionAlarmName                   = "数据面元etcd连接异常"
    redisConnectionAlarmName                          = "数据面Redis连接异常"
    criticalRedisConnectionAlarmName                  = "数据面Redis持续连接异常"
    vpcExceededThresholdAlarmName                     = "数据面VPC网卡不足"
    swrConnectionAlarmName                            = "数据面SWR连接异常"
    obsConnectionAlarmName                            = "数据面OBS连接异常"
    insufficientMinInstanceAlarmName                  = "数据面预留实例拉起异常"
    routerEtcdLeaseAlarmName                          = "数据面路由etcd续租异常"
    ntpOffsetAlarmName                                = "数据面NTP时钟偏移超出阈值"
    idleModeNotificationAlarmName                     = "数据面空闲模式通知异常"
    poolDeploymentAlarmName                           = "数据面pool deployment更新失败"
    rotatedTokenAlarmName                             = "数据面获取token异常"
    restartingPodAlarmName                            = "数据面pod重启告警"
    faaSSchedulerConnectionAlarmName                  = "数据面FaaS Scheduler连接异常"
    routerETCDConnectionAbnormalContinuouslyAlarmName = "数据面路由etcd持续连接异常"
    metaETCDConnectionAbnormalContinuouslyAlarmName   = "数据面元etcd持续连接异常"
    etcdOperationFailedContinuouslyAlarmName          = "数据面操作etcd持续异常"
    ntpConnectionAlarmName                            = "数据面NTP服务连接异常"
)
// workflow
const (
	EtcdErrAlarmName = "数据面workflow etcd连接异常"
	MysqlErrAlarmName = "数据面workflow mysql连接异常"
	RedisErrAlarmName = "数据面workflow redis连接异常"
	DispatcherHealthCheckAlarmName = "数据面workflow dispatcher连接异常"
)
```

### 2.3 约束

#### 2.3.1上下游依赖需求

管理面无需修改，已经有对告警名称进行判断。

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

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤** | **预期结果**                         |
| -------- | ---------- | -------- | ------------ | ------------------------------------ |
| 1        | XX         | 手动用例 | 构造告警     | 能产生告警并且告警名称为细化后的名称 |