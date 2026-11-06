[TOC]

# AR.SR.IREQ02597546.035.005 【functionCore】【可容灾】函数路由自动恢复

关联AR:
+ AR.20210621221420.027: 函数元数据ETCD和函数实例信息ETCD分开部署  

## Story 整体设计

### 功能描述
ETCD 故障时对比 ETCD、k8s数据，发现不一致自动恢复函数路由。

### 场景分析
1. 元数据 ETCD 元数据信息和 k8s functionService 数据保持一致性  
![](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/13dS?t=1631584013645)

2. k8s functionService status 字段数据和路由 ETCD pod 节点信息保持一致性   

3. k8s client 订阅和 workermanager 强依赖，如果 k8s client 报错 InstanceManager 重启

### 方案详细设计

#### 方案设计
1. 监听 ETCD，在故障重连时查询全量 ETCD 数据信息，和 k8s 数据进行对比，不一致时进行路由恢复  
- 元数据 ETCD 故障重连 —— 对比元数据 ETCD 元数据和 k8s functionService spec 字段数据是否一致，如果不一致更新 k8s functionService 数据
```go
type FunctionServiceSpec struct {
	Replicas         *int32               `json:"replicas"`
	DeadLetterConfig DeadLetterConfigSpec `json:"deadLetterConfig,omitempty"`
	Code             CodeSpec             `json:"code,omitempty"`
	Configuration    ConfigurationSpec    `json:"configuration"`
	Concurrency      ConcurrencySpec      `json:"concurrency,omitempty"`
	Tags             map[string]string    `json:"tags,omitempty"`
}
```
- 路由 ETCD 故障重连 —— 对比路由 Pod 节点数据和 k8s functionService status 字段数据是否一致，如果不一致更新路由 ETCD  数据

	```go
	type FunctionServiceStatus struct {
		FunctionServiceName    string           `json:"functionServiceName,omitempty"`
		FunctionDeploymentName string           `json:"functionDeploymentName,omitempty"`
		Instances              []WorkerInstance `json:"instances,omitempty"`
		TemporaryPods []string `json:"temporaryPods,omitempty"`
	}
	```

#### 外部交互分析&&上下游依赖需求
上游需求：函数元数据 ETCD 和路由 ETCD 分开部署  ———— 对接人张同。

### 对外接口
NA


## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*
不涉及

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*
不涉及

#### 可用性 Availability
*处于按要求执行状态的能力*
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*
不涉及

## LLT 用例
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | 元数据 ETCD 故障重启，数据恢复 |  元数据ETCD失联或重启，元数据发生以下变化 1)删除部分函数， 看恢复后函数实例是否都被删除 2）升级部分函数，看函数能否升级 3）新增部分函数,看新函数能否正常冷启动元数据 | 函数正常调用，函数元数据信息保持一致 |
| 2 | 路由 ETCD 故障重启，数据恢复  | 路由 ETCD 故障重启，元数据发生变化，functionService 数据变更，ETCD 恢复后查看数据是否一致 | 函数正常调用，节点信息保持一致 |
| 3 | 预留函数实例路由恢复 |  路由 ETCD 失联或重启，函数预留实例全部挂掉，ETCD 恢复后能否恢复路由 | ETCD 恢复后函数正常调用，路由恢复 |