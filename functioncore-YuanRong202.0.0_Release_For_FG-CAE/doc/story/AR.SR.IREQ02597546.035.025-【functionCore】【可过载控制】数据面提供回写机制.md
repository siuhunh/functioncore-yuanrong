# AR.SR.IREQ02597546.035.025 【functionCore】【可过载控制】数据面提供回写机制

AR.SR.IREQ02597546.035.025 【functionCore】【可过载控制】数据面提供回写机制

AR.SR.IREQ02597546.035.024【functionCore】【可过载控制】数据面将集群情况写到redis

## 1. Story 整体设计

### 1.1 功能描述

1）AR.SR.IREQ02597546.035.025 【functionCore】【可过载控制】数据面提供回写机制

当集群达到负载状态时，新的请求返回集群负载的错误信息。

2）AR.SR.IREQ02597546.035.024【functionCore】【可过载控制】数据面将集群情况写到 redis

数据面将集群的资源池信息写入 redis，包括资源池所有规格的可用 pod 数量、总 pod 数量。

### 1.2 方案详细设计

1）AR.SR.IREQ02597546.035.025 【functionCore】【可过载控制】数据面提供回写机制

如何确定集群处于负载的状态：

- 函数特化的阶段有两个，一是获取空闲的 pod，二是给这个 pod 发送特化请求。
- 如果在第一步，获取空闲的 pod 时，发生了错误，说明获取空闲的 pod 失败了。则判断集群资源不足，无法创建新的 pod；表示当前集群处理不了这个请求。

- 管理面收到资源不足的错误码后，会将此次请求转发给其他集群处理。

返回的错误码和错误信息：

- 错误码：150430
- 错误信息：”the cluster is overload and unavailable now"
- 错误发生的组件：instance manager 组件内部

2）AR.SR.IREQ02597546.035.024【functionCore】【可过载控制】数据面将集群情况写到redis

- 写入 redis 的数据内容

  ```go
  1. 某个规格池子中空闲 pod 的数量
  2. 某个规格池子中总的 pod 的数量（特化+空闲）
  ```

  提供所有规格池子的数据

- 写入频率：5 s 一次（做成可配的）

  ```go
  // PoolMgrConfig defines pool manager config
  type PoolMgrConfig struct {
  	// 新增配置
  	PoolRecordingCfg  PoolRecordingConfig      `json:"poolRecordingCfg,omitempty" valid:",optional"`
  }
  
  // PoolRecordingConfig pool recording config
  type PoolRecordingConfig struct {
  	// record pool info to redis by interval, unit is second, default is 5s
  	RecordingInterval int `json:"recordingInterval" valid:",optional"`
  }
  ```

  ```yaml
  "poolMgrConfig": {
          "poolRecordingCfg": { # 新增配置项
              "recordingInterval": 5
          },
          # 已有配置
          "workerAuthEnable": false
  }
  ```

  

- 写入 redis 的数据格式

  ```go
  key：ClusterState_{clusterID}
  value:
  - key: 池子规格（例如："fusion-300-128")
  - value: json 字符串（例如：{"idle_pod_nums": 20, "total_pod_nums": 100} ）
  ```

- clusterID 的获取方式

  通过 os.Getenv("CLUSTER_ID") 的方式，从环境变量中获取，获取为空，则打印错误日志，不写入 redis 中。

- 写入 redis 失败不做重试。（已和SE对齐）

### 1.3 对齐和管理面对接的数据格式

1）AR.SR.IREQ02597546.035.025 【functionCore】【可过载控制】数据面提供回写机制

返回的错误码和错误信息：

- 错误码：150430
- 错误信息：”the cluster is overload and unavailable now"
- 错误发生的组件：instance manager 组件内部

2）AR.SR.IREQ02597546.035.024【functionCore】【可过载控制】数据面将集群情况写到redis

写入 redis 的数据格式：

```go
key：ClusterState_{clusterID}
value:
- key: 池子规格（例如："fusion-300-128")
- value: json 字符串（例如：{"idle_pod_nums": 20, "total_pod_nums": 100} ）
```



### 1.2 方案详细设计

### 1.4 上下游依赖需求

上游需求：

单函数多集群（方凯）：https://clouddevops.huawei.com/domains/551/workitem/kanban/FE2021092400217

### 2.1 安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
 不涉及

### 2.2 韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及

### 2.3 隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及

### 2.4 可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
不涉及

### 2.5 可用性（Availability）

*【处于按要求执行状态的能力】*  
不涉及

### 2.6 安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及

## 3. HLT 用例

| 序号 | 用例名                                  | 测试步骤                                             | 预期结果                                                     |
| ---- | --------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| 1    | 【可过载控制】数据面提供回写机制        | 构造集群资源不足的场景，调用函数特化pod              | 预期返回 150430 的错误码和错误信息                           |
| 2    | 【可过载控制】数据面将集群情况写到redis | 根据 key：ClusterState_{clusterID} 查看redis中的数据 | redis 中的数据每 5s 更新一次，数据和集群内的资源池总数、空闲数保持一致 |

