[TOC]

# 预留实例与 VPCPullTirrgger 支持多集群设置

关联AR:
+ AR.20210621221420.028   ——预留实例支持多集群设置

## Story 整体设计

### 功能描述
数据面多集群场景下，能够指定集群拉起预留实例和 VPCPullTrigger 实例，支持各集群配置最大实例最小实例数量。

### 场景分析
场景：

1. 管理面与数据面 ETCD 相分离，数据面多集群场景下需要指定某一集群拉起预留实例和 VPCPullTrigger 实例。
2. 数据面各个集群可以支持不同的最大实例，最小实例数量配置。

[融合ETCD集群模型](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=dbe757f705c942c8a47e5eb47d79d600&versionUUID=)![融合ETCD集群模型](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/13dS?t=1631584013645)


### 方案详细设计

#### 现状分析
#### 方案设计
1. VPCPullTrigger 和预留实例

    1. 管理面写入元数据 ETCD，Key 如下：

    ```go
    // 函数元信息 key 当前已有
    /sn/functions/business/yrk/tenant/{tenantID}/function/{functionName}/version/{version} 
    {
        "instanceMetaData": {
            "minInstance": 10, // 抽离
            "maxInstance": 10，// 抽离
            "concurrentNum": 10
        }
    }
    
    // 预留实例 key  新增，需管理面往公共ETCD写入
    /instances/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}
    {
        "instanceMetaData": {
            "minInstance": 10,
            "maxInstance": 10
        }
    }
    
    ```

    2. 数据面 FunctionInstanceManager watch **instances** 这个 key，带 clusterID watch

    ```go
    // watch key 函数元信息key，公共信息
    // /sn/functions/business/yrk/tenant/{tenantID}/function/{functionName}/version/{version} 
    
    // watch key 实例元信息key，携带集群信息，指定配置
    // /instances/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}
    ```

    3. clusterID 配置，values.yaml 新增字段，字段写入 FunctionInstanceManager 环境变量。

       由于当前数据面集群没有字段可以唯一标识集群，故在 values.yaml 文件内增加 clusterID，当前集群的唯一身份标识。

       ```yaml
         # Specifies the ID of the cluster on the data plane. 
         # Ensure that the ID of the cluster is different from that of the cluster.
         clusterID: ClusterID
       ```

2. 流程设计：

    [预留实例拉起时序图](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=a83e14d969ff4e7394090f7d576dc8ba&versionUUID=)![预留实例拉起时序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/13sG?t=1632280139645)

3. [FunctionService更新时序](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=860bc2d5cbc34dd39124394e822fbbb7&versionUUID=)![FunctionService更新时序](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/145m?t=1632281910555)

#### 开源软件选型
不涉及

#### 外部交互分析&&上下游依赖需求
依赖管理面增加往 ETCD 写入 key。

1. 如果配置了最大实例、预留实例或 VPCPullTrigger，向元数据 ETCD 写入下面的key。

   ​															 ——夏嘉诚  联调时间: 10.15

```go
/instances/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}
{
    "instanceMetaData": {
        "minInstance": 10,
        "maxInstance": 10
    }
}
```

### 对外接口
*描述对外新增的接口*

### 约束
*特性约束描述*


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

## 用例
| 序号 | 用例名                                    | 测试步骤                                                     | 预期结果                                                     |
| :--: | :---------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
|  1   | 正常功能验证-预留实例、VPCPullTrigger拉起 | 1. 函数界面配置预留实例或VPCPullTrigger实例<br>2. 保存函数，去环境上查看函数实例 | 1. 预留实例成功拉起<br>2. 查看函数fs，确认预留实例拉起正常<br>3. 查看函数fs，确认预留实例配比是否更新 |
|  2   | 正常功能验证-最大实例配置                 | 1. 基于用例1，配置修改最大实例配比，<br>2. 保存函数，去环境上查看函数实例 | 1. 查看预留实例是否更新<br>2. 查看函数fs，确认最大实例配比是否更新 |
|  3   | 正常功能验证-函数删除                     | 1. 基于用例1,2，删除相关函数<br>2. 去环境上查看函数实例<br>3. 去环境上查看ETCD数据信息 | 1. 实例成功删除<br>2. ETCD信息更新，函数信息删除，实例信息删除 |
|  4   | 异常场景-写入时延问题                     | 1. 界面上同时修改最大实例数和规格数据<br>2. 保存后查看函数fs<br>3. 多次测试 | 1. 规格和最大实例数据都被更新<br>                            |

