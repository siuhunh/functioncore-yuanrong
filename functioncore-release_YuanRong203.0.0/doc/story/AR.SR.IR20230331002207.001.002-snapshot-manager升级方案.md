# Snapshot-Manager组件快照升级

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

AR.SR.IR20230331002207.001.002

### 1.2特性场景分析

*(1）Runtime-java框架升级场景*

涉及到javaruntime改动的runtime-manager升级前, 需要提前修改snapshot-manager的deployment和configmap，重制快照。

*(2）使用时间及频度*

Runtime-java框架升级场景

*(3）描述该特性主要有哪些场景、子场景及关键任务操作。*

**快照无损升级场景****回退场景**

## 2. 特性/功能实现原理

### 2.1总体方案

#### 1. 前置需要已经支持[支持多版本快照](https://wiki.huawei.com/domains/867/wiki/5357/WIKI20230313863488)：

1. etcd中函数元数据拓展里的snapshotMataData，snapshot元数据中都已经有snapshotVersions数组ImageTag版本信息

#### 2. 升级场景

升级评审时确定升级范围场景，若有涉及到javaruntime的修改，则需要在升级前手动修改**snapshot-manager** 的deployment和configmap：

1. containers中snapshot-manager的image为最新版本
2. containers中env里WORKER_TAG，RUNTIME_MANAGER_TAG value为最新版本
3. 正常情况下，1和2中的最新版本号都应该保持一致（functioncore的版本号）
4. 若javaruntime的修改，需要在configmap中配置最新版本的javaruntime信息，如下
   ```
   "multiSnapshotConfig": {
          "enable": true,
          "snapshotCheckInfo": {
              "javaRuntimeSize": 231232,
              "javaRuntimeHash": "8e542011b75d294b8a59a69633dded48d2a1254e3f29a0aad149673b30bf0657"
           }
      }
   ```

升级前部署**snapshot-manager** ，触发重制快照检查任务，**snapshot-manager**获取当前RUNTIME_MANAGER_TAG的value，遍历所有snapshot元数据，匹配检查当中的ImageTag字段（数据结构参见[支持多版本快照](https://wiki.huawei.com/domains/867/wiki/5357/WIKI20230313863488)）：

1. 若snapshotVersions不为空且imageTag不相等，
    1. 若javaRuntimeSize和javaRuntimeHash均相等，则忽略
    2. 若javaRuntimeSize和javaRuntimeHash任意不相等，则触发重制快照流程
2. 若snapshotVersions不为空且ImageTag字段匹配当前ImagTag，则忽略。

升级支持**通过并发度配置提升重制快照速度** - 实现和worker-manager保持一致，在deployment中`checkpointPodConfig.maxConcurrency`配置

#### 3. 观测场景

需要在每个集群都都手动部署**snapshot-manager**，运维通过LTS上snapshot-manager的日志判断所有新快照是否已经制作完成：

1. snapshot-manager的日志应该统计每一个快速启动函数的re-checkpoint时间
2. snapshot-manager的日志统计重制当前所有快速启动函数的进度，上报预估时间，如
   ```
   当前并发度为5，已重制40%函数，剩余60%函数待重制
   ```
3. snapshot-manager的日志应该打出重制完成的标志日志。

#### 4. 回退场景

函数元数据拓展里的snapshotMataData保存最新两次版本的快照，回退后，worker-manager根据自己的平台版本，选择对应的快照

#### 5. 其他可能异常场景

**并发写同一snapshot key**针对snapshot-manager和worker-manager可能同时写同一ectdkey的场景：

1. snapshot-manager重制过程中，客户触发重制快照？目前客户修改代码重新上传不能触发重制快照，只有发布新版本才会重制快照，不同版本的key不一样，此场景无风险
2. 就算写同一key,  snapshot key中保留数组，谁后写谁追加，不会被覆盖。

#### 6. snapshot-manager退出场景

每个集群的snapshot-manager在重制完成后都应该自动退出

### 2.2模块详细设计

#### 2.2.1 WorkerManager子模块设计

[worker-manager快照多版本](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=0506ccacb14843d58f74801c44551db3)![](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3bJa?t=1678779868898)

##### 2.2.1.1 worker-manager适配调用请求

worker-manager接受调用请求，根据当前runtime-manager的ImageTag选择函数元数据拓展里的snapshot信息，转发给worker去恢复对应版本的快照包

1. worker-manager获取当前runtime-manager的`imageTag`, 获取configmap中multiSnapshotConfig的`javaRuntimeSize`, `javaRuntimeHash`至`PoolMgrConfig`

```
"multiSnapshotConfig": {
     "enable": true,
     "snapshotCheckInfo": {
       "javaRuntimeSize": 231231,
       "javaRuntimeHash": "testhash"
     }
   }
```

2. worker-manager选择函数元数据中的snapshotVersions中`imageTag`和PoolMgrConfig.PoolContainerImageTags.RuntimeImageTag一致的快照版本，至worker恢复快照 (因为不存在`javaRuntimeSize`，`javaRuntimeHash`不一致，而`imageTag`一致的情况，所以只用判断`imageTag`)

```
{
 "appId": "",
 "bucketId": "bucket-test-log1",
 "objectId": "khj.zip",
 "bucketUrl": "",
 "sha256": "16c2acdc2dd4ccc08b94ca197fc3af754d2495897196c669ddcca1a01deac8cd",
 "size": 17498941,
 "snapshotVersions": [
     {
         "objectId": "khj.zip.k0059126416460",
         "sha256": "16c2acdc2dd4ccc08b94ca197fc3af754d2495897196c669ddcca1a01deac8cd",
         "size": 17498941,
         "snapshotCheckInfo": {
             "javaRuntimeSize": 231232,
             "javaRuntimeHash": "testhash"
         },
         "imageTag": "k0059126416460",
         "timestamp": "2023-04-03 14:07:15.050594661 +0800 CST m=+258987.810698955"
     }
 ]
}
```

#### 2.2.2 SnapshotManager子模块设计

[snapshot-manager](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=65be564b67d14db3be54d55cad39f6e0)![](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3bKE?t=1678781168751)

##### 2.2.2.1 部署snapshot-manager

snapshot-manager必须以job形式部署   这样才能完成任务后就退出

手动部署snapshot-manager,  参考woker-manager的deployment，如下类似

需要保证

1. WORKER_TAG和RUNTIME_MANAGER_TAG为即将升级的functioncore版本
2. configMap中`multiSnapshotConfig`配置即将升级的javaruntime的信息

```
"multiSnapshotConfig": {
     "enable": true,
     "snapshotCheckInfo": {
       "javaRuntimeSize": 231231,
       "javaRuntimeHash": "testhash"
     }
   }
```

```
apiVersion: apps/v1
kind: Job
metadata:
  labels:
    app: snapshot-manager
  name: snapshot-manager
  namespace: default
spec:
  containers:
      - name: snapshot-manager
      image: 
  ....
  env:
         - name: WORKER_TAG
              value: 
         - name: RUNTIME_MANAGER_TAG
              value:
```

##### 2.2.2.2 snapshot-manager实现

snapshot-manager共用 worker-manager 代码，拷贝并修改 worker-manager 配置，拉起 worker-manager 二进制，生成 snapshot-manager 组件。修改worker-manager 配置，新增`is-snapshot-mode=true` 启动参数，在worker-manager中走snapshot-manager的流程，实现基本功能

```
args:
         - ' --is-snapshot-mode=true'
```

**实现关注点**

1. **snapshot-manager流程需要忽略worker-manager 部分原有的事件监听，事件处理，定时器等：**

    1. 关闭monitoring server redis-cluster
    2. 关闭worker-manager http server，不接受客户端请求
    3. 保留init worker cache，拉起pod特化请求需要
    4. 保留pat pods cache，特化带有VPC逻辑的函数pod需要
    5. 关闭poolmgr initialize pool，createPool等后台任务
    6. 关闭checkpoint相关清理的后台任务
    7. 忽略所有k8s事件的监听处理
    8. 忽略所有metaEtcd事件的监听处理
    9. 忽略预留实例的处理
2. worker-manager新增checkpointMode 判断快照制作是由管理面触发还是snapshot-manager触发

    1. CheckpointModeUpdate 标识由snapshot-manager触发，重制快照
    2. CheckpointModeCreate 标识由管理面触发，第一次制作快照

```
// CheckpointModeUpdate - update existing snapshots by re doing checkpoint
CheckpointModeUpdate = "update"
// CheckpointModeCreate - create snapshots by doing checkpoint for the first time
CheckpointModeCreate = "create"
```

3. snapshot-manager服务完成后自动退出

##### 2.2.2.3 snapshot-manager功能

1. 重启后，遍历前缀为`snapshot/business/yrk/cluster`的snapshot元数据，检查快照版本是否匹配，若没有匹配的版本则重制快照，检查逻辑如下，快照多版本需求上线后，所有snapshot元数据都应该已有snapshotVersions：
    1. 若snapshotVersions不为空且imageTag不相等，
        1. 若javaRuntimeSize和javaRuntimeHash均相等，则忽略
        2. 若javaRuntimeSize和javaRuntimeHash任意不相等，则触发重制快照流程
    2. 若snapshotVersions不为空且ImageTag字段匹配当前ImageTag，则忽略。(imageTag相等则javaRuntimeSize，javaRuntimeHash一定相等)
2. 快照重制完成后，自动退出。
    1. .spec.template.spec.restartPolicy = "Never"
    2. batch/v1

##### 2.2.2.3 snapshot-manager功能

1. 重启后，遍历前缀为`snapshot/business/yrk/cluster`的snapshot元数据，检查快照版本是否匹配，若没有匹配的版本则重制快照
    1. 若snapshotVersions不为空且imageTag不相等，
        1. 若javaRuntimeSize和javaRuntimeHash均相等，忽略重制快照流程，只更新etcd中snapshot元数据已有版本快照的imageTag和时间戳
        2. 若javaRuntimeSize和javaRuntimeHash任意不相等，触发重制快照流程，更新追加etcd中snapshot元数据的新版本快照信息
    2. 若snapshotVersions不为空且ImageTag字段匹配当前ImageTag，则忽略处理 (imageTag相等则javaRuntimeSize，javaRuntimeHash一定相等)
2. 快照重制完成后，自动退出。

### 2.3 约束

#### 2.3.1上下游依赖需求

快照支持多版本 [https://wiki.huawei.com/domains/867/wiki/5357/WIKI20230313863488](https://wiki.huawei.com/domains/867/wiki/5357/WIKI20230313863488)

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security）

NA, 不涉及

### 3.2韧性（Resilience）

运行完退出，不涉及

### 3.3隐私性（Privacy）

NA, 不涉及

### 3.4可靠性（Reliability）

参见测试用例

### 3.5可用性（Availability）

参见测试用例

### 3.6安全（Safety）

同快照流程保持一致

### 3.7可维护性

NA

### 3.8可测试性

参见测试用例

### 3.9性能

NA

### 3.10资源

snapshot-manager重制快照的并发资源支持可配置

### 3.11资料

NA

### 3.12升级兼容性

快照多版本需求先升级后，保证升级兼容性

## 4. 用例

| **序号** | **用例名**            | 用例类型          | **测试步骤**                                                                                                                  | **预期结果**                                                                                                                                                                |
| ---------------- | ----------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1              | 部署snapshot-manager        | 手工用例          | 提供部署模板，使用helm install手动部署                                                                                              | 部署成功                                                                                                                                                                          |
| 2              | 重制快照流程 - 触发         | 手工用例          | 1. 部署snapshot-manager ，配置RUNTIME_MANAGER imageTag不一致，配置configMap中javaruntime信息不一致                                  | 触发重制快照流程，观察ETCD snapshot元数据新增配置版本的快照信息                                                                                                                   |
| 3              | 重制快照流程 - etcd写入检查 | 手工用例          | 1. 部署snapshot-manager ，配置RUNTIME_MANAGER imageTag不一致，配置configMap中javaruntime信息不一致                                  | 触发重制快照流程，检查ETCD snapshot元数据新增配置版本的快照信息:1. objectID后缀携带新版本imageTag  2. snapshotCheckInfo中与configMap中配置的信息一致，timestamp与集群时区保持一致 |
| 4              | 重制快照流程 - 不触发条件   | 手工用例          | 1. 部署snapshot-manager ，配置RUNTIME_MANAGER imageTag不一致，配置configMap中javaruntime信息一致                                    | 不触发重制快照流程，观察ETCD snapshot元数据没有配置版本的快照信息                                                                                                                 |
| 5              | 重制快照流程 - 并发做快照   | 手工用例          | 1. 部署snapshot-manager ，配置RUNTIME_MANAGER imageTag不一致，配置configMap中javaruntime信息不一致  2. 触发新版本的正常制作快照流程 | 1. 触发重制快照流程，观察ETCD snapshot元数据有新增配置版本的快照信息  2. 新版本的正常做快照的流程正常                                                                             |
| 6              | worker-manager适配多版本    | 手动用例/自动用例 | 1. 依赖前置用例2，mock管理面，将snapshot元数据新增的配置版本的快照信息复制到函数元数据的snapshotMetaData中 2. restore调用成功       | 检查worker下载的快照包信息与当前版本匹配，需要检查worker的日志                                                                                                                    |
| 7              | 回退场景                    | 手动用例/自动用例 | 1. 依赖前置用例5，新增的配置版本的快照信息已在函数元数据的snapshotMetaData中 2. 回退worker-manager 3. restore调用成功               | 检查worker下载的快照包信息与回退版本匹配，需要检查worker的日志                                                                                                                    |
| 8              | snapshot-manager观测机制    | 手动用例          | 1.依赖用例2，触发重制快照流程                                                                                                       | 重制快照流程中观测进度日志                                                                                                                                                        |
| 9              | snapshot-manager退出机制    | 手动用例          | 1. 依赖用例2，用例8, 触发重制快照流程和日志观测                                                                                     | 日志显示已完成后，snapshot-manager退出                                                                                                                                            |
