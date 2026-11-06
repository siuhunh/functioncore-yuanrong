[TOC]

# AR.SR.IREQ02597546.039.004 【functionCore】支持集群弹性伸缩

AR.SR.IREQ02597546.039.004 【functionCore】支持集群弹性伸缩

## Story 整体设计

### 功能描述

自定义 CA 插件，实现对集群状态的感知，从而实现集群节点自动扩容和缩容。

扩容：根据集群中 pod 的状态，由插件调用 CCE 能力，扩容新的节点；

缩容：根据集群中 node 节点的资源使用情况，有插件调用 CCE 能力，实现缩容；

说明：

1）当前扩容是对节点进行开机操作，或购买新的节点，补充到节电池中。

2）当前缩容是对节点进行关机操作，关机后的节点只收取磁盘费用，不收取 cpu 和 memory 的费用。

3）节点缩容操作不会对集群原有节点造成影响。


### 方案详细设计

#### 现状分析

当前 CCE 集群原生支持节点扩容，但扩容后无法给节点自动进行打标签等操作。CCE 集群原生不支持缩容。

CA 扩缩容插件在 CCE 集群原生能力的基础上，支持给扩容出的新节点打上自定义标签；支持节点缩容；

#### 方案设计
[节点扩缩容插件流程图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=adca6ab8c9604ed4ae55b833bd506134&versionUUID=)![节点扩缩容插件流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/195Y?t=1640054241545)

扩容：

1）CA 插件每隔 10s 扫描一次集群节点，优先检查集群资源占用率是否超过 80%，超过则按照节点池配置扩容对应数量的节点

2）如果资源占用率没有超过 80%，则检查是否有 pending pod，当集群节点出现 pending pod，CA 插件会通过计算所有 pending pod 需要的资源，计算出当前需要扩容的节点数

缩容：

1）CA 插件每隔 10s 扫描一次集群节点的 cpu memory 占用情况，当 cpu 和 memory 占用小于阈值（当前默认 50%）时，将节点加入到 unneeded 列表中，当此节点在 unneeded 列表中存在 1min 以上时，则给这个节点打上污点标签，防止新的 pod 部署到该节点上；最后，等待 10min 后，将此节点关机。

```yaml
- --scale-down-enabled=true # 缩容开关
- --scale-down-delay-after-add=1m0s # 节点扩容之后启动缩容的等待时间
- --scale-down-unneeded-time=1m0s   # 加入 unneeded 列表后打上污点标签的间隔时间
- --scale-down-utilization-threshold=0.5 # 缩容检查 cpu 和 memory 的阈值
- --max-graceful-termination-sec=100     # 打上污点标签后关机的间隔时间
- --unremovable-node-recheck-timeout=2m0s # 节点从 unneeded 列表中移除后再次检查的间隔时间

spec:
      containers:
      - command:
        - ./cluster-autoscaler
        - --v=4
        - --stderrthreshold=info
        - --cluster-name=77f3e3b6-2afb-11ec-b443-0255ac101e52 # 需配置集群的 cluster id
        - --cloud-config=/config/cloud-config
        - --cloud-provider=huaweicloud
        - --scale-down-enabled=true # 节点缩容的开关
        - --scale-down-delay-after-add=1m0s # 节点扩容之后启动缩容的等待时间
        - --scale-down-unneeded-time=1m0s # 加入 unneeded 列表后打上污点标签的间隔时间
        - --scale-down-utilization-threshold=0.5 # 触发缩容的 cpu 和 memory 阈值，例如 cpu 和 memory 占用率都低于 50% 触发节点缩容
        - --max-graceful-termination-sec=100 # 打上污点标签后关机的间隔时间
        - --unremovable-node-recheck-timeout=2m0s # 节点从 unneed 列表中移除后再次检查的间隔时间
        - --nodes=0:6:crc-nodepool-default # 节点池配置

```



#### 安装部署
准备：

1）部署 CA 插件之前，需要创建一个新的节点池，供 CA 插件扩缩容节点使用；

2）需要获取 CCE 集群的配置信息，包含如下配置信息（注释处）：

```yaml
# serverless-cluster-autoscaler-secret.yaml
cce-endpoint=https://cce.cn-north-6.myhuaweicloud.com # cce 集群地址
cce-k8s-endpoint=https://87672602-c5d1-11eb-8a8e-0255ac102118.cce.cn-north-6.myhuaweicloud.com # cce 集群 k8s 地址
ecs-endpoint=https://ecs.cn-north-6.myhuaweicloud.com # ecs 集群地址
project-id=8a156eb618dd406fa7e95ffa6805cc72 # project id
access-key=xxxxx # ak 鉴权信息
secret-key=xxxxx # sk 鉴权信息
region=cn-north-6 # region
domain-id=6b2b877ca57148dba5e11d53800bb00a # domain id

# cluster-autoscaler-deployment.yaml
    spec:
      serviceAccountName: cluster-autoscaler-account
      containers:
        - name: cluster-autoscaler
          image: swr.cn-north-4.myhuaweicloud.com/serverless-cluster-autoscaler:dev
          imagePullPolicy: Never          
          command:
            - ./cluster-autoscaler
            - --v=4
            - --stderrthreshold=info
            - --cluster-name=87672602-c5d1-11eb-8a8e-0255ac102118 # 集群的 cluster id
            - --cloud-config=/config/cloud-config
            - --cloud-provider=huaweicloud            
            - --scale-down-enabled=true
            - --scale-down-delay-after-add=1m0s                 
            - --scale-down-unneeded-time=1m0s
            - --scale-down-utilization-threshold=0.5
            - --max-graceful-termination-sec=100
            - --unremovable-node-recheck-timeout=2m0s
            - --nodes=0:10:crc-nodepool-default                  # 新创建的节点池的最小节点数、最大节点数、节点池名称
```



部署方式：

CA 插件以 deployment 的方式部署到 CCE 集群，一个集群部署一个 CA 插件。

部署命令：

```sh
kubectl apply -f serverless-cluster-autoscaler-svcaccount.yaml
kubectl apply -f serverless-cluster-autoscaler-secret.yaml
kubectl apply -f cluster-autoscaler-deployment.yaml
```



#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*

### 对外接口
无

### 约束

1）CA 插件只支持配置一个新的资源池（不包含默认资源池）

2）一个集群需安装一个CA插件

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

## LLT 用例
| 序号 | 用例名       | 测试步骤                                                     | 预期结果 |
| ---- | ------------ | ------------------------------------------------------------ | -------- |
| 1    | 扩容         | 1. 模拟资源占用率超过 80%<br />2. 观察是否有新的节点加入集群<br />3. 校验新的节点是否打上指定的 node-role 或 tenant_id 的标签 | 满足     |
| 2    | 扩容         | 1. 拉起大规格 pod，出现 pending pod<br />2. 查看是否有新的节点加入集群<br />3. 查看新的节点是否打上指定的 node-role 或 tenant_id 标签 | 满足     |
| 2    | 缩容         | 1. 使得节点资源占用小于 50%<br />2. 查看该节点是否处于 Not Ready 状态（说明：当前节点是一个接一个的缩容，当上一个缩容完成才会缩容下一个节点） | 满足     |
| 3    | 扩容到最大值 | 1. 设置最大值为 6，拉起需要20个大规格的 pending pod<br />2. 查看是否有新的节点加入集群，是否达到最大值 6，预期扩容总节点数不超过最大值 6 | 满足     |
| 4    | 缩容到最小值 | 1. 设置最小值是 1，使得节点池的其他节点都处于空闲状态<br />2. 等待 10min 后查看，空闲的节点是否为最小值 1，预期总节点数不低于最小值 1 | 满足     |