[TOC]

# [WIP] 支持自定义镜像函数

关联AR：

        AR.SR.IREQ02597546.034.001: 支持部署和弹性自定义容器镜像
        AR.SR.IREQ02597546.034.002: 支持调用自定义容器镜像

## 1. Story 整体设计
### 1.1 功能描述

自定义镜像：用户打包上传容器镜像，由平台加载并启动运行，调用方式与http函数类似

功能checklist：
- ~~[] 创建函数版本 -- 不需要特殊处理~~
- ~~[] 创建函数别名 -- 不需要特殊处理~~
- [x] 下载镜像
- [x] 部署函数
    - [x] 普通部署
    - [x] 预留实例
    - [x] vip指定节点部署
    - ~~[] 资源池 -- 自定义函数镜像不走资源池~~
    - [x] custom-resource
    - [x] reclaim
    - [] prewarm -- 遗留
    - [] datasystem -- 遗留
- [x] 支持配置环境变量
- [x] 支持配置加密环境变量
- [x] vpc
- [x] 支持挂载外部数据盘
- ~~[] 添加依赖代码包（layer） -- 都打在镜像里，不实现~~
- [x] 支持设定用户进程uid，gid
- [] 支持512M数据盘限制
- [x] 限制容器capability
- [] 监控函数日志
- [x] 初始化计费
- [x] 调用计费
- [x] 超时时间 -- 不需要特殊处理，需验证
- [x] 支持函数并发 -- 不需要特殊处理，需验证
- [x] 调用init
- [x] 调用invoke
- [x] http函数调用
- [] 调用时返回该调用间的日志
- [] 调用时返回使用内存
- [] oom检测 -- 遗留
- [] 防计费偷跑 -- 遗留
- [] 调用链 -- 遗留，当前仅java函数支持

### 1.2 场景分析

#### 1.2.1 FaaS的门槛

> 本段摘自 https://developer.aliyun.com/article/772788

Serverless 形态的云服务帮助开发者承担了大量复杂的扩缩容、运维、容量规划、云产品打通集成等责任，使得开发者可以专注业务逻辑、提高交付速度 (Time-to-market) ，持续优化成本。Function-as-a-Service (FaaS) 作为云上最早也是应用最广泛的 Serverless 计算形态，在几年的时间内吸引了大批开发者，逐渐建立了 Serverless 优先的选型逻辑。 然而从传统应用迁移到 FaaS 在开发者体验上还面临诸多挑战:

1. 环境不统一：各厂商定义的交付物格式，运行环境兼容性、丰富度都不尽相同，需要开发者适配，甚至重新编译。
2. 学习成本：打包依赖库、构建成压缩代码包和熟悉的开发部署方式不同
3. 服务限制：如代码包限制在百 MB 级别，迫使交付物代码依赖分离，加大管理和发布难度。
4. 交付物缺乏版本管理：格式不标准，最佳实践不统一，需要开发者自行负责。
5. 生态不成熟：缺少流行开源工具（如 CI/CD 流水线）的支持和集成。

另一方面，容器在可移植性和交付敏捷性上实现了颠覆式创新。围绕容器的生态沉淀非常丰富且成熟，被广泛接受使用，应用容器化正在快速成为开发和部署的事实标准。然而容器本身并没有减轻运维、扩缩容、闲置成本、和云服务集成等难题。

|        比较项         |           容器技术           |          FaaS           |
|:------------------:|:----------------------------:|:-----------------------:|
|     应用可移植性      |  Builds once, runs anywhere  |       云厂商锁定        |
|    开发、构建工具链    |     丰富、标准的开源生态      |       云厂商提供        |
| 持续构建、交付 （CI/CD） |     丰富、标准的开源生态      |       云厂商提供        |
|   代码、环境分享复用   |        标准镜像 Layer        |    FaaS 优化的 Layer    |
|      运行时环境       |       高度灵活、自定义        |   平台指定的运行环境    |
|       运维责任        | Serverfull, 开发运维人员负责 | Serverless, 云厂商负责  |
|       成本优化        |     容器细粒度编排、部署      | 请求粒度计费，无闲置成本 |
|       伸缩速度        |  和镜像大小相关，10s-分钟级   |     极速，100ms 级别     |
|  事件驱动、云产品集成  |          弱，需自建           |        核心能力         |

#### 1.2.2 函数计算支持容器镜像

把容器镜像作为函数，旨在将容器优秀的开发、部署、生态（上线前）与函数计算自身的面运维、零闲置成本等特性（线上）相结合，升级开发者体验：

- 降低学习成本：无需修改代码或是重新编译，也无需学习FaaS的部署方式，镜像扔上来就能用。
- 更大代码限制：将依赖都打包在镜像内，避免代码合依赖分离，简化分发和部署。
- 更好生态：复用容器技术已有的持续集成、测试、版本管理等工具。

#### 1.2.3 和自定义函数（Custom Runtime）、HTTP函数（HTTP）的差异

|       比较项       |   自定义函数   |    HTTP函数    |    自定义镜像    |
|:----------------:|:--------------:|:--------------:|:----------------:|
|     支持的语言     |    任何语言    |    任何语言    |     任何语言     |
|        名称        | custom-runtime |      http      | custom-container |
|      启动方式      |    进程启动    |    进程启动    |    容器化启动    |
|      运行环境      |    指定环境    |    指定环境    | 高度灵活、自定义  |
| 与worker的连接方式 | 作为http客户端 | 作为http服务端 |  作为http服务端  |
|    依赖部署方式    |     Layer      |     Layer      |  直接打在镜像中  |
|     冷启动时延     |       快       |       快       |        慢        |

### 1.3 方案详细设计

#### 1.3.1 部署、特化、回收

FunctionAccessor收到请求后，将请求发送给proxy，实例不存在的情况下，proxy向FunctionInstanceManager发送扩容请求

FunctionInstanceManager做如下操作：

##### 从etcd源数据中获取并构建FunctionService

etcd数据格式：
```go
// runtime名
FunctionMetaInfo.FuncMetaData.Runtime = "custom-container"

// 新增FunctionMetaInfo.ExtendedMetaData中的CustomContainerConfig -- 非自定义函数镜像时留空不使用
type CustomContainerConfig struct {
    ControlPath string // 区分函数类型，当前有http和event

    // 以下字段将直接设置到扩容时创建的pod的container spec中
    Image string // 即用户自定义镜像tag
    Command []string
    Args []string
    WorkingDir string
}
```

FunctionService数据格式新增CustomContainerConfig与etcd中的相同。

##### 在podfetcher中直接通过kubernetes的api创建pod

- 创建pod不走资源池，也不会检查poolmgr中是否有相应资源规格的池子以及池子数量是多少。
- 调用中如果设置了NodeLabel，创建的pod也会调度到相应的节点上（例如vip大客户函数）。
- ForceDeploy字段会被忽略，因为现在永远不走资源池。

##### pod中的字段

###### init-container

与其他函数没有差别

###### container

2个container，一个为worker，一个为用户自定义函数镜像（runtime）

- worker与其他函数的worker没有差别，但日志、下载代码功能被阉割了
- runtime
    1. CustomContainerConfig的几个与kubernetes有关的字段会被直接设置在runtime上
    2. FunctionMetaInfo.EnvMetaData解析后会被设置在runtime的环境变量上
    3. kubenetes SecurityContext中的RunAsUser和RunAsGroup会被设置为1003，与其他函数的1003 snuser统一

##### 下载用户镜像

用户镜像储存在自己的swr服务中，需要权限才能下载，所以FunctionInstanceManager会在创建pod前使用swr api生成并设置好临时登录指令，确保用户镜像可以被kubernetes下载，具体步骤如下：

1. FunctionInstanceManager查找元数据etcd中的用户委托

    /sn/agency/business/yrk/tenant/<tenantid>/domain/<domainId>/agency/<agencyid>

    获取并解密token

2. [使用swr api生成临时登录指令](https://support.huaweicloud.com/api-swr/swr_02_0052.html)
3. 创建一个kubernetes secret并设置`.dockerconfigjson`为临时登录指令
4. 将刚才创建的secret设置为自定义镜像函数pod的ImagePullPolicy
5. kubernetes会使用设置的ImagePullPolicy下载用户镜像
6. 设置secret的owner reference为pod，使删除pod时secret也被删除。

##### 环境变量

环境变量/加密环境变量存储在FunctionMetaInfo.EnvMetaData中，

```go
// EnvMetaData define env info of functions
type EnvMetaData struct {
    EnvKey            string // 供解密时使用的key
    Environment       string // 非加密环境变量 （加密储存）
    EncryptedUserData string // 加密环境变量 （加密储存）
}
```

> 环境变量与加密环境变量在后台存储都是加密的，防止用户写入隐私数据后被明文存储。普通函数中：加密配置的变量值只写入context，非加密配置的变量值会写入runtime环境变量。参考 AR-20210630-006-001 支持设置用户环境变量并加解密。

普通函数中：FunctionInstanceManager在特化时将EnvMetaData传给worker，worker在

1. 非graph场景：使用EnvKey解密Environment字段并设置在runtime上；
2. graph场景：使用localauth解密Environment和EncryptedUserData并设置在runtime上。

自定义函数镜像直接在FunctionInstanceManager中解析Environment和EncryptedUserData字段，并设置在runtime容器的EnvVar上。

> 安全性：因为环境变量是通过kubernetes设置的，可能无法做到完全安全，已与SE对齐澄清。

##### 挂载外部数据盘

挂载外部数据盘的配置在FunctionMetaInfo.ExtendedMetaData.FuncMountConfig中，

```go
type FuncMountConfig struct {
	FuncMountUser FuncMountUser `json:"mount_user" valid:",optional"`
	FuncMounts    []FuncMount   `json:"func_mounts" valid:",optional"`
}

type FuncMountUser struct {
	UserID  int `json:"user_id" valid:",optional"`
	GroupID int `json:"user_group_id" valid:",optional"`
}

type FuncMount struct {
	MountType      string `json:"mount_type" valid:",optional"`
	MountResource  string `json:"mount_resource" valid:",optional"`
	MountSharePath string `json:"mount_share_path" valid:",optional"`
	LocalMountPath string `json:"local_mount_path" valid:",optional"`
	Status         string `json:"status" valid:",optional"`
}
```

在普通函数的特化中，runtime manager负责将FuncMounts中的路径挂载进runtime容器中，并将runtime进程设置为FuncMountUser中所指定的uid、gid。
自定义函数镜像会设置runtime容器SecurityContext中的RunAsUser和RunAsGroup达成相同效果。

在创建自定义镜像函数的pod时，FunctionInstanceManager将通过kubernetes设置runtime的[nfs volume](https://kubernetes.io/docs/concepts/storage/volumes/#nfs)为其挂载数据盘。

##### 计时

FunctionInstanceManager在创建pod后将一直等待pod的Ready状态。如果达到超时时间，或用户容器已处于CrashLoopBackOff或ImagePullBackOff状态，则特化失败。FunctionInstanceManager从监控中拿到pod的状态（Status.ContainerStatuses）并返回。

下载镜像、等待镜像Ready不计费。

##### 预留实例

预留实例的特化流程与普通特化相同，与普通特化不同的是，预留实例的扩容缩容请求不是由proxy发起的，而是FunctionInstanceManager自己维护的

##### 健康检查/异常状态

用户调用时返回一个可选的http头：x-cff-status，如果这个头的值为false，则认为用户函数异常，worker退出。不返回x-cff-status时认为用户函数正常。

##### custom-resource 自定义资源（gpu）

自定义镜像函数与普通函数几乎没有差别。普通函数将自定义资源提前写在每个池子的deployment中；自定义镜像函数在扩容时直接往pod中填写自定义资源的信息。

##### 回收 Reclaim

自定义镜像函数与普通函数没有区别。proxy向FunctionInstanceManager发送缩容请求后，不会立即删除pod，而是将pod放入回收队列中。每次特化请求会先在回收队列查找已特化的pod并返回，回收队列为空时才会部署特化。回收队列中的pod会定期删除。

#### 1.3.2 监控

##### 监控函数日志：管理面实现

1. 管理面cffagent是daemonset模式部署，它可以挂载每个节点的/var/log/pods（默认）路径为只读，并看到kubernetes为当前节点内的每个pod收集的日志，路径格式为：

    ```
    /var/log/pods/default_pool1-500-500-nodejs10.15.2-d9f48ddfd-mhh9n_6942e2be-2f7c-46a8-877a-08124c3bde7f/runtime/1.log
    ──────┬────── ───┬─── ────────────────────┬────────────────────── ──────────────────┬───────────────── ───┬─── ───┬─
          │          │                        │                                         │                     │       │
          ▼          │                        ▼                                         ▼                     ▼       │
       默认路径       ▼                       pod名                                  pod uid                容器名      ▼
                 命名空间                                                                                           日志文件
    ```

    > 日志文件的数字为第几次启动，从0开始。例如pod刚生成时日志文件为0.json，runtime挂了重启后第二个容器的日志为1.json，第三个为2.json，以此类推...

2. cffagent实时收集自定义函数镜像stdout、stderr的日志，并写入新的日志文件中。


疑问点：

1. 用户设置的lts的groupID，streamID如何通过文件名的方式传给cffagent解析？
方案：etcd元数据中有这两个信息，如何传递给cffagent？

2. cffagent每30秒（将来改为10秒）收集一次日志；k8s为容器收集的日志会因容器消亡而删除。丢日志问题如何解决？
方案：增大worker的prestop延迟退出时间

##### 监控函数指标：遗留

proxy是daemonset模式部署，它可以定时查询所在节点的kubelet收集的容器指标，并记录与自定义容器镜像相关的内容

1. kubelet暴露的http端口中的/metrics/cadvisor可以查询到该节点所有容器的指标（使用的prometheus格式）

    ```
    container_cpu_cfs_periods_total{container="",id="/kubepods/burstable/pod1463eaa1-d54c-4405-8fdb-242d83dd7770",image="",name="",namespace="default",pod="es-aka-master-0"} 829060 1631326169810
    container_cpu_cfs_periods_total{container="",id="/kubepods/burstable/pod1463eaa1-d54c-4405-8fdb-242d83dd7770/684ea017e234bd4cfd2a6990a795da34ccbbbbdb6571130af4b4c767edba1ac4",image="",name="",namespace="",pod=""} 829040 1631326165799
    container_cpu_cfs_periods_total{container="",id="/kubepods/burstable/pod1d4d04ae-1646-4cff-912a-ec0778d88a1e",image="",name="",namespace="default",pod="http-access-69b9b5dbfd-zxrrg"} 158072 1631326181860
    container_cpu_cfs_periods_total{container="",id="/kubepods/burstable/pod1d4d04ae-1646-4cff-912a-ec0778d88a1e/a3ffb6bbd8bc1ec5f275a4c9817a144d34af95deb53a57204a8b98b16cd6d8ab",image="",name="",namespace="",pod=""} 158063 1631326167795
    ...
    ```

    格式很容易解析

    ```
    <metric name>{<label name>=<label value>, ...} <value> <optional timestamp>
    ```

    cadvisor提供的指标有很多，[文档地址](https://github.com/google/cadvisor/blob/master/docs/storage/prometheus.md)

2. 安全连接kubelet暴露的http端口

    TODO

    kubelet默认暴露10250 https端口，但查询/metrics/cadvisor需要鉴权
    kubelet可以增加启动参数 --read-only-port=[port] 新开一个http非鉴权的只读端口暴露/metrics/cadvisor

3. 定时收集内存指标收集container_memory_rss

    > 其等同于普通函数runtime manager中查询的runtime容器内的/sys/fs/cgroup/memory/memory.stat中的rss字段

##### 支持512M数据盘限制：遗留

配置容器的ephemeral storage为512M。kubernetes文档中说超过限制后容器会被evict，但需要/var/lib/kubelet和/var/lib/docker在同一个盘中
CCE集群无法配置这两个路径的挂载地址（已与CCE的人反复确认过），故无法支持512M数据盘限制

[参考资料](https://blog.csdn.net/sdmei/article/details/101017405)

还有一种docker的方式，环境上已经配了，最小限制是10G，用户写到10G数据后会报空间不足，不会被杀掉

##### 限制容器capability

runtime容器设置的capability为

add KILL
drop ALL

#### 1.3.3 调用

### 1.4 上下游依赖需求

## 2. 可信软件
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

| 序号 | 用例名                      | 测试步骤                                                                          | 预期结果                                               |
|------|-----------------------------|-------------------------------------------------------------------------------|----------------------------------------------------|
| 1    | 普通调用                    | 1. 上传镜像 2. 调用自定义镜像函数                                                 | 调用成功，返回计费信息                             |
| 2    | 预留实例调用                | 1. 上传镜像 2. 设置预留实例 3. 等待预留实例ready 4. 调用自定义镜像函数            | 调用成功，冷启动速度显著增加                            |
| 3    | vip指定节点调用             | 1. 上传镜像 2. 设置vip 3. 调用自定义镜像函数                                      | 调用成功，函数跑在vip节点上                             |
| 4    | vip预留实例调用             | 1. 上传镜像 2. 设置vip 3. 设置预留实例 4. 等待预留实例ready 5. 调用自定义镜像函数 | 调用成功，函数跑在vip节点上，冷启动速度显著增加          |
| 5    | 环境变量                    | 1. 上传镜像 2. 设置环境变量 3. 调用自定义镜像函数                                 | 调用成功，镜像函数内可以看见设置的环境变量              |
| 6    | 加密环境变量                | 1. 上传镜像 2. 设置加密环境变量 3. 调用自定义镜像函数                             | 调用成功，镜像函数内可以看见设置的环境变量              |
| 7    | vpc                         | 1. 上传镜像 2. 设置vpc 3. 调用自定义镜像函数                                      | 调用成功，镜像函数内可访问vpc网段                       |
| 8    | 挂载外部数据盘              | 1. 上传镜像 2. 设置外部数据盘 3. 调用自定义镜像函数                               | 调用成功，外部数据盘已挂载进容器                        |
| 9    | 挂载外部数据盘并设置uid、gid | 1. 上传镜像 2. 设置外部数据盘 3. 设置uid、gid 4. 调用自定义镜像函数                | 调用成功，外部数据盘已挂载进容器，1号进程的uid、gid已设置 |
| 10   | 容器直接退出                | 1. 上传有问题的镜像 2. 调用自定义镜像函数                                         | 特化失败                         |
| 11   | 容器第一次调用后退出        | 1. 上传有问题的镜像 2. 调用自定义镜像函数                                         | 第一次调用成功，随后pod退出，再调用时创建新的pod    |                    |
| 12   | 错误的端口                  | 1. 上传镜像 2. 函数元信息中填入的是未监听的端口 2. 调用自定义镜像函数             | 调用失败                       |