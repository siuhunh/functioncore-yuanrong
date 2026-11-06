[TOC]

# functiontask、workermanager服务支持配置热加载

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

*描述该特性需求来源或背景，比如：XXX竞争对手分析、XXX客户需求、XXX内部优化等。描述该特性对用户带来什么具体价值，如果没有该特性，对客户或竞争力带来什么损失。*

需求来自于内部运维优化需求。

当前YuanRong微服务的配置变更后，需要重启微服务才能生效，现网变更难。例如：现网变更worker-manager的资源池大小配置，需要1.修改worker-manager配置 2切流到其他集群 3.重启worker-manager。这种方式不仅效率低，还有切流带来的风险。

针对当前配置变更难的情况，需要实现YuanRong微服务关键配置热加载的能力，实现配置修改后无需重启即可生效的能力，提升运维效率，减少配置变更带来的风险

此AR只限定于functiontask、worker-manager两个微服务的关键配置热加载

### 1.2特性场景分析

*描述该特性的业务使用场景内容包括：*

*(1）场景触发条件及对象：什么角色/工具/接口等在什么具体情况下使用该特性，使用对象技能如何？*

*(2）使用时间及频度*

*(3）描述该特性主要有哪些场景、子场景及关键任务操作。*

[配置热加载用例图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=eb21d526382243b99f8823d9321bb664)![配置热加载用例图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Fix?t=1657538225823)

当现网出现问题，需要变更functiontask（worker-manager）配置来规避问题时

运维人员修改functiontask（worker-manager）配置，触发配置热生效能力，实现不重启functiontask（worker-manager）并生效修改的配置

## 2. 特性/功能实现原理

### 2.1总体方案

*该章节主要阐述该特性的详细设计，包括选择什么器件，使用什么算法，架构如何布局，打算UI**如何呈现，希望获取什么用户信息支撑后续设计等。*

k8s支持pod的configmap修改后，10s内自动更新Pod的配置文件

- 前提：通过目录挂载的configmap有热更新能力，其他ENV或者subPath挂载的方式还不支持热更新
- functiontask/worker-manager排查&验证后，均使用目录挂载的方式，configmap侧支持热更新

服务只需要监听本地配置文件的变化，如果文件的hash值发生变更，读取最新的配置，并刷新热生效配置

[服务支持配置热加载](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=b61ad40628a3446fbd27bf230c5a8574)![服务支持配置热加载](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2AJU?t=1657590143127)

1.服务进程启动时，启动配置文件监听

2.运维人员修改服务的configmap

3.k8s 10秒左右更新服务容器内的配置文件

4.服务监听到配置文件变化，如果文件的hash值发生变更，读取最新的配置，并刷新热生效配置

### 2.2模块详细设计

*各模块的具体实现，包括模块对模块实现、外接口定义、模块类设计、

#### 2.2.1common模块配置文件监听公共能力设计

##### 2.2.1.1模块实现

*以时序图的形式描述模块的功能需求和非功能需求的实现过程*

在common模块中新增FileWatcher子模块，提供公共的文件监听能力，供functiontask、worker-manager以及后续模块使用

##### 2.2.1.2模块对外接口定义

*描述模块的对外接口（如果有），所有外部模块只关注模块接口，不关注模块实现*

```
// 文件监听器初始化
func InitFileWatcher(stopCh <-chan struct{}) error
// 获取文件监听器
func GetFileWatcher() FileWatcherInterface
// 文件监听器API
type FileWatcherInterface interface {
	Start()
	RegisterCallback(filename string, callback FileChangedCallback)
}
// 配置监听回调函数
type FileChangedCallback func(filename string, opType OpType)
// 设置文件监听器创建函数
type CreateFileWatcher func(stopCh <-chan struct{}) (FileWatcherInterface, error)
```

##### 2.2.1.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*

[配置监听类图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=3ee25bc835ad4718a6beb201ec0f3997)

![文件监听公共模块](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2CVj?t=1657004348909)

1.外部通过InitFileWatcher来初始化文件监听模块

2.初始化成功后，通过GetFileWatcher接口得到FileWatcherInterface，再调用RegisterCallback来注册文件回调函数

3.外部不感知具体的配置监听能力实现方式，只关注自己需要监听的文件和回调函数，实现接口依赖

4.defaultFileWatcher实现了默认的配置文件监听能力，使用fsnotify开源库实现配置监听能力，当监听到文件变化时，如果文件的hash值发生变化，则触发用户回调函数

5.通过SetCreator来指定FileWatcherInterface的创建，实现本地可测试

#### 2.2.2functiontask模块配置热加载设计

##### 2.2.2.1模块实现

[functiontask配置热加载](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=e7ca78f6a03047ffabfe1bdb58c8a82d)![functiontask配置热加载](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Fmh?t=1657588497759)

1.functiontask注册/home/sn/config/config.json配置文件的监听，设置callback函数

2.callback函数读取新配置，并将关键配置刷新到全局配置变量中，再调用服务注册的回调函数

3.服务的回调函数会将配置通知到使用配置的子模块，由子模块（例如内存监控模块）执行最终的更新动作

**functiontask需要热加载的配置项**

| 配置项                    | 配置项说明                         | 是否需要动态加载                                       |
| ------------------------- | ---------------------------------- | ------------------------------------------------------ |
| **IdleTimeForWorker**     | **worker空闲缩容时间**             | **是**                                                 |
| RequestQueueTimeout       | 请求排队超时时间                   | 否，管理面超时60s，队列超时40s，不需要热加载           |
| EtcdConfig                | 元数据etcd配置                     | 否                                                     |
| RouterETCDConfig          | 路由etcd配置                       | 否                                                     |
| RetryConfig               | 无使用                             | 否                                                     |
| InvokeLimit               | 无使用                             | 否                                                     |
| RequestConfig             | workmanager连接超时时间            | 否                                                     |
| LocalAuth                 | 本地鉴权配置                       | 否                                                     |
| HTTPSConfig               | http配置                           | 否                                                     |
| MetricsServerConfig       | redis配置                          | 是，已动态加载，连接redis失败时，动态读取              |
| WorkerMetricsTimeWindow   | 中心缩容上报时间                   | 否                                                     |
| **AutoScalerConfig**      | **弹性配置**                       | **是，除RequestLatencyAggregationThreshold外均热生效** |
| AuthenticationEnable      | 与frontend鉴权开关                 | 否                                                     |
| ConnectionAuthCheck       | 与worker鉴权开关                   | 否                                                     |
| AlarmServerAddress        | 告警配置                           | 否                                                     |
| MaxRequestBodySize        | 请求体最大限制                     | 否                                                     |
| **MemoryControlConfig**   | **内存水位配置**                   | **是**                                                 |
| **MemoryEvaluatorConfig** | **请求响应内存使用量倍数估计配置** | **是**                                                 |

##### 2.2.2.2模块对外接口定义

functiontask内部，无对外接口

##### 2.2.2.3模块类设计

无新增类，无需描述



#### 2.2.3worker-manager模块配置热加载设计

##### 2.2.3.1模块实现

1worker-manager注册/home/sn/config/worker-manager.conf配置文件的监听，设置callback函数

2.callback函数读取新配置，并将关键配置刷新到全局配置变量中，再调用服务注册的回调函数

3.服务的回调函数会将配置通知到使用配置的子模块，由子模块（例如poolmgr）执行最终的更新动作

**worker-manager需要热加载的配置项**

| 配置项                    | 配置项说明                         | 是否需要动态加载                                       |
| ------------------------- | ---------------------------------- | ------------------------------------------------------ |
| PoolConfig.PoolSize   | 资源池数量                       | 是                                                 |
| ElasticScalingConfig  | 扩缩容参数                       | 后续考虑支持，目前这部分配置托管在deployment中，改造工作量较大 |

![worker-manager配置热加载](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2FkV?t=1657542898581)

1. worker-manager为主备模式，主备均需要对配置文件进行监听，并设置callback函数
2. callback函数读取新配置，并将PoolSize配置刷新到全局配置变量中，再调用服务注册的回调函数
3. 服务注册的回调函数分为两类：同步配置变化——根据刷新的全局配置，去对应刷新内存中的结构体数据缓存；处理配置变化——修改对应pool的deployment中的replicas字段，自适应拉起/杀掉对应的监控协程等
4. 针对第一类回调函数，主备节点均需注册并执行；针对第二类回调函数，仅需主节点去执行
5. 主备倒换期间去修改配置的临界场景，若是先升主，配置文件再变化，由新主处理即可，自然没问题；若是配置文件先变化且旧的主没来得及处理变化，因为新的主已经在第一个回调函数中同步了配置变化，故其启动流程也会使用到最新的配置并使其生效
6. 特别的，poolsize修改可能会与自动扩缩容产生交集，为了避免非预期的影响，当前采用如下策略：
   a. 若新的poolsize大于当前实际replicas，才会去更新pool deployment中的replicas；
   b. 若新的poolsize小于当前实际replicas，不会立刻去更新pool deployment中的replicas，后面流量降低后，扩缩容机制会将pool deployment中的replicas调和至新的poolsize；
   c. 若新的poolsize为0，认为是资源池禁用动作，会立刻去更新pool deployment中的replicas为0；

##### 2.2.3.2模块对外接口定义

无对外接口

##### 2.2.3.3模块类设计

无新增类



### 2.3 约束

#### 2.3.1上下游依赖需求

#### 2.3.2其他约束

当前配置项值校验，只校验值是否为0，没有对范围进行校验

当前现网configmap配置变更流程已规范化，配置变更需要经过SE、维护PL评审，由专业运维人员操作，无风险

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

现网配置的修改需要满足以下条件：

1.运维人员具有绿区访问权限

2.运维人员账户配置了操作配置的权限

如果不满足，无法进行配置的修改。因此，不认为配置修改有被恶意威胁的可能，不涉及

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
同3.1安全性分析，配置修改不在威胁分析的攻击路径上，不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

1.当FileWatcher子模块初始化失败时，不影响服务正常启动和运行，只是动态配置不生效

2.functiontask只热加载关键配置，不做配置全量刷新，避免未知配置变更风险

3.worker-manager只热加载关键配置，不做配置全量刷新，避免未知变更风险

4.配置文件读取失败或者反序列化失败时，配置不更新，对服务无影响

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

同可靠性分析，可靠性分析的4点不仅提升了可靠性，也同时提升了可用性

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
同3.1安全性分析，配置修改不在威胁分析的攻击路径上，不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

1.配置更新时，functionbus、worker-manager打印配置项名、配置新值、配置旧值

2.defaultFileWatcher有监控健康打印，10分钟打印一次，如果一直没打印，说明监控失效

3.配置文件变更开始以及结束时，打印变更文件名、文件类型、变更类型、callback执行时间

### 3.8可测试性

通过SetCreator来指定FileWatcherInterface的创建，方便本地开发者测试

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

配置修改频率极低，生效时间在10s左右，无特别性能要求，1分钟左右均可接受

配置变更频率极低，对函数调用性能无任何影响

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

公共监听使用一个go协程，可以忽略，fsnotify已经在worker使用，无资源使用风险

配置变更频率极低，配置文件不大于1M，对cpu、内存的影响可以忽略

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

可维护性需求，用户无感知，无资料要求

## 4. 用例

| **序号** | **用例名**                      | 用例类型                  | **测试步骤**                                            | **预期结果**                                                 |
| -------- | ------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------ | -------- |
| 1 | TestInitFileWatcher | LLT | 创建文件并监听，向文件写入内容 | 向文件写入内容后，成功触发回调函数 |
| 2 | TestHotLoadConfig | LLT | mock文件监听器，触发functiontask回调函数 | functiontask配置项更新成预设的值 |
| 3      | functiontask显式设置optional热加载配置 | ST | 修改functiontaskconfigmap，显式设置optional的热加载配置项 | 10s左右，functiontask配置热生效，日志打印配置项名、配置新值、配置旧值 |
| 4       | functiontask修改热加载配置   | ST             | 修改functiontaskconfigmap，修改热加载配置项         | 10s左右，functiontask配置热生效，日志打印配置项名、配置新值、配置旧值 |
| 5       | functiontask修改热加载配置，配置项名错误 | ST | 修改functiontaskconfigmap，修改热加载配置项，设置错误的配置项名 | functiontask配置未更新                                    |
| 6       | 静态场景：增大一个或多个PoolSize  | ST | 老的资源池大小10，实际replicas为10，修改worker-manager configmap，修改新PoolSize为20 | 10s左右，worker-manager配置热生效，日志打印配置项名、配置新值、配置旧值，对应pool的deployment中replicas直接更新20 |
| 7       | 静态场景：减小一个或多个PoolSize  | ST | 老的资源池大小10，实际replicas为10，修改worker-manager configmap，修改新PoolSize为5 | 10s左右，worker-manager配置热生效，日志打印配置项名、配置新值、配置旧值，对应pool的deployment中replicas不会立刻更新，而是被1min周期缩容更新到5 |
| 8      | 动态扩容场景：增大一个或多个PoolSize  | ST | 老的资源池大小10，实际replicas为20，修改worker-manager configmap，修改新PoolSize为15 | 10s左右，worker-manager配置热生效，日志打印配置项名、配置新值、配置旧值，对应pool的deployment中replicas保持20，然后被1min周期缩容更新到15 |
| 9       | 动态扩容场景：再次增大一个或多个PoolSize  | ST | 老的资源池大小10，实际replicas为20，修改worker-manager configmap，修改新PoolSize为30 | 10s左右，worker-manager配置热生效，日志打印配置项名、配置新值、配置旧值，对应pool的deployment中replicas直接更新30 |
| 10      | worker-manager修改PoolSize从0->非0 | ST | 老的资源池大小0，实际replicas为0，修改worker-manager configmap，修改新PoolSize为5 | 10s左右，worker-manager配置热生效，日志打印配置项名、配置新值、配置旧值，对应pool的deployment中replicas直接更新5，日志打印对应pool的两个监控协程有拉起动作 |
| 11      | worker-manager修改PoolSize从非0->0 | ST | 老的资源池大小5，实际replicas为10，修改worker-manager configmap，修改新PoolSize为0 | 此操作被识别为资源池禁用操作，对应pool的deployment中replicas直接更新0，日志打印对应pool的两个监控协程退出 |
| 12      | 主备倒换期间修改PoolSize | ST | 修改热加载配置项，同时停主，触发主备倒换 | 10s左右，worker-manager配置热生效，日志打印配置项名、配置新值、配置旧值，对应pool的deployment中replicas更新为配置的值 |
