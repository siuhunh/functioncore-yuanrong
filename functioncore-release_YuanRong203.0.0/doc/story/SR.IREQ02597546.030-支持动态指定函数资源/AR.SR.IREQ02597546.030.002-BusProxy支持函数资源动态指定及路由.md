[TOC]

# 标题

关联AR:
+ AR1: AR.SR.IREQ02597546.030.002 - BusProxy 支持函数资源动态指定及路由
+ AR2: AR.SR.IREQ02597546.030.001 - FunctionInstanceManager 动态指定函数资源

## Story 整体设计

### 功能描述

支持用户动态指定 CPU / Memory 资源进行函数调用。

### 场景分析

> * 用户函数在执行函数时，遇到处理图片，文件等场景。由于图片，文件大小不确定，默认规格作元数据保存的方式，就显得不灵活了。
>
> * 动态资源指定：用户执行函数的时候可以依据自己需要处理的信息源大小，预估需要使用的规格，指定规格调度；不依赖元信息内的静态资源。

### 方案详细设计

#### 现状分析

当前发布包不支持该场景，需要开发。

#### 方案设计

![支持动态指定资源函数调度](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/11c9?t=1631322741102)

1. 用户指定资源调度，管理面 rdispatcher 收到后将 CPU / Memory 写入 header 传入 FunctionAccessor。

   暂定： `X-Instance-CPU`，`X-Instance-Memory`

   元戎管理面场景：FunctionAccessor 上游 adminservice 也需要适配 header，进行转发。

2. FunctionAccessor 在请求过滤时判断传入是否有相关字段，如果有，写入后续请求 header 传给下游 BusProxy。

3. FunctionAccessor 本地实例列表结构修改：
  
    - FunctionAccessor 在发起请求调度到 BusProxy 时，有两个选择：
    
        1. 如果实例存在，在本地会维护实例调度信息，根据负载均衡算法随机选取实例进行调度。
        
            * 问题: 当前维护实例信息通过 urn 字段维护，非函数urn，来源：ETCD信息，实例相关的 key。不支持不同资源信息的维护。
            
            ```go
                urn = fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", urnutils.DefaultURNProductID, urnutils.DefaultURNRegion,
                     		business, tenant, urnutils.DefaultURNFuncSign, function, version)
            ```
           
            * 方案：urn 拼接 CPU / Memory 信息，维护同一个 urn 下不同规格的实例信息。作 key 写入 worker 实例列表时，处理 urn：
            
            ```go
                urn = fmt.Sprintf("%s:%s-%s", urn, insResource.CPU, insResource.Memory)
            ```
            
            增加 CPU-Memory 字段以支持不同资源规格实例的调度。
            
            * CPU-Memory 来源：ETCD 实例信息。
        
        2. 如果实例不存在，依据 hash 算法随机选取 proxy 进行调度，proxy 会向下游申请实例。

4. BusProxy 查看发送过来的请求是否携带 CPU / Memory，创建 requestDispatcher 放入本地请求调度列表。

   functionKey 结构：

   ```go
   functionKey = tenantID + "/" + funcName + "/" + funcVersion
   ```

   修改如下，用以创建 requestDispatcher。

   ```go
   functionKeyWithResource = tenantID + "/" + funcName + "/" + funcVersion + "/" + CPU-Memory
   ```

   - 如果不存在，从缓存/ ETCD 去取CPU / Memory，即通过元数据得到值，拼接 functionKey，用以创建 requestDispatcher;
   - 如果存在，直接将 CPU / Memory 拼接到 functionKey 后面，用以创建 requestDispatcher。
   - 如果需要扩容，将 CPU / Memory 传入下游 functionInstancemanager。
   - 扩容实例成功或者本地已有实例，requestDispatcher 将请求加入调度队列，根据 functionKey 去分配实例执行调度。
   
5. BusProxy 之间的 tcp 消息体需要携带 CPU / Memory

   ```go
       type TCPRequest struct {
       	LogType         string
       	RawData         []byte
       	RequestID       uint64
       	UserData        string
       	Priority        int
       	DirectRequest   bool
       	CanBackpressure bool
       	CPU             string  // 新增
       	Memory          string  // 新增
       	TraceCtx        context.Context
       }
   ```
   
6. 本地实例调度列表需要增加资源信息控制，watch worker 事件时增加 CPU / Memory，来源同 FunctionAccessor，watch 事件控制，从实例信息拿。

7. workerBroker 使用 functionKeyWithResource 创建。

8. functionInstanceManager 收到上游 BusProxy 发过来的扩容请求，解析上游请求携带的 CPU / Memory 信息，用以扩容对应规格实例。向 ETCD 
   写实例信息时，增加 CPU / Memory 字段。

   ```yaml
         {
             "ip": "**.**.**.**",
             "port": "****",
             "status": "****",
             "p2pPort": "****",
             "nodeIP": "**.**.**.**",
             "nodePort": "****",
             "nodeName": "****",
             "applier": "****",
             "ownerIP": "****",
             "cpu": ****,
             "memory": ****
         }
   ```

#### 开源软件选型
不涉及。

#### 外部交互分析&&上下游依赖需求

1. 需要上游管理面 rdispatcher 根据用户需求按 header 传入指定资源。 -- 潘炜程，联调时间：0910

    `X-Instance-CPU`，`X-Instance-Memory`。

2. FunctionAccessor 需要解析CPU / Memory，传入 BusProxy。 实例列表维护修改。-- 张同，联调时间:0909

3. BusProxy 根据传入的 CPU / Memory 指定 functionKey，获取 requestDispatcher，加入请求队列，进行函数调度。 
   TCP 消息体增加 CPU / Memory。监听实例，增删实例列表，维护实例信息。增删workerBroker。转发扩容请求携带 CPU / Memory 传入instanceManager -- 张同，联调时间：0909

4. instancemanager 需要根据 BusProxy 传入请求，解析 CPU / Memory，如果不存在默认使用元数据，存在则使用传入值。 
   扩容实例后，写入ETCD的value需要携带 CPU / Memory。 -- 李彪， 联调时间：0909

### 对外接口
不涉及。

### 约束
不涉及。


## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  

不涉及。

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  

不涉及。

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  

不涉及。

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*  

1. 组件升级顺序：FunctionAccessor / BusProxy 需要先于 FunctionInstanceManager 升级，否则 FunctionInstanceManager 功能不可用。
2. 升级后，如果现网原有实例，需要保证原有实例可用。
   1. FunctionAccessor 选择实例调度需要识别到这个实例存在。
   2. BusProxy 请求调度需要调度到这个实例，不触发扩容请求。

#### 可用性 Availability
*处于按要求执行状态的能力*  

不涉及。

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  

不涉及。

## 用例
| **序号** | **用例名**                                               | **测试步骤**                                                 | **预期结果**                                                 |
| -------- | -------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | 不指定 CPU / Memory 场景正常执行                         | 1. 初次调用，走扩容逻辑。2. 特化后继续调用。                 | 1. 函数正常执行；2. 实例拉起的规格是指定规格。3. 开启functiontask debug日志，查看functionKey为修改后的扩展functionKey结构。4. 特化后继续调用，实例不增加。 |
| 2        | 指定 CPU / Memory 场景正常执行                           | 1. 初次调用，走扩容逻辑；2. 特化后继续调用。                 | 1. 函数正常执行。2. 实例拉起的规格是指定规格。3. 开启functiontask debug日志，查看functionKey为扩展functionKey结构。4. 特化后继续调用，实例不增加。 |
| 3        | 指定 CPU / Memory，与默认元数据的 CPU / Memory一致，执行 | 1. 以元数据的 CPU / Memory 进行调度，拉起实例；2. 指定资源调度，以元数据的 CPU / Memory 进行函数调用 | 1. 函数都能正常执行。2. 两种方式使用的规格一致，实例数共用一个，不会拉起多个。 |
| 4        | 默认资源实例和指定其他资源的实例互不影响。               | 1. 调用普通函数A，实例拉起。2. 调用函数A，指定 CPU / Memory，不同于元数据默认值，会继续拉起实例。 | 1. 拉起一个函数的两个实例，指定资源调度和不指定资源调度不能调到对方实例。 |

