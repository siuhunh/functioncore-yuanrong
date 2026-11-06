[TOC]

# AR.SR.01ec69f6 -【serviceBridge】新增getLocalInstance接口，优化原getInstance接口时延

关联需求：

AR.SR.01ec69f6.001 【JavaRuntime】新增getLocalInstance接口，优化原getInstance接口时延

AR.SR.01ec69f6.002 【ServiceBridge】FGDriver适配getLocalInstance接口调用


## 1. 需求场景分析

### 1.1 特性需求来源与价值概述
**需求来源:**

问题单转需求：[DTS2023050511408](https://dts-szv.clouddragon.huawei.com/DTSPortal/ticket/DTS2023050511408)

【压测-SDV-函数有状态互调】 默认配置 有状态函数互调 50线程压测报错 function invocation exception, error: internal system error 发现默认版本的functiontask 参数未调整，需要版本默认调好参数带入发布包 再task 组件加上相应的环境变量。

### 1.2 特性场景分析
**场景：** 

无状态函数并发调用有状态函数（包含bridge调用场景）

**条件：** 

无状态函数中缓存Function对象（同一个context），通过同一Function对象调用

**现象：** 

使用同一个Function对象压测有状态调用，触发有状态令牌桶限流约束，报internal system error

**场景分析：**

1. 有状态约束：同一个Function对象不断发起invoke请求，触发系统内部令牌桶限流。令牌桶大小默认1000，初始值默认1000，令牌消耗后，默认每秒刷新1个令牌桶。（防止用户递归调用A调用B，B调用A；无限制调用）
2. 性能考虑：如果每次新建Function对象，调用getInstance接口，用户函数会同FunctionCore通信，存在性能损耗
3. bridge函数现状：用户集成bridge sdk调用后端bridge函数（有状态函数），一次invoke后，bridge sdk会缓存Function对象，后续invoke调用通过缓存的Function对象进行调用，减少通信时延

为了解决有状态令牌桶限制（不缓存Function对象），同时保证有状态互调的性能：

- 在 java runtime 中提供 getLocalInstance 接口， 从缓存中读取 instanceId，避免同 functiontask 通信（需求AR.SR.01ec69f6.001）
- 在 servicebridge 中适配 getLocalInstance 接口，不再缓存 Function 对象（需求AR.SR.01ec69f6.002）


## 2. 特性/功能实现原理

### 2.1 总体方案

#### 2.1.1 设计目标和原理
目标：

在用户反复调用指定有状态函数场景下，提供一种方案，减少java runtime同core的交互，同时保证没有令牌桶限流问题。

#### 2.1.2 方案概览

1. **java runtime：**

   在 java runtime 中缓存 instanceId 的 map，其中 key 为 functionName + InstanceName(stateKey)，value 为对应的 instanceId，在多次调用同一个有状态函数时，直接从缓存中读取 instanceId, 避免同 core 交互。保证原有 Function.getInstance(funcName, instanceName) 接口语义不改变情况下，新增接口 getLocalInstance(funcName, instanceName)。

2. **servicebridge：**

   servicebridge 中集成新的 java runtime 接口 getLocalInstance ，不再缓存 Function 对接，避免令牌桶问题。



### 2.2 模块详细设计
#### 2.2.1 java runtime 接口功能

1. getLocalInstance

   新增接口，已CCB评审

   > 接口定义：
   >
   > ```java
   > public void getLocalInstance(String functionName, String instanceName)
   > ```

   getLocalInstance 接口从 sdk 缓存内尝试获取 instanceID ，新增接口实现如下语义：

   - 缓存中有值，则直接从缓存返回 instanceID 给 Function 对象

   - 若是第一次访问，缓存获取不到，则调用原有 getInstance 接口，访问 task 获取
     - 若不存在，正常返回原有异常
     - 若存在，返回 runtime sdk，基于 functionName 和 instanceName 构建缓存

2. getBridgeFunc

   新增功能：在获取到 instanceID 后，基于 functionName  和 instanceName 构建缓存

3. release/terminate

   新增功能：清除缓存信息

4. invoke

   新增功能：调用invoke时，若捕获到不存在措施（错误码：4026/4041）

[java-runtime接口](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=7545281fdf364fa09ccfcb90bcf4a9a0)![java-runtime接口](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/e6afd82fafcb4433a5113bd4cbf5802b?t=1686726445098)

#### 2.2.2 bridge sdk中调用bridge func

1. 现状分析

   bridge sdk中调用bridgefunc分为两步：

   1）创建bridge函数

   ```java
   Function func = new Function(context)
   String bridgeId = getbridgefunc(String bridgeFuncName); // 生成状态路由，bridgeId(instanceId)
   ```

   2）绑定已创建的状态路由发起调用，并缓存

   ```java
   // FunctionGraphDriver.java
   Function func = new Function(context);
   func.getInstance(bridgeFuncName, bridgeId); // bridgeId实际是statekey
   
   func.invoke(payload);
   ```

   [bridge函数调用场景时序图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=599043c696f84a609877589674c1e809)![bridge函数调用场景时序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/223811a9c4ce44fcabf28ed887ef041e?t=1686726508790)

   

2. 修改后

   1）创建bridge函数

   ```java
   Function func = new Function(context)
   String bridgeId = getbridgefunc(String bridgeFuncName); // 生成状态路由，bridgeId(instanceId)
   ```

   2）绑定已创建的状态路由发起调用，sdk中不再缓存

   ```java
   // FunctionGraphDriver.java
   Function func = new Function(context);
   func.getLocalInstance(bridgeFuncName, bridgeId); // bridgeId实际是statekey
   
   func.invoke(payload);
   ```

   

   [bridge函数调用场景时序图改造后](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=86c730691cf1460d88f7d1526f13305a)![bridge函数调用场景时序图改造后](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/92cce07945384165be5b3947f75630cc?t=1686726545101)



## 3. 可信软件

### 3.1安全性（Security）

不涉及

### 3.2韧性（Resilience）

不涉及

### 3.3隐私性（Privacy）

不涉及

### 3.4可靠性（Reliability）

不涉及

### 3.5可用性（Availability）

不涉及

### 3.6安全（Safety）

不涉及

### 3.7可维护性

不涉及

### 3.8可测试性



### 3.9性能

不涉及

### 3.10资源

不涉及

### 3.11资料

涉及，管理面统一对外提供；

### 3.12升级兼容性

新增接口，不影响升级

invoke、terminate、release、getInstance只增加缓存逻辑，升级后，对原业务没有影响

## 4. 用例

| **序号** | **用例名**              | 用例类型 | **测试步骤** | **预期结果**                                           |
| -------- | ----------------------- |--| ------------ | ------------------------------------------------------ |
| 1        | 无状态函数互调 | ST用例   | 无状态函数调用无状态函数                                     | 功能正常     |
| 2        | 无状态函数使用getInstance调用有状态函数                  | ST用例   | 1. new Functoin(context)<br>2. func.getInstance(functionName, instanceName)<br>3. func.invoke（payload） | 功能正常     |
| 3        | 无状态函数使用getLocalInstance调用有状态函数             | ST用例   | 1. new Function(context)<br>2. func.getLocalInstance(functionName, instanceName)<br>3. func.invoke(payload) | 功能正常     |
| 4        | 并发压测使用getInstance调用有状态函数                    | ST用例 | 1. new Function(context)<br>2. func.getInstance(functionName, instanceName)<br/>3. func.invoke(payload) | 功能正常 |
| 5 | 并发压测使用getLocalInstance调用有状态函数 | ST用例 | 1. new Function(context)<br/>2. func.getLocalInstance(functionName, instanceName)<br/>3. func.invoke(payload) | 功能正常 |
| 6 | 使用getLocalInstance调用多个有状态函数，内部缓存数据正常 | UT | 1. 调用有状态A，调用有状态B，内部缓存size为2<br>2. terminate有状态A，内部缓存size为1<br>3. 构造有状态B函数路由找不到异常，内部缓存size为1 | 缓存数据正常 |
| 7 | 用户函数调用bridge函数 | ST用例 |              | 功能正常 |
| 8 | 并发调用bridge函数 | ST用例 | | 功能正常 |



