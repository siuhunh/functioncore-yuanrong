# Java-Runtime提供Context获取接口，避免ServiceBridge互调场景被限流
# 关联 AR

AR.SR.IR20230331002271.011.001
AR.SR.IR20230331002271.011.002

# 背景描述

FunctionTask会根据context的traceId做限流，令牌桶默认1秒补充一次。当前M2S会在函数入口将Context存为环境变量，ServiceBridge在创建连接时获取该变量，造成

1. ServiceBridge对M2S的不合理依赖
2. 微服务通过连接池使用ServiceBridge Connection的场景，Connection创建之后就无法刷新context，traceId无法更新，导致高并发场景下很可能被限流

# Story 整体设计

## 功能描述

* Java-Runtime提供Context获取接口，在用户函数内部可以调用该接口获取到当前请求的context
* ServiceBridge修改获取context的方式，在互调时刷新context，避免全局存储

## 场景分析

ServiceBridge获取context的场景主要有以下：

* 微服务使用m2s托管成serverless函数后，BridgeSDK 注册 Driver 时需要通过m2s提供的 YuanRongHelper 获取 context 进行注册，之后在需要生成 Function 对象时传入
* 普通微服务想要访问 ServiceBridge，通过 LivedataDriver 或 HttpDriver 访问 BridgeAgent 函数进而访问 BridgeFunc 的时候，Agent函数内向 DriverManager 注册 Driver 时传入context，后续调用 BridgeSDK 时使用，之后在需要生成 Function 对象时传入

由于4月份迭代有一个getInstance优化的需求，[需求文档](https://wiki.huawei.com/domains/867/wiki/5357/WIKI20230404956572)
在这个需求中，为了性能，FunctionGraphDriver 中会缓存 Function 对象，而 Function 对象的创建是需要传入context的，这就导致后续 invoke 的时候都是用的缓存的对象，每个对象中 context 都已经固化了，并不是跟每次调用相关联的 context

## 方案设计

### Java-Runtime侧

在runtime中，调用初始化函数的 InitializerService.init 、 调用函数的 InvokeService.invoke 和 Restore函数的 RestoreService.restore 中都会生成Context，且三个流程类似，以invoke流程为例，设计如下

新增 **ContextHolder** 类用于持有 Context 的线程变量，并提供保存、获取、删除的方法给 ServiceBridge使用

1. 在 init 和 invoke 开始，反射调用用户方法之前调用saveContext 将 Context 对象存入 ContextHolder 中的 InheritableThreadLocal 类型的成员变量
2. 调用用户方法，若用户方法中使用了BridgeSDK，那么在 BridgeSDK 中要 getBridge/invoke bridgefunc/releaseBridge （实际只有invoke中会使用到context）时创建 Function 对象的时候调用 getContext 获取当前的线程变量传入
3. 调用用户方法结束，无论结果如何，调用 removeContext 将线程变量移除，避免资源泄露

当有并发请求时，每个并发的调用在runtime侧从线程池中获取线程后，生成context然后去执行请求（执行用户代码），因此在用户代码中通过 getContext 获取到的 Context 对象每个线程都能保持相互独立，从而保证调用 BridgeFunc 的context 中的traceId 每个请求是不一样

[Context流转场景](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=cca05c6d4a844fc094f5b33159c8019d)![Context流转场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/a4241c90ebdb47d2af93709f5596633e?t=1686567834259)

对内接口：

| 方法名称 | 返回参数类型 | 入参 |
| --- | --- | --- |
| saveContext |void  | Context |
| removeContext | void | void |
| getContext| Context| void |

其中 saveContext 和 removeContext 由子类 ContextProcessor 实现，在runtime内部流程中使用，不对外直接暴露，对下游（用户函数）仅暴露 getContext 接口

ContextHolder参考写法

```Java
public class ContextHolder {
    private static final ThreadLocal<Context> THREADLOCAL = new InheritableThreadLocal<>();

    /**
     * Get Context
     *
     * @return Context
     */
    public static Context getContext() {
        return THREADLOCAL.get();
    }
}
```

ContextProcessor参考写法

```Java
public class ContextProcessor extends ContextHolder {
    /**
     * Save Context
     *
     * @param context Context
     */
    public static void saveContext(Context context) {
        THREADLOCAL.set(context);
    }

    /**
     * Remove Context
     */
    public static void removeContext() {
        THREADLOCAL.remove();
    }
}
```

### ServiceBridge侧

1、当前invoke时获取Function对象的方式：

```Java
func = getFunctionObject(bridgeFuncName, instanceId);
ObjectRef<String> ref = func.invoke(request.toString());
```

getFunctionObject 的主要逻辑是：

```Java
Function func = funcMap.get(key);
return func != null ? func : newAndPutFunction(bridgeFuncName, instanceId);
```

2、计划修改成invoke时候再创建

```Java
Context context = ContextHolder.getContext();
Function func = new Function(context);
func.getLocalInstance(bridgeFuncName, instanceId);
ObjectRef<String> ref = func.invoke(request.toString());
```

并提供FunctionGraphDriver的无参构造函数，Agent函数可以在注册Driver的时候不设置context


### 开源软件选型

无新开源软件引入

### 外部交互分析&&上下游依赖需求

1、由于需要将之前缓存 Function 对象去掉，会依赖于《Java-runtime新增getLocalInstance接口供ServiceBridge使用，优化原getInstance接口时延》的需求，否则会造成性能损失。
2、ServiceBridge 侧会依赖 Java-Runtime 提供的 ContextHolder，但由于两个代码仓是同时构建的，在 ServiceBridge 合入代码的时候会需要中心仓上已发布的 Java-Runtime 提供这部分接口，因此考虑先个人先发布SNAPSHOT版本到中心仓，ServiceBridge 引入的 Runtime 更新为 SNAPSHOT版本，用于上库，流水线构建时版本会设置统一，并不会使用该 SNAPSHOT 版本

### 对外接口

无

### 约束

本需求解决的限流场景是如下所示：
![解决的限流场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3d4c87cc69db4b1c8d02ab9a7a540f0d?t=1686714646288)

但是对于如下的这种场景，不在本次需求的覆盖范围之内，这是因为单次请求内调用了N次 Bridge函数，即使每次调用Bridge函数都获取context，但是因为单次请求只会有一个线程，Context的线程变量都是一样的
![暂不考虑的场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/e687238706bb44f0842db55009546bcf?t=1686714782701)

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

测试感知的主要是 BridgeSDK 的日志中会新增打印 traceId，两次请求主调函数的同一个实例，日志里体现的 traceId 是需要不同的

**java-runtime侧：**

| 用例名| 测试步骤| 预期结果|
| --- | --- | --- |
| testGetContextInSingleThread | 单线程下1次存入context，然后重复获取context | 2次获取的Context里的traceId是一致的 |
|testGetContextInMultiThread |开启2个线程，每个线程各自存入context，然后各自获取context | 2次获取的Context里的traceId是不一致的 |
| testGetContextAfterRemoved | 单线程下1次存入context，然后获取context，之后删除context再获取 | 第一次能获取到设置的traceId，第二次获取到的context是null|

**ServiceBridge侧：**

| 用例名| 测试步骤| 预期结果|
| --- | --- | --- |
| testGetContextWhenSerialInvocation | 1、设置context；2、调用Bridge函数时获取context；3、再设置不同的context；4、调用Bridge函数获取context，比对context | 两次context不同|
| testGetContextWhenConcurrentInvocation | 1、设置两个线程，两个线程分别设置不同的context；2、在自己的线程中调用Bridge函数时获取context，比对context | 两个线程中获取到的context不同


