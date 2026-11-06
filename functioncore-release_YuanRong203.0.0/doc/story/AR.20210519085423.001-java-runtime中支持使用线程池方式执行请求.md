[TOC]

# 【元戎】java runtime中支持使用线程池方式执行请求 AR.20210519085423.001

## Story 整体设计

### 功能描述
Java runtime中支持使用线程池方式执行请求，提供并发处理worker请求的能力

### 场景分析
+ Java runtime收到大量invoke请求

### 方案详细设计

#### 现状分析
+ Java Runtime中目前使用Executors来创建CachedThreadPool实现线程池，此线程池默认核心线程数为0，最大线程数为2147483647（int32最大值），超时时间为60s，默认阻塞式队列（默认大小为int32最大值）

```java
private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
```

#### 方案设计
+ Java Runtime 流程图 ![Java Runtime 流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/pW9?t=1623311510633)

+ 使用ThreadPoolExecutor实现线程池，配置核心线程数为CPU_NUMS + 1，最大线程数为Integer.MAX_VALUE（2147483647），超时时间为60s，默认阻塞式队列（默认大小为Integer.MAX_VALUE）

```java
private static final int CPU_NUMS = Runtime.getRuntime().availableProcessors();

private static final ThreadPoolExecutor THREAD_POOL = new ThreadPoolExecutor(
        CPU_NUMS + 1, Integer.MAX_VALUE, 60L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
        new RuntimeThreadFactory(), new ThreadPoolExecutor.DiscardPolicy());
```

# 开源软件选型
*新引入的开源软件：原因、选型调研情况*

#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*

### 对外接口
*描述对外新增的接口*
+ 无
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

## LLT 用例
| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
| 1        | Invoke请求                                                    | 线程池繁忙，队列持续增长                  | 线程池繁忙，队列持续增长                     |
