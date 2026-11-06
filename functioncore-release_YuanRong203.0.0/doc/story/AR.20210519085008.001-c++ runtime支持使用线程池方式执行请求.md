[TOC]

# 【元戎】c++ runtime支持使用线程池方式执行请求 AR.20210519085008.001

## Story 整体设计

### 功能描述
为防止c++ runtime收到大量invoke请求时产生过多线程，改用线程池方式执行请求，提供并发处理worker请求的能力

### 场景分析
+ c++ runtime收到大量invoke请求

### 方案详细设计

#### 现状分析
+ c++ runtime 中目前没有使用线程池，每次invoke请求都会创建一个新的线程执行

```cpp
    // psudo code
    ...
    case PACKET_TYPE_REQUEST: {
        std::thread handleThread(handleRequestMessage);
        handleThread.detach();
        break;
    }
    ...
```

#### 方案设计
+ 在runtime启动时创建一个固定大小的线程池，每次有请求时将请求塞入队列中，线程池中的线程轮询从队列头拿请求并执行。
![story: c++ 线程池](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/rfI?t=1624500653980)

```cpp
class ThreadPool {
public:
    using HandleFunc = std::function<void()>;

    // 在后台启动大小为n的线程池
    void Init(size_t n);

    // 往线程池队列中塞入一个请求
    void Handle(HandleFunc fn);
};
```

```cpp
    ...
    case PACKET_TYPE_REQUEST: {
        threadPool.Handle(handleRequestMessage);
        break;
    }
    ...
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
