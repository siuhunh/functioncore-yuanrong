[TOC]

# 普通实例Load、Init日志关联函数调用请求traceID

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

需求来源：客户需求

需求描述：针对普通实例，其load、init日志只关联、合并到首次invoke（即冷启动）日志中，不再于请求列表页面单独呈现，避免让用户产生疑惑

需求价值：用户体验提升

### 1.2 特性场景分析

当用户调用函数时，能方便的通过请求id来追溯到函数日志，其load、init日志只关联、合并到首次invoke（即冷启动）日志中，不再于请求列表页面单独呈现。

## 2. 特性/功能实现原理

### 2.1 现状

**invoke成功场景：**

[函数调用成功](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=4021d3c7749945159ecb26a0787a3fde)![函数调用失败](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/322c)



**invoke失败场景：**

[函数调用失败](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=4021d3c7749945159ecb26a0787a3fde)![函数调用失败](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2g0R?t=1663256424168)

在函数监控页面查看其日志：

[函数相关日志](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=dd64929096eb4cd6ab23d8c620e757d8)![函数相关日志](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2g0S?t=1663256518430)

可以看到本次冷启动产生了3条日志，请求ID各不相同：

- 蓝色框内的2条日志分别为load、init日志，它们对应的请求ID并非管理面下发的traceID，而是worker在loadfunction时内部生成的UUID；
- 红色框内的日志是invoke调用日志，它对应的请求ID是管理面下发的traceID；



用户的直观感受：

- 从请求ID来看，很难直接将最后一条调用失败的日志与前两条load、init日志进行关联，有疑惑；



**init失败，导致invoke失败：**

- **在init失败时，也补充invoke日志信息，辅助用户定位**

  - 当init失败时，会默认补充一条invoke id相关的日志
  - 用户通过invoke id，能够在LTS中找到对应日志，并从中提取instanceid
  - 用户根据instanceid + stage做过滤检索，即可以关联上之前的init日志

  [效果展示](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=6d24e42561cd4cd5abbb3e2a50e3b2c8)![效果展示](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2k5U?t=1664307995083)

  

### 2.2 总体方案

**目标方案：全面优化日志布局呈现，提升用户体验，分三阶段进行，此次需求制作第一阶段**

- 1、**针对普通实例，其load、init日志只关联、合并到首次invoke（即冷启动）日志中，不再于请求列表页面单独呈现，避免让用户产生疑惑；**
- 2、针对预留实例，先维持现状，在请求列表页面单独展示init、load日志，待后续总体方案设计好再考虑优化；
- 3、关于如何在请求列表页面体现冷启动标识，待进一步调研分析；





### 2.3 模块详细设计

#### 2.3.1 worker子模块设计

##### 2.3.1.1模块实现

只针对普通实例：

1. worker在收到bus的invoke请求时，将load、init的日志缓存在内存中，不在load、init这2个阶段落盘，并进行大小限制，例如10MB，超过大小限制的日志进行丢弃，只保留最新的日志。
1. 到了invoke阶段，使用收到的invoke的TraceID将缓存中的load、init日志的部分信息进行替换后，再落盘。
1. 在 invoke替换阶段 的耗时与内存 保留invoke请求本身的不做更改
1. load或者init失败，按之前补的空的invoke日志需要正确显示load或者init失败的原因

**补充：**

**资料中需要体现10MB的大小限制**

**自定义镜像的调用成功也需要做**







##### 2.3.1.2模块对外接口定义

pkg/worker/functionlog 新增接口

```
// FinishCopy is to copy load and init log to mem in normal instance 将LogRecorder保存到缓存中并释放老的logRecorder
func (r *LogRecorder) FinishCopy(ic invokectx.InvokeCtx)
```

```
// FinishSave is to save load and init log to disk in normal instance
func (r *LogRecorder) FinishSave()
```

```
// ReplaceTraceID is to replace load and init log in normal instance  invoke阶段使用收到的invoke的TraceID进行替换
func (r *LogRecorder) ReplaceTraceID(traceID string) 
```

```
// WriteLimit write limited log to logRecorder. not reserved to disk  在初始化阶段worker收到日志，日志保存且不落盘
func (r *LogRecorder) WriteLimit(funcLog *FunctionLog)
```



```
// WriteLimit is to save limitSize log 在初始化阶段worker收到日志，日志保存且不落盘
func (r *FixSizeRecorder) WriteLimit(funcLog *FunctionLog)
```

```
// Stage -
func (r *LogRecorder) Stage() string
```

```
// Generator -
func (r *LogRecorder) Generator() LogHeaderFooterGenerator {
   return r.generator
}
```

新增全局变量缓存

```
// LogRecorderBox -
var LogRecorderBox = make([]*LogRecorder, 0, 2)
```

##### 2.3.1.3模块类设计

不涉及新增类

### 2.3 约束

#### 2.3.1上下游依赖需求

不涉及

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

需补充资料来指导用户进行日志的关联定位。

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名**   | 用例类型 | **测试步骤**                                                | **预期结果**                                                 |
| -------- | ------------ | -------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| 1        | 普通实例调用 | ST       | 多runtime，成功调用，观察其调用日志                         | 返回的request id在LTS中可以检索默认的invoke日志，且load、init日志都可在第一次invokeID中找到 |
| 2        | 普通实例调用 | ST       | 多runtime，构造init失败，观察其调用日志                     | 返回的request id在LTS中可以检索默认的invoke日志，且load、init日志都可在第一次invokeID中找到 |
| 3        | 普通实例调用 | ST       | 多runtime，成功调用，初始化打印日志20MB，只能打印出小于10MB | 初始化打印日志20MB，只能打印出小于10MB                       |

