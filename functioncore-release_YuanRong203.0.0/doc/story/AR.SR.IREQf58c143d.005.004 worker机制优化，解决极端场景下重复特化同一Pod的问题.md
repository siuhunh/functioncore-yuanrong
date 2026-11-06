[TOC]

# 【functioncore】worker-manager/worker机制优化，解决极端场景下重复特化同一Pod的问题

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

问题单ccb转需求，极端场景优化。worker-manager在极端条件下可能会再次发送特化请求给一个已经特化的pod，优化worker保证自身运行。
问题单链接：https://dts-szv.clouddragon.huawei.com/DTSPortal/ticket/DTS2022090709782

### 1.2特性场景分析

当前逻辑：
Worker-manager选中Pod后，会先更新本地缓存中该Pod的标志位usedFlag为true，让后续特化请求无法再选中；同时调用K8S接口修改Pod注解中的used字段为true，触发Pod update事件，将该Pod从本地缓存中彻底剔除。双保险机制。

问题分析：
02:35:28.811 worker-manager选中Pod后，K8S更新Pod的used字段没有很快成功，卡住了，此阶段Pod的在K8S中的used标签为false，同时在内存中usedFlag的值为true；

02:35:29.079 worker-manager续租失败，进程重启，内存中usedFlag丢失，同时更新Pod的used标签的协程没做完也跟随退出；（租约可配置，续租失败重启的处理动作是否合理，统一审视）

02:35:37.505 worker-manager重启后，重新生成PodList缓存，但是因为Pod的在K8S中的used标签为false，会误以为该Pod是池子里的Pod，会将其重新加入PodList缓存，且内存中usedFlag的值为false；

02:41:08.404 后续请求到来，又被选中，导致错误，且删掉了该Pod；

Worker-manager本质上缺乏对Pod是否已经特化的感知能力，已有的双保险机制，在极端异常场景下，依旧存在误判断的可能，导致Pod重复被选中并特化；

Worker收到重复特化请求时，http请求处理逻辑会panic，此时Worker-manager收到the server closed connection before returning the first response byte。


## 2. 特性/功能实现原理

### 2.1总体方案

修改worker逻辑，重复特化时返回错误。worker-manager特化失败后优雅退出pod，将pod从池子中剔除，避免再次选中。

### 2.2模块详细设计

#### 2.2.1worker子模块设计

##### 2.2.1.1模块实现

worker处理特化请求的最后会关闭LoadFunctionRespondedSignal的全局channel。该channel的目的有：

1. worker优雅退出时等待特化请求处理结束
2. 特化结束时关闭一个go协程

删除全局channel：

1. 优雅退出时使用http.Shutdown方法等待请求结束后优雅退出（已有逻辑）
2. 每次特化请求创建一个临时的channel，负责在特化结束时关闭go协程

### 2.2.2worker-manager子模块设计

##### 2.2.2.1模块实现

特化失败优雅删除pod，将pod从池子中剔除，返回161900 worker内部错误让管理面负责重试（已有逻辑）

不尝试恢复cache，恢复逻辑复杂：

1. 上下文已丢失，无法获取实例applier和cr/id
2. 如果实例是预留实例，恢复后会影响当前系统中函数的预留实例个数

### 2.3 约束

#### 2.3.1上下游依赖需求

#### 2.3.2其他约束

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

较难测试，可以通过

1. 对已特化的worker再次手动发送特化请求，验证worker逻辑的正确性
2. 手动发送特化请求给一个池子中的worker，再让worker-manager特化该worker，验证worker-manager逻辑的正确性

### 3.9性能

不涉及

### 3.10资源

该机制会将健康的实例删除，重试会重新特化实例，占据部分资源。

### 3.11资料

不涉及

### 3.12升级兼容性

不涉及

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤** | **预期结果** |
| -------- | ---------- | -------- | ------------ | ------------ |
| 1        | worker多次特化不会panic  | 手动用例      | 对已特化的worker再次手动发送特化请求    | worker返回161900错误           |
| 2        | worker-manager多次特化worker         | 手动用例       | 手动发送特化请求给一个池子中的worker，再让worker-manager特化该worker  | worker-manager返回161900，并删除该worker所在的pod   |