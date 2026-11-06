[TOC]

# 【functioncore】CacheManager支持函数热度缓存

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

**需求背景**：当用户输入函数数量很多时，会加载超过cache manager的最大内存造成OOM。   (当前cache manager的内存分配为4Gi，当函数以每个100kb，加载4w个函数即可产生OOM)

**解决方案**：根据cache manager的最大内存，新增LRU缓存能力，支持函数热度缓存，对长时间无调用的函数优先被替换。

**价值概述**：使能函数热度缓存，提高cache manager的命中率，完善内存的最大占用看护。

### 1.2特性场景分析

- 函数加载
  1. cache manager从etcd感知到需加载的函数，从obs获取函数加载到CacheMap，对需要持久化的函数存储在evs
  2. cache manager启动时，从evs加载持久化的函数到CacheMap
- 函数使用
  1. worker向obs和cache manager请求获取函数内容，cache manager在loadFunction 函数将已缓存的目标函数进行应答至worker。

![img](https://clouddevops.huawei.com/vision-file-storage/api/file/download/upload-v2/2022/2/14/l00581265/d8271bcce98f4210a5df1af44963f7ca/image.png) 

## 2. 特性/功能实现原理

### 2.1总体方案

*该章节主要阐述该特性的详细设计，包括选择什么器件，使用什么算法，架构如何布局，打算UI**如何呈现，希望获取什么用户信息支撑后续设计等。cloudmodeling地址：*https://cloudmodeling.tools.huawei.com/

**LRU算法**：最近最少使用算法（Least Recently Use），广泛的应用于缓存机制中。当缓存使用的空间达到上限后，就需要从已有的数据中淘汰一部分以维持缓存的可用性。

```shell
0      3      5      1
------------------------
1      0      3      5
2      1      0      3
3      2      1      0
4      4      2      2
init   get3   put5   get1
```

### 2.2模块详细设计

#### 2.2.1lru子模块设计

##### 2.2.1.1模块实现

1. cacheMemMax：允许最大缓存值，值为/sys/fs/cgroup/memory/memory.stat的hierarchical_memory_limit*0.8，即当前最大内存的80%为LRU缓存块的阈值
2. 从obs获取到函数时，增加LRU缓存处理：
   - 当前缓存 + 新增函数大小 < cacheMemMax，直接缓存加载新函数
   - 当前缓存 + 新增函数大小 > cacheMemMax，弹出最久未使用的缓存函数，直至能存入新函数为止
3. worker向cache manger请求函数时，做热度处理，将最近调用的函数objectID置LRU链表头。



[lru-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/f29373794c59405eaeecb87f3c553847?projectDiagramUUID=2d9c7d0859ad4be08ba5f0f5a51f00a9&versionUUID=)![lru](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Okc?t=1659614468076)

##### 2.2.1.2新增模块对外接口定义

在pkg/cache-manager/utils下新增lru模块，实现对cache manager的LRU缓存机制。新增接口

- NewLru()，初始化Lru结构体，cacheMemMax为允许缓存的最大内存，codeSize为当前缓存大小，list为LRU链表，cache指向server.CacheMap

  ```go
  type Lru struct {
  	cacheMemMax int64
  	codeSize    int64
  	list        *list.List
  	cache       *sync.Map
  	lock        *sync.Mutex
  }
  
  type ListNode struct {
  	key     interface{}
  	val     interface{}
  	valSize int64
  }
  
  func NewLru(cache *sync.Map) *Lru {}
  ```

- CacheAdd()，从obs获取函数内容后，server.CacheMap成功存储后调用，将新增函数ObjectID存储至lru.list并将新加载函数置lru链表头

  ```go
  func (lru *Lru) AddCache(key interface{}, value interface{}, valSize int64) {}
  ```

- CacheGet()，worker从cache manager在server.loadFunction成功获取函数后，将调用函数置lru链表头

  ```go
  func (lru *Lru) GetCache(key interface{}) (val interface{}, ok bool) {}
  ```

- CacheDel()，在server.CacheMap成功删除时调用，将目标函数从lru链表中删除

  ```go
  func (lru *Lru) DelCache(key interface{}) {}
  ```

### 2.3 约束

#### 2.3.1上下游依赖需求

不涉及

#### 2.3.2其他约束

不涉及

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

不涉及

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

1. 在无法正确读取/sys/fs/cgroup/memory/memory.stat内容时，默认设置cacheMemMax为4 * 0.8Gi

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

LLT测试用例具有稳定性

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

内存：需要新增双链表存储LRU数据结构。每新增一个函数需新增一个链表节点，约110B，预估最大8W个函数，新增内存8Mb左右。

时延：worker获取函数时，将该函数提至表头，8w个用例下，新增耗时约0-2μs，几乎无影响

cpu：同时延

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

同3.9

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

无。

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

兼容之前的版本，支持组件独立升级。内部特性，组件外不感知。

## 4. 用例

| **序号** | **用例名**    | 用例类型 | **测试步骤**                             | **预期结果**                                                 |
| -------- | ------------- | -------- | ---------------------------------------- | ------------------------------------------------------------ |
| 1        | TestGetMemMax | LLT      | cache manager正常启动                    | 能够正确从/sys/fs/cgroup/memory/memory.stat的hierarchical_memory_limit中获取cache manger的内存最大值 |
| 2        | TestLru       | LLT      | 按顺序向cacheMap写入、获取、删除缓存内容 | 能够正确向Lru列表加载内容，能够根据cacheMemMax正确更新缓存内容 |