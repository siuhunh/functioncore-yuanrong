[TOC]

# 【元戎】Python语言支持公共依赖库 AR.SR.IR20230331002271.003.001

## Story 整体设计

### 功能描述
公共依赖挂载方案支持python

### 场景分析
外部客户验证python runtime函数冷启动性能比阿里云FC慢，经分析定位主要有两个优化点：
1. 为客户配置了两个大依赖包opencv（115MB）和numba（31MB），下载解压依赖包耗时花了5.5秒。需补充支持python公共依赖挂载和link优化
2. 函数初始化入口加载可以优化，因为初始化入口和函数执行入口都是同一个model可以判断避免重复加载。

### 方案详细设计

公共依赖挂载为将节点上"/paas/dependencies"目录挂载到worker容器的"/opt/function/dependencies"（已实现）
公共依赖link为客户配置依赖包类型为 public 时，将"/opt/function/dependencies"下对应的公共依赖文件link到 "/opt/function/code" 目录下，
例如函数配置 python3.6 的 公共依赖 opencv ，为将"/opt/function/dependencies/python3.6/opencv" link到 "/opt/function/code/"目录下。

python 函数初始化入口加载优化：当前函数支持入口和函数初始化入口加载都会调用 imp.load_module 接口来加载 model，但是该接口每次都会读取函数入口文件进行加载，
并不会判断 sys.modules 是否已经存在了相同model，所以可以补充检查 sys.modules 是否已经存在来减少初始化入口加载时延。

补充修改：因当前依赖包支持多版本的，所以公共依赖在节点路径和link时生成挂载路径时需要区分版本，修改方案如下：
1. 函数元数据中layer信息添加version字段，如下：
```
// Layer define layer info
type Layer struct {
    AppID          string `json:"appId" valid:"stringlength(1|128),optional"`
    BucketID       string `json:"bucketId" valid:"stringlength(1|255),optional"`
    ObjectID       string `json:"objectId" valid:"stringlength(1|255),optional"`
    BucketURL      string `json:"bucketUrl" valid:"url,optional"`
    ETag           string `json:"etag" valid:"optional"`
    Sha256         string `json:"sha256" valid:"optional"`
    Name           string `json:"name" valid:",optional"`
    Version        string `json:"version" valid:",optional"`
    DependencyType string `json:"dependencyType" valid:",optional"`
}
```
2. 公共依赖路径规则
（1）老依赖没有版本：PublicDependenciesPath + language + "/" + layer.Name 
（2）新依赖支持多版本：PublicDependenciesPath + language + "/" + layer.Name + "@" + layer.Version
   

# 开源软件选型
*新引入的开源软件：原因、选型调研情况*
不涉及

#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*
需要新增python公共依赖opencv(4.5.1.48)和numba(0.53.1)--朱安东
需要管理面将python公共依赖同步到数据面节点--张睿

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
| -------- | ---------------------------------------------------------- | ------------------------------------ | ---------------------------------------- |
| 1        | 配置python公共依赖函数执行成功                                  | 创建函数，配置python公共依赖，点击测试函数  | 函数执行成功                   |
| 2        | 配置python私有依赖函数执行成功                                  | 创建函数，配置python私有依赖，点击测试函数  | 函数执行成功                   |
| 3        | 函数配置初始化入口执行成功                                       | 创建函数，配置初始化入口，点击测试函数      | 函数执行成功                   |
| 4        | 配置python公共依赖比私有依赖冷启动快                             | 创建两个函数，分别opencv和numba的公共和私有依赖，点击测试函数 | 函数执行成功，配置公共依赖的冷启动比私有依赖快  |
| 5        | 配置公共依赖-老依赖不带版本                                     | 创建函数，配置python老公共依赖，点击测试函数 | 函数执行成功，link依赖包成功，为对应老公共依赖  |
| 6        | 配置公共依赖-新依赖多版本                                       | 创建函数，配置python新公共依赖，点击测试函数 | 函数执行成功，link依赖包成功，为对应版本公共依赖  |
