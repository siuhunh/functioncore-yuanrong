[TOC]

# AR.20210526114107.001 runtime支持init接口

## 一、Story 整体设计

### 1.1 功能描述
**目的**  
为了减少应用层初始化对函数执行延时的影响，函数计算推出了 initializer 功能。分离初始化逻辑和请求处理逻辑，在调度上做相应的优化，提升函数调用性能。

**功能**  
Initializer 是函数的初始化逻辑入口，不同于请求处理逻辑入口的 handler，在有函数初始化的需求场景中，设置了 Initializer 后，FunctionGraph 首先调用 initializer 完成函数的初始化，之后再调用 handler 处理请求；如果没有函数初始化的需求则可以跳过initializer，直接调用 handler 处理请求。

**使用对象**  
用户函数可设置是否配置函数初始化入口，并配置函数初始化入口方法

**场景**  
例如深度学习场景下加载规格较大的模型、数据库场景下连接池构建、函数依赖库加载等等。

### 1.2 场景分析
#### 1.2.1 函数最小实例
配置了函数最小实例的函数，会预先特化 worker 实例，为函数分配可执行的容器资源。特化过程包括启动 worker 和runtime 容器，下载函数代码包，runtime 中执行初始化逻辑。如果用户设置了委托，初始化需要获取用户的 AK/SK 信息，传入初始化方法的 context 参数中。
#### 1.2.2 普通函数执行
用户函数执行调度包括以下几个阶段：

1. 预先为函数分配执行函数的容器资源。
2. 下载函数代码。
3. 通过 runtime 运行时加载代码。
4. 用户函数内部进行初始化逻辑。
5. 函数处理请求并将结果返回。

每当有新函数实例创建时，系统会首先调用 initializer。系统保证 initializer 执行成功后才会执行 handler 逻辑。
#### 1.2.3 函数更新
用户更新函数信息，管理面更新 ETCD 的函数元数据，运行面 workmanager 会 kill 已经特化的 worker 实例。在用户调用函数时重新执行上一节的函数执行调度逻辑。
#### 1.2.4 委托用户权限更新
管理面需要实时更新委托用户的 AK/SK 信息并写入 ETCD ，运行面同时监听 ETCD 的 AK/SK 信息并更新缓存。在新的函数调用来临时将最新的 AK/SK 信息写入 context 中

### 1.3 方案详细设计

#### 1.3.1 现状分析
**FunctionGraph 目前支持以下 Node.js 运行环境：**
- Node.js 6.10
- Node.js 8.10
- nodejs 10.16
- nodejs 12.13

Initializer 入口格式为：[文件名].[initializer名]  
initializer示例如下：
```javascript
exports.initializer = function(context, callback) {
	callback(null, '');
};
```
- 函数名  
exports.initializer 需要与实现 initializer 接口时的 Initializer 字段相对应
- context 参数  
context 参数中包含一些函数的运行时信息。例如：request id、临时 AccessKey、function meta 等

- callback 参数  
callback 参数用于返回调用函数的结果，其签名是 function(err, data)，与 Nodejs 中惯用的 callback 一样，它的第一个参数是error，第二个参数 data。如果调用时 error 不为空，则函数将返回 HandledInitializationError，由于屏蔽了初始化函数的返回值，所以 data 中的数据是无效的，可以参考上文的示例设置为空。

**FunctionGraph 目前支持以下 Python 运行环境**
- Python 2.7
- Python 3.6

Initializer 入口格式为：[文件名].[initializer名]  
initializer 示例如下：
```python
def my_initializer(context):
	print("hello world!")
```
- 函数名  
my_initializer 需要与实现 initializer 接口时的 Initializer 字段相对应
- context 参数  
context 参数中包含一些函数的运行时信息，例如：request id、临时 AccessKey、function meta 等

**FunctionGraph目前支持以下 Java 运行环境**
- Java8 2.7 (runtime = Java8)

Java需要定义一个类并实现函数计算预定义的初始化接口，类和函数执行入口类保持一致。

当前元戎数据面 worker 和 runtime 不支持函数初始化的配置执行。nodejs、java、python runtime需要增加初始化方法执行逻辑。

#### 1.3.2 方案设计
1. 获取函数初始化方法配置信息  
用户页面定义函数，将元数据信息写入 ETCD，其中包括定义的函数初始化方法和初始化超时时间。
数据面解析数据后， worker 通过环境变量 INITIALIZER_HANDLER、INITIALIZER_TIMEOUT 将初始化信息写入 runtime

2. 获取 AK/SK/Token 信息
如果用户函数配置了委托（xrole 或者 approle 字段不为空），需要获取 AK/SK/Token 信息。  
管理面将 AK/SK/Token 信息写入 ETCD 并定时维护。workManagr 监听 ETCD key ,获取到 AK/SK/Token 信息并维护本地缓存。  
etcd key：/sn/agency/business/yrk/tenant/${tenantid}/domain/${domainId}/agency/${agencyid}   
在 worker 特化时 workManagr 从本地缓存获取 AK/SK/Token 信息，并传递到 worker , worker 写入 runtime context。

3. runtime 定义初始化方法逻辑  
initializer 接口规范，各个 runtime 的 initializer 接口有以下共性：
- 无自定义参数  
 Initializer 不支持用户自定义参数，只能获取 FunctionGraph 提供的 context 参数中的变量进行相关逻辑处理。
- 无返回值  
  开发者无法从 invoke 的响应中获取 initializer 预期的返回值。
- 超时时间  
  开发者可单独设置 initializer 的超时时间，与handler的超时相互独立，但最长不超过 300 秒。
- 执行时机  
  运行函数逻辑的进程称之为函数实例，运行在容器内。FunctionGraph会根据用户负载伸缩函数实例。每当有新函数实例创建时，系统会首先调用 initializer。系统保证一定 initializer 执行成功后才会执行 handler 逻辑。
- 最多成功执行一次  
  FunctionGraph保证每个函数实例启动后只会成功执行一次initializer 。如果执行失败，那么该函数实例执行失败，选取下一个实例重新执行，最多重试3次。一旦执行成功，在该实例的生命周期内不会再执行initializer，收到Invoke请求之后只执行请求处理函数。
- initializer入口命名  
  除Java外，其他runtime的initializer入口命名规范与原有的执行函数命名保持一致，格式为 [文件名].[ initializer名]，其中initializer名可自定义。Java需要定义一个类并实现函数计算预定义的初始化接口。
- 计量计费  
  Initializer 的执行时间也会被计量，用户需要为此付费。worker 会记录初始化花费时间，在函数第一次调用时将初始化时间加上。

4. worker 特化时 runtime 执行初始化方法
worker 特化时如果初始化方法不为空，通过 invoke 接口请求 runtime。runtime invoke 接口在 invoke_id 为 initializer 且初始化方法未执行时会执行初始化方法。

#### 1.3.3 外部交互分析&&上下游依赖需求

1. 管理面写入 ETCD 元数据初始化结构定义：

    "extendedMetaData":{
        "image_name":"latest-5qe8e",
        "role":{
            "xrole":"cff",
            "app_xrole":null
        },
        "func_vpc":"*FuncVpc",
        "mount_config":"*MountConfig",
        "strategy_config":{
            "concurrency":-1
        },
        "extend_config":"string",
        "initializer":{
            "initializer_handler":"index.initializer",
            "initializer_timeout":3
        },
        "enterprise_project_id":"string"
    }

2. ETCD 中 AK/SK/Token key 和 value 定义：  
key 格式为：  
    /sn/agency/business/yrk/tenant/${tenantid}/domain/${domainId}/agency/${agencyid}  
value 结构为:
```go
AccessKey	string		`json:"accessKey"`
SecretKey	string		`json:"secretKey"`
Token		string		`json:"token"`
```

### 1.4 对外接口
不涉及

## 二、可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  
不涉及

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*  
不涉及

#### 可用性 Availability
*处于按要求执行状态的能力*  
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  
不涉及

## 三、LLT 用例
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | 无初始化函数正常调用 |  编写函数，不包含初始化方法 | 函数正常调用 |
| 2 | 函数初始化方法正常调用 | 编写函数，包含初始化方法。分别测试 nodejs、java、python 各个语言版本 runtime | 函数正常调用，且初始化方法也正常执行 |
| 3 | 函数初始化方法执行超时  | 编写函数，包含初始化方法，设置初始化方法超时时间小于执行时间  |  初始化方法执行超时，函数调用失败 |
| 4 | 函数初始化方法执行失败  | 编写函数，包含初始化方法，设置初始化方法参数不对等异常  |  初始化方法执行失败，函数调用失败 |
| 5 | 函数初始化方法计费 | 编写函数，包含初始化方法 | 函数正常调用，且第一次函数调用计费信息包含初始化方法调用时间 |