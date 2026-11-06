[TOC]

# 【FunctionCore】支持对初始化异常的函数进行计费


## 1. Story整体设计
### 1. 功能描述
1）执行函数初始化接口异常时，支持这段时间的计费时间信息上报。
2）执行自定义函数启动时异常，支持函数启动时的时间记录，进行计费信息上报。

### 2. 场景分析
1. 如果函数配置了初始化入口，在特化函数实例时，如果初始化失败，删除特化实例并上报初始化失败的计费信息到redis，functiongraph需采集上报的计费信息

[初始化异常和自定义运行时特化异常计费信息上报-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=b7ed82d7c00a4e52b6b40b3c0148e772&versionUUID=)
![初始化异常和自定义运行时特化异常计费信息上报](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/10T8?t=1629456401938)

2. 特化运行时语言为自定义运行时的函数实例，如果特化失败，删除特化实例并上报特化失败的计费信息到redis，functiongraph需采集上报的计费信息

[自定义运行时特化异常计费信息上报-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=5b64248595b648a3b8d5b117e6f69640&versionUUID=)
![自定义运行时特化异常计费信息上报](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/10TW?t=1629456364361)

### 3. 方案详细设计

#### 3.1 执行函数初始化接口异常时，支持这段时间的计费时间信息上报
(1) 当前函数初始化异常未返回给function-instance-manager，初始化异常实际在invoke阶段返回，应该在特化阶段直接返回，并删除特化pod.
1. 初始化函数执行异常需直接响应给function-instance-manager
2. 初始化异常响应报文格式如下：
````
{
   "code": 4011,
   "message" {"log":"XXXXXX", summary:"XXXX", errorMessage:"XXXX"}
}
````
其中message内存储了初始化异常日志（log），计费信息（summary）, 错误信息（errorMessage）
3. function-instance-manager收到初始化异常响应，需删除特化实例，并上报计费信息到redis(函数实例指标统计上报的redis)

(2) functionGraph需适配初始化失败返回的消息格式（如上2.2），并采集上报到redis计费信息

(3) 计费上报的运行时长（duration）
1. 执行初始化函数超时：运行时长=配置的初始化超时时间
2. 执行初始化函数异常（语法错误等）：运行时长=worker收到runtime返回的init响应（endTime）- worker发送init请求到runtime（starTime）

#### 3.2 执行自定义函数启动时异常，支持函数启动时的时间记录，进行计费信息上报。
(1) 自定义运行时在特化时，是执行用户提供的shell脚本，所以启动异常需要进行计费上报，启动异常场景拉取调用请求一直未收到(发送请求前sleep或exit 1)，导致启动超时。
1. function-instance-manager在特化运行时语言为自定义运行时，如果特化启动异常需上报计费信息到redis(函数实例指标统计上报的redis)
2. 自定义运行时启动失败响应修改为如下格式：
````
{
   "code": 4011,
   "message" {"log":"XXXXXX", summary:"XXXX", errorMessage:"XXXX"}
}
````
其中message内存储了初始化异常日志（log），计费信息（summary）, 错误信息（errorMessage）
3. 自定义运行时启动时异常：启动runtime超时30秒（4011），启动runtime（自定义）运行时进程异常退出（4013），拉取调用请求一直未收到
4. 当前自定义运行时初始化超时异常是在invoke阶段返回，改造为特化时返回

(4) 计费上报的运行时长（duration）
1. 启动custom runtime超时：运行时长=30秒
2. 启动runtime（自定义）运行时进程异常退出：运行时长=自定义运行时异常退出（endTime）- 启动自定义运行时（startTime）

custom runtime是否ready检查：
functionGraph是在超时时间内for循环检查ready通道，检查超时时间为1秒
元戎为HotLoad监控ready通道，如果未收到信号，一直未ready则超时30秒

#### 3.3 上报到redis的计费信息
key： Failure-Fee-projectID@package@functionName@version@memsize@timestamp
memsize为函数配置内存大小，例如128 256 512
timestamp就int64, 单位毫秒
Value: { "summary":"XXXX" }
summary结构：
````
type InvokeSummary struct {
	FuncDigest         string        `json:"funcDigest"`      // Function Summary
	Duration           float64       `json:"duration"`        // Running time (unit: ms)
	BillingDuration    int           `json:"billingDuration"` // Charging time (unit: ms)
	MemorySize         int           `json:"memorySize"`      // Memory configuration (unit: MB)
	MemoryUsed         float64       `json:"memoryUsed"`      // Memory usage (unit: MB)
    ErrorType          String        `json:"errorType"`      
}
````
errorType: initError(初始化异常)，customRuntimeStartError(自定义函数启动时异常)
           
需确认：预留实例（初始化失败和自定义运行时启动失败）计费信息是否要写入redis，直到特化成功？
本需求不考虑预留实例计费场景，当前预留实际计费正常场景和异常场景计费时间点跟普通计费不一致。
5、使用预留实例的情况下，资源消耗计算稍有不同，FunctionGraph根据预留实例设置在实例创建成功后开始计时。如果预留实例存活时间不足1分钟，将按照 1 分钟计算，超过一分钟的部分，按照秒的粒度向上取证计算。例如一个512M的函数预留实例运行时间为 51 秒，将按照1分钟计算，计算资源消耗为 0.5G * 60s = 30GB-秒。运行时间为 60.5 秒，则计费时间为 61 秒，计算资源消耗为0.5G * 61s = 30.5GB-秒。

扩容调研
预留实例（最小实例数）：特化pod一共有三次机会，特化失败三次后，退出特化流程，例如预留实例5个，最多失败15次。(不重试情况：取消特化和已到达最大实例数，但是资源不足错误情况是否要一直重试？？)
普通扩容：按需扩容，扩容请求不重试，扩容失败不会导致invoke请求失败，只是请求无法被调度到实例，留在调度队列中。


### 4. 上下游依赖需求

functionGraph定时采集并清理上报到redis的失败计费信息。

## 2. 可信软件

1. 安全性（Security） 
   *【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 
   不涉及，计费信息无敏感信息
2. 韧性（Resilience）
   *【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
   不涉及
3. 隐私性（Privacy） 
   *【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
   不涉及
4. 可靠性（Reliability）
   *【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
5. 可用性（Availability）
   不涉及
6. 安全（Safety）
   *【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
   不涉及
## 3. 用例

| **序号**  | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ----------------------------------------------------------- | ----------------------------------- | ---------------------------------------- |
| 1        | 首次调用函数，未配置初始化入口                                   | 创建并调用函数                         | 函数执行成功  |
| 2        | 首次调用函数，配置初始化入口                                     | 创建函数并配置函数初始化入口，然后调用函数  | 函数执行成功（包括初始化） |
| 3        | 首次调用函数，配置初始化入口，初始化耗时超过配置的初始化超时时间        | 创建函数并配置函数初始化入口，初始化sleep 4秒，然后调用函数| 函数执行失败，返回初始化失败的计费信息及日志，查看上报到redis的计费信息存在  |
| 4        | 首次调用函数，配置初始化入口，初始化函数有语法错误                   | 创建函数并配置函数初始化入口，编写语法错误的初始化函数，然后调用函数| 函数执行失败，返回初始化失败的计费信息及日志，查看上报到redis的计费信息存在  |
| 5        | 自定义运行时，首次调用函数                                       | 创建自定义运行时函数并调用 | 函数执行成功  |
| 6        | 自定义运行时，首次调用函数，初始化超时                             | 创建自定义运行时函数，脚本内先sleep 33秒，然后调用函数 | 函数执行失败，返回计费信息及日志，查看上报到redis的计费信息存在  |
| 7        | 自定义运行时，首次调用函数，在初始化等阶段（自定义）运行时进程异常退出   | 创建自定义运行时函数，脚本内先exit，然后调用函数 | 函数执行失败，返回计费信息及日志，查看上报到redis的计费信息存在  |

注：上报redis数据需管理面打开计费开关（测试环境），让测试人员方便测试。