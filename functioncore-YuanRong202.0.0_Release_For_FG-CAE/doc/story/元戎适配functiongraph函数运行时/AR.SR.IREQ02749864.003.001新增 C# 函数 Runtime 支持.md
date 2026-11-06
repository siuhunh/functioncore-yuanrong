[TOC]

# AR.SR.IREQ02749864.003.001新增 C# 函数 Runtime 支持

## 一、 需求场景分析

### 1.1 特性需求来源与价值概述
functionGraph v1 版本支持 C#，为了切流兼容性，v2 版本需要同步提供 C# 函数 Runtime 支持

### 1.2 特性场景分析
对于C#，runtime 需要提供支持 C#(.NET Core 3.1) 版本
C#(.NET Core 2.0)、C#(.NET Core 2.1) -- 当前构建环境不支持
1）支持 socket 与 worker go 通信，支持 pb 消息  
2）支持加载用户代码，环境变量设置 
3）支持函数初始化接口与函数调用接口  

### 场景分析
worker、 runtimemanager、runtime进程交互时序图如下

[新增C#函数Runtime-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=a0b941b768cd41749d53e9a15115a9b9&versionUUID=)![新增C#函数Runtime](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1Gyo?t=1638495995145)


### 二、方案详细设计

#### 2.1 方案设计
1. 构建 C# 基础工程  
	1) 提供编译构建脚本。编译构建机器需要安装 dotnet sdk 
	2) 代码仓流水线构建

2. runutimemanager 镜像内置多版本 dotnet sdk，c# 函数 pod 特化时控制启动 runtime 进程
因为当前 c# 函数用户较少，runutimemanager 不预启动 runtime 进程，在函数特化时再启动对应 runtime 进程

runutimemanager 新增 C# 3个版本的 runtime 配置如下
```yaml
"runtimeConfigs": {
    ...
    "C#(.NET Core 3.1)": {
        "port": 31545,
        "enable": false,
		"excludeSpecifications": [128],
        "workerDir": "/opt/function"
    },
    "C#(.NET Core 2.1)": {
        "port": 31546,
        "enable": false,
		"excludeSpecifications": [128],
        "workerDir": "/opt/function"
    },
    "C#(.NET Core 2.0)": {
        "port": 31547,
        "enable": false,
		"excludeSpecifications": [128],
        "workerDir": "/opt/function"
    }
}
```
3. C# runtime 支持 socket 与 worker 通信，支持 pb 消息
   pb 消息结构体由 protobuf 自动生成，需要提供 protobuf 结构体生成脚本

4. C# runtime 提供 load 接口，支持加载用户代码，环境变量设置
   动态加载用户函数目录 /opt/function/code 目录下的 dll 文件
   设置用户配置的环境变量到 context 中
   
5. C# runtime 支持函数调用 handler 接口

	函数Handler定义
	**ASSEMBLY::NAMESPACE.CLASSNAME::METHODNAME**

	- .ASSEMBLY为应用程序的.NET程序集文件的名称，假设文件夹名称为HelloCsharp。
	- NAMESPACE、CLASSNAME即入口执行函数所在的namespace和class名称。
	- METHODNAME即入口执行函数名称。例如：  
	  创建函数时Handler：HelloCsharp::Example.Hello::Handler
6. C# runtime 支持 初始化 init 接口,执行初始化函数
	init 函数定义
		**ASSEMBLY::NAMESPACE.CLASSNAME::METHODNAME**

7. 提供 sdk 函数包,可发布到官网供用户下载使用
	namespace HC.Serverless.Function.Common
	
	Context接口 : Context类中提供了许多属性供用户使用
	| 属性名  |  属性说明 |
	| ------------ | ------------ |
	| String RequestId  | 请求ID。  |
	| String ProjectId  |  Project Id |
	| String PackageName  | 函数所在分组名称  |
	| String FunctionName  | 函数名称  |
	| String FunctionVersion  | 函数版本  |
	| Int MemoryLimitInMb  | 分配的内存。  |
	| Int CpuNumber |  获取函数占用的CPU资源，单位为millicore（1 core=1000 millicores）。取值与MemorySize成比例，默认是128M内存占0.1个核（100 millicores），函数占用的CPU为基础CPU：200 millicores，再加上内存按比例占用的CPU，计算方法：内存/128 * 100 + 200。 |
	| String Accesskey  | 获取用户委托的AccessKey（有效期24小时），使用该方法需要给函数配置委托。  |
	| String Secretkey | 获取用户委托的SecretKey（有效期24小时），使用该方法需要给函数配置委托。  |
	| String Token  | 获取用户委托的Token（有效期24小时），使用该方法需要给函数配置委托。  |
	| Int RemainingTimeInMilliSeconds  | 函数剩余运行时间  |
	| String GetUserData(string key,string defvalue=" ")  | 通过key获取用户通过环境变量传入的值。  |
	
	IFunctionLogger 接口 : 用户日志打印接口


#### 外部交互分析&&上下游依赖需求
暂无

### 对外接口
不涉及


## 三、可信软件

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

用户代码执行异常
用户代码加载异常
用户代码执行超时

#### 可用性 Availability
*处于按要求执行状态的能力*  
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  
不涉及

### 可维护性
不涉及

### 可测试性
本地 LLT 框架待搭建
外部接口测试可以沿用 v1 的 c# 测试用例

### 性能
冷启动规格 100K 45ms 对比v1
c# runtime 不预先启动，不影响当前冷启动时延和性能

### 资源
runtime-manager 镜像需要内置相关 dotnet sdk，会新增几百兆基础镜像大小

### 资料
相关 c# 函数开发材料 https://support.huaweicloud.com/devg-functiongraph/functiongraph_02_0450.html

### 升级兼容性
需要新增相关配置，不影响旧版本功能

## LLT 用例
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | C# runtime预启动成功 |  修改runtimemanager配置预启动为true,查看 runtimemanager 是否预启动成功 | runtimemanager 启动 C# runtime 成功|
| 2 |  配置 C# 初始化函数，查看是否执行成功 |配置 C# 初始化函数, 查看执行成功 | 初始化函数执行一次且成功 |
| 3 |  配置 C# 函数，查看是否执行成功 |配置 C# 函数, 查看执行成功 | 函数调用成功 |
| 4 |  异常加载测试 |配置 C# 函数入口不存在，执行函数 | 报错函数加载失败 |
| 5 |  函数执行超时 |配置 函数超时时间小于函数执行时间，执行函数 | 报错函数执行超时 |
| 6 |  环境变量设置 |配置环境变量，执行函数 | 查看环境变量是否设置成功 |
| 7 |  6M大小消息用例 |创建6M大小消息函数，执行函数 | 函数调用成功 |
| 8 |  日志用例 |context、console 标准输出采集 | 函数日志采集成功 |