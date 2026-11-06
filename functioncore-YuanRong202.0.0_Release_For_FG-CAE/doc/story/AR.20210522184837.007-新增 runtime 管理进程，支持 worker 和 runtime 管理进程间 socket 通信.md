[TOC]

# 移植代码到 runtime 管理进程，支持 worker 和 runtime 管理进程间 socket 通信

关联AR: AR.20210522184837.007 需求描述：worker管理模块支持动态提权
## Story 整体设计

### 功能描述

用户运行的进程跟 YuanRong 运行的组件隔离，增强安全

- 新增 runtime 管理进程，由管理进程拉起各个语言的 runtime 进程；

- 打通 worker 和 runtime 管理进程之间的 socket 通信；
- 用户函数运行在 runtime 进程中，runtime 进程 与 worker 已支持网络隔离，进一步增强安全性；

### 场景分析

[FunctionGraph交付需求-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=5215ab2f4a4b49b69f4818c24f14b79c&versionUUID=)![FunctionGraph交付需求](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/o64?t=1622548787735)

启动流程：

- nodemanager 先拉起 runtime 管理进程pod，再由 runtime 管理进程拉起所有语言的 runtime 进程；
- nodemanager 拉起 worker 进程pod，worker与所有语言的 runtime 进程建立 socket 连接；
- worker 和 runtime 等待接收特化请求；

正常场景：函数特化请求到达 worker，worker 发送 socket 请求给 runtime 管理进程，由runtime 管理进程 kill 其他无关的 runtime 进程；

- worker 收到特化请求后，先向 runtime 管理进程发送 socket 请求，kill 其他无关的 runtime 进程；
- 然后 worker 向指定 runtime 进程发送特化请求，继续执行特化操作；

异常场景：

- 函数特化请求到达worker，worker发送socket请求给runtime管理进程，请求发送失败，则返回特化失败；
- 函数特化请求到达worker，worker发送socket请求给runtime管理进程，请求处理失败，则返回特化失败；
- 所有 runtime 进程都退出，且不属于重启状态时，worker 感知 socket 断链，会向 runtime manager 发送 socket 请求，获取 runtime 进程退出的错误信息，并执行优雅退出的流程。
- runtime manager 进程是容器的主进程，其生命周期由 k8s 管理，runtime manager 进程异常退出后，k8s 会重新拉起 runtime manager 进程。
- worker 和 runtime 之间通信的异常场景处理办法与之前一致，保持不变。


### 方案详细设计

#### 方案设计

1）新增 runtime 管理进程，负责拉起各个语言的 runtime 进程

- 新增健康检查，通过socket与k8s通信

- runtime 管理进程是一个独立的镜像，由 nodemanager 负责拉起，镜像中包含了 nodejs、python、java、custom runtime 的二进制和源代码；

- 使用 dockerfile-runtime-manager.yaml 进行编译部署 runtime 管理进程 pod；

- 在 cmd、build、pkg 目录下分别新增 runtime-manager 文件夹作为代码目录

- runtime-manager 启动的二进制名称为：runtime-manager

- runtime 防火墙增加暴露端口如下：

  | 暴露端口号    | runtime 语言     |
  | ------------- | ---------------- |
  | 31530（原有） | runtime 管理进程 |
  | 31531         | nodejs6.10       |
  | 31532         | nodejs8.10       |
  | 31533         | nodejs10.16      |
  | 31534         | nodejs12.13      |
  | 31535         | python2.7        |
  | 31536         | python3.6        |
  | 31537         | java8            |
  | 31538         | custom-runtime   |


2）worker 和 runtime 进程间增加 socket 通信

- 消息格式（request 和 response 共用一种消息格式，由Body的内容作区分）

```go
type MessageType byte

const (
	headerLen                   = 43
	packetIDLen                 = 36
)

const (
	StopProcess     MessageType = iota
)

type Header struct {
	MagicNumber byte        // 1 byte
	Version     byte        // 1 byte
	MsgType     MessageType // 1 byte
	BodyLen     uint32      // 4 byte
	PacketID    string      // 36 byte
}

type Message struct {
	Header Header
	Body   []byte
}
```




#### 外部交互分析&&上下游依赖需求
下游依赖：
      1. 【FunctionCore】函数指定uid和gid
      2. ~~【FunctionCore】函数默认磁盘空间限制~~
      3. ~~【FunctionCore】函数支持公网访问控制~~
      4. 【FunctionCore】函数支持动态卷挂载
      5. 【FunctionCore】函数执行详情
      6. 【FunctionCore】日志打印
      7. 【FunctionCore】调用运行信息

联调计划：

- 完成模块解耦、编译、镜像，构造接口支撑依赖的需求联调：2021/6/4
- 完成功能特性联调：2021/6/24

### 对外接口
无对外接口

### 约束
暂无约束

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  

worker 和 runtime 之间做了网络隔离，满足安全性要求；

runtime 管理进程 和 runtime 之间有以下安全措施：

- runtime 管理进程由root用户启动，其他runtime进程由1002用户启动；
- runtime 管理进程拉起其他 runtime 进程时，清空继承的环境变量；
- 限制其他 runtime 进程的文件读写权限；（配置文件只读，日志文件可读写；文件读写权限最小化处理；）

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
*说清楚预置条件、输入（构造步骤、构造内容）、输出（预期结果）*

| 语言           | 输入（构造步骤、构造内容）                                   | 输出（预期结果） |
| -------------- | ------------------------------------------------------------ | ---------------- |
| nodejs6.10     | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| nodejs8.10     | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| nodejs10.16    | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| nodejs12.13    | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| python2.7      | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| python3.6      | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| java8          | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |
| custom-runtime | 1.设置 isShareContainer 开关为 true<br />2.重新部署nodemanager，等待拉起 fusion 池子<br />3.调用函数 | 返回函数调用成功 |

