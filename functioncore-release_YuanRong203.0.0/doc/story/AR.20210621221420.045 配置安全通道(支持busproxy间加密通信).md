[TOC]

# 【functionCore】策略：配置安全通道（支持busproxy间加密通信）

关联AR:
+ AR.SR.IREQ02597546.028.002【functionCore】Busproxy作为通信客户端，主动与worker建链

## Story 整体设计

### 功能描述
FunctionBus 负责对节点内和节点间函数调用消息进行路由转发，节点间的 busproxy 通信基于 TCP 协议，需求支持通信加密，可以通过开关配置是否开启加解密通信。  

### 场景分析
1. FunctionBus 节点间调用运行视图  
[AR.20210621221420.045 配置安全通道支持busproxy间加密通信运行视图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=adc3b68954eb4110b350ebca32f46d27&versionUUID=)![AR.20210621221420.045 配置安全通道支持busproxy间加密通信运行视图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/uq2?t=1626159507667)

FunctionBus 有以下消息流：

| 消息流 | 功能 | 通信方式 |
| ------------ | ------------ | ------------ |
| IF1 | functionAccessor 发送消息/请求给 busproxy  | HTTP/HTTPS  |
| IF2 | busproxy 和 functiontask 消息通信 | channel |
| IF3 | busproxy 和 worker 消息通信 | TCP |
| IF4 | busproxy 和其他 busproxy (远程节点)消息通信  | TCP |
| IF5 | busproxy 发送扩缩容消息给 WorkerManager | HTTP |

busproxy 每个节点一个，和 functiontask 一起部署在一个进程内。busproxy间的互相调用存在于以下场景：
- 有状态函数间互相调用
- 扩容后，busproxy 管理远程 worker，转发请求到其他 busproxy  

### 方案详细设计
#### 方案设计
##### 1. 增加通信加密开关，加载证书和秘钥文件  
   使用当前已有的 HTTPS 统一开关 Values.global.internalTLS.TLSENABLE 控制是否使用加密通信。
   当打开加密开关时，需要提供证书和秘钥文件，通过挂载方式从 k8s 的 secret 中加载。
- **证书与秘钥文件**
  - **文件挂载**
    
    |  文件名称 |  功能 |
    |:---|:---|
    | trust.cer  |  授信CA  |
    | server.cer | 服务器证书 |
    | server_key.pem |  已加密服务端私钥文件  |
    | cert_pwd  |  用于存放解密私钥的对称加密密文文件  |
    
    cert_pwd, server_key.pem, server.cer, trust.cer四个文件，通过K8s的secret方式来供容器挂载，命名为https-certs。权限默认给0440最低权限，挂载源为 Readonly。
  - **更新维护方式**
    证书始终与Functiongraph保持一致。手动更新，需要先更新K8s的https相关secret再重启使用的相关组件
  - **证书校验**
    busproxy 之间通信证书不校验，TLS配置中关闭证书校验开关。因为已经有组件间鉴权，不影响安全
  - **加密套件与模式**
    加密套件与模式等字段与FunctionGraph对齐，在配置文件中读到组件中

#####  2.  FunctionBus 增加 TLS Server, TLS Client
当通信加密开关开启时，functionBus 创建 TLS Server 取代 tcp Server。tcp Server 目前只用于 busproxy 之间的通信。  
异常场景考虑：busproxy 之间建链失败(对端退出\未启动， ip 不存在)，busproxy 通信加解密失败。

#### 外部交互分析&&上下游依赖需求
上游依赖：【functionCore】Busproxy作为通信客户端，主动与worker建链。约定联调时间 2021/07/21,对接人张玥。

### 对外接口
无

## 可信软件

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

## LLT 用例
| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  busproxy 通信不加密 | 关闭通信加密开关，进行函数调用 | 调用成功  |
| 2 |  busproxy 加密通信成功 | 开启通信加密开关，进行函数调用 | 调用成功  |
| 3 |  busproxy 加密通信失败 | 构建异常场景：1. 配置错误的证书 2. 连接失败 进行函数调用 | 调用失败  |
