[TOC]

# 【functionCore】busproxy和worker通信支持TLS加密通信



关联AR：

+ AR.SR.20210621221420.032.029 【functionCore】busproxy 和 worker增加双向鉴权 



## Story 整体设计

### 功能描述

FunctionBus 负责对节点内和节点间函数调用消息进行路由转发，节点内的 busproxy 与 worker 通信基于 TCP 协议，需求支持通信加密，可以通过开关配置是否开启加解密通信。

## 

### 场景分析

1. FunctionBus 节点间调用运行视图  
  [AR.20210621221420.045 配置安全通道支持busproxy间加密通信运行视图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=adc3b68954eb4110b350ebca32f46d27&versionUUID=)![AR.20210621221420.045 配置安全通道支持busproxy间加密通信运行视图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/uq2?t=1626159507667)

  FunctionBus 有以下消息流：

  | 消息流 | 功能                                        | 通信方式   |
  | ------ | ------------------------------------------- | ---------- |
  | IF1    | functionAccessor 发送消息/请求给 busproxy   | HTTP/HTTPS |
  | IF2    | busproxy 和 functiontask 消息通信           | channel    |
  | IF3    | busproxy 和 worker 消息通信                 | TCP        |
  | IF4    | busproxy 和其他 busproxy (远程节点)消息通信 | TCP        |
  | IF5    | busproxy 发送扩缩容消息给 WorkerManager     | HTTP       |

  当本节点资源充足是，busproxy消息会发往本节点worker。



### 方案详细设计

#### 方案设计

##### 1. 复用busproxy之间通信加密开关，证书与秘钥文件。

使用当前已有的 HTTPS 统一开关 Values.global.internalTLS.TLSENABLE 控制是否使用加密通信。当打开加密开关时，需要提供证书和秘钥文件，busproxy通过挂载方式从 k8s 的 secret 中加载，worker通过从共享目录中读取。

- **证书与秘钥文件**
  - **文件说明**

    | 文件名称       | 功能                               |
    | :------------- | :--------------------------------- |
    | trust.cer      | 授信CA                             |
    | server.cer     | 服务器证书                         |
    | server_key.pem | 已加密服务端私钥文件               |
    | cert_pwd       | 用于存放解密私钥的对称加密密文文件 |

    cert_pwd, server_key.pem, server.cer, trust.cer四个文件，通过K8s的secret方式来供容器挂载，命名为https-certs。权限默认给0440最低权限，挂载源为 Readonly。worker通过从共享目录中读取，读取完后会被删除。
  - **证书校验**
    busproxy 需要校验 worker 发过来的服务器证书，TLS配置中开启证书校验开关。
  - **加密套件与模式**
    加密套件与模式等字段与FunctionGraph对齐，在配置文件中读到组件中。

##### 2. Worker增加TLS Server

当通信加密开启时，Worker 创建TLS Server 取代TCP Server。BusProxy 用 TLS Client与其进行通信。

通信流程：

[BusProxy与Worker鉴权流程](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=b357b16be8fa460b9ceea807ff9f44a5)![BusProxy与Worker鉴权流程](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/10Bt?t=1629272163642)



## 可信软件

#### 安全性 Security

*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  
证书挂载权限默认给0440最低权限，挂载源为 Readonly。

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

| 序号 | 用例名                | 测试步骤                                                     | 预期结果 |
| ---- | --------------------- | ------------------------------------------------------------ | -------- |
| 1    | busproxy 通信不加密   | 关闭通信加密开关，进行函数调用                               | 调用成功 |
| 2    | busproxy 加密通信成功 | 开启通信加密开关，进行函数调用                               | 调用成功 |
| 3    | busproxy 加密通信失败 | 构建异常场景：1. 配置错误的证书。 2.证书正规，但不配套。 3.busproxy和worker配置的AKSK不配套。进行函数调用 | 调用失败 |





