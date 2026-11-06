# 支持https安全通信 FuncInstanceManager， worker

本story包含两个AR：

1. AR.SR.20210621221420.032.025 【FuncInstanceManager】FuncInstanceManager->worker支持安全通信
2. AR.SR.20210621221420.032.024 【FuncInstanceManager】Busproxy->FuncInstanceManager支持安全通信

相似AR：

1. AR.20210621221420.045 配置安全通道支持busproxy间加密通信运行视图
2. AR-20210630-015-002 支持https安全通信：FunctionAccessor, BusProxy

关联AR：

1. AR.SR.20210621221420.032.026 worker 容器保证证书安全

## 1. Story 整体设计
### 1.1 功能描述
- **背景**
  functiongraph交付的组件里Busproxy到FuncInstanceManager和FuncInstanceManager到worker之间有租户信息之类的敏感信息，所以增加支持https通信增强安全。
- **方案**
  在AR-20210630-015-002的基础上，将Busproxy到FuncInstanceManager和FuncInstanceManager到worker之间的http通信改为https增加安全性。
### 1.2 场景分析
- **Busproxy到FuncInstanceManager**
  系统间组件通信，包含租户信息等敏感信息，需要增加https通信能力提高安全。Busproxy增加支持https client，FuncInstanceManager增加支持https server

- **FuncInstanceManager到worker**
  两个组件间的信息包含租户信息等敏感信息，需要增加https通信能力提高安全。FuncInstanceManager增加支持https client，worker增加支持https server

- **其他非交付组件**
  本次改造不涉及

### 1.3 方案详细设计
- **复用HTTPS开关**
   复用AR-20210630-015-002中新增的HTTPSENABLE的全局https开关，用于统一设定所有组件的https开关与deployment中是否加载环境变量。当https开关打开时，则将当前http接口改为https

- **证书与秘钥文件**
  - **文件挂载**

    |  文件名称 |  功能 |
    |:---|:---|
    | trust.cer  |  授信CA  |
    | server.cer | 服务器证书 |
    | server_key.pem |  已加密服务端私钥文件  |
    | cert_pwd  |  用于存放解密私钥的对称加密密文文件  |

    cert_pwd, server_key.pem, server.cer, trust.cer四个文件，通过K8s的secret方式来供容器挂载，命名为https-certs。权限默认给0440最低权限
  - **更新维护方式**
    证书始终与Functiongraph保持一致。手动更新，需要先更新K8s的https相关secret再重启使用的相关组件
  - **证书校验**
    证书不校验，TLS配置中关闭证书校验开关。因为已经有组件间鉴权，不影响安全
  - **加密套件与模式**
    加密套件与模式等字段与FunctionGraph对齐(localAuth)，在配置文件中读到组件中

- **流程图**
   [支持https安全通信：FuncInstanceManager, worker-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=c2ddd55af94d48ae8bc2057a9c7a2fc9&versionUUID=)![支持https安全通信：FuncInstanceManager, worker](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/ySP?t=1628062166206)

- **限制**
    不支持部分组件打开https，部分组件关闭https的方式。

- **worker 容器保证证书安全**
    由AR.SR.20210621221420.032.026实现

- **其他**
    1. worker的环境变量、证书挂载由FuncAccessManager完成
    2. FuncAccessManager的环境变量、证书挂载在helm脚本中

### 1.4 上下游依赖需求
不涉及

## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*
不涉及
### 2.2 韧性（Resilience）
*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*
不涉及
### 2.3 隐私性（Privacy）
*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及
### 2.4 可靠性（Reliability）
*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
不涉及
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】*
不涉及
### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*
不涉及
## 3. HLT 用例
| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  进行https请求调用成功 | 开启组件的https开关，发出https请求进行调用 | 调用成功  |
| 2 |  进行https请求调用失败 | 构建异常场景：1. 配置错误的证书 2.未打开https开关 3.部分组件打开https开关，部分关闭 | 调用失败返回请求解析失败  |