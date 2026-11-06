[TOC]

# 【functioncore】数据面对接CCMS

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

**Kubernetes****场景容器工作负载访问云服务方法：**

I.**使用****iam****账号，将账号和口令放在****secret****中，缺点如下：**

①一般是永久凭据，缺少轮转机制

②不同的工作负载使用同一账号，难以实现权限最小化和权限精细化控制


II.**Workload Identity**

1.凭据定期自动轮转，无需用户实现

2.可以针对每个实例创建细粒度权限

### 1.2特性场景分析

数据面访问云服务的场景：

cache manager 和 worker 从 obs 下载用户函数代码包。

## 2. 特性/功能实现原理

### 2.1总体方案

不再使用固定的 ak/sk 访问 OBS。采用 CCE 的 OIDC （OpenID Connect）Token 获取临时的 IAM Token ，再通过该 token 访问 IAM 获取临时的 OBS 服务的 ak/sk/token ， 进而访问 OBS 。在临期时，获取新的临时 token 。

### 2.2模块详细设计

1. 凭据轮转管理：

IAM Token 通过 CCE 集群热更新，有效期 24h 。 

临时云服务 token 有效期 24h 。

临时云服务 token 临期前 12h （固定在每天早晚 9 点） 通过重新读取 IAM Token ， 访问 IAM 获取新的 token 。如获取失败，间隔 10min 重试， 如果重试 3 次失败，产生紧急告警。另 ： 已验证 IAM Token 过期后， 临时云服务 token 在有效期内仍可以使用。

2. 业务使用：

cache manager 自行获取和管理凭据。

worker 获取轮转凭据的方式：

1） 自行获取 ----- 劣：申请 token 过多

2） 通过 cache manager 定期获取 ----- 优：单集群仅 cache manage 获取凭据；劣：新增接口；池化的 worker 需要定时获取/函数特化时获取，影响冷启动；增加 cache manager 故障面影响；

3） worker manager 特化 worker 时传递 ----- 调用 obsclient 提供的 Refresh 接口进行更新（内存操作）。

obs client 源码参考：https://github.com/huaweicloud/huaweicloud-sdk-go-obs/blob/782df337584adbb102afe9dd5e3eeaf6950e269e/obs/client.go

```go
// Refresh refreshes ak, sk and securityToken for obsClient.

func (obsClient ObsClient) Refresh(ak, sk, securityToken string)
```



因此，使用 3）的方式，由 worker manager 获取和管理凭据，在特化时传给 worker。



#### 2.2.1凭据轮转子模块设计

##### 2.2.1.1模块实现

common 目录下增加 tokenrotation 模块，负责实现凭据轮转。以下为该模块获取临时 token 的 shell 版本。

```
id_token=$(cat /var/run/secrets/tokens/csms-token)

token_body="{\"auth\":{\"id_token\":{\"id\":\"${id_token}\"},\"scope\":{\"project\":{\"id\":\"${PAAS_PROJECT_ID}\",\"name\":\"${REGION_ID}\"}}}}"

iam_token=$(curl -k https://iam.${REGION_ID}.myhuaweicloud.com/v3.0/OS-AUTH/id-token/tokens -d "${token_body}" -XPOST -H "X-Idp-Id:${IDENTITY_PROVIDER}" -H "Content-Type: application/json" -v 2>&1 | grep X-Subject-Token | cut -d':' -f2)

// temp_token 解析其 "access""secret""securitytoken"字段
temp_token=$(curl -k https://iam.${REGION_ID}.myhuaweicloud.com/v3.0/OS-CREDENTIAL/securitytokens -d '{"auth":{"identity":{"methods":["token"],"token":{"duration_seconds":86400}}}}' -H "X-Auth-Token:${iam_token}" -H "Content-Type: application/json")
  
obsutil config -i="$ak" -k="$sk" -t="$stsToken" -e obs.cn-north-7.ulanqab.huawei.com
```

ＩＡＭ　接口文档：

https://apiexplorer.developer.huaweicloud.com/apiexplorer/doc?product=IAM&api=CreateTemporaryAccessKeyByToken



配置项修改:

https://codehub-y.huawei.com/YuanRong_Kernel/deployment/merge_requests/53/diffs

cache manager, worker manager 通过配置项开启凭据轮转
worker: worker manager 传递了凭据，则依赖此凭据。否则，使用默认凭据。 


##### 2.2.1.2模块对外接口定义

*描述模块的对外接口（如果有），所有外部模块只关注模块接口，不关注模块实现*

##### 2.2.1.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*

### 2.3 约束

#### 2.3.1上下游依赖需求

1. **最新的****CCE 1.19.16**以及以上集群：上线进展 https://clouddevops.huawei.com/domains/1666/wiki/5/WIKI2022071801178

2. 需要预先参考 https://clouddevops.huawei.com/domains/4044/wiki/526/WIKI2022021100591?title=5bbc41b8-084d-e647-f8cb-43b508e432e0 完成 ①在CCE侧获取集群的JWKS（即集群的service account token的签名公钥）② 在IAM配置身份提供商、group和策略

   除文档所述外： 

   ​	1）需要为用户组授权 obs 只读权限。

   ​	2）身份转换规则中，将负载的 service account 账号和 group 做映射， 涉及的 service account 账号为 cache-manager 和 worker-manager

3. 现网升级人员根据实际环境，修改部署模板参数，是否开启凭据轮转。

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

凭据 24h 轮转一次；

仅为凭据赋予了 obs 只读权限。

获取到的凭据加密存放在内存中。

不用的凭据及时清理内存。

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

如在 token 有效期内发生了泄露, token 失效方式如下:

参见: https://support.huaweicloud.com/api-iam/iam_30_0001.html

- 如果在Token有效期内进行如下操作，

  当前Token将立即失效。

  - 删除/停用IAM用户。
  - 修改IAM用户密码、访问密钥。
  - IAM用户权限发生变化（如帐号欠费无法访问云服务、申请公测通过、IAM用户权限被修改等）。

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

凭据有效期 24h ， 提前 12h （固定在每天早晚 9 点）尝试申请新的凭据，申请到后使用新的凭据，避免因不能及时获取新的凭据，导致无法访问 obs ， 对 FunctionGraph 服务的正常使用造成影响。
考虑到 worker 仅在下载代码包流程中使用该秘钥，且下载代码包存在超时时间 < 5min , 因此可以认为传递给 worker 的凭据始终可以正常使用。

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

兼容之前的配置项，使用固定的 ak / sk 

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

单组件独立升级不影响功能。如彻底删除系统中的 obs 的凭据，则需 cache manager/worker/worker manager 均升级至新版本。

## 4. 用例

| **序号** | **用例名**                                   | 用例类型     | **测试步骤** | **预期结果**                                                 |
| -------- | -------------------------------------------- | ------------ | ------------ | ------------------------------------------------------------ |
| 1        | 获取 iam token                               | LLT          | mock iam     | 能够获取 mock iam的返回值                                    |
| 2        | 定时轮转                                     | LLT          | 定时器到期   | 重新触发凭据获取                                             |
| 3        | 开启凭据轮转,函数基本调用（函数代码包1k/1M） | 绿区门槛用例 | 函数冷启动   | worker 从 cache manager 可以拿到函数代码包；worker 从 obs 可以拿到函数代码包。 |
| 4        | 关闭凭据轮转,函数基本调用（函数代码包1k/1M） | 黄区门槛用例 | 函数冷启动   | worker 从 cache manager 可以拿到函数代码包；worker 从 obs 可以拿到函数代码包。 |

