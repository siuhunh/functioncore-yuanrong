[TOC]

# 【functionCore】函数委托信息的 AK/SK 需要加密

## Story 整体设计

### 功能描述
函数委托信息的 AK/SK 属于敏感信息，在管理面对该数据信息进行加密，传输到数据面之后进行解密，在传输过程中保证安全性。


### 场景分析

1. 函数委托信息的 AK/SK 主要包括两个场景的传输使用：

- **初始化函数中使用  AK/SK**
管理面实时更新委托用户的 AK/SK 信息并写入 ETCD ，运行面 fninstance-manager 同时监听 ETCD 的 AK/SK 信息并更新缓存。在runtime 特化时传递 AK/SK 信息，在执行初始化函数时将 AK/SK 信息写入参数 context 中供用户使用。
———— 当前 ETCD 中的 AK/SK 信息为加密，需要再管理面进行加密写入，在 worker 中进行解密使用

- **函数调用时使用 AK/SK**
用户函数调用时，管理面发送 http/https 请求到 fnaccessor，AK/SK 信息在 header 中进行传递。最终 worker 将 AK/SK 信息写入函数参数 context 中供用户使用。
———— 当前 header 中的 AK/SK 信息已加密，需要在 worker 中进行解密使用

2. ETCD 中 AK/SK/Token key 和 value 定义：
key 格式为：
    /sn/agency/business/yrk/tenant/${tenantid}/domain/${domainId}/agency/${agencyid}
value 结构为:
```go
AccessKey	string		`json:"accessKey"`
SecretKey	string		`json:"secretKey"`
Token		string		`json:"token"`
```

### 方案详细设计

#### 方案设计

1. 初始化函数 context 参数在 worker 中解密
调用 common 已封装的解密方法 localauth.Decrypt() 进行解密，解密失败则 AK/SK 信息为空。
2. 函数调用参数 context 参数在 worker 中解密
调用 common 已封装的解密方法 localauth.Decrypt() 进行解密，解密失败则 AK/SK 信息为空

#### 外部交互分析&&上下游依赖需求
1. 管理面需要对写入ETCD AK/SK 信息做加密 --对接人 夏嘉诚

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
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | 初始化函数使用 AK/SK |  编写函数，包含初始化方法，在方法中打印AK/SK 信息 | 函数正常调用且打印 AK/SK 信息明文正确 |
| 2 | 函数正常调用使用 AK/SK | 编写函数，包含委托信息。在方法中打印AK/SK 信息 | 函数正常调用，且打印 AK/SK 信息明文正确 |
| 3 | 函数异常调用使用 AK/SK | 缺少加解密文件。编写函数，包含委托信息。在方法中打印AK/SK 信息 | 函数正常调用，打印 AK/SK 信息为空 |