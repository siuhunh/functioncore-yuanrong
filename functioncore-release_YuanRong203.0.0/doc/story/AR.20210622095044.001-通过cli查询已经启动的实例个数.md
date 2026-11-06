[TOC]

# 【元戎】通过cli查询已经启动的实例个数

## Story 整体设计

### 功能描述
通过cli查询已经启动的实例个数

### 场景分析
+ 用户可以通过cli中的get命令查询到当前实例运行的个数

### 方案详细设计

#### 现状分析
+ 用户通过yrctl get 可以获得函数的元信息

```
[root@serverlessnative jhello]# yrctl get helloworld
Function info:
{
    "name": "helloworld",
    "description": "",
    "id": "sn:cn:yrk:12345678901234561234567890123456:function:0-jhello-helloworld:$latest",
    "Created": "2021-06-30 03:09:23.172 UTC",
    "LastModified": "2021-06-30 08:14:19.455 UTC",
    "memory": 500,
    "runtime": "java1.8",
    "versionDesc": "$latest",
    "versionNumber": "$latest",
    "timeout": 900,
    "functionUrn": "sn:cn:yrk:12345678901234561234567890123456:function:0-jhello-helloworld",
    "handler": "com.huawei.example.HelloWorld",
    "codeSha256": "dba02d7480793cc2e41dad66fad5749aea40e357a4be90577d79b7fe4b716ee5",
    "codeSize": 597719,
    "revisionId": "20210630081419455",
    "maxInstance": 100,
    "concurrentNum": 1,
    "downloadUrl": "http://8.0.210.108:30110/bucket-test-log1/helloworld-1625040859475?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=root%2F20210630%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20210630T092401Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=c05034f77800e9964327cbde6b1202b22969a4d1e93ec74edc0357b405af9787",
    "status": "succeeded"
}
```

#### 方案设计
+ 在function-repository的getFunction中查询etcd时新增查询实例数功能，并返回instanceNum字段。

+ 函数的实例数可以通过查询/sn/workers 获取，每新创一个实例就会才etcd中维护一条信息。
```
/sn/workers/business/yrk/tenant/12345678901234561234567890123456/function/0-jhello-helloworld/version/$latest/defaultaz/defaultaz-#-pool4-500-500-java1.8-789c8b87cb-6nrp7
{"ip":"10.42.0.102","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"8.0.210.108","nodePort":"22423","nodeName":"serverlessnative","applier":"serverlessnative.huawei.com","ownerIP":"8.0.210.108"}

/sn/workers/business/yrk/tenant/12345678901234561234567890123456/function/0-jhello-helloworld/version/$latest/defaultaz/defaultaz-#-pool4-500-500-java1.8-789c8b87cb-wnddx
{"ip":"10.42.0.100","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"8.0.210.108","nodePort":"22423","nodeName":"serverlessnative","applier":"serverlessnative.huawei.com","ownerIP":"8.0.210.108"}
```

+ 函数每当有状态改变就会更新函数状态，现有etcd信息如下：
```
/repo/FunctionStatus/yrk/12345678901234561234567890123456//0-jhello-helloworld/$latest/
{"Status":"succeeded"}
```
  改造后新增查询函数实例数：
```
/repo/FunctionStatus/yrk/12345678901234561234567890123456//0-jhello-helloworld/$latest/
{"InstanceNum":1,"Status":"succeeded"}
```

+ 新增实例数后调用yrctl get：
```
[root@serverlessnative jhello]# yrctl get helloworld
Function info:
{
    "name": "helloworld",
    "description": "",
    "id": "sn:cn:yrk:12345678901234561234567890123456:function:0-jhello-helloworld:$latest",
    "Created": "2021-06-30 03:09:23.172 UTC",
    "LastModified": "2021-06-30 08:14:19.455 UTC",
    "memory": 500,
    "runtime": "java1.8",
    "versionDesc": "$latest",
    "versionNumber": "$latest",
    "timeout": 900,
    "functionUrn": "sn:cn:yrk:12345678901234561234567890123456:function:0-jhello-helloworld",
    "handler": "com.huawei.example.HelloWorld",
    "codeSha256": "dba02d7480793cc2e41dad66fad5749aea40e357a4be90577d79b7fe4b716ee5",
    "codeSize": 597719,
    "revisionId": "20210630081419455",
    "maxInstance": 100,
    "concurrentNum": 1,
    "downloadUrl": "http://8.0.210.108:30110/bucket-test-log1/helloworld-1625040859475?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=root%2F20210630%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20210630T092401Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=c05034f77800e9964327cbde6b1202b22969a4d1e93ec74edc0357b405af9787",
    "status": "succeeded",
    "instanceNum": 1
}
```

# 开源软件选型
*新引入的开源软件：原因、选型调研情况*

#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*

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
| **序号** | **用例名**                                                   | **测试步骤**                          | **预期结果**                             |
| -------- | ---------------------------------------------------------- | ----------------------------------- | --------------------------------------- |
| 1        | 存在实例 调用yrctl get                                        | 使用yrctl get命令获取元信息             | 获取到instanceNum                       |
| 2        | 不存在实例 调用yrctl get                                      | 使用yrctl get命令获取元信息             | instanceNum为0                       |