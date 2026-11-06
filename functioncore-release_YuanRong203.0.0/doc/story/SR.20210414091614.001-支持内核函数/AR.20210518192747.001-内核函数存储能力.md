[TOC]

# 【元戎】内核函数存储能力 AR.20210518192747.001

## Story 整体设计

### 功能描述
*描述基本功能，推荐使用场景分析的工程方法5W1H1E:*
+ *Why: 这个功能的目的    --必要性*
+ *Who: 谁来用这个功能   --使用对象*
+ *When: 什么时间/背景使用   --使用时间*
+ *Where: 什么地点使用   --使用地点*
+ *How: 如何使用   --使用方式*
+ *What happen: 使用过程中可能影响什么功能   --功能交互*
+ *Experience: 用户的体验如何，包括性能安全、韧性、可靠性、可用性、隐私等方面的体验   --体验*

> *不适合5W1H1E 方法的的场合:*
> *1. 不适用于非功能性或功能点离散的的特性，包括平台、协议等*
> *2. 简单单一的场景可以不用5W1H1E*



### 场景分析
+ 对接内核函数，内核函数存储在独立的区域，区别于普通函数。
+ 内核函数路径为：/yr/kernelFunctions/，查询内核函数路径：/repo/KernelFunctionVersion/functionName/versions


### 方案详细设计

#### 现状分析
+ 普通租户有租户信息，用户数据是隔离的。内核函数存储不区分租户（无租户概念 - 不存储租户相关信息）。
#### 方案设计
[内核函数存储流程-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=807d64c23a794e169030560865b2dfdc&versionUUID=)![内核函数存储流程](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/o3p?t=1626319937576)

+ 普通租户有用户数据隔离，内核函数对所有租户可见。内核函数存储不区分租户（无租户概念 - 不存储租户相关信息）。

+ 内核函数数据，查询表中KEY中无租户信息。

| KEY                                         | VALUE |
| ------------------------------------------- | ------ |
| /repo/KernelFunctionVersion/helloworld/$latest/ | 如下 |

+ VALUE 例子：
```json
{
  "Function": {
    "Name": "helloworld",
    "ReversedConcurrency": 0,
    "Version": 0,
    "Description": "",
    "Tag": null,
    "CreateTime": "2021-07-12 11:25:11.455 UTC",
    "UpdateTime": "2021-07-13 01:44:15.123 UTC"
  },
  "FunctionVersion": {
    "Package": {
      "Signature": "6e17117b795c25246b605df370448618540af6777e6d865520de6ebcada38aee",
      "Size": 3740,
      "BucketID": "bucket-test-log1",
      "ObjectID": "helloworld-1626140655126"
    },
    "RevisionID": "20210713014415123",
    "Handler": "libhelloworld.so",
    "CPUSize": 500,
    "MemorySize": 500,
    "Runtime": "cpp11",
    "Timeout": 900,
    "Version": "$latest",
    "Environment": "08aa02bec239c0cbdc798d9e:1941a59d8812b88b595e7b06bafdfdc9f149",
    "CustomResources": "{}",
    "Description": "$latest",
    "StatefulFlag": 0,
    "PublishTime": "2021-07-12 11:25:11.455 UTC",
    "MinInstance": 0,
    "MaxInstance": 100,
    "ConcurrentNum": 100,
    "Status": "succeeded"
  },
  "FunctionLayer": null
}
```
+ 内核函数详细信息表，供其他组件使用：

| KEY                                                          | VALUE |
| ------------------------------------------------------------ | ------------ |
| /yr/kernelFunctions/function/helloworld/version/$latest | 如下 |

+ VALUE 样例
```json
{
  "funcMetaData": {
    "name": "helloworld",
    "description": "",
    "functionUrn": "sn:cn:::function:helloworld",
    "reversedConcurrency": 0,
    "tags": null,
    "functionVersionUrn": "sn:cn:::function:helloworld:$latest",
    "codeSize": 3740,
    "codeSha256": "6e17117b795c25246b605df370448618540af6777e6d865520de6ebcada38aee",
    "handler": "libhelloworld.so",
    "runtime": "cpp11",
    "timeout": 900,
    "version": "$latest",
    "versionDescription": "$latest",
    "statefulFlag": false,
    "deadLetterConfig": "",
    "layers": [
    ],
    "created": "2021-07-12 11:25:11.455 UTC",
    "revisionId": "20210713014415123"
  },
  "s3MetaData": {
    "appId": "61022",
    "bucketUrl": "http://bucket-test-log1.hwcloudtest.cn:18085",
    "bucketId": "bucket-test-log1",
    "objectId": "helloworld-1626140655126"
  },
  "envMetaData": {
    "environment": "--",
    "envKey": "--"
  },
  "resourceMetaData": {
    "cpu": 500,
    "memory": 500,
    "customResources": "{}"
  },
  "instanceMetaData": {
    "minInstance": 0,
    "maxInstance": 100,
    "concurrentNum": 100
  }
}
```
+ 内核函数支持增删改查，repo中提供相应的对外接口

+ 增删改查API如下：

    | 类型   | API                                                          | 说明             |
    | ------ | ------------------------------------------------------------ | ---------------- |
    | POST   | /function-repository/v1/kernelFunctions/                     | 创建内核函数     |
    | DELETE | /function-repository/v1/kernelFunctions/:functionName        | 删除内核函数     |
    | PUT    | /function-repository/v1/kernelFunctions/:functionName/cfg    | 更新内核函数     |
    | GET    | /function-repository/v1/kernelFunctions/                     | 查询内核函数列表 |
    | GET    | /function-repository/v1/kernelFunctions/:functionName        | 查询单个内核函数 |

## 内核函数存储能力 <a name="zh-cn_topic_0253862089_section999597101615"></a>
+ 区别于普通函数数据，存储在独立目录，不存储用户相关的数据
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
+ 针对不同租户角色，在AdminAccess层进行权限校验。
+ 因当前无租户信息，所以先独立出内核函数逻辑。
+ 内核函数对所有用户可见，在存储时应去掉用户相关的信息。
#### 韧性 Resilience
+ 基于现有的架构，韧性由现有的架构保证

#### 隐私性 Privacy
+ 基于现有的架构，隐私性由现有的架构保证

#### 可靠性 Reliability
+ 基于现有的架构，可靠性由现有的架构保证

#### 可用性 Availability
+ 基于现有的架构，可用性由现有的架构保证

#### 安全 Safety
+ 基于现有的架构，安全性由现有的架构保证
+ 当前对接etcd数据库，数据安全性由现有的架构保证

## LLT 用例
+ 创建内核函数，存在两个表里面，数据正确

