[TOC]

# 【元戎】内核函数管理能力 AR.20210518193125.001

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
+ 对接内核函数，内核函数存储在独立的表，区别于普通函数
+ 内核函数的用户组类型固定：kernel，可以操作内核函数：增、删、改、查、使用
+ 普通用户的权限：查询、使用
```text
1、普通函数的增删改查操作的KEY：/sn/functions/
2、内核函数的增删改操作的KEY：/yr/kernelFunctions/，查询操作的KEY：/repo/KernelFunctionVersion/
```


### 方案详细设计

#### 现状分析
+ 内核函数无管理能力
#### 方案设计
+ 总体方案：
```text
1、内核函数的权限管理，交由iam-adaptor来处理 – 待设计/暂不开发
2、内核函数校验由admin-service模块校验 -- 暂时只校验用户组是否为 “kernel”
3、内核函数的用户组类型固定：kernel，可以操作内核函数：增、删、改、查、使用；
4、普通用户的权限：查询、使用
5、内核函数通过/kernelFunctions的API操作
6、内核函数信息表的KEY：/yr/kernelFunctions/，查询表的KEY：/repo/KernelFunctionVersion/
```
[内核函数管理-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=4d33474879934440a7e7d408a8199425&versionUUID=)![内核函数管理](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/s3m?t=1626319898686)

[内核函数管理-增删改查-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=6e47a8c7c2cd4f81b754507c23b274f3&versionUUID=)![内核函数管理-增删改查](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/sDY?t=1626319908559)

+ CLI层
```text
1、新增内核函数操作：yrctl kernelfunc deploy/list/delete/get
2、内核函数yaml文件重新设计（如下）：去掉字段：service、functionType
3、yrctl deploy/list/delete 不带kernel的命令，无法真正操作内核函数
4、不支持yrctl invoke 内核函数，通过普通函数调用invokeSys接口来调用内核函数
5、yrctl kernelfunc create 新增内核函数样例 -  Empty Example、Kernel function Example
```
+ 内核函数yaml样例：
```yaml
description: this is a helloworld kernel function
default:
  runtime: cpp11
  memorySize: 500
  timeout: 900
functions:
  helloworld:
    handler: libhelloworld.so
    runtime: cpp11
    memorySize: 500
    timeout: 900
```


+ admin-service 层
```text
1、admin-service模块增加内核函数的API
2、admin-service到iam-adaptor鉴权时查看用户组的名字
3、admin-service的kernelFunctions API校验用户组名字是否为 “kernel”，否则返回无权限
``` 
+ API 如下：

    | 类型   | API                                          | 说明             |
    | ------ | -------------------------------------------- | ---------------- |
    | POST   | /serverless/v1/kernelFunctions/              | 创建内核函数     |
    | DELETE | /serverless/v1/kernelFunctions/:urn          | 删除内核函数     |
    | PUT    | /serverless/v1/kernelFunctions/:urn          | 更新内核函数     |
    | GET    | /serverless/v1/kernelFunctions/              | 查询内核函数列表 |
    | GET    | /serverless/v1/kernelFunctions/:urn          | 查询单个内核函数 |

+ Repo 层
```text
1、Repo模块增加内核函数的API
2、Repo中内核函数去掉租户及functiontype
```
+ API 如下：

    | 类型   | API                                                          | 说明             |
    | ------ | ------------------------------------------------------------ | ---------------- |
    | POST   | /function-repository/v1/kernelFunctions/                     | 创建内核函数     |
    | DELETE | /function-repository/v1/kernelFunctions/:functionName        | 删除内核函数     |
    | PUT    | /function-repository/v1/kernelFunctions/:functionName/cfg    | 更新内核函数     |
    | GET    | /function-repository/v1/kernelFunctions/                     | 查询内核函数列表 |
    | GET    | /function-repository/v1/kernelFunctions/:functionName        | 查询单个内核函数 |

# 开源软件选型
无

#### 外部交互分析&&上下游依赖需求
+ 内核函数存储能力

### 对外接口
+ admin-service 层

    | 类型   | API                                          | 说明             |
    | ------ | -------------------------------------------- | ---------------- |
    | POST   | /serverless/v1/kernelFunctions/              | 创建内核函数     |
    | DELETE | /serverless/v1/kernelFunctions/:urn          | 删除内核函数     |
    | PUT    | /serverless/v1/kernelFunctions/:urn          | 更新内核函数     |
    | GET    | /serverless/v1/kernelFunctions/              | 查询内核函数列表 |
    | GET    | /serverless/v1/kernelFunctions/:urn          | 查询单个内核函数 |

### 约束
+ CLI逻辑供开发自验证，不保证内核函数调用的完善逻辑

## 可信软件

#### 安全性 Security
+ 针对用户组在admin-access层的kernelFunctions的API进行校验。
+ 新增内核函数信息表、内核函数查询表
#### 韧性 Resilience
+ 基于现有的架构，韧性由现有的架构保证

#### 隐私性 Privacy
+ 基于现有的架构，隐私性由现有的架构保证。

#### 可靠性 Reliability
+ 基于现有的架构，可靠性由现有的架构保证

#### 可用性 Availability
+ 基于现有的架构，可用性由现有的架构保证

#### 安全 Safety
+ 基于现有的架构，安全性由现有的架构保证
+ 暂无租户管理系统，数据鉴权由租户管理系统来保证

## LLT 用例
+ 1、创建内核函数：进行增、删、改、查操作，功能正常。
+ 2、删除内核函数：查询为空、删除不存在

