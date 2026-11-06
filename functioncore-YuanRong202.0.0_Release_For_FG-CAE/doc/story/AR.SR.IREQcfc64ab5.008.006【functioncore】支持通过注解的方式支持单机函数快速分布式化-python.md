# 【functioncore】支持通过注解的方式支持单机函数快速分布式化


## Story 整体设计

### 功能描述

+ 支持通过注解的方式支持单机函数快速分布式化

### 场景分析

python runtime函数代码内使用def关键字定义的function，在添加@fn.invoke注解后支持分布式计算，其执行结果通过fn.get接口获取。

### 方案详细设计

通过注解支持分布式计算场景：用户函数内部分计算是串行执行，可以改造使用线程并行执行，但改造存在较大工作量，同时单个函数实例资源有限。
示例场景：for循环执行add函数10次，每次执行add函数有一定执行时延（示例代码内为sleep 2s）
```python
import time


def add(a, b):
    time.sleep(2)
    return a + b


def handler (event, context):
    rets = []
    for i in range(10):
        ret = add(1, 2)
        rets.append(ret)
    return rets
```
使用单机分布式注解改造后，只需新增了4行代码，实现了单机分布式并行执行，在函数单实例并发度为1的情况下，会拉10个函数实例并行执行add函数， 最后通过fn.get获取所有add函数执行的结果
```python
import fn
import time


@fn.invoke()
def add(a, b):
    time.sleep(2)
    return a + b


def handler (event, context):
    fn.init(context)
    rets = []
    for i in range(10):
        ret = add.invoke(1, 2)
        rets.append(ret)
    res = fn.get(rets)
    return res
```

[支持通过注解的方式支持单机函数快速分布式化](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=298b2c3d2436480a9ea08a71500bbd7b)![支持通过注解的方式支持单机函数快速分布式化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3FzM?t=1672975286988)

### 开源选型

- cloudpickle=1.6.0
（来源黄区预研版本）
  
单机的Python序列化模块有自带的pickle，但是在Python的分布式计算中进行序列化则是使用cloudpickle。之所以在分布式计算中Python的序列化使用cloudpickle模块的原因有：

1. cloudpickle是使用value序列化的方式，而pickle则是使用reference序列化的方式。因此在反序列化时pickle需要运行环境内存在序列化对象的定义，因为pickle进行序列化的只是对象（函数、类对象）的参数；而cloudpickle在序列化时会把对象的定义和参数值一并序列化，所以在分布式计算中传递cloudpickle序列化对象时接受方可以没有对象的定义（如果序列化的是类对象，那么接收方可以没有类的定义）。
2. pickle模块不能序列化lambda函数，cloudpickle可以序列化lambda函数。

### 限制

1. 因注解是python3新特性，所以只支持 python3.6 和 3.9 runtime，不支持python2.7。
2. 函数执行入口和函数初始化入口不可添加@fn.invoke()注解，如果添加则在handler.invoke执行时报错。
3. @fn.invoke装饰的函数和入参通过cloudpickle.dumps()和binascii.hexlify()序列化不可超过6MB。
4. @fn.invoke装饰的函数分布式运行的实例默认继承主函数配置的内存规格、单实例并发度和超时时间。
5. @fn.invoke装饰的函数入参支持python基础类型数字、字符串、列表、元组、字典和客户定义类对象，请勿传递线程对象或锁。
5. @fn.invoke装饰的函数返回值支持python基础类型数字、字符串、列表、元组、字典和客户定义类对象，请勿返回线程对象或锁。
6. @fn.invoke装饰的函数是分布式执行，请勿通过函数内定义的全局变量进行共享访问更新。


### 对外接口

#### init

##### 定义

```python
def init(context):
```

##### 说明

初始化fn sdk

##### 参数

| **名称** | **类型** | **描述**      |
| -------- | -------- | ------------- |
| context  | Context   | Runtime提供的函数执行上下文 |

##### 返回值
无返回值


##### 异常
无


##### 错误处理

无

##### 样例代码

```python
import fn

@fn.invoke()
def add(a, b):
    return a + b

def handler (event, context):
    fn.init(context)
    return "helloworld"
```

#### invoke

##### 定义

```python
def invoke(*args, **kwargs)->函数装饰器;
```

##### 说明

装饰使用def关键字定义的function

##### 参数

| **名称** | **类型** | **描述** |
| -------- | -------- | -------- |
|  func(Callable)  |  FunctionType  |  需调用的函数  |
|  invoke_options  |  class:InvokeOptions  |  调用参数选项：当前仅支持内存，单位mb，数值可选128、256、...8192  |

##### 返回值

返回一个装饰器

##### 异常

无

##### 错误处理

无

##### 样例代码

```python
import fn

@fn.invoke(invoke_options=InvokeOptions(memory=128))
def add(a, b):
    return a + b


def handler (event, context):
    ret = add.invoke(1, 2)
    print(fn.get(ret))
    return "helloworld"

```

#### options

##### 定义

```python
FunctionProxy.options(invoke_options: InvokeOptions)->FunctionProxy;
```

##### 说明

注解函数调用前支持设置调用选项

##### 参数

| **名称** | **类型** | **描述** |
| -------- | -------- | -------- |
|  invoke_options  |  class:InvokeOptions  |  调用参数选项：当前仅支持内存，单位mb，数值可选128、256、...8192  |

##### 返回值

@fn.invoke注解装饰的函数代理对象

##### 异常

传入非InvokeOptions对象的TypeError

##### 错误处理

无

##### 样例代码

```python
import fn

@fn.invoke
def add(a, b):
    return a + b


def handler (event, context):
    ret = add.options(invoke_options=InvokeOptions(memory=128).invoke(1, 2)
    print(fn.get(ret))
    return "helloworld"

```

#### get

##### 定义

```python
def get(obj_refs: Union[ObjectRef, List], timeout=_DEFAULT_TIMEOUT)->List;
```

##### 说明

获取obj_refs的结果数组

##### 参数

| **名称** | **类型** | **描述** |
| -------- | -------- | -------- |
|    obj_refs   |  ObjectRef or ObjectRef List |  单个ObjectRef或ObjectRef数组   |
|    timeout   |  int |  等待ObjectRef数组全量结果超时时间   |

##### 返回值

obj_refs是数组则返回结果数组，obj_refs是单个ObjectRef则返回单个结果

##### 异常

获取obj_refs的结果超时

##### 错误处理

无

##### 样例代码

单个obj_ref：

```python
import fn

@fn.invoke
def add(a, b):
    return a + b


def handler (event, context):
    ret = add.invoke(1, 2)
    print(fn.get(ret))
    return "helloworld"

```

obj_ref数组：

```python
import fn
import time


@fn.invoke()
def add(a, b):
    time.sleep(2)
    return a + b


def handler (event, context):
    fn.init(context)
    rets = []
    for i in range(10):
        ret = add.invoke(1, 2)
        rets.append(ret)
    res = fn.get(rets)
    return res
```


### 4. 上下游依赖需求

单机函数快速分布式化基于函数间调用Functionsdk实现，需补充支持函数间调用支持指定内存规格（高枫）

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

| 序号 | 用例名         | 测试步骤                                               | 预期结果 |
| ---- | -------------- | ------------------------------------------------------ | -------- |
| 1    | 函数调用并获取结果 | invoke请求，返回object_ref，请求成功后通过fn.get获取结果 | 函数调用成功，获取结果符合期望    |
| 2    | @fn.invoke注解支持内存选项 | @fn.invoke注解设定InvokeOption内存256MB, invoke请求，返回object_ref，请求成功后通过fn.get获取结果 | 函数调用成功，获取结果符合期望    |
| 3    | fn.get支持传入obj_refs数组 | for循环执行@fn.invoke注解的函数10次，得到一个长度为10的objectRef数组，fn.get获取10次调用结果 | 函数调用成功，获取结果符合期望 |
| 4    | fn.get支持传入超时等待时间 | for循环执行@fn.invoke注解的函数10次，其中有一次子函数执行时间超过等待时间 | 函数调用报错 |
| 5    | @fn.invoke注解函数支持返回python基础类型 | 注解函数返回python基础类型 | 函数调用成功 |


## 遗留问题

无