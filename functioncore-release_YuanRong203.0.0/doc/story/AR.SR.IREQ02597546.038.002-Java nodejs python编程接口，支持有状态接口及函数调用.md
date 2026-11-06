[TOC]

# Java nodejs python编程接口，支持有状态接口及函数调用

## 1. Story整体设计

### 1. 功能描述
需求：
1. Java nodejs python编程接口，支持有状态接口及函数调用.

### 2. 场景分析
java、nodejs、python 三个runtime语言添加有状态接口和函数调用API.
		
### 3. 方案详细设计
#### nodejs:
1. 新增状态初始化函数
若开发者需要开发内置状态管理函数时，需要实现状态初始化函数，示例如下：
````
module.exports.initState = function(event, context) {
    context.logger.info("initializing state!");
    context.state = {value: 0};
};
````
initState 为开发者定义状态的名称，开发者定义的状态初始化函数，当本函数被携带某一stateID第一次调用时，会通过该函数初始化状态数据对象；

2. context新增两个属性
context.state：获取此次调用所携带的状态数据对象，数据格式为JSON对象；如果此次调用不带状态，则值为null。平台会在每次函数执行结束后保存最新的状态数据。
context.stateID：获取此次调用所携带的状态ID，String格式；如果此次调用不带状态，则值为null。stateID表示实例的ID值，state表示当前的状态值。

3. 开放SN API
首先要引入SN库
````
const { SN } = require("snc-javascript-sdk");
````
调用函数：
````
async SN.invoke(funcName[:funcVer], event[, stateID]) : SNFuture
async SN.invokeByKey(funcName[:funcVer], event, stateKey) : SNFuture
````
生成状态ID接口：
````
async SN.generateID(key) : string
````
获取结果接口：
````
async SN.getSNFuture(future): Promise<any>
````
删除状态接口：
````
async SN.terminate(stateID): Promise<SNFuture>
````
等待调用结果接口：
````
async SN.wait(snfutureList, waitNum[, timeout]): Promise<any>
````
设置状态存活时间接口：
````
(method) SN.setTimeout(stateID, timeout): Promise<SNFuture>
````
状态保存接口：
````
(method) SN.saveState(): Promise<void>
````
注: 有状态不支持nodejs6

#### python:
1. 新增状态初始化函数
若开发者需要开发内置状态管理函数时，需要实现状态初始化函数，示例如下：
````
def initState(event: dict, context: Context):
    context.state = {"value": 0}
````
initState 为开发者定义状态的名称，开发者定义的状态初始化函数，当本函数被携带某一stateID第一次调用时，会通过该函数初始化状态数据对象；

2. context新增API
get_state()
获取此次调用所携带的状态数据对象，数据格式为dict对象；如果此次调用不带状态，则值为None。平台会在每次函数执行结束后保存最新的状态数据。
set_state(self, state: dict)
设置此次调用所携带的状态数据对象，数据格式为dict对象；
get_state_id()
获取此次调用所携带的状态ID，str类型；如果此次调用不带状态，则值为None。stateID表示实例的ID值，state表示当前的状态值。
set_state_id(self, state_id: str)
有状态调用时设置的有状态id
get_trace_id()
获取调用请求的trace_id，该函数对应graph context的getRequestID(), 替换snsdk内使用context的get_trace_id()为getRequestID()
get_invoke_property()
获取调用属性
get_invoke_id()
获取调用id

替换fdk内对函数名的正则表达式为


2. 开放SN API
首先要引入SN库
````
from sncsdk import SN
````
调用函数：
````
invoke(func_name, payload, state_id=None)
invoke_by_key(func_name, payload, key)
````
注：函数间调用API内部会对调用的函数名进行检查，老的正则表达式如下：
````
FUNC_NAME_REG = r'^[a-z][a-z0-9-]{0,30}[a-z0-9]$|^[a-z]$'
FUNC_NAME_ALIAS_REG = \
    r'^([a-z][a-z0-9-]{0,30}[a-z0-9]|[a-z])[:][a-z0-9-$]{1,16}$'
````
需适配支持functiongraph函数名正则，函数名长度限制：63
````
^[a-zA-Z]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$
````

生成状态ID接口：
````
generate_id(key)
````
获取结果接口：
````
get_snfuture(snfuture)
````
删除状态接口：
````
terminate(state_id)
````
等待调用结果接口：
````
wait(snfuturelist, wait_num=None, timeout=None)
````
设置状态存活时间接口：
````
set_timeout(state_id, timeout)
````
状态保存接口：
````
save_state()
````



#### java:
若开发者需要开发内置状态管理函数时，需要实现状态初始化函数，示例如下：
注意：定义的状态类型需要可被Gson序列化和反序列化，否则可能会无法正确读取持。
````
package com.huawei.example;

import com.google.gson.JsonObject;
import com.huawei.sn.SN;
import com.huawei.sn.runtime.Context;
import com.huawei.sn.runtime.RequestHandler;
import com.huawei.sn.runtime.StateReader;
import com.huawei.sn.runtime.log.LogFactory;
import com.huawei.sn.runtime.log.RunLog;

/**
 * AddOne state autoincrement function
 *
 * @since 2020/1/20
 */
public class JavaHandler implements RequestHandler<JsonObject, JsonObject>, StateReader<Object, Counter> {
    @Override
    public JsonObject handleRequest(JsonObject jsonObject, Context context) {
        ......
    }

    @Override
    public Counter initState(Object event, Context context) {
        Counter counter = new Counter(0);
        context.setState(counter);
        return counter;
    }
}
````
开发者定义的状态初始化函数，当本函数被携带某一stateID第一次调用时，会通过该函数初始化状态数据对象；此方法需要和入口函数定义在同一类中，event和context参数定义与入口函数中的参数定义一致。
方法实现StateReader泛型接口，可以自己定义输出类型。请注意：开发者需要调用 context.setState(state) 方法将初始化状态写入上下文中，而由于平台需要反序列化持久化的状态数据，此处需要声明State类型的返回。状态类型定义如下：
````
package com.huawei.example;

/**
 * Counter counter
 *
 * @since 2020/1/20
 */
public class Counter {
    /**
     * stateful data
     */
    public int data;

    public Counter(int data) {
        this.data = data;
    }
}
````
2. context新增state相关函数
````
import com.huawei.sn.runtime.Context;

Object context.getState();
````
说明：获取此次调用所携带的状态数据对象，返回值为Object类型，开发者使用前需要转换为自己声明的类型；如果此次调用不带状态，则值为null。平台会在每次函数执行结束后保存最新的状态数据。
````
import com.huawei.sn.runtime.Context;

void context.setState(Object state);
````
说明：将开发者状态数据写入context中。
参数类型描述stateObject开发者状态数据对象
````
import com.huawei.sn.runtime.Context;

String context.getStateID();
````
说明：获取此次调用所携带的状态ID，String格式；如果此次调用不带状态，则值为空。

3. 开放SN API
首先要引入SN库
````
import com.huawei.sn.SN;
````
调用函数：
````
public <T> SNFuture<T> invoke(String funcName, String payload)
````
调用函数接口，被调用函数是无状态的。
````
public <T> SNFuture<T> invoke(String funcName, String payload, String stateID)
````
调用函数接口，被调用函数是内置状态管理函数，使用状态ID作为状态标识。
````
public <T> SNFuture<T> invokeByKey(String funcName, String payload, String key)
````
调用函数接口，被调用函数是有状态的，使用状态key作为状态标识。

生成状态ID接口：
````
public String generateID(String key)
````
获取结果接口：
````
public <T> T getSNFuture(SNFuture<T> snfuture)
````
删除状态接口：
````
public SNFuture terminate(String stateID)
````

等待调用结果接口：
````
public <T> List<SNFuture<T>> wait(List<SNFuture<T>> snfutureList, int waitNum, int timeout)
````
等待指定个数的请求结果，并指定超时时间。
````
public <T> List<SNFuture<T>> wait(List<SNFuture<T>> snfList, int waitNum)
````
等待指定个数的请求结果。
````
public <T> List<SNFuture<T>> wait(List<SNFuture<T>> snfutureList)
````
等待全部请求结果。
````
public <T> SNFuture<T> wait(SNFuture<T> snfuture)
````
等待指定请求的结果。

设置状态存活时间接口：
````
public SNFuture setTimeout(String stateID, int timeout)
````
状态保存接口：
````
public void saveState()
````

### 4. 上下游依赖需求

functiontask和statemanager (丁震)

## 2. 可信软件

1. 安全性（Security） 
   *【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 
   不涉及
2. 韧性（Resilience）
   *【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
   不涉及
3. 隐私性（Privacy） 
   *【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
   不涉及
4. 可靠性（Reliability）
   *【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
   不涉及
5. 可用性（Availability）
   不涉及
6. 安全（Safety）
   *【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
   不涉及

## 3. 用例

| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ---------------------------------------------------------- | ---------------------------------- | ---------------------------------------- |
| 1        | java有状态函数                                               | 创建函数，新增初始化状态函数，测试有状态函数（自增） | 成功自增结果                      |
| 2        | nodejs有状态函数                                             | 创建函数，新增初始化状态函数，测试有状态函数（自增）  | 成功自增结果                     |
| 3        | python有状态函数                                             | 创建函数，新增初始化状态函数，测试有状态函数（自增）  | 成功自增结果                     |
| 4        | java函数间调用（有状态）                                       | 创建函数，函数内调用有状态函数           | 成功返回结果                              |
| 5        | nodejs函数间调用（有状态）                                     | 创建函数，函数内调用有状态函数           | 成功返回结果                              |
| 6        | python函数间调用（有状态）                                     | 创建函数，函数内调用有状态函数           | 成功返回结果                              |
| 7        | java函数间调用（无状态）                                       | 创建函数，函数内调用无状态函数           | 成功返回结果                              |
| 8        | nodejs函数间调用（无状态）                                     | 创建函数，函数内调用无状态函数           | 成功返回结果                              |
| 9        | python函数间调用（无状态）                                     | 创建函数，函数内调用无状态函数           | 成功返回结果                              |