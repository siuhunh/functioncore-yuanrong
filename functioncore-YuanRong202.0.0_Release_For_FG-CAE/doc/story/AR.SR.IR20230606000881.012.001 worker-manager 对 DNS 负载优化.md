[TOC]

# AR.SR.IR20230606000881.012.001 worker-manager 对 DNS 负载优化

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

函数支持配置内网域名，用户可以在初始化和函数调用阶段通过域名直接访问对应 IP 服务。但是目前访问 DNS 解析域名造成 DNS 服务负载过高，需要优化当前对 DNS 服务的请求，减少访问的频率，降低服务的负载。

内网域名解析过程  
![图片说明](http://image.huawei.com/tiny-lts/v1/images/b1835d83c9b3785f7d4cf59aff27d302_317x252.png@900-0-90-f.png)

用户函数中支持配置域名

![图片说明](http://image.huawei.com/tiny-lts/v1/images/f267232381ce306cee15058bf2b5f340_910x479.png@900-0-90-f.png)

### 1.2特性场景分析

**1. 数据面当前域名解析架构流程**

![图片说明](http://image.huawei.com/tiny-lts/v1/images/52c49d024263d02f777817000d1b7155_872x561.png@900-0-90-f.png)

**流程说明:**  
1）worker-manager 监听 ETCD key 感知函数元数据变化，如果新增函数或者函数元数据有更新且配置了域名，则 worker-manager 通过 omsvc 接口查询 DNS 服务，获取域名对应的 ip 且缓存在内存中  
2）worker-manager 中启动定时任务，10 分钟遍历一次所有函数 cache, 如果配置了域名则通过 omsvc 接口查询 DNS 服务，获取域名对应的 ip 更新缓存  
3）初始化流程中的域名和 ip 映射关系通过 worker-manager 缓存获取，在请求中透传到 worker 写入 runtime host 文件  
4）函数调用流程中，域名和 ip 映射关系在管理面访问 DNS 或者缓存解析后，通过 head 字段一路透传数据面最终写入 runtime host 文件  

**原流程存在的问题：**  
1. 当前遍历请求和函数相关联，多个函数配置同一个域名也会访问多次 DNS 服务，访问请求存在冗余
2. omsvc 每次都直接访问 DNS,造成 DNS 访问负载过高

## 2. 特性/功能实现原理

### 2.1总体方案

![图片说明](http://image.huawei.com/tiny-lts/v1/images/842324a60575bbcc900e7f70ca82d248_561x375.png@900-0-90-f.png)

1）在 omsvc 和 DNS 服务之间增加 redis 缓存，减少直接访问 DNS 服务的频率。  
2）去除数据面访问 DNS 服务的冗余请求，同一 vpc 同一域名只访问一次解析接口。  

### 2.2模块详细设计

1. omsvc 将域名与内网ip映射关系写入 redis 缓存 （管理面需求实现）  
1）omsvc 调用dns服务获取域名与内网 ip 映射关系，写入 redis 并设置 TTL 过期时间（和 DNS 缓存数据的更新时间和 TTL 时间相关）
2）写入 redis key 和 /sn/dns/{projectId}/{vpcId}/{domainName} ，由vpcID + domainName 唯一确定

2. worker-manager 访问 omsvc 接口增加 由vpcID参数传递  
URL: {dnsAddress}/v2/recordsets?zone_type={ZoneType}&type={Type}&name={DomainName}**&vpc_id={vpcID}**

3. 创建一个全局 map，存储 dominName 和 域名与内网ip映射关系  
```go
	var dnsCache sync.Map
	var dnsDomainNames map[string][]string
	var funcSvcIDList []string

	dnsCache.Store("svcID_funcSvcID" , "vpcID&domainName")
	dnsCache.Store("func_vpcID&domainName" , funcSvcIDList)
	dnsCache.Store("vpcID&domainName" , dnsDomainNames)
```

4. worker-manager 监听元数据 ETCD  
	1）函数元数据有新增函数  
	- 没有配置域名服务，跳过  
	- 如果配置了域名服务，vpcID&domainName 已存在，则往 dnsCache 写入key svcID_funcSvcID,更新 key vpcID&domainName_func  
	- 如果配置了域名服务，vpcID&domainName 不存在，则往 dnsCache 写入key svcID_funcSvcID, func_vpcID&domainName"，查询omsvc接口写入 key vpcID&domainName  
	
	2)函数有修改  
	- 没有配置域名服务，查询 dnsCache 中 svcID_funcSvcID，如果未修改则跳过。有修改更新原 dnsCache 相关值  
	- 配置域名服务，查询 dnsCache 中 svcID_funcSvcID，如果未修改则跳过。有修改更新原 dnsCache 相关值，走上面新增处理逻辑  

	3）函数删除  
	- 没有配置域名服务，跳过  
	- 如果配置了域名服务，删除 dnsCache 相关值  
	- 如果 vpcID&domainName_func 列表为空，则删除相关 vpcID&domainName  

5. worker-manager 定时任务更新 dnsCache  
10min 一次遍历 dnsCache，获取 vpcID&domainName 查询 omsvc 接口更新相关域名缓存映射  

6. worker-manager初始化函数 dns 信息 从dnsCache 中获取  


### 2.3 约束

#### 2.3.1上下游依赖需求

 ![图片说明](http://image.huawei.com/tiny-lts/v1/images/ae3b821a609d78c661c2b71b424452a6_1454x47.png@900-0-90-f.png)

#### 2.3.2其他约束

**约束：**  
1）DNS 服务存在限流，每个租户一段时间内访问次数有限制  
2）一个 VPC 可以绑定多个域名，同一内网域名名称可以绑定在不同 VPC 下。但同一 VPC 下内网域名名称不能重复  
3）一个内网域名下可以有多个记录集  
4）修改或删除域名解析记录后，理论上生效的最长时间是解析记录修改或删除前设置的TTL值。DNS 页面支持配置的最小TTL时间为 5 分钟，但 FunctionGraph 支持的域名修改生效时间为 10 分钟。  
注：TTL(Time-To-Live) 指解析记录在本地DNS服务器中的缓存时间。  

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  

不涉及

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
不涉及

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*  
不涉及

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】  
不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*  
不涉及
### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*  
不涉及

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*  
减少对 DNS 负载压力

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*  
兼容旧版本

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤** | **预期结果** |
| -------- | ---------- | -------- | ------------ | ------------ |
| 1        | dnCache相关增删操作LLT       | LLT      | dnsCache增删操作 |正确读取dnsCache相关内容
| 2        | 函数配置域名         | ST       | 初始化函数中打印域名对应ip           | 打印正确          |
| 3        | 函数修改域名         | ST       | 初始化函数中打印域名对应ip           | 打印修改后数据     |
| 4        | 函数删除域名         | ST       | 初始化函数中打印域名对应ip           | 打印为空          |

LLT覆盖场景：  
1.新增函数，新增域名  
2.新增函数，复用域名  
3.新增函数，配置多个域名  
4.修改函数，多个域名，修改一个  
5.修改函数，多个域名，删除一个  
6.修改函数，删除域名，其他函数无引用  
7.修改函数，删除域名，其他函数有引用  
8.修改函数，无域名配置，未修改域名配置  
9.修改函数，有域名配置，未修改域名配置  
10.修改函数，原无域名配置，新增有复用域名  
11.修改函数，原无域名配置，新增未复用域名  
12.修改函数，原有域名配置，新增有复用域名  
13.修改函数，原有域名配置，新增未复用域名  
14.删除函数，无域名配置  
15.删除函数，有域名配置  
