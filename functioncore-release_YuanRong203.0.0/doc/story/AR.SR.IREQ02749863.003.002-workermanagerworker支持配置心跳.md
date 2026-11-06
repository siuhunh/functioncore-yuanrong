[TOC]

# worker-manager/worker支持心跳配置

关联AR：

1. AR.SR.IREQ02749863.003.001：java runtime提供heartbeat回调，检测java卡死场景

## 1. Story整体设计

### 1. 功能描述

背景：

​	当前平台的runtime健康检查只能检测runtime进程在不在，无法检测进程是否正常。

​	当前平台无法检测异常场景举例：

​		runtime进程用户代码执行异常（陷入死循环、死锁），进程存在，但无法正常工作

​		runtime进程OOM，进程存在，但是内部线程卡死，无法正常工作

​	现在需要**支持检测用户java进程是否正常**的功能，在用户java进程不正常时，runtime通过已有的心跳机制上报worker，worker进行异常处理。

需求：
1. worker-manager/worker支持将用户配置的心跳检测函数参数传递给runtime。

### 2. 场景分析

时序图：

[心跳检测函数参数配置监听](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=db300e2cf6da47759a956b7e7d0119e5)![心跳检测函数参数配置监听](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1qSm?t=1652322456971)

​	1.用户在函数的高级配置界面配置心跳检测函数入口后，点击保存，rdispatcher将配置项写入etcd的函数元数据

​	2.etcd通知Worker Manager，Worker Manager更新函数的元数据缓存

[[Invoke函数冷启动顺序图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/8e77cb2bca3148e2a26bcb20970567e3?projectDiagramUUID=44a6406f326f4416aa1cac6ca1559327&versionUUID=)![Invoke函数冷启动顺序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1rQZ?t=1652514535506)

1.worker manager在Specialize Worker Req中将心跳检测函数配置项HeartbeatHandler下发到worker

2.worker在Specialize Runtime Req中将心跳检测函数配置项HeartbeatHandler下发到Runtime

### 3. 方案详细设计
​	1.数据结构

在函数元数据的extendedMetaData中，增加heartbeater的heartbeat_handler选项，由管理面写入

heartbeat_handler是心跳函数参数入口，与initializer的initializer_handler类似

	"extendedMetaData": {
		"image_name": "latest-210821100213@qootx",
		"role": {
			"xrole": "function_admin",
			"app_xrole": "function_admin"
		},
		"func_vpc": {
			"domain_id": "1d60cc02b9814b9e8cab1ff36886cacb",
			"namespace": "",
			"vpc_name": "vpc-wtm-test2",
			"vpc_id": "9a0e9ae8-9ea6-4ba7-8c6b-06952f4c9b5e",
			"subnet_name": "subnet-4ed0",
			"subnet_id": "49ec1c3b-3957-455a-a1c6-2dc17b3d04c1",
			"tenant_cidr": "10.9.0.0/24",
			"host_vm_cidr": "",
			"gateway": "10.9.0.1",
			"xrole": "function_admin",
			"domain_names": ""
		},
		"endpoint_tenant_vpc": null,
		"mount_config": null,
		"strategy_config": {
			"concurrency": -1,
			"concurrentNum": 20
		},
		"extend_config": "",
		"initializer": {
			"initializer_handler": "",
			"initializer_timeout": 0
		},
		"enterprise_project_id": "0",
		"log_tank_service": {
			"logGroupId": "00b0189a-ebfe-4eb4-acef-7d5cad8dec17",
			"logStreamId": "96be63c2-2251-4510-9951-7ac364d77514"
		},
		"tracing_config": {
			"tracing_ak": "",
			"tracing_sk": ""
		},
		"custom_container_config": {
			"control_path": "",
			"image": "",
			"command": null,
			"args": null,
			"working_dir": "",
			"uid": 0,
			"gid": 0
		},
		"heartbeat": {
			"heartbeat_handler": ""
		}
	}

2.配置参数传递

[参数传递类图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=557560c970de4a6fa69e1fbd6d828f22)![参数传递类图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1qb8?t=1652337498467)

3.约束

当前只支持Java语言

4.兼容性

worker在特化流程中判断语言是否支持心跳（当前只配置java支持），只有支持的语言才在grpc里附带上心跳配置参数，对其他语言无影响

### 4. 上下游依赖需求

​	依赖Function Graph管理面写入函数的heartbeat_handler配置项-潘炜程

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

| **序号** | **用例名**                     | **测试步骤**                                                 | **预期结果**                                                 |
| -------- | ------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | 所有语言配置心跳检测函数冷启动 | 1.不配置心跳检测函数<br />2.冷启动函数<br />3.所有语言均测试 | 函数成功执行，runtime向worker上报正常心跳（runtime日志打印） |
| 2        | 非Java语言配置异常心跳检测函数 | 1.非Java语言配置心跳检测函数<br />2.冷启动函数<br />         | 函数成功执行，runtime向worker上报正常心跳（runtime日志打印） |
| 3        | Java配置正常心跳检测函数冷启动 | 1.Java函数配置正常心跳检测函数<br />2.冷启动函数             | 函数成功执行，runtime向worker上报正常心跳（runtime日志打印） |
| 4        | Java配置异常心跳检测函数冷启动 | 1.Java配置异常心跳检测函数<br />2.冷启动函数                 | 函数执行失败，runtime向worker上报异常心跳（runtime日志打印） |
