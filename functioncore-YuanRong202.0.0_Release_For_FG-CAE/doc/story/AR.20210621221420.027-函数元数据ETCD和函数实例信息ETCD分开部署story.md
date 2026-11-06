[TOC]

# 函数元数据ETCD和函数实例信息ETCD分开部署 story 设计

关联AR:
+ AR:  AR.20210621221420.027 

### 功能描述

当前架构，元数据和实例信息相关只写到一个ETCD，存在如下问题：

1. 运行面多集群下，组件 watch ETCD 数据会 watch 到其他运行面集群的信息，如实例信息，节点信息。集群间数据相耦合，会造成数据冗余，脏数据等问题。同时也会造成数据缓存的存储瓶颈。
2. 多集群场景下，各个运行面集群和管理面集群都会对 ETCD 进行读写，会造成ETCD 存储和性能的瓶颈。

基于上述问题场景，融合版本架构下，支持元数据和路由 ETCD 数据解耦合。

1. 管理面只感知写入函数元信息。
2. 运行面对元信息只读，同时读写运行面独有的实例、节点等信息。

### 场景分析
融合版本架构：

[融合ETCD集群模型](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=dbe757f705c942c8a47e5eb47d79d600&versionUUID=)![融合ETCD集群模型](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/13dS?t=1632962408753)

当前架构：管理面，运行面共用一套 ETCD，读写信息都往一个 ETCD。

融合后架构：

1. 管理面向元数据 ETCD 写函数元信息：包括 /sn/functions, /sn/alias 等key，原有写入不会发生变动。
2. 运行面向元数据 ETCD watch 函数元信息，触发相应事件。
3. 运行面向路由 ETCD watch 实例信息，节点信息，触发相应事件。
4. 运行面向路由 ETCD 写实例信息，节点信息。


### 方案详细设计

#### 现状分析
#### 方案设计

新增路由 ETCD 配置，原有配置当作元数据 ETCD 使用。

values.yaml

|             配置项              | 默认值 |           描述            |
| :-----------------------------: | :----: | :-----------------------: |
|     global.routingEtcdUser      |  root  |     运行面ETCD用户名      |
|   global.routingEtcdPwdFusion   |   ""   |      运行面ETCD密码       |
| global.https.enable.routingEtcd |  true  | 运行面ETCD开启tls安全通信 |
| global.clusterEtcd.routingEtcd  |   ""   |      运行面ETCD地址       |

functionAccessor， functiontask， workermanager 组件 configmap 新增配置:

```yaml
"routingEtcd": {
    "servers": {{ .Values.global.clusterEtcd.routingEtcd }},
    "user": "{{ .Values.global.routingEtcdUser }}",
    "password": "{{ .Values.global.routingEtcdPwdFusion }}",
    "cafile": "/home/sn/resource/rutimePlaneETCD/ca.pem",
    "certfile": "/home/sn/resource/rutimePlaneETCD/cert.pem",
    "keyfile": "/home/sn/resource/rutimePlaneETCD/key.pem",
    "sslEnable": {{ .Values.global.https.enable.routingEtcd }}
},
```

functionAccessor，workermanager 组件 deployment 新增配置，functiontask 组件 daemonset 新增配置：

```yaml
# 新增挂载项
volumeMounts:
{{- if .Values.global.https.enable.routingEtcd }}
- mountPath: /home/sn/resource/routingEtcd
  name: runtimeplane-etcdca
  readOnly: true
{{- end }}

volumes:
{{- if .Values.global.https.enable.routingEtcd }}
- name: runtimeplane-etcdca
  secret:
    defaultMode: 0440
    secretName: routingEtcd-secret
{{- end }}
```

如果开启 TLS 通信，需要挂载证书。

```sh
kubectl create secret generic routingEtcd-secret --from-file=ca.pem=${https_path}/ca.pem --from-file=cert.pem=${https_path}/cert.pem --from-file=key.pem=${https_path}/key.pem
```

代码涉及模块：FunctionAccessor， BusProxy， FunctionInstanceManager

FunctionAccessor：新增路由 ETCD

1. 维护两个 ETCD 元数据 ETCD，路由ETCD 创建两个 ETCD client
2. 保留订阅方向
   1. /sn/functions,  /sn/alias      ——元数据 ETCD
   2. /sn/workers,    /sn/nodes    ——路由 ETCD

BusProxy：新增路由ETCD

1. 维护两个 ETCD 元数据 ETCD，路由 ETCD 创建两个 ETCD client
2. 保留订阅方向
   1. /sn/functions,  /sn/alias      ——元数据 ETCD
   2. /sn/workers,    /sn/nodes    ——路由 ETCD

FunctionInstanceManager：新增路由 ETCD

1. VPCHandler client用户管理创建 patservice，VPCPulltrigger 相关，读写路由 ETCD，订阅key：/net/tenant
2. 委托相关，读元数据 ETCD，订阅 key: /sn/agency
3. FunctionService 缓存，主动获取 value 信息，读元数据 ETCD，订阅 key: /sn/functions
4. 事件驱动 reactor，需读元数据 ETCD，读写路由 ETCD。当前只有一个 ETCDClient，需要改造为两个，同步修改订阅事件。
5. 缓存实例数，读路由 ETCD，主动获取数据，订阅 key: /sn/workers
6. 函数实例查询，读路由 ETCD，主动获取数据，订阅key: /sn/workers

#### 开源软件选型
无引入

#### 外部交互分析&&上下游依赖需求
拆分之后需要每个运行面有自己的 ETCD，存在部署需求。——施广帅

### 对外接口
不涉及

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

## 用例
| 序号 |                  用例名                   |                           测试步骤                           |                           预期结果                           |
| :--: | :---------------------------------------: | :----------------------------------------------------------: | :----------------------------------------------------------: |
|  1   | 正常功能，函数读写-保证元数据ETCD成功访问 | 1. 页面创建函数，触发ETCD写入事件<br>2. 运行面观察相关组件日志<br>3. 页面删除函数，触发ETCD删除事件<br>4. 运行面观察组件日志 | 1.Put事件，成功查看到日志变化<br>2. Delete事件，成功查看到日志变化 |
|  2   |  正常功能，实例读写-保证路由ETCD成功访问  | 1. 通过postman或者rdispatcher指定运行面集群进行函数调度<br>2. 运行面观察组件日志<br>3. 删除pod，作优雅退出<br>4. 运行面观察组件日志 | 1. 执行函数调用，拉起pod，触发Put事件，组件成功查看到put事件<br>2. 删除pod，触发Delete事件，组件成功查看到delete事件 |
|  3   |  正常功能，节点读写-保证路由ETCD成功访问  |           1. 运行面进行节点增删<br>2. 查看组件日志           | 1. 增加节点，组件成功查看到put事件<br>2. 删除节点，组件成功查看到delete事件 |
|  4   |       异常场景-集群间信息不互相影响       | 1. 重复2，3的操作，观察其他集群组件日志<br>2. 查看管理面ETCD数据 | 1. 其他集群组件不会受到实例信息影响<br>2. 管理面ETCD不会有worker信息 |

