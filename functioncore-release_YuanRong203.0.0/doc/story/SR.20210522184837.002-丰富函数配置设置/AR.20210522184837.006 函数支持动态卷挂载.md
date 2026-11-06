[TOC]

# AR.20210522184837.006 函数支持动态卷挂载

关联AR:
+ AR-20210630-009-001: 如果函数配置了 VPC 网络，需要先启动 VPC 网络，启动 PatService(FuncInstanceManager)
+ AR.20210522184837.003: 函数指定 uid 和 gid
+ AR.20210522184837.007: worker 管理模块支持动态提权

## Story 整体设计

### 功能描述

**目的：**

文件系统挂载功能为函数提供了按需扩展的文件存储，可为多个函数、多个实例提供共享访问。函数需要支持动态卷，可以像访问本地文件系统一样对这些共享文件进行读写等操作。  

**功能**  
融合 FunctionGraph 函数需要支持以下文件系统配置：

- SFS 文件系统  
  弹性文件服务（Scalable File Service，SFS）提供按需扩展的高性能文件存储（NAS），可以共享访问。适用于多种应用场景，包括HPC、媒体处理、文件共享、内容管理和 Web 服务等。

- SFS Turbo 文件系统  
  SFS Turbo 分为 SFS Turbo 标准型、SFS Turbo 标准型-增强版、SFS Turbo 性能型和 SFS Turbo 性能型-增强版。适用于多种应用场景，包括高性能网站、日志存储、压缩解压、DevOps、企业办公、容器应用等。

- ECS 共享目录  
  ECS 共享目录是通过 nfs 服务，把 ECS 上的指定目录设置为共享文件系统，函数（和 ECS 相同的 VPC 配置）可以挂载对应目录进行读写等操作，实现计算资源的动态扩展。此类型适合业务不太频繁的场景。

**使用对象**  
用户可以在函数定义界面设置是否进行动态卷挂载

**优势**  
使用文件系统挂载功能具有以下优势：

1. 函数执行空间不仅是之前 /tmp 的512M，可以极大扩展存储空间。
2. 多个函数之间可以共享访问已经配置好的文件系统。
3. ECS 计算资源动态扩展，利用 ECS 已有的存储能力实现更大的计算能力。

### 场景分析
**创建委托**  
为函数添加文件系统配置需要先给函数设置相关服务的委托。
创建委托时，委托类型选择云服务，云服务选择FunctionGraph。委托必须要具备支持在函数中操作当前区域内的所有资源的权限。

**设置VPC**  
sfs turbo 涉及VPC内部网络访问，添加 sfs turbo 文件系统前需要给函数配置 sfs turbo 对应的 VPC  
ECS 共享目录也需要给函数配置 ECS 对应的VPC

**使用非 root 的普通用户挂载文件系统到Linux云服务器**  
1. 执行如下命令挂载文件系统  
    mount -t nfs -o nosuid,vers=3,timeo=600,nolock 挂载地址 本地路径
2. 如果是SFS3.0，通用文件系统，使用命令
    mount -t nfs -o vers=3,timeo=600,noresvport,nolock,proto=tcp 挂载地址 本地路径

|  参数 |  说明 |
| ------------ | ------------ |
|  nosuid |  禁止 suid 操作和设定 sgid 位 |
|  vers |  文件系统版本 |
|  timeo |  指定时长（单位为 0.1 秒），即 NFS 客户端在重试向云中的文件系统发送请求之前等待响应的时间 |
|  挂载地址 | SFS容量型文件系统的格式为：文件系统域名:/路径，例如：example.com:/share-xxx。SFS Turbo文件系统的格式为：文件系统IP:/，例如192.168.0.0:/。  |
|  本地路径 | 云服务器上用于挂载文件系统的本地路径，例如“/local_path”。  |

2. 挂载完成后，执行如下命令，查看已挂载的文件系统。  
mount -l  
如果回显包含如下类似信息，说明挂载成功。  
    example.com:/share-xxx on /local_path type nfs (rw,vers=3,timeo=600,nolock,addr=)

### 方案详细设计
#### 方案设计
1. InstanceManager 监听 ETCD，获取函数元数据进行缓存，并持久化到磁盘 function service。  
ETCD 元数据外部挂载文件字段信息示例如下：  


    "extendedMetaData": {
    	"mount_config": {
    		"mount_user": {
    			"user_id": 1001,
    			"user_group_id": 1001
    		},
    		"func_mounts": [
    			{
    				"id": "ccc6f799-96f9-4f47-9d67-ce4d267d90b9",
    				"mount_type": "sfs",
    				"mount_resource": "eb4ebf7a-db82-4602-82ce-7e1e57a8ef46",
    				"mount_share_path": "sfs-nas01.cn-north-7.ulanqab.huawei.com:/share-77644e2e",
    				"local_mount_path": "/home/fs",
    				"status": "active"
    			}
    		]
    	}
    }

2. InstanceManager 函数特化时封装请求包含外部挂载文件字段信息，发送到 worker。worker 通过 rpc 连接调用 workerManager 进行外部文件挂载。  
- 挂载前需要指定用户和用户组启动 runtime 进程
- 挂载前需要先进行 VPC 初始化，确保网络连接访问正常
- 创建本地挂载目录，权限赋值为 0750 ,将目录所属用户和用户组改为指定 uid、gid  
    本地路径页面限制为二级目录，且不能为已存在目录。
- 如果为 ECS 连接检查地址连通性和是否共享指定目录（nfs server 需要监听 111/20048 等端口用于检测）
- 通过 syscall.Mount() 方法挂载外部文件目录  
    func Mount(source string, target string, fstype string, flags uintptr, data string) (err error)
    
**函数调用时序图**  
[AR.20210522184837.006 函数支持动态卷挂载-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=c88de17834f449d1847887e6005f7cd9&versionUUID=)![AR.20210522184837.006 函数支持动态卷挂载](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/quZ?t=1624101425635)


#### 外部交互分析&&上下游依赖需求

1. functionGraph 管理面需要将外部文件挂载信息写入 ETCD  
2. 函数指定 uid 和 gid, runtimeManager 需要以指定用户用户组启动

### 对外接口
NA  

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
本地挂载路径页面已经做了配置路径限制，不能为已存在目录，只能为二级目录且不能配置特殊字符 . 等。

## LLT 用例
| 序号 | 预置条件 | 预期结果 |
| ------------ | ------------ | ------------ |
| 1 | 不配置挂载外部文件系统 |  函数调用成功  |
| 2 | 配置挂载 SFS 文件系统   |  成功挂载，函数调用成功   |
| 3 | 配置挂载 SFS Turbo 文件系统 | 成功挂载，函数调用成功 |
| 4 | 配置挂载 ECS 共享目录 | 成功挂载，函数调用成功  |
| 5 | 配置挂载多个不同类型文件系统 | 成功挂载，函数调用成功  |
| 6 | 挂载本地目录已存在或者权限不够 | 挂载失败，函数部署失败  |
| 7 | 被挂载的文件没有权限或者路径不存在 | 挂载失败，函数部署失败  |
| 8 | 安全组端口未开放、网络不通 | 挂载失败，函数部署失败  |
