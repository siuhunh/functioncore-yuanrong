[TOC]

# 【元戎】etcd支持https通信 AR.SR.20210621221420.032.022

## Story 整体设计

### 功能描述
现在etcd默认用http通信，根据安全需求“对于同一个服务，同时支持明文传输和TLS密文传输的场景下，产品应默认关闭明文端口，
默认仅开放TLS密文端口，需要默认开启”，默认开启https。


### 场景分析
针对FuntionGraph交付场景，FunctionAccessor、FunctionIntanceManager、functiontask和etcd通信默认开放TLS密文传输。


### 方案详细设计

#### 方案设计
+ 证书准备 
  
  FunctionGraph提供证书，包括三个文件ca.pem、client.pem、client-key.pem，其中client-key.pem是psm-tool加密后的，需要解密后使用。

  
+ 客户端认证流程
  
  配置文件HTTPS.Enable.ETCD默认为true。
  证书通过secrets加载到k8s中，使用原先的挂载项ca-secret,证书挂载在路径/home/sn/resource/ca下。之前YuanRong只挂载了ca证书开启单向认证，
  现在config文件中补充certfile和keyfile，读取时没有指定路径默认为空。
  
  原先的config：
  ```yaml
  "cafile": "/home/sn/resource/ca/ca.pem",
  "sslEnable": {{ .Values.global.HTTPS.Enable.ETCD }}
  ```
  现在的config：
  ```yaml
  "cafile": "/home/sn/resource/ca/ca.pem",
  "certfile": "/home/sn/resource/ca/client.pem",
  "keyfile": "/home/sn/resource/ca/client-key.pem",
  "sslEnable": {{ .Values.global.HTTPS.Enable.ETCD }}
  ```
  已有etcdConfig数据结构：
  ```go
  type etcdConfig struct {
  	URL       []string `json:"url" valid:"required"`
  	Username  string   `json:"username" valid:"required"`
  	Password  string   `json:"password" valid:"required"`
  	CaFile    string   `json:"cafile,omitempty" valid:",optional"`
  	CertFile  string   `json:"certfile,omitempty" valid:",optional"`
  	KeyFile   string   `json:"keyfile,omitempty" valid:",optional"`
  	SslEnable bool     `json:"sslEnable,omitempty"`
  }
  ```
  证书用客户端增加解密代码，增加在pkg/common/etcd3/watcher.go中，psm-tool对应的解密代码参考pkg/common/localauth/crypto.go

异常点考虑：
经过初步验证，如果修改配置项打开HTTPS无法在保留原先数据存储目录data.etcd和etcd.log的情况下重启ETCD，
需要用数据快照snapshot保存和重现原先etcd中的数据。

#### 开源软件选型
无

#### 外部交互分析&&上下游依赖需求
默认开启HTTPS，黄区测试环境需要适配。

### 对外接口
无

### 约束
无


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
| **序号** | **用例名**                 | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------- | ------------------------------------ | ---------------------------------------- |
| 1        | 客户端和etcd挂载不同的CA证书   | 验证是否能通信                  |CA认证失败                     |


