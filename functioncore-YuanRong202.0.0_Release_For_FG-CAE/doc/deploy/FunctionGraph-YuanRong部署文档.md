[TOC]
# 元戎数据面部署文档

 ## ETCD 组件部署

[ETCD 集群部署参考](./ETCD部署.md)

## YuanRong 组件部署

### 组件部署

1. 获取发布包

   [开发获取地址](http://100.107.35.38:8080/view/VersionBuild/job/YuanRong_develop_VersionBuild_EulerOSV2R9/ )

   [发布包获取]()

2. 配置初始化

    解压

    ```sh
    # 解压
    tar -xvzf ServerlessNative_deploy.tgz
    # 进入文件夹
    cd ServerlessNative
    # 初始化
    bash config.sh -p ${主节点 IP} -v ${imageTag} -d ${镜像仓库}
    ```

    * 主节点: 集群的主节点机器 IP

    * imageTag: 镜像版本，可从发布包内获取

    * 镜像仓库: 待补充

3. 部署前准备

    1. [helm 安装]()

    2. 开启鉴权：
        1. 获取用于解密的证书，由 FunctionGraph 提供，数据面需要挂载证书，保持与 FunctionGraph 一致。

        2. 获取SSL的证书，支撑 HTTPS，由 FunctionGraph 提供，数据面需要挂载证书，与FunctionGraph 保持一致。
        
        3. 获取ETCD开启HTTPS相关的证书，由 FunctionGraph 提供，数据面需要挂载证书，与FunctionGraph 保持一致。

    |    用途    |       证书        |
    | :--------: | :---------------: |
    |  支撑解密  | common_shared.key |
    |            |     root.key      |
    | 支撑 https  |     cert_pwd      |
    |            |    server.cer     |
    |            |  server_key.pem   |
    |            |     trust.cer     |
    | 支撑 etcd   |     ca.pem        |
    |            |    cert.pem       |
    |            |     key_enc.pem   |

    挂载命令：

    ```sh
    # 挂载解密相关
    kubectl create secret generic local-secret --from-file=common_shared.key=${secret_path}/common_shared.key --from-file=root.key=${secret_path}/root.key
    
    # 挂载 https 相关
    kubectl create secret generic https-certs --from-file=cert_pwd=${https_path}/cert_pwd --from-file=server.cer=${https_path}/server.cer --from-file=server_key.pem=${https_path}/server_key.pem --from-file=trust.cer=${https_path}/trust.cer
    ```

   # 挂载 etcd 证书相关
    kubectl create secret generic ca-secret --from-file=ca.pem=${etcd_path}/ca.pem --from-file=cert.pem=${etcd_path}/cert.pem --from-file=key.pem=${etcd_path}/key_enc.pem
    ```
    
    注：
    * secret_path：表示支撑解密的两个文件在机器上的路径
    * https_path：表示支撑 HTTPS 的四个文件在机器上的路径
    * etcd_path：表示支撑etcd开启HTTPS的三个文件在机器上的路径

4. 更改 values.yaml 配置

    注：

    * 以下配置 AK , SK 相关均由 FunctionGraph 生成配置

    ```sh
    vim values.yaml
    ```

    1. 必要配置：
        **注意：global.k8sCidr.hostCidr 中存在必填项，请注意配置**

        |              配置项              |                             描述                             |                值                 |         限制         |
        | :------------------------------: | :----------------------------------------------------------: | :-------------------------------: | :------------------: |
        |           global.s3AccessKey     |                   obs-accessKey，明文配置                    |           obs-AK 的明文            |          无          |
        |           global.s3SecretKey     |   obs-secretKey，密文配置，加密由 FunctionGraph 加密算法支持   |           obs-SK 的密文            |          无          |
        |          global.s3Mode           |            模式选择：obs 非数据面，选择集群外访问             |            outCluster             | inCluster/outCluster |
        |        global.s3Endpoint         |                      配置 obs 访问地址                       | obs.cn-north-7.ulanqab.huawei.com |          无          |
        |      global.localAuthEnable      |                      适配解密算法的开关                      |               true                |      true/false      |
        |     global.internalTLS.tlsEnable |                  选择是否开启 https/tls 的开关                   |               true                |      true/false      |
        |     global.functionCapability    | functioncore 提供的函数能力.当 functionCapability: 1时，提供支撑 functiongraph 的能力，同时只支持调用函数功能。当 functionCapability: 0时，则提供当前所有函数功能。 |               1                |      0/1      |
        |     global.k8sCidr.svcCidr       |                  配置 worker 隔离网络                   |当前 CCE 集群的 service 网段信息，可以在集群管理中获取到                |      无      |
        |     global.k8sCidr.podCidr       |                  配置 worker 隔离网络                   |当前 CCE 集群的 pod 网段信息，可以在集群管理中获取到                |      无      |
        |     global.k8sCidr.hostCidr      |                  配置 worker 隔离网络                   |当前 CCE 集群的 node 网段信息，根据当前 CCE 集群的 VPC 名，在 VPC 服务管理中找到 VPC 的网段信息          |    192.254.0.0/16为必须项，集群信息与此网段逗号分隔        |
        | global.isNodeAffinity | 开启 vip 函数独占 vm 的功能 |true | 开启前需保证所有普通节点都被打上 node-role=data-plane 的标签、etcd节点被打上node-etcd=router的标签、vip节点打上tenant_id=xxx(随机字符串)的标签 |
    2. 可选配置：

        |            配置项             |                             描述                             |       值       |    限制    |
        | :---------------------------: | :----------------------------------------------------------: | :------------: | :--------: |
        |  global.authenticationEnable  |                           鉴权开关                           |   默认为 true   | true/false |
        |   global.replicas.frontend    |                   frontend 启动的实例数量                    |  默认3个实例   |            |
        | global.replicas.workerManager |                 workermanager 启动的实例数量                 |  默认3个实例   |            |
        |    global.hpa.max.frontend    |                  frontend 弹性扩缩最大实例                   |  默认5个实例   |            |
        |    global.hpa.min.frontend    |                  frontend 弹性扩缩最小实例                   |  默认3个实例   |            |
        |       global.poolSize.*       | 配置各种规格池子的数量，FunctionGraph-Yuanrong 数据面只涉及 fusion 池子 |                |            |
        | global.monitoring.serverAddr  |              指定 redis 的 IP，由 FunctionGraph 提供              |  port 默认6379  |            |
        | global.monitoring.serverMode  |  指定 redis 模式，单点模式则值为 single，集群模式则值为 cluster  | single/cluster |            |
        |  global.monitoring.password   |           redis 访问密码的密文，由 FunctionGraph 提供           |                |            |
        |  global.monitoring.clusterID  |             部署多个元戎集群时 clusterID 不能重复              |                |            |
        |          global.aKey          |        元戎数据面间鉴权使用，由 FunctionGraph 加密提供         |                |            |
        |          global.sKey          |        元戎数据面间鉴权使用，由 FunctionGraph 加密提供         |                |            |
        |     global.log.rolling        | 日志滚动开关（超过大小打包压缩），FunctionGraph 场景需设置为 false |  默认为 true    |            |
        |     global.https.enable.etcd  | 用于开启元数据etcd集群的https，FunctionGraph场景需设置为true         |  默认为true    |            |
        |     global.customCA           | 用于开启etcd集群的https，挂载相关证书，FunctionGraph场景需设置为true |  默认为true    |            |
        | global.clusterEtcd.arrayEtcd  |  配置元数据etcd集群IP                                          |  默认为空    |            |
        | global.clusterEtcd.arrayHetcd |  配置元数据etcd集群IP                                         |  默认为空    |            |
        |     global.isCluster          |  用于开启etcd集群模式                                        |  默认为false   |            |
        |     global.worker.useMinio    |  用于标识是否使用 minio                                      |  默认为 false   |            |
        | global.etcdUser | 配置元数据etcd用户名 | 默认为root | |
        | global. etcdPassword | 指定元数据 etcd 访问的密文，由 FunctionGraph 加密提供 |  | |
        | global.routerEtcdUser | 配置路由etcd用户名 | 默认为空 | |
        | global.routerEtcdPwd | 配置路由etcd密码 | 默认为空 | |
        | global.https.enable.routerEtcd | 用于开启路由etcd集群的tls | 默认为true | |
        | global.clusterEtcd.routerEtcd | 配置路由etcd集群ip | 默认为空 | |

5. helm 部署
```sh
   helm install function-accessor -f values.yaml ./charts/frontend
   helm install worker -f values.yaml ./charts/worker
   helm install function-task -f values.yaml ./charts/functiontask
   helm install function-instance-manager -f values.yaml ./charts/worker-manager
   helm install cache-manager -f values.yaml ./charts/cache-manager
```

### 组件卸载
```sh
   helm uninstall function-accessor
   helm uninstall worker
   helm uninstall function-task
   helm uninstall function-instance-manager
   helm uninstall cache-manager
```

### 组件升级
```sh
   helm upgrade function-accessor -f values.yaml ./charts/frontend
   helm upgrade worker -f values.yaml ./charts/worker
   helm upgrade function-task -f values.yaml ./charts/functiontask
   helm upgrade function-instance-manager -f values.yaml ./charts/worker-manager
   helm upgrade cache-manager -f values.yaml ./charts/cache-manager
```
注：当前使用了滚动升级的升级方式，通过逐步替换的策略，保证整体系统的稳定，在初始化升级的时候就可以及时发现和解决问题，避免问题不断扩大。滚动升级配置如下：
```sh
  strategy:
    rollingUpdate:
      maxSurge: 25%
      maxUnavailable: 25%
    type: RollingUpdate
```
maxSurge: 25% 表示滚动升级时会先启动的 pod 个数（replicas*25%）
maxUnavailable: 25% 表示滚动升级时允许的最大 Unavailable 的 pod 个数（replicas*25%）

## workflow 部署

[workflow 部署参考](./workflow部署.md)

## 工具和文件清理

> 清除 helm 工具，secret，https 相关文件。 待补充。

## 安全组网概述

> 安全组网是指在建设元戎网络时需应用的安全技术与策略。

> 公有云CCE集群为元戎组件提供安全的集群网络，白名单访问。

> 针对元戎网络面临的安全风险，元戎的服务组件运行在独立的VPC内，外部服务通过VPC-EP通道同本VPC通信。

> 元戎组件仅提供唯一对外接口（函数调用），且不存在未公开接口。使用https协议通信，每一次均需要认证鉴权后才解析数据，其中认证凭据加密后存储在系统中。

> 元戎集群内服务不直接对外网提供服务，建议由产品对接外网服务，进行用户的鉴权及集群流控控制。

> 元戎需部署在单独的CCE集群，保证CCE集群内每一个HOST只接入数据面网络，因busproxy通过K8机制监听HOST所有22423、32568端口请求的原因，可以通过HOST任意网络访问，如CCE集群组网未做好网络三面隔离，则存在风险。

