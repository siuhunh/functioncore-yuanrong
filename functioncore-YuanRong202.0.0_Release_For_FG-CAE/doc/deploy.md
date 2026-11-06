[TOC]

# 部署安装

## 概述

本文档主要面向开发和运维人员，介绍元戎内核的部署安装过程，整体流程分为运行环境要求和准备、部署安装，其中部署安装分为第三方组件部署、元戎组件部署和开发环境安装几个步骤。

+ 环境准备阶段，主要介绍目前支持的硬件架构、操作系统、集群规模以及前置准备工作
+ 部署安装阶段，会分节介绍元戎内核依赖的开源组件部署、元戎组件部署以及开发环境安装，其中开发环境包括 CLI 和各语言 SDK, 以及增强特性的部署指导

> 注: 本安装部署指导当前仅针对 **华为公司黄区网络**，其它网络环境请自行调整

## 环境要求和准备

当前元戎支持 `x86_64` 和 `aarch64` 硬件架构，并针对 Ubuntu/EuleOS/Centos 三个操作系统做过部署安装验证。由于元戎内核组件基于镜像部署，部署流程在各操作系统上基本一致，后续流程针对不同操作系统有差异的地方将会特别描述。

+ 对主机系统的要求如下:
    * 创建 `/home/disk/Volumes` 目录作为数据卷，该目录磁盘空间大于200G
    * 线上运行环境请参考随版本发布的通信矩阵配置正确的防火墙规则避免集群通信异常（开发测试环境可以使用 `systemctl disable firewalld` 简单规避）
    * 咨询实验室管理员，正确配置 DNS 以支持解析黄区大网域名(例如 docker-hub.tools.huawei.com)
    * 系统硬件资源总量建议不低于如下配置，否则可能会由于资源不足无法成功部署:

| 参数      | 规格   |
| :---      | :---  |
| CPU       | 32U   |
| Memory    | 64G   |

+ 安装 [docker](https://docs.docker.com/engine/) 容器引擎，建议版本号大于 19.03
    * 针对不同操作系统安装 docker 容器引擎的命令如下:
        * Ubuntu: 配置正确的 apt 源后执行 `apt install docker.io`
        * EuleOS/Centos: 配置正确的 yum 源后执行 `yum install docker-engine`
    * 由于公司内元戎内核发布镜像保存于 [Habor 仓库](https://docker-hub.tools.huawei.com/)，为了支持下载 Habor 仓库镜像，需将该域名配置到容器引擎白名单。可以执行命令 `echo '{"insecure-registries": ["docker-hub.tools.huawei.com"]}' > /etc/docker/daemon.json` 完成配置，然后使用 `systemctl daemon-reload; systemctl restart docker` 重启 docker 服务
+ 安装 [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) 用于部署 Kubernetes 对象，建议版本号大于 1.19.4
+ 安装 [helm](https://helm.sh/docs/intro/install/) 用于部署 Kubernetes 应用，建议版本号大于 3.2
+ 安装 xmllint 命令用于命令行解析 xml 文件
    * Ubuntu: `apt install libxml2-utils`
    * EuleOS/Centos:`yum install libxml2`
+ 安装 python 用于命令行解析 json 格式
    * Ubuntu: `apt install python`
    * EuleOS/Centos:`yum install python`
+ 安装 etcdctl 用于初始化配置数据，建议版本大于 v3.2.32, 安装命令如下:
```sh
curl -kL https://github.com/etcd-io/etcd/releases/download/v3.2.32/etcd-v3.2.32-linux-amd64.tar.gz -o etcd.tar.gz
tar -zxvf etcd.tar.gz
cd etcd-v3.2.32-linux-amd64
cp etcdctl /usr/local/bin
```


## 部署安装

### 第三方组件部署

元戎内核的正确运行需要依赖部分开源第三方服务，包括 rancher/nfs-client/docker_registry/etcd/minio/redis 等必选依赖和 keycloak/prometheus/grafana/ELK/mysql 等可选依赖。这些依赖的安装配置和脚本已汇总到 [tools/basic](../tools/basic) 目录，支持一键部署，本节所述步骤均在该目录下执行。

+ 根据运行环境配置 `config.xml` 文件，相关参数如下

| 参数                 | 描述                                                         |
| :------------------- | ------------------------------------------------------------ |
| **local_ip**         | 配置为运行环境的本机 IP 地址                                 |
| **nic_name**         | 配置为运行环境的本机 IP 的网卡名，默认为 eth0                |
| **rancher_https**    | 配置部署的 rancher 的 https 访问端口，默认为8146             |
| **rancher_password** | 配置部署的 rancher 的访问密码                                |
| **nfs_serverip**     | 配置部署的 nfs 的 server 的 IP 地址，选择快速部署时也是运行环境的本机 IP 地址 |
| **prometheus_ip**    | 配置部署的 prometheus 的 IP 地址，选择快速部署时可以不部署 prometheus |
| **redis_password**    | 配置部署的 redis 的密码 |
| **S3_AK**            | 配置部署的 S3 的 accessKey |
| **S3_SK**            | 配置部署的 S3 的 secretKey |
| **core_etcd_password**    | 配置部署的 ds_core_etcd 的密码 |
| **mysql_password**    | 配置部署的 mysql 的root账号的密码，选择快速部署时可以不部署 mysql |
| **keycloak_password**    | 配置部署的 keycloak 的admin账号的密码，选择快速部署时可以不部署 keycloak |

+ 内网环境可以在各个节点上配置容器引擎的代理，或者先使用 `bash sources/pull_basic_images.sh` 命令下载依赖的第三方组件镜像
+ 集群部署需确保各节点时间同步，可以分别在各个节点上手动执行 `bash sources/ntp.sh` 命令实现
+ 执行如下命令部署所依赖的第三方组件: `bash process.sh -o install -m quick` 或者 `bash process.sh -o install -m all` 部署集群
    * `bash process.sh -o install -m quick` 部署必选的第三方组件，包括 rancher/nfs-client/docker_registry/etcd/minio/redis
    * `bash process.sh -o install -m all` 部署全部第三方组件，包括安全和维测相关组件 keycloak/prometheus/grafana/ELK/mysql
    * 如果之前安装过集群，建议在所有集群机器上先执行以下 `bash process.sh -o uninstall -m all` 避免残留容器、数据卷和网络配置影响
        - 注意该操作会暴力清除所有运行中的容器及其关联的数据卷、删除 k8s/cni/rancher 相关的主机目录、清除所有网络配置，请谨慎使用。 如果你的环境还有其它容器服务，建议参考 `bash process.sh -h` 手动执行各种清理操作
    * 更多用法使用 `bash process.sh -h` 查看脚本使用帮助
+ 部署完成后，可在浏览器输入 `https://${local_ip}:${rancher_https}` 查看第三方组件 etcd/minio/nfs/redis 状态，等状态全部为 `Running` 即表示部署成功


### 元戎组件部署

+ 从[编译输出地址](./build.md#output)目录下取得 `ServerlessNative_*.tgz` 用于部署元戎组件，部署流程参考如下脚本流程
    * 其中 `${image_repo}` 表示镜像仓库地址
        - EulerOS 基础镜像构建的发布件地址为 `docker-hub.tools.huawei.com/yuanrong_storage`
        - Ubuntu 基础镜像构建的发布件地址为 `docker-hub.tools.huawei.com/serverlessnative`
    * 其中 `${local_ip}` 表示本机 IP 地址，确保集群节点都可以访问该 IP
    * 其中 `${docker_tag}` 表示编译的镜像 tag, 版本构建可以参考 imageTag 文件，本地构建参考[编译指导](./build.md#docker_tag)
    * 如果之前已经安装过可以先执行 `helm uninstall functioncore` 卸载，更多操作和帮助参考 [helm 官方指导](https://helm.sh/docs/helm/)
    ```bash
    tar -xzvf ServerlessNative_*.tgz # 解压
    cd ServerlessNative # 进入解压后的目录
    sed -i "/accountauthtype/s/\"[^\"]*\"/\"Noauth\"/2" ./charts/iam-adaptor/templates/iam-adaptor-configmap.yaml # 关闭用户管理以方便快速验证
    bash config.sh -d "${image_repo}" -p "${local_ip}" -v "${docker_tag}" # 基础配置
    ```
    * 执行此脚本后，会在本目录下生成value.yaml文件，在升级版本时，需要用户自行根据环境资源来配置value.yaml中各个资源池global.poolsize的大小，以保证各个组件实例可以正常拉起。如果资源充足，可直接执行下一步，如果资源较小可将global.poolsize.xxx500500配置成1，global.poolsize.xxx15003000和global.poolsize.xxx400016000配置成0。xxx代表nodejs、java、python、cpp、cppbin、custom的总称，修改完此配置，可进入下一步。
    ```bash
    helm install functioncore ./ # 一键部署 FunctionCore 组件
    ```
+ 若 FunctionCore 集群中的 etcd server 开启了 https 认证，到 datas 目录下执行 `bash init-iam-data.sh -u ${etcd_user} -p ${etcd_user_password} -i huawei -e https://${local_ip}:32379 -c ${caPath}`初始化 FunctionCore 基础配置数据，然后执行 `kubectl rollout restart deployment iam-adaptor` 重启 iam-adaptor 服务
    * 更多帮助执行 `bash init-iam-data.sh -h` 查看帮助
+ 部署完成后，可在浏览器输入 `https://${local_ip}:${rancher_https}` 查看 FunctionCore 所有组件状态，等状态全部为 `Running` 即表示部署成功

### 开发环境安装

+ 从[编译输出地址](./build.md#output)目录下取得 functioncore 发布件
+ 其中 cli 目录为命令行工具，安装方法参考如下脚本流程:
    * 如果需要自动补全功能，需安装 [bash-completion](https://github.com/scop/bash-completion) 后将 `source <(yrctl completion bash)` 最佳到 ~/.bashrc 文件
```sh
cd functioncore/cli/pkg/ && tar -xf cli.tar
bash install.sh
cat >> ~/.bashrc <<EOF
export ResourcePath=~/.cli/resource/
export PATH="\${PATH}:~/.cli/bin"
EOF
```
+ 其中 cppruntime 目录下包含了 c++ 语言的 SDK，安装方法参考如下脚本流程:
```sh
cd functioncore/cppruntime && tar -xzf cpp-runtime-sdk.tar.gz
cp -rf include/* /usr/local/include/
cp -rf lib/* /usr/local/lib/
```
+ 其中 javaruntime 目录下包含了 java 语言的 SDK, 使用 `mvn install:install-file -Dfile=sn-runtime-sdk-1.0.0.jar -DgroupId=com.huawei.sn -DartifactId=sn-runtime-sdk -Dversion=1.0.0 -Dpackaging=jar` 安装



### 增强特性说明

+ [通信加密鉴权部署](./deploy/encryption_authentication.md)
+ [状态外置部署](./deploy/deploy_redis_external.md)
+ [支持本地加载函数](./deploy/local_load_function_package.md)
+ [支持其他兼容s3的对象存储服务](./deploy/use_other_s3.md)
+ [keycloak 用户管理](./deploy/user_manager.md)
+ [自定义资源配置挂载目录](./deploy/custom_resource_path_config.md)
