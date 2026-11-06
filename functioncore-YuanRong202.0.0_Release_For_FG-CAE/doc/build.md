[TOC]

# 编译

## 概述

本文档主要介绍元戎内核的编译过程，整体流程分为编译环境准备、源码编译、镜像制作，各步骤的主要目的和依赖关系如下:

+ 环境准备阶段，主要是准备 docker/helm/golang/gcc/java/python/nodejs 等语言的构建环境以及 code-generator/protobuf 等依赖软件，以支撑后续步骤的执行
+ 源码编译阶段，将元戎所有组件的源码编译为二进制发布件，和相应的运行依赖以及配置文件一起按组件分别存放到输出目录。同时提供 Helm 的安装包以方便用户快速基于镜像部署元戎内核相关服务
+ 镜像制作阶段，针对元戎的发布件，将各组件制作为容器镜像，以便快速部署

## 环境准备

+ 安装 [docker](https://docs.docker.com/engine/) 容器引擎用于运行构建镜像和构建发布镜像
+ 安装 [helm](https://helm.sh/docs/intro/install/) 用于制作部署包
+ golang/gcc/java/python/nodejs 等语言的构建环境以及 code-generator/protobuf 等依赖软件的安装过程可以参考对应官网学习，为了方便快速构建，可以直接下载已经全部安装就绪的构建镜像用于源码编译: `docker pull docker-hub.tools.huawei.com/serverlessnative/compile:latest`
  * 随着代码和依赖的更新，构建镜像可能会持续更新，因此如果本地编译失败，可以尝试更新一下构建镜像

## 源码编译

+ 各组件编译请单独下载组件源码，全量编译需下载所有组件源码并归档到同级目录，例如 `/usr1/code/YuanRong` 目录下。为了方便快速搭建构建环境，本文主要介绍基于构建镜像的源码编译方法
```sh
git clone -b develop ssh://git@szv-y.codehub.huawei.com:2222/YuanRong_Kernel/functioncore.git
git clone -b develop ssh://git@szv-y.codehub.huawei.com:2222/YuanRong_Kernel/runtime-nodejs.git
git clone -b develop ssh://git@szv-y.codehub.huawei.com:2222/YuanRong_Kernel/runtime-python.git
git clone -b develop ssh://git@szv-y.codehub.huawei.com:2222/YuanRong_Kernel/runtime-java.git
git clone -b develop ssh://git@szv-y.codehub.huawei.com:2222/YuanRong_Kernel/runtime-cpp.git
git clone -b develop ssh://ssh://git@szv-y.codehub.huawei.com:2222/YuanRong_Kernel/workflow.git
```
+ 启动构建容器，并将代码目录映射至容器内 `docker run -it -v /usr1/code/YuanRong:/home/code docker-hub.tools.huawei.com/serverlessnative/compile:latest /bin/bash`
+ 在容器内依次执行如下命令完成源码编译和 Helm 的安装包制作:
```sh
source /etc/profile
cd /home/code/functioncore/build
# 源码编译
bash build.sh -m functioncore # 单组件编译请查看帮助
# Helm 安装包制作
cd helm-build
bash build.sh -m functioncore
```
+ 完成后 `exit` 退出容器，部署包生成在源码同级 output 目录(例如 `/usr1/code/YuanRong/output`)下，其中 Helm 安装包保存在 IndependentProjectChart 目录。 <span id="output"></span>

## 镜像制作

+ 镜像制作前确保完成源码编译
+ 在主机上定位到 functioncore 源码的 build 目录(例如 `/usr1/code/YuanRong/functioncore/build`)下执行 `sh docker_build.sh -v ${docker_tag} -m all` 可以构建全量镜像 <span id="docker_tag"></span>
  * 其中 `${docker_tag}` 为开发自定义的镜像 tag, 用于安装部署时配置镜像版本，需避免包含空格等特殊符号
  * 针对各组件的镜像制作可查看帮助
+ 编译完成后镜像会以 tar 后缀保存到源码同级 output 目录(例如 `/usr1/code/YuanRong/output`)下，此时可以看到该目录下包含如下子目录
    * IndependentProjectChart 目录下包含了元戎的 helm 安装包
    * functioncore 目录下包含了元戎各组件的二进制、脚本、配置文件、容器镜像等



