[TOC]

# 【functionCore】自定义镜像特性增强目录控制

## 1. Story整体设计

### 1. 功能描述
限制自定义镜像容器的可写层大小，防止用户写入过大内容导致爆盘

### 2. 场景分析
因为FunctionGraph集群使用的是欧拉v2r7系统，默认docker的存储引擎使用devicemapper，kubelet中无法采集devicemapper的容器可写层大小，所以pod配置中的ephemeral storage限制不生效，需要绕过kubernetes从docker底层计算自定义镜像容器的可写层大小。

### 3. 方案详细设计

#### 概述
实现一个新的daemonset组件，名为storage-eviction-manager。该组件会监控docker容器的创建，看到是自定义镜像的runtime容器，就用inotify监控磁盘大小，超过512M限制则用docker命令将它杀掉。

#### 限制
容器被杀时请求调用无法感知，正在执行的调用请求会返回function invocation exception。

#### 在容器内使用docker api

1. 挂载本地的`/var/run/docker.sock`到容器内

    ```yaml
    # storageevictionmanager-daemonset.yaml
    ...
        volumeMounts:
        - mountPath: /var/run/docker.sock
            name: docker-socket
    ...
    volumes:
    - name: docker-socket
        hostPath:
        path: /var/run/docker.sock
    ...
    ```

2. import "github.com/docker/docker/client" （非新引入）

#### 在容器内监控其他容器磁盘大小

1. 挂载本地的`/var/lib/docker`到容器内

    ```yaml
    # storageevictionmanager-daemonset.yaml
    ...
        volumeMounts:
        - mountPath: /var/lib/docker
            name: docker-rootdir
    ...
    volumes:
    - name: docker-rootdir
        hostPath:
        path: /var/lib/docker
    ...
    ```

2. `/var/lib/docker/devicemapper/mnt/[id]/rootfs` 下有容器完整的文件系统映射

    id是docker inspect中DeviceName的最后一部分，即`b5a300a741409e762b7224f1b0f2a372574f7b16e12b972f7109b8a920feed52`，使用docker inspect命令可查

    ```bash
    # docker inspect 0fb471b0886c
    ...
    "GraphDriver": {
        "Data": {
            "DeviceId": "231545",
            "DeviceName": "docker-252:1-262145-b5a300a741409e762b7224f1b0f2a372574f7b16e12b972f7109b8a920feed52",
            "DeviceSize": "10737418240"
        },
        "Name": "devicemapper"
    },
    ...
    ```

3. storage-eviction-manager使用sn用户启动，同时申请DAC_OVERRIDE的capability，以便有权限查看其他容器的文件大小。

    ```Dockerfile
    # Dockerfile
    RUN setcap 'cap_dac_override+ep' ${DEPLOY} ...
    ```

4. 自定义镜像的runtime容器名都会以/k8s_runtime_custom打头，每秒使用docker container list查询节点上的所有容器，过滤出正在运行的自定义镜像runtime；

5. 使用go的fsnotify库（底层inotify）监控容器根目录及其子目录，计算容器总使用量，之后通过事件的通知计算出改变量x，并将x作为容器当前的可写层大小。

### 4. 上下游依赖需求
无

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

| 序号 | 用例名               | 测试步骤                                                     | 预期结果         |
| ---- | -------------------- | ------------------------------------------------------------ | ---------------- |
| 1    | 写入磁盘             | 调用自定义镜像，容器内写入超过512M                              | 容器被杀          |

