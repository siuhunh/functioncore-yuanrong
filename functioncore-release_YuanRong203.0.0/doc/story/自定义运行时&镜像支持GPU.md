[TOC]

# YuanRong设计文档模板

AR.SR.IREQc019b94d.008.003  worker-manager支持GPU自定义运行时不配置资源池 

AR.SR.IREQc019b94d.008.004  支持GPU节点标签，GPU节点上只部署配置GPU函数 

**AR.SR.IREQc019b94d.008.005 GPU支持自定义镜像**

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

1.客户有使用GPU函数的需求，例如koomap等用户

2.竞争力构建，当前阿里函数计算FC支持自定义镜像使用GPU

### 1.2特性场景分析

#### 1.2.1自定义运行时介绍

FunctionGraph支持自定义编程语言运行时。您可以使用可执行文件（名称为bootstrap）的形式将运行时包含在函数的程序包中，当调用一个FunctionGraph函数时，它将运行函数的处理程序方法。

自定义的运行时在FunctionGraph执行环境中运行，它可以是Shell脚本，也可以是可在linux可执行的二进制文件。

**自定义运行时特化流程**

1.自定义运行时也使用资源池

2.自定义运行时通过runtime-manager拉起bootstrap用户进程

3.worker启动http server，处理bootstrap的获取函数请求

[自定义运行时特化](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=0350e45775b54fb59f6409adcb4ae7fb)![自定义运行时特化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/35GE?t=1669962137654)

#### 1.2.2 CCE支持使用GPU

**[gpu-beta插件](https://support.huaweicloud.com/usermanual-cce/cce_10_0141.html)**

gpu-beta插件是支持在容器中使用GPU显卡的设备管理插件，集群中使用GPU节点时必须安装gpu-beta插件

gpu-beta插件需要配置驱动包地址，购买gpu节点后，使用插件在节点安装驱动包

gpu-beta会将驱动映射到容器中

```shell
# 容器中执行
cd /usr/local/nvidia/bin && ./nvidia-smi
```

```shell
nvidia-smi
Sat Jan  7 17:12:44 2023       
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 510.108.03   Driver Version: 510.108.03   CUDA Version: 11.6     |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|                               |                      |               MIG M. |
|===============================+======================+======================|
|   0  NVIDIA A30          Off  | 00000000:00:0D.0 Off |                    0 |
| N/A   49C    P0    34W / 165W |      0MiB / 24576MiB |      0%      Default |
|                               |                      |             Disabled |
+-------------------------------+----------------------+----------------------+
                                                                               
+-----------------------------------------------------------------------------+
| Processes:                                                                  |
|  GPU   GI   CI        PID   Type   Process name                  GPU Memory |
|        ID   ID                                                   Usage      |
|=============================================================================|
|  No running processes found                                                 |
+-----------------------------------------------------------------------------+
```

**[gpu调度](https://support.huaweicloud.com/usermanual-cce/cce_10_0345.html)**

通过 nvidia.com/gpu 指定申请GPU的数量，支持申请设置为小于1的数量，比如 nvidia.com/gpu: 0.5，这样可以多个Pod共享使用GPU

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpu-test
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gpu-test
  template:
    metadata:
      labels:
        app: gpu-test
    spec:
      containers:
      - image: nginx:perl
        name: container-0
        resources:
          requests:
            cpu: 250m
            memory: 512Mi
            nvidia.com/gpu: 0.5   # 申请GPU的数量
          limits:
            cpu: 250m
            memory: 512Mi
            nvidia.com/gpu: 0.5   # GPU数量的使用上限
      imagePullSecrets:
      - name: default-secret
```

#### 1.2.3 价格

| 型号          | 规格                           | 价格         |
| ------------- | ------------------------------ | ------------ |
| ac7.6xlarge.4 | 24核 \| 96GiB                  | 5.6495/小时  |
| pi3.6xlarge.4 | 24核 \| 96GiB \|nvidia-a30 24G | 18.1285/小时 |

GPU的价格数倍于节点价格，因此需要对GPU节点作为GPU函数专用

## 2. 特性/功能实现原理

### 2.1总体方案

### 2.2模块详细设计

#### 2.2.1 支持GPU节点标签，GPU节点上只部署配置GPU函数 

**节点增加GPU标签**

```
kubectl label nodes ${nodeName} node-role=data-plane-gpu
```

**亲和调用**

使所需要的pod能够调度到GPU节点上

function-task支持调度到GPU节点上

```yaml
 nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: node-role
                    operator: In
                    values:
                      - data-plane
                      - data-plane-gpu
                      - data-plane-scaled
```

gpu-custom-runtime 函数只支持调度到GPU节点上

```yaml
 nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: node-role
                    operator: In
                    values:
                      - data-plane-gpu
```

**反亲和调度**

**使非选中的pod无法调度到GPU的节点上**

是否可以在购买时候执行打上污点

GPU节点污点 (cce支持)

```
kubectl taint node ${nodeName} node-type=gpu:NoSchedule
```

pod对于污点的容忍度

cff-agent、function-task、gpu-custom-runtime都需要进行配置污点容忍

```yaml
      tolerations:
      - key: node-type                
        operator: Equal
        value: gpu
        effect: NoSchedule
```

#### 2.2.2 worker-manager支持GPU自定义运行时不配置资源池 



[gpu特化](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=c35e996c754d45a08b6bb56d9a3b2831)![gpu特化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GKY?t=1673078002412)

k8s配置GPU大小是以卡的张数为单位，用户配置GPU大小是以显存（GB）为单位，worker-manager没法去感知当前显卡是多少GB的规格，通过配置来感知这个映射关系，配置支持热加载

```go
type GPUPodConfig struct {
	GPUMemoryCapacity int64 `json:"gpuMemoryCapacity" valid:",optional"`
	WaitReadyTimeout  int64 `json:"waitReadyTimeout" valid:",optional"`
}
```

抽象接口，便于后续对checkPoint和custom-image的收编

```go
type PodHandler interface {
	FilterByPodCreateReq(req *PodCreateReq) bool
	FilterByPodName(podName string) bool
	CreatePod(req *PodCreateReq, template *PodTemplate) (*corev1.Pod, error)
	UpdatePod(pod *corev1.Pod)
}
```

pod函数名

```yaml
custom.runtime.gpu-1000-1024-1024-6f33ece9-a6d9-4f93-86e0-9eb4a69e5840
```

配置NVIDIA环境变量

LD_LIBRARY_PATH=/usr/local/nvidia/lib64

#### 2.2.3NVIDIA GPU周边支持

CCE只支持NVIDIA显卡 

**CUDA：**是Nvidia推出的只能用于自家GPU的并行计算框架。只有安装这个框架才能够进行复杂的并行计算。主流的深度学习框架也都是基于CUDA进行GPU并行加速的。因此需要在GPU节点上安装cuda。

nvidia-docker 做了cuda的容器化功能，通过prestart hook将cuda api lib和driver挂载到容器中，因此可以在容器内调用cuda

**常用python依赖后续加入到公共依赖**

python  torch（1.8G）、tensorflow（1.4G）、numba（50M）

**新增GPU节点：**

https://wiki.huawei.com/domains/867/wiki/5357/WIKI20230110633817

#### 2.2.4自定义镜像支持gpu

针对gpu函数 自定义镜像大小从原本 `2G`限制放开到`20G`

cuda驱动是直接打在自定义镜像中

函数名

```yaml
custom.image.gpu-1000-1024-1024-6f33ece9-a6d9-4f93-86e0-9eb4a69e5840
```

携带cuda驱动基础镜像

```
 registry-cbu.huawei.com/w00456327/gpubase:v1 
```

### 2.3 约束

#### 2.3.1上下游依赖需求

1、管理面支持custom-runtime运行时配置GPU显存大小

#### 2.3.2其他约束

 1、单个集群只支持一种GPU类型

 2、新增GPU节点需要安装cuda驱动

安装的cuda版本需要与GPU卡一致，安装网卡之后可以查询

```shell
nvidia-smi
Sat Jan  7 17:12:44 2023       
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 510.108.03   Driver Version: 510.108.03   CUDA Version: 11.6     |
```

3、暂不支持插件自动扩缩容

4、 CCE支持显卡只有NVIDIA  Tesla系列

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

#### 3.4.1 pod在创建过程中故障异常

特性差异在pod特化过程，其它流程不做变动

特化过程异常失败，3次拉起机会

#### 3.4.2  新增GPU节点可能存在问题

1、cff-agent起不来

```shell
chown -R 1000:1000 /var/paas/sys/log
```

2、registry-cbu.huawei.com 无法认证，导致镜像下载不下来

```shell
cat /etc/docker/daemon.json 
{
  "storage-driver": "overlay2",
  "insecure-registries" : ["registry-cbu.huawei.com"]
}
```

**整体资源告警**

CCE节点资源告警

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

输出custom  runtime GPU 用户使用指导文档初稿，后续有正式上现网之后再上到华为云资料里

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名**                     | 用例类型 | **测试步骤**                                                 | **预期结果**       |
| -------- | ------------------------------ | -------- | ------------------------------------------------------------ | ------------------ |
| 1        | 支持GPU-custom-runtime函数调度 | ST       | 1、创建custom-runtime运行时<br/>2、配置GPU显存<br/>3、执行GPU函数 | 执行成功           |
| 2        | GPU节点专属调用                | 手动观测 | 查看GPU节点是否有cff-agent、function-task、gpu-custom-runtime之外其它pod调度 | GPU节点没有其它pod |
| 3        | 支持GPU-custom-image函数调度   | ST       | 1、创建custom-image运行时<br/>2、配置GPU显存<br/>3、执行GPU函数 | 执行成功           |

