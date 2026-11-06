[TOC]

# AR.SR.IREQf58c143d.005.013 【functioncore】worker-manager对k8s接口访问统一

## 1. 需求场景分析

场景：worker-manager 中使用 kubernetes.ClientSet 结构体变量来调用 k8s 接口，难以 mock 接口，只能 mock 方法；

### 1.1 特性需求来源与价值概述

需求来源：内部代码优化

价值概述：统一接口访问，方便后续增加代码

### 1.2特性场景分析

代码优化：使用 kubernetes.Interface 接口来调用 k8s api，统一使用接口访问，不依赖具体的实现，便于后续 mock 接口

## 2. 特性/功能实现原理

- 修改代码

  修改前：

```go
var KubeClientSet      *kubernetes.Clientset
// GetKubernetesClient Get Kubernetes Client
func GetKubernetesClient() *kubernetes.Clientset {}
```

​		修改后：

```go
var KubeClientSet      kubernetes.Interface
// GetKubernetesClient Get Kubernetes Client
func GetKubernetesClient() kubernetes.Interface {}
```



- 后续 mock 接口的方式

```go
kubernetesClient = fake.NewSimpleClientset()
	kubernetesClient.CoreV1().(*fakecorev1.FakeCoreV1).PrependReactor("create", "secrets", func(action testing.Action) (handled bool, ret runtime.Object, err error) {
				return true, &v1.Secret{}, errors.New("Error creating secret")
			})
```



### 2.3 约束

暂无

#### 2.3.1上下游依赖需求

无

#### 2.3.2其他约束

NA

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

内部优化，不涉及

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

内部优化，不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

内部优化，不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

worker-manager 重启后，会重建缓存信息，保证第二个 pat 也能够被纳管和及时清理；

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

无

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

内部优化，不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

无

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

无

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

无

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

复用原有流程，不增加资源消耗；

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部优化，客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

单组件代码修改，支持组件独立升级

## 4. 用例

| **序号** | **用例名**                  | 用例类型 | **测试步骤**                                            | **预期结果** |
| -------- | --------------------------- | -------- | ------------------------------------------------------- | ------------ |
| 1        | mock k8s api 访问返回 ok    | LLT      | 1. mock 接口的方式来测试方法<br />2. 预期 mock 能够生效 | 符合预期     |
| 2        | mock k8s api 访问返回 error | LLT      | 1. mock 接口的方式来测试方法<br />2. 预期 mock 能够生效 | 符合预期     |
| 3        | 跑本地 ST 用例              | ST       | 全部通过                                                | 符合预期     |


*注：属于内部优化，测试无需增加用例*