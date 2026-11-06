# FuncInstanceManager动态指定函数资源


## 1. Story 整体设计
### 1.1 功能描述
FuncInstanceManager动态指定函数资源，根据busproxy的请求中的资源选择容器部署内部支持函数信息维护
### 1.2 场景分析
OBS的业务场景：图片大小不同，处理图片的函数是同一个，但所需的CPU/MEM不同，需要支持在调用时指定函数运行的资源规格

### 1.3 方案详细设计

**函数调用**

扩容接口增加条件
```go
cpu int
memory int 
其中CPU memory的规格需要满足池子的要求,如果超出规格的请求，会出现获取不到pod的报错，FuncInstanceManager不做规格的校验操作
```
当前规格如下：

CPU MEN  NUM
300 128: 20
400 256: 10
600 512: 5
800 768: 2
1000 1024: 1
1200 1280: 1
1400 1536: 1
1600 1792: 1
1800 2048: 1
2200 2560: 1
2600 3072: 1
3000 3584: 1
3400 4096: 1

以上两个条件会作为获取pod的判断依据，对于invoke时，不传CPU
memory的场景，由bus获取默认的数据在接口调用的时候传递给FuncInstanceManager

workermanager处理流程

![处理流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/11bk)

详细分析

![缓存更新图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/11bk)

- 1.正确更新luckyPod 中的workerInfo信息 
  - 辑缩容情求
  - watch到ETCD的functioninstancehandler watch到事件
```go
type workerInfo struct {
    id string
    nodeName string
    cpu int
    memory int 
}
```
- 2.特化pod时，传递给请求中的CPU memory
- 3.workerCache本身的key为podName，携带有CUP等资源信息，无需更新

## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*
不涉及
### 2.2 韧性（Resilience）
*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*
不涉及
### 2.3 隐私性（Privacy）
*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及
### 2.4 可靠性（Reliability）
*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
FuncInstanceManager重启后，数据怎么恢复？ 
- 重启后缓存会重新加载ETCD的数据，对于新的请求，会重新获取对应池子的worker

FuncInstanceMgr 和 Bus、frontend都重启了，怎么恢复？
- FuncInstanceMgr
  是主备模式，只有一个节点接收请求，都重启并不影响主节点获取pod的操作
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】* 

维测怎么做，想看下各种规格的实例数量？ 
- 梁卓然的AR中有涉及 
重启是函数对应的已部署不同规格的实例怎么重新维护？
- 当前组件内有记录函数与函数实例信息的key：此数据在workermanager重启后，会从k8s中获取，同时会做给ETCD的补偿操作，此需求保证这段逻辑正常即可


### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*
不涉及
## 3. HLT 用例
| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  函数指定资源调用 | 能够创建出来指定资源的pod，且请求调用到对应的pod  |成功创建出函数实例|
| 2 |  函数扩容 |  函数扩容流程正常 | 正常扩容 |
| 3 |  冒烟 | 代码提交前跑一次冒烟 | 用例正常执行 |

| 4| workermanager重启| 维护用例 | 用例正常执行 |