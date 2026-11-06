# AR.SR.IREQ02597546.033.006-系统部署升级支持按集群进行配置管理

## 1. Story 整体设计
### 1.1 功能描述
  当前配置文件放在functioncore代码仓库中，管理和出包没有单独列出；现网各个values没有基线化上库，无法按照版本管理。<br>
  将helm包相关的内容单独放在一个仓库中，方便管理，也方便之后提高自动化效率。
  

### 1.2 场景分析
1. 将所有配置文件和出安装包脚本单独放在一个代码仓中，以后各位开发修改配置文件都向这个仓库提交，出包也从这个仓库拉代码
2. 所有现网region集群的values.yaml在一个代码仓中按照版本基线化，每次更新版本后上库；如果当前版本现网有问题回退上一版本

### 1.3 方案详细设计
- **创建新的代码仓库** <br>
1. 为配置文件创建新仓库YuanRong_Kernel/deployment，并且绿区黄区同步 <br>
2. 在此仓库创建FunctionGraph_ICSL分支 <br>
3. Workflow独立仓库单独部署，配置文件和出包脚本不单独拿出到此仓库。但现网workflow集群的values.yaml的归档依然需要到此仓库上

- **代码仓库目录设计** <br/>
  
  | 目录 | 内容 |
  | ------------ | ------------ |
  | YuanRong_Kernel/deployment/config | 原functioncore/config目录 |
  | YuanRong_Kernel/deployment/helm-build | 原functioncore/build/helm-build目录 |
  | YuanRong_Kernel/deployment/values | 归档的基线化values.yaml文件 |

  | 详细目录 | 详细内容 |
  | ------------ | ------------ |
  | YuanRong_Kernel/deployment/config/交付组件名 | 交付组件的配置文件 |
  | YuanRong_Kernel/deployment/helm-build| 编译脚本和xml文件 |
  | YuanRong_Kernel/deployment/helm-build/sources | values.yaml文件和其相关脚本 |
  | YuanRong_Kernel/deployment/helm-build/sources/data | 第三方组件相关数据初始化脚本 |
  | YuanRong_Kernel/deployment/values/Beijing4/Cluster01 | 归档的北京四集群01基线化values.yaml文件 |
  | YuanRong_Kernel/deployment/values/Beijing4/workflow | 归档的北京四workflow集群基线化values.yaml文件 |
   
- **YuanRong_Kernel/deployment/config目录** <br/>
  将FunctionGraph_ICSL分支的functioncore/config迁移至/deployment/config下

- **YuanRong_Kernel/deployment/helm-build目录** <br/>
  1. 将FunctionGraph_ICSL的functioncore/build/helm-build迁移至/deployment/config下。
  2. 修改生成helm包的脚本，适配目录变更

- **YuanRong_Kernel/deployment/values目录** <br/>
  将各个region及其各个集群的values.yaml配置文件归档到此仓库的FunctionGraph_ICSL分支，在此目录下做基线化归档。升级版本时更新对应的配置文件，如果当前版本后面遇到问题回退则同步回退此归档文件。 <br>
  示例：北京四的yuanrong集群01的配置文件归档在：YuanRong_Kernel/deployment/values/Beijing4/Cluster01/values.yaml <br>
  之后增加新的region或新的集群时，也在此基础上按照上述格式增加
  
- **修改机制** <br/>
  1. 对于config目录和helm-build目录的修改，各位开发均可以修改。有values.yaml的对外修改需要更新到FunctionGraph-YuanRong部署文档上
  2. 对于values目录的修改，只有在部署新的现网region、集群或升级现网环境时才可以由当时的负责人提交修改。上库时自提专用单，在commit ID中写明region、集群、版本号 <br>
     目前各region间OBS地址和AKSK、镜像仓地址、etcd、mysql和redis的地址和密码，鉴权AKSK等配置项都是不一样的；公共的项就是组件的配置项，比如日志目录，layer数，pool数之类的，如果某region有特殊要求也要修改

- **Tag机制** <br/>
  当前CMC上已经有版本号对应发布包和commit ID，不需要额外打tag

### 1.4 上下游依赖需求
创建新的代码仓库YuanRong_Kernel/deployment。对接人吴潇潇
YuanRong_Kernel/deployment仓库绿区黄区同步。对接人张凯立
版本编译对接。对接人叶颖凯，联调时间本周四

## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
不送检基线化已经填写好的values.yaml，不涉及安全问题
### 2.2 韧性（Resilience）
*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及
### 2.3 隐私性（Privacy）
*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及
### 2.4 可靠性（Reliability）
*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
不涉及
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】*  
不涉及
### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及
## 3. HLT 用例
| 序号 | 用例名 | 测试步骤 | 预期结果 |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  版本编译正常 | 进行版本编译 | 正常生成ServelessNative发布包 |
| 2 |  版本包校验 | 进行版本包校验并部署 | 版本包不缺失，不冗余；交付的组件部署成功 |
