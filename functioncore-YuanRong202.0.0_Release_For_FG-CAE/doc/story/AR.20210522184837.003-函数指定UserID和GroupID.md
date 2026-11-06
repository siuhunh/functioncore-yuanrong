# AR.20210522184837.003-函数指定UserID和GroupID

## 1. Story 整体设计
### 1.1 功能描述
- **背景** <br/>
  某些用户函数需要从文件系统挂载卷来加载额外需要的文件，如果需要从文件系统中读取挂载卷，需要指定的属主和属组才能成功读取挂载的文件。当前元戎内核需要增加对此功能的支持
- **方案** <br/>
  用户可以在界面对需要从挂载卷中读取文件的函数指定UserID和GroupID，由管理面写入etcd，数据面监听到相关数据后将在特化时把对应语言版本的runtime以指定的ID重新启动

### 1.2 场景分析
  用户在函数里写文件，文件的属组和runtime进程属组是一样的，如果挂进来的文件系统对UID和GID有要求的话，runtime不按照要求来，就可能会无法读写 <br/>
  当用户在界面创建需要从文件系统读取挂载卷中文件的函数时，需要指定UserID和GroupID以设定用户函数权限，由管理面写入etcd。InstanceManager读到后向下传，worker特化时将对应语言版本的runtime以指定的ID重新启动

### 1.3 方案详细设计
- **流程图** <br/>
[函数指定UserID和GroupID-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=7aa62506c72441088b1a47de1e20855c&versionUUID=)![函数指定UserID和GroupID](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qVy?t=1623920760069)

- **读取数据** <br/>
  如果InstanceManager从etcd的函数元信息中的ExtendedMetaData.MountConfig字段中监听到函数的UserID和GroupID字段，则写入cache并更新CR，代表需要指定此函数runtime的UID与GID

- **传输数据** <br/>
  InstanceManager将相关数据传给worker，worker传给RuntimeManager，可以复用已有流程

- **指定ID** <br/>
  RuntimeManager如果拿到的UserID和GroupID的字段非默认，则进入指定ID的特化流程。先停掉所有的runtime，再指定UserID加入给定的GroupID和默认属组以读取新功能和继承原有功能，然后拉起该函数对应的语言和版本的runtime

- **增加配置** <br/>
  InstanceManager的functionservice配置文件增加指定ID的相关字段以将ID落盘，保证InstanceManager重启后依然可以拿到数据
  
- **ID限制** <br/>
  前端做限制，functiongraph进行处理，数据面不做合法性校验<br/>
  传过来的UserID和GroupID范围均为1到65534，默认均为1000，当不为1000时指定为输入的ID

- **更换ID** <br/>
  函数每个版本创建后，预留函数已经指定，不能更换UID和GID，否则会调用报错无权限访问挂载文件。 需要函数发布新版本才可以更换UID和GID

### 1.4 上下游依赖需求
1. 管理面将函数指定的UserID和GroupID写入etcd的函数元数据中。预计联调时间6月18日，对接人夏嘉诚
2. RuntimeManager提供删除全语言版本的runtime接口和runtime指定ID与语言版本启动的接口。预计提供时间6月18日，支撑人李永胜

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
不涉及
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】*  
不涉及
### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及
## 3. HLT 用例
| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  指定正确函数ID | 1.创建需要从文件系统中读取挂载卷的函数<br/> 2.指定正确的函数UserID和GroupID<br/> 3.调用函数 | 调用成功。可以通过在函数中打印runtime的UID和GID，或在容器中查看runtime的UID和GID验证  |
| 2 |  指定错误函数ID | 1.创建需要从文件系统中读取挂载卷的函数<br/> 2.指定错误（权限不足）的函数UserID和GroupID<br/> 3.调用函数 | 读取挂载卷中的文件失败，提示无权限。函数调用失败  |
| 3 |  不指定函数ID | 1.创建需要从文件系统中读取挂载卷的函数<br/> 2.不指定函数UserID和GroupID（默认）<br/> 3.调用函数 | 读取挂载卷中的文件失败，提示无权限。函数调用失败  |
