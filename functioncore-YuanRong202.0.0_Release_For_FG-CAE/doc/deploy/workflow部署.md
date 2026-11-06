# workflow部署

资源准备

| 资源  | 规格  | 数量  | 备注 |
| ------------ | ------------ | ------------ | ------------ |
| rds数据库  | 8C16G  | 1  | 用于保存管理态数据的数据库 |
| rds数据库  | 16C128G | 4  | 用于保存运行态数据的数据库 |
| DDM  | 2节点 16C32G  | 1  | 管理运行态数据库的分库分表逻辑库 |
| etcd  | 3节点 8C16G  | 1  | |

数据库初始化

1. 登录管理态数据库，执行部署包中charts/workflow/resources/datas/data/workflow_manager.sql脚本创建库表， 并创建workflow用户， 拥有
2. 在DDM管理台创建workflow逻辑库关联申请4台的运行态数据库，配置单个逻辑库分片数为16
3. 在DDM管理台创建workflow用户并关联workflow逻辑库，并给与全部权限
4. 登录DDM逻辑库，执行部署包中charts/workflow/resources/datas/data/workflow_manager.sql脚本创建运行态数据库表

初始化配置

```sh
   # 解压
   tar -xvzf Workflow_deploy.tgz
   # 进入文件夹
   cd Workflow
   # 初始化
   bash config.sh -p ${主节点 IP} -v ${imageTag} -d ${镜像仓库}
```

更改workflow配置：

 ```sh
   vim charts/workflow/values.yaml
   ``` 
   | 配置项 |        描述        |       默认值        | 限制 |
   | :----: | :----------------: | :-------------: | :--: |
   | global.repository  | 配置workflow镜像仓地址 |  |  |
   | global.tag  | workflow镜像tag |  |  |
   | monitorImage  | 监控容器镜像地址 |  |  |
   | global.database.driver  | 配置运行态数据库类型 | mysql | ddm/mysql |
   | global.database.ip  | 配置运行态数据库访问地址 |  |  |
   | global.database.port  | 配置运行态数据库访问端口 | 3306 |  |
   | global.database.name  | 配置运行态数据库实例名称 | workflow |  |
   | global.database.userName  | 配置运行态数据库用户名 | workflow |  |
   | global.database.password  | 配置运行态数据库访问密钥密文 |  |  |
   | global.database.dataDeleteInterval  | 配置运行态数据保存时间（天） | 15 | 0-180 |
   | global.wfBase.driver  | 配置管理态数据库类型 | mysql | mysql |
   | global.wfBase.ip  | 配置管理态数据库访问地址 |  |  |
   | global.wfBase.port  | 配置管理态数据库访问端口 | 3306 |  |
   | global.wfBase.name  | 配置管理态数据库实例名称 | workflow |  |
   | global.wfBase.userName  | 配置管理态数据库用户名 | workflow |  |
   | global.wfBase.password  | 配置管理态数据库访问密钥密文 |  |  |
   | global.etcdPwdFusion  | 指定 etcd 访问的密文，由 FunctionGraph 加密提供 |  |  |
   | global.aKey | 元戎数据面间鉴权使用，由 FunctionGraph 加密提供 |  |  |
   | global.sKey | 元戎数据面间鉴权使用，由 FunctionGraph 加密提供 |  |  |
   | global.https.enable.etcd | 用于开启etcd集群的https，FunctionGraph场景需设置为true）| 默认为true | true/false |
   | global.authenticationEnable | 鉴权开关 | 默认为true | true/false |
   | global.localAuthEnable | 适配解密算法的开关 | 默认true | true/false |
   | global.internalTLS.tlsEnable | 选择是否开启 https/tls 的开关 | 默认true | true/false |
   | global.customCA | 用于开启etcd集群的https，挂载相关证书，FunctionGraph场景需设置为true | 默认为true | true/false |
   | global.clusterEtcd.arrayHetcd |  配置etcd集群IP |     |  |
   |  global.isCluster |  用于开启etcd集群模式 | 默认为false | true/false |
   | global.log.rolling | 日志滚动开关（超过大小打包压缩）| 默认为true | true/false |
   |  global.functionServiceAddress.dispatcher.name |  用于指定要访问的dispacther服务名称 |  |  |
   |  global.functionServiceAddress.dispatcher.name |  用于指定要访问的dispacther服务地址 |  |  |
   |  global.functionServiceAddress.dispatcher.name |  用于指定要访问的frontend服务名称 |  |  |
   |  global.functionServiceAddress.dispatcher.name |  用于指定要访问的frontend服务地址 |  |  |
   |  global.functionCapability |  functioncore 提供的函数能力.当 functionCapability: 1时，提供支撑 functiongraph 的能力，同时只支持调用函数功能。当 functionCapability: 0时，则提供当前所有函数功能。 | 默认为1 | 0/1 |


部署命令：

```sh
# 部署workflow
helm install workflow -f values.yaml ./charts/workflow
```
### 组件卸载
```sh
   helm uninstall workflow
```

### 组件升级
```sh
   helm upgrade workflow -f values.yaml ./charts/workflow
```
