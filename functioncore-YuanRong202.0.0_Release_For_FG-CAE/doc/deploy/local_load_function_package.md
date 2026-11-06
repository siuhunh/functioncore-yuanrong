# 支持本地加载函数

本项目函数包默认保存在 [MinIO](https://min.io/) 对象存储服务，可能涉及 AGPLv3 的 License ，对 License 有顾虑且没有其[它可替换服务](./use_other_s3.md)的场景(主要针对存储等线下交付业务)，本项目支持从运行主机目录加载函数包，[安装部署](../deploy.md)各步骤需要做如下修改:

+ 整体流程不变，部署元戎组件时，执行 `bash config.sh` 和 `helm install functioncore ./` 之间做如下修改:
  * values.yaml 文件 `global.StorageType` 设置为 `local` , 配置本地函数路径 `PackageHostPath`
+ 函数部署流程不变，仍然执行 sn deploy 注册函数，不过注册函数后还需要将编译的函数包拷贝到各个运行主机的 PackageHostPath 对应子目录下
  * 主机函数包保存路径: `${PackageHostPath}/${tenantID}/${service-name}/${function-name}.zip` 其中 PackageHostPath 为配置的主机目录, tenantID 暂时只支持 groupID (目前默认 realm-export.json 的 groupID 为 11dba2b7407f41078925d8f598b33366 ), service-name 为服务名称, function-name 为函数名称
  * 确保函数文件夹权限为 755, 函数文件权限为 444

> 注: 启用本地加载函数包可能会引入安全风险，请确保所有函数均可控。