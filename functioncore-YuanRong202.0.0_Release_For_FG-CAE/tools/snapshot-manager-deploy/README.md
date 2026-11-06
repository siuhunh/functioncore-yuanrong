# Snapshot-Manager 部署

## 生成部署文件
需要在k8集群主节点或安装了``kubectl``的节点上,在当前目录执行：

```bigquery
cd ./scripts
sh generate_deployment.sh
```
Options:
```bigquery
    -g 生成绿区的Snapshot-Manager部署文件
    -t 指定个人镜像TAG, 不带该参数默认使用黄区每日构建最新版本镜像TAG
        黄区每日构建: http://10.244.181.91:8080/view/7_FG_VersionBuild/job/FunctionGraph_CMC_Download_Compile/
        绿区每日构建: http://7.219.11.5:8080/job/FunctionGraph_CMC_Download_Compile/
    -r  指定个人镜像仓目录. 不带该参数默认使用黄区每日构建镜像仓目录,
        黄区每日构建镜像仓目录: cd-docker-hub.szxy5.artifactory.cd-cloud-artifact.tools.huawei.com/yuanrong_euleros_x86/
        绿区每日构建镜像仓目录: registry-cbu.huawei.com/functiongraph/
     -h usage help
```
Example:
```bigquery
生成黄区每日构建最新版本的Snapshot-MANAGER部署文件，执行脚本：sh generate_deployment.sh
生成绿区每日构建最新版本的Snapshot-MANAGER部署文件，执行脚本：sh generate_deployment.sh -g
生成个人镜像的Snapshot-MANAGER部署文件，执行脚本：sh generate_deployment.sh -r yuanrong.docker.tool.huawei.com/k00591264/ -t k0059126424990
```


执行成功后，同级目录下生成两个yaml文件：
```bigquery
snapshot-manager.yaml
worker-manager.yaml
```

## 部署Snapshot-Manager
执行
```bigquery
kubectl create -f snapshot-manager.yaml
```
