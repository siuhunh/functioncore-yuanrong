# Snapshot-Manager 部署

## 生成部署文件
需要在元戎数据面版本部署完成后，在k8S集群主节点或安装了``kubectl``的节点上，在发布包snapshot-manager-deploy目录下执行（每个集群都需要运行一次）：

```bigquery
cd ./scripts
sh generate_deployment.sh -r {镜像仓库地址} -t {镜像TAG}
```
Options:
```bigquery
    -t 指定镜像TAG
    -r 指定镜像仓目录
       黄区每日构建镜像仓目录: cd-docker-hub.szxy5.artifactory.cd-cloud-artifact.tools.huawei.com/yuanrong_euleros_x86/
       绿区每日构建镜像仓目录: registry-cbu.huawei.com/functiongraph/
     -h usage help
```
Example:
```bigquery
生成镜像的Snapshot-MANAGER部署文件，执行脚本：sh generate_deployment.sh -r cd-docker-hub.szxy5.artifactory.cd-cloud-artifact.tools.huawei.com/yuanrong_euleros_x86/ -t 202.1.0.B001.20241214063221
```

执行成功后，同级目录下生成两个yaml文件：
```bigquery
snapshot-manager.yaml
worker-manager.yaml
```
## 开启租户白名单（可选）
白名单租户功能打开时，不在白名单租户内的用户不会升级快照。
打开`snapshot-manager.yaml`文件，在`POOLMGR_CONFIG`中新增以下配置
```json
{
  "snapshotTenantIDsConfig": {
    "enable": true,
    "tenantIDs": ["tenant-1", "tenant-2"]
  }
}
```
新增配置时注意格式和需要添加转义符

## 部署Snapshot-Manager
执行
```bigquery
kubectl create -f snapshot-manager.yaml
```

## 查询Snapshot-Manager
job创建后，snapshot-manager的pod进入running状态，执行
```bigquery
kubectl get job
```
获取当前正在运行的job名称

## 清理Snapshot-Manager
pod执行完毕后进入complete状态，执行
```bigquery
kubectl delete job {job名称} --force
```
进行job清理，complete状态的pod会退出