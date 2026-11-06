# 状态外置部署

状态内置是插件化功能，本项目支持状态外置的方式部署元戎组件以满足部分场景的轻量化诉求，[安装部署](../deploy.md)流程需要做如下修改:

+ 部署元戎组件时，执行 `bash config.sh` 和 `helm install functioncore ./` 之间新增如下修改:
  * values.yaml 文件中修改 `global.replicas.STATEMANAGER` 数量为 0
  * charts/functiontask/templates/functiontask-config.yaml 文件中修改 `data.conf.json` 文件中的 `persistent_state/persistent_future/init_state_client` 三个字段取值为 **false**
+ 若是已经部署完成后可以将 redis-cluster 实例数量减少到 0 或者删除对应服务, 参考命令 `kubectl scale StatefulSet redis-cluster --replicas=0`/`kubectl delete StatefulSet redis-cluster`

