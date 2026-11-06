# 配置路径 

worker-manager-service-configmap->worker-manager-kubeconfig->volumeConfig
需要在volumeConfig中增加一个customResourceMountPaths字段，它的value值为json数组

json中包含三个必填字段
name : 定义一个挂载名字
containerPath: 容器中的路径
hostPath: 宿主机中的路径

示例如下
```
[{
    "name": "cuda",
    "containerPath": "/usr/local/cuda2",
    "hostPath": "/usr/local/cuda-10.2"
}]
```

